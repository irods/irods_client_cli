#include "command.hpp"

#include <irods/rodsClient.h>
#include <irods/thread_pool.hpp>
#include <irods/connection_pool.hpp>
#include <irods/thread_pool.hpp>
#include <irods/filesystem.hpp>
#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <iostream>
#include <string>
#include <array>
#include <vector>
#include <memory>
#include <fstream>
#include <stdexcept>
#include <iterator>
#include <thread>
#include <mutex>
#include <atomic>
#include <algorithm>

// clang-format off
namespace fs   = boost::filesystem;
namespace po   = boost::program_options;

namespace io   = irods::experimental::io;
namespace ifs  = irods::experimental::filesystem;
// clang-format on

namespace
{
    constexpr auto operator ""_MB(unsigned long long x) noexcept -> int
    {
        return x * 1024 * 1024;
    }
} // anonymous namespace

namespace irods::cli
{
    class put : public command
    {
    public:
        auto name() const noexcept -> std::string_view override
        {
            return "put";
        }

        auto description() const noexcept -> std::string_view override
        {
            return "Uploads files and directories into iRODS.";
        }

        auto help_text() const noexcept -> std::string_view override
        {
            return "The help text.";
        }

        auto execute(const std::vector<std::string>& args) -> int override
        {
            rodsEnv env;

            if (getRodsEnv(&env) < 0) {
                std::cerr << "Error: Could not get iRODS environment.\n";
                return 1;
            }

            po::options_description desc{"Allowed options"};
            desc.add_options()
                ("physical_path", po::value<std::string>(), "The physical path of the file to read from [use '-' for stdin].")
                ("logical_path", po::value<std::string>()->default_value(env.rodsHome), "The logical path of an existing collection.")
                ("connection_pool_size,c", po::value<int>()->default_value(4), "Connection pool size for handling directories.");

            po::positional_options_description pod;
            pod.add("physical_path", 1);
            pod.add("logical_path", 1);

            po::variables_map vm;
            po::store(po::command_line_parser(args).options(desc).positional(pod).run(), vm);
            po::notify(vm);

            if (vm.count("physical_path") == 0) {
                std::cerr << "Error: Missing physical path.\n";
                return 1;
            }

            return ("-" == vm["physical_path"].as<std::string>())
                ? put_from_stdin(env, vm["logical_path"].as<std::string>())
                : put_from_physical_path(env, vm);
        }

    private:
        auto put_from_stdin(const rodsEnv& _env, const std::string& _logical_path) -> int
        {
            if (_logical_path.empty()) {
                std::cerr << "Error: The logical path is empty.\n";
                return 1;
            }

            try {
                irods::connection_pool conn_pool{1, _env.rodsHost, _env.rodsPort, _env.rodsUserName, _env.rodsZone, 600};

                auto conn = conn_pool.get_connection();

                if (ifs::client::exists(conn, _logical_path) && !ifs::client::is_data_object(conn, _logical_path)) {
                    std::cerr << "Error: The logical path points to something other than a data object.\n";
                    return 1;
                }

                io::client::default_transport tp{conn};

                if (io::odstream out{tp, _logical_path}; out) {
                    std::array<char, 4_MB> buffer{};

                    while (std::cin && out) {
                        std::cin.read(&buffer[0], buffer.size());
                        out.write(&buffer[0], std::cin.gcount());
                    }
                }
                else {
                    std::cerr << "Error: Could not open output stream [path => " << _logical_path << "].\n";
                    return 1;
                }
            }
            catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << '\n';
                return 1;
            }

            return 0;
        }

        auto put_from_physical_path(const rodsEnv& _env, const po::variables_map& _vm) -> int
        {
            try {
                const auto from = fs::canonical(_vm["physical_path"].as<std::string>());
                const ifs::path to = _vm["logical_path"].as<std::string>();

                if (fs::is_regular_file(from)) {
                    put_file(_env, from, to / from.filename().string());
                }
                else if (fs::is_directory(from)) {
                    const auto pool_size = _vm["connection_pool_size"].as<int>();
                    irods::connection_pool conn_pool{pool_size, _env.rodsHost, _env.rodsPort, _env.rodsUserName, _env.rodsZone, 600};
                    irods::thread_pool thread_pool{static_cast<int>(std::thread::hardware_concurrency())};
                    put_directory(conn_pool, thread_pool, from, to / std::rbegin(from)->string());
                    thread_pool.join();
                }
                else {
                    std::cerr << "Error: Path must point to a file or directory.\n";
                    return 1;
                }
            }
            catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << '\n';
                return 1;
            }

            return 0;
        }

        auto put_file_chunk(irods::connection_pool& _cpool,
                            const fs::path& _from,
                            const ifs::path& _to,
                            unsigned long _offset,
                            unsigned long _chunk_size) -> void
        {
            try {
                std::ifstream in{_from.c_str(), std::ios_base::binary};

                if (!in) {
                    throw std::runtime_error{"Cannot open file for reading."};
                }

                auto conn = _cpool.get_connection();
                io::client::default_transport tp{conn};
                io::odstream out{tp, _to};

                if (!out) {
                    throw std::runtime_error{"Cannot open data object for writing [path: " + _to.string() + "]."};
                }

                if (!in.seekg(_offset)) {
                    throw std::runtime_error{"Seek failed [path: " + _from.generic_string() + "]."};
                }

                if (!out.seekp(_offset)) {
                    throw std::runtime_error{"Seek failed [path: " + _to.string() + "]."};
                }

                std::array<char, 4_MB> buf{};
                unsigned long bytes_pushed = 0;

                while (in && bytes_pushed < _chunk_size) {
                    in.read(buf.data(), std::min(buf.size(), _chunk_size));
                    out.write(buf.data(), in.gcount());
                    bytes_pushed += in.gcount();
                }
            }
            catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << '\n';
            }
        }

        auto put_file(const rodsEnv& _env, const fs::path& _from, const ifs::path& _to) -> void
        {
            try {
                const auto file_size = fs::file_size(_from);

                // If the local file's size is less than 32MB, then stream the file
                // over a single connection.
                if (file_size < 32_MB) {
                    irods::connection_pool cpool{1, _env.rodsHost, _env.rodsPort, _env.rodsUserName, _env.rodsZone, 600};

                    // If the local file is empty, just create an empty data object
                    // on the iRODS server and return.
                    if (file_size == 0) {
                        auto conn = cpool.get_connection();
                        io::client::default_transport tp{conn};
                        io::odstream out{tp, _to};

                        if (!out) {
                            throw std::runtime_error{"Cannot open data object for writing [path: " + _to.string() + "]."};
                        }

                        return;
                    }

                    put_file(cpool.get_connection(), _from, _to);

                    return;
                }

                using int_type = unsigned long;

                constexpr int_type thread_count = 3;
                irods::connection_pool cpool{thread_count, _env.rodsHost, _env.rodsPort, _env.rodsUserName, _env.rodsZone, 600};
                irods::thread_pool tpool{static_cast<int>(thread_count)};

                const int_type chunk_size = file_size / thread_count;
                const int_type remainder = file_size % thread_count;

                {
                    auto conn = cpool.get_connection();
                    io::client::default_transport tp{conn};
                    io::odstream{tp, _to};
                }

                for (int_type i = 0; i < thread_count; ++i) {
                    irods::thread_pool::post(tpool, [&, offset = i * chunk_size] {
                        put_file_chunk(cpool, _from, _to, offset, chunk_size);
                    });
                }

                if (remainder > 0) {
                    irods::thread_pool::post(tpool, [&, offset = thread_count * chunk_size] {
                        put_file_chunk(cpool, _from, _to, offset, remainder);
                    });
                }

                tpool.join();
            }
            catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << '\n';
            }
        }

        auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void
        {
            try {
                const auto file_size = fs::file_size(_from);

                // If the local file is empty, just create an empty data object
                // on the iRODS server and return.
                if (file_size == 0) {
                    io::client::default_transport tp{_comm};
                    io::odstream out{tp, _to};

                    if (!out) {
                        throw std::runtime_error{"Cannot open data object for writing [path: " + _to.string() + "]."};
                    }

                    return;
                }

                std::ifstream in{_from.c_str(), std::ios_base::binary};

                if (!in) {
                    throw std::runtime_error{"Cannot open file for reading [path: " + _from.generic_string() + "]."};
                }

                io::client::default_transport tp{_comm};
                io::odstream out{tp, _to};

                if (!out) {
                    throw std::runtime_error{"Cannot open data object for writing [path: " + _to.string() + "]."};
                }

                std::array<char, 4_MB> buf{};

                while (in) {
                    in.read(buf.data(), buf.size());
                    out.write(buf.data(), in.gcount());
                }
            }
            catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << '\n';
            }
        }

        auto put_directory(irods::connection_pool& _conn_pool,
                           irods::thread_pool& _thread_pool,
                           const fs::path& _from,
                           const ifs::path& _to) -> void
        {
            ifs::client::create_collections(_conn_pool.get_connection(), _to);

            for (auto&& e : fs::directory_iterator{_from}) {
                irods::thread_pool::post(_thread_pool, [this, &_conn_pool, &_thread_pool, e, _to]() {
                    const auto& from = e.path();

                    if (fs::is_regular_file(e.status())) {
                        put_file(_conn_pool.get_connection(), from, _to / from.filename().string());
                    }
                    else if (fs::is_directory(e.status())) {
                        put_directory(_conn_pool, _thread_pool, from, _to / std::rbegin(from)->string());
                    }
                });
            }
        }
    }; // class put
} // namespace irods::cli

// TODO Need to investigate whether this is truely required.
//extern "C" BOOST_SYMBOL_EXPORT irods::cli::put cli_impl;
irods::cli::put cli_impl;

