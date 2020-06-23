#include "command.hpp"

#include <irods/rodsClient.h>
#include <irods/connection_pool.hpp>
#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>
#include <irods/filesystem.hpp>

#include <boost/config.hpp>
#include <boost/program_options.hpp>

#include <iostream>
#include <string>
#include <array>
#include <vector>

namespace fs = irods::experimental::filesystem::client;
namespace io = irods::experimental::io;

namespace po = boost::program_options;

namespace irods::cli
{
    class get : public command
    {
    public:
        auto name() const noexcept -> std::string_view override
        {
            return "get";
        }

        auto description() const noexcept -> std::string_view override
        {
            return "Downloads data object to local disk.";
        }

        auto help_text() const noexcept -> std::string_view override
        {
            return "The help text";
        }

        auto execute(const std::vector<std::string>& args) -> int override
        {
            po::options_description desc{"Allowed options"};
            desc.add_options()
                ("logical_path", po::value<std::string>(), "The logical path of a data object.")
                ("physical_path", po::value<std::string>(), "The physical path to write to.");

            po::positional_options_description pod;
            pod.add("logical_path", 1);
            pod.add("physical_path", 1);

            po::variables_map vm;
            po::store(po::command_line_parser(args).options(desc).positional(pod).run(), vm);
            po::notify(vm);

            if (vm.count("logical_path") == 0) {
                std::cerr << "Error: Missing logical path.\n";
                return 1;
            }

            if (vm.count("physical_path") == 0) {
                std::cerr << "Error: Missing physical path.\n";
                return 1;
            }

            if ("-" != vm["physical_path"].as<std::string>()) {
                std::cerr << "Error: Physical path must be '-'.\n";
                return 1;
            }

            rodsEnv env;

            if (getRodsEnv(&env) < 0) {
                std::cerr << "Error: Could not get iRODS environment.\n";
                return 1;
            }

            const auto logical_path = vm["logical_path"].as<std::string>();
            irods::connection_pool conn_pool{1, env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 600};

            if (!fs::is_data_object(conn_pool.get_connection(), logical_path)) {
                std::cerr << "Error: Logical path does not point to a data object.\n";
                return 1;
            }

            auto conn = conn_pool.get_connection();
            io::client::default_transport dtp{conn};

            if (io::idstream in{dtp, logical_path}; in) {
                std::array<char, 4 * 1024 * 1024> buffer{};

                while (in && std::cout) {
                    in.read(&buffer[0], buffer.size());
                    std::cout.write(&buffer[0], in.gcount());
                }
            }
            else {
                std::cerr << "Error: Could not open input stream [path => " << logical_path << "]\n";
            }

            return 0;
        }
    }; // class get
} // namespace irods::cli

// TODO Need to investigate whether this is truely required.
//extern "C" BOOST_SYMBOL_EXPORT irods::cli::get cli_impl;
irods::cli::get cli_impl;

