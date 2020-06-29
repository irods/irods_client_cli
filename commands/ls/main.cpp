#include "command.hpp"

#include <irods/rodsClient.h>
#include <irods/connection_pool.hpp>
#include <irods/filesystem.hpp>

#include <boost/config.hpp>
#include <boost/program_options.hpp>

#include <fmt/format.h>

#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include <iomanip>
#include <ctime>
#include <sstream>

namespace fs = irods::experimental::filesystem;
namespace po = boost::program_options;

namespace irods::cli
{
    class ls : public command
    {
    public:
        auto name() const noexcept -> std::string_view override
        {
            return "ls";
        }

        auto description() const noexcept -> std::string_view override
        {
            return "List the contents of a collection.";
        }

        auto help_text() const noexcept -> std::string_view override
        {
            return "The help text.";
        }

        auto execute(const std::vector<std::string>& args) -> int override
        {
            po::options_description options{""};
            options.add_options()
                ("acls,A", "")
                ("l,l", "")
                ("L,L", "")
                ("r,r", "")
                ("t,t", po::value<std::string>(), "")
                ("bundle", "")
                ("logical_path", po::value<std::string>(), "");

            po::positional_options_description positional_options;
            positional_options.add("logical_path", 1);

            po::variables_map vm;
            po::store(po::command_line_parser(args).options(options).positional(positional_options).run(), vm);
            po::notify(vm);

            rodsEnv env;

            if (getRodsEnv(&env) < 0) {
                std::cerr << "Error: Could not get iRODS environment.\n";
                return 1;
            }

            const auto logical_path = vm.count("logical_path") ? vm["logical_path"].as<std::string>() : env.rodsCwd;
            auto conn_pool = irods::make_connection_pool();
            auto conn = conn_pool->get_connection();

            if (const auto s = fs::client::status(conn, logical_path); fs::client::is_collection(s)) {
                if (vm.count("l")) {
                    if (vm.count("r")) {
                        for (auto&& e : fs::client::recursive_collection_iterator{conn, logical_path}) {
                            print_one_liner_description(conn, e);
                        }
                    }
                    else {
                        for (auto&& e : fs::client::collection_iterator{conn, logical_path}) {
                            print_one_liner_description(conn, e);
                        }
                    }
                }
                else if (vm.count("L")) {
                    if (vm.count("r")) {
                        for (auto&& e : fs::client::recursive_collection_iterator{conn, logical_path}) {
                            print_multi_line_description(e);
                        }
                    }
                    else {
                        for (auto&& e : fs::client::collection_iterator{conn, logical_path}) {
                            print_multi_line_description(e);
                        }
                    }
                }
            }
            else if (fs::client::is_data_object(s)) {
                if (vm.count("l")) {

                }
                else if (vm.count("L")) {

                }
            }
            else {
                std::cerr << "Error: Logical path does not point to a collection or data object.\n";
                return 1;
            }

            return 0;
        }

    private:
        auto print_one_liner_description(rcComm_t& conn, const fs::collection_entry& e) -> void
        {
            auto tm = std::chrono::system_clock::to_time_t(e.last_write_time());
            std::stringstream ss;
            ss << std::put_time(std::localtime(&tm), "%F %T");

            fmt::print("{:<10} {} {:<10} {:>15} {} & {}\n",
                       e.owner(),
                       0,
                       "demoResc",
                       fs::client::data_object_size(conn, e),
                       ss.str(),
                       e.path().object_name().c_str());
        }

        auto print_multi_line_description(const fs::collection_entry& e) -> void
        {

        }
    }; // class ls
} // namespace irods::cli

// TODO Need to investigate whether this is truely required.
//extern "C" BOOST_SYMBOL_EXPORT irods::cli::touch cli_impl;
irods::cli::ls cli_impl;

