#include "command.hpp"

#include <irods/rodsClient.h>
#include <irods/connection_pool.hpp>
#include <irods/filesystem.hpp>
#include <irods/irods_exception.hpp>

#include <boost/program_options.hpp>
#include <boost/dll.hpp>
#include <boost/progress.hpp>

#include "experimental_plugin_framework.hpp"

#include <iostream>
#include <string>

namespace fs = irods::experimental::filesystem;
namespace po = boost::program_options;
namespace ia = irods::experimental::api;

namespace {
    std::atomic_bool exit_flag{};

    void handle_signal(int sig)
    {
        exit_flag = true;
    }
}



namespace irods::cli
{
    void print_progress(const std::string& p)
    {
        static boost::progress_display prog{100};
        try {
            auto x = std::stoi(p.c_str(), nullptr, 10);
            while(prog.count() != x) {
                ++prog;
            }
        }
        catch(...) {
        }
    } // print_progress

    class cp : public command
    {
    public:
        auto name() const noexcept -> std::string_view override
        {
            return "repl";
        }

        auto description() const noexcept -> std::string_view override
        {
            return "Replicates collections or data objects";
        }

        auto help_text() const noexcept -> std::string_view override
        {
            auto help =R"(
Given a fully qualified path, replicate the collection or object

irods repl [options] --source_resource <originating replica location> fully_qualified_logical_path
      --all                  : update all replicas of a given data object
      --admin_mode           : operate as an administrator to replicate user data
      --destination_resource : target resource for the replication
      --logical_path         : fully qualified logical path of the data object
      --number_of_threads    : number of threads to use in recursive operations
      --progress             : request progress as a percentage
      --source_resource      : origin of the data object(s)
      --update               : update a specific replica on destination resource)";

            return help;

        }

        auto execute(const std::vector<std::string>& args) -> int override
        {
            signal(SIGINT,  handle_signal);
            signal(SIGHUP,  handle_signal);
            signal(SIGTERM, handle_signal);

            bool update_all_replicas{false};
            bool admin_mode{false};
            bool update_one_replica{false};
            bool progress_flag{false};
            int  thread_count{4};

            std::string source_resource{}, destination_resource{};

            using rep_type = fs::object_time_type::duration::rep;

            po::options_description desc{""};
            desc.add_options()
                ("update_all_replicas", po::bool_switch(&update_all_replicas), "update all replicas of a given data object")
                ("admin_mode", po::bool_switch(&admin_mode), "operate as an administrator to replicate user data")
                ("destination_resource", po::value<std::string>(&destination_resource), "target resource for the replication")
                ("logical_path", po::value<std::string>(), "fully qualified logical path of the data object")
                ("number_of_threads", po::value<int>(&thread_count), "number of threads to use in recursive operations")
                ("progress", po::bool_switch(&progress_flag), "request progress as a percentage")
                ("source_resource", po::value<std::string>(&source_resource), "origin of the data object(s)")
                ("update_one_replica", po::bool_switch(&update_one_replica), "update a specific replica on destination resource");

            po::positional_options_description pod;
            pod.add("logical_path", 1);

            po::variables_map vm;
            po::store(po::command_line_parser(args).options(desc).positional(pod).run(), vm);
            po::notify(vm);

            if (vm.count("logical_path") == 0) {
                std::cerr << "Error: Missing source logical path.\n";
                return 1;
            }

            rodsEnv env;

            if (getRodsEnv(&env) < 0) {
                std::cerr << "Error: Could not get an iRODS environment.\n";
                return 1;
            }

            if(source_resource.empty()) {
                std::cerr << "Error: missing --source_resource.\n";
                return 1;
            }

            if(update_all_replicas && update_one_replica) {
                std::cerr << "Error: update_all_replicas and update_one_replica are incompatible\n";
                return 1;
            }

            if(!update_all_replicas && destination_resource.empty()) {
                std::cerr << "Error: destination_resource is missing.\n";
                return 1;
            }

            if(update_all_replicas && !destination_resource.empty()) {
                std::cerr << "Error: cannot specify destination_resource with update_all_replicas.\n";
                return 1;
            }

            irods::connection_pool conn_pool{1, env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 600};
            auto conn = conn_pool.get_connection();

            const auto logical_path = vm["logical_path"].as<std::string>();

            {
                const auto object_status = fs::client::status(conn, logical_path);

                if (!fs::client::is_collection(object_status) && !fs::client::is_data_object(object_status)) {
                    std::cerr << "Error: Logical path does not point to a collection or data object. Do you need a fully qualified path?\n";
                    return 1;
                }
            }

            std::string progress{};

            auto progress_handler = progress_flag ? print_progress : [](const std::string&) {};

            auto request = json{{"logical_path",    logical_path},
                                {"source_resource", source_resource},
                                {"progress",        progress_flag}};

            if(!destination_resource.empty()) {
                request["destination_resource"] = destination_resource;
            }

            if(admin_mode) {
                request["admin_mode"] = true;
            }

            if(update_one_replica) {
                request["update_one_replica"] = true;
            }

            if(update_all_replicas) {
                request["update_all_replicas"] = true;
            }


            auto cli = ia::client{};
            auto rep = cli(conn,
                           exit_flag,
                           progress_handler,
                           request,
                           "replicate");

            if(exit_flag) {
                std::cout << "Operation Cancelled.\n";
            }

            if(rep.contains("errors")) {
                for(auto e : rep.at("errors")) {
                    std::cout << e << "\n";
                }
            }

            return 0;
        }

    }; // class cp

} // namespace irods::cli

// TODO Need to investigate whether this is truely required.
extern "C" BOOST_SYMBOL_EXPORT irods::cli::cp cli_impl;
irods::cli::cp cli_impl;

