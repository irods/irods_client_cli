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

    class rm : public command
    {
    public:
        auto name() const noexcept -> std::string_view override
        {
            return "rm";
        }

        auto description() const noexcept -> std::string_view override
        {
            return "Removes collections or data objects";
        }

        auto help_text() const noexcept -> std::string_view override
        {
            auto help =R"(
Given a fully qualfied or relative path, remove the collection or object

irods rm [options] fully_qualified_logical_path

      --unregister        : unregister data instead of unlinking data
      --no_trash          : do not move items to the trash can
      --number_of_threads : number of threads to use in recursive operations
      --progress          : request progress as a percentage)";
            return help;

        }

        auto execute(const std::vector<std::string>& args) -> int override
        {
            signal(SIGINT,  handle_signal);
            signal(SIGHUP,  handle_signal);
            signal(SIGTERM, handle_signal);

            bool progress_flag{false}, no_trash{false}, unregister{false};
            int thread_count{4};

            using rep_type = fs::object_time_type::duration::rep;

            po::options_description desc{""};
            desc.add_options()
                ("logical_path", po::value<std::string>(), "logical path to collection or object to remove")
                ("unregister", po::bool_switch(&unregister), "unregister data instead of unlinking data")
                ("no_trash", po::bool_switch(&no_trash), "do not move items to the trash can")
                ("number_of_threads", po::value<int>(&thread_count), "number of threads to use in recursive operations")
                ("progress", po::bool_switch(&progress_flag), "request progress as a percentage");

            po::positional_options_description pod;
            pod.add("logical_path", 1);

            po::variables_map vm;
            po::store(po::command_line_parser(args).options(desc).positional(pod).run(), vm);
            po::notify(vm);

            if (vm.count("logical_path") == 0) {
                std::cerr << "Error: Missing logical path.\n";
                return 1;
            }

            rodsEnv env;

            if (getRodsEnv(&env) < 0) {
                std::cerr << "Error: Could not get iRODS environment.\n";
                return 1;
            }

            const auto logical_path = vm["logical_path"].as<std::string>();
            irods::connection_pool conn_pool{1, env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 600};
            auto conn = conn_pool.get_connection();

            {
                const auto object_status = fs::client::status(conn, logical_path);

                if (!fs::client::is_collection(object_status) && !fs::client::is_data_object(object_status)) {
                    std::cerr << "Error: Logical path does not point to a collection or data object. Do you need a fully qualified path?\n";
                    return 1;
                }
            }

            std::string progress{};

            auto progress_handler = progress_flag ? print_progress : [](const std::string&) {};

            auto cli = ia::client{};
            auto rep = cli(conn,
                           exit_flag,
                           progress_handler,
                           {{"logical_path", logical_path},
                            {"unregister",   unregister},
                            {"no_trash",     no_trash},
                            {"thread_count", thread_count},
                            {"progress",     progress_flag}},
                           "recursive_remove");

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

    }; // class rm

} // namespace irods::cli

// TODO Need to investigate whether this is truely required.
extern "C" BOOST_SYMBOL_EXPORT irods::cli::rm cli_impl;
irods::cli::rm cli_impl;

