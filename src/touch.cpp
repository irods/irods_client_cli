#include "touch.hpp"

#include <irods/rodsClient.h>
#include <irods/connection_pool.hpp>
#include <irods/filesystem.hpp>

#include <boost/program_options.hpp>

#include <iostream>
#include <string>
#include <chrono>

namespace fs = irods::experimental::filesystem;

namespace po = boost::program_options;

namespace irods::command
{
    int touch(int _argc, char* _argv[])
    {
        using rep_type = fs::object_time_type::duration::rep;

        po::options_description desc{"Allowed options"};
        desc.add_options()
            ("help,h", "Produce this message.")
            ("logical_path", po::value<std::string>(), "The logical path of a collection or data object.")
            ("modification_time", po::value<rep_type>(), "The new modification time in seconds.");

        po::positional_options_description pod;
        pod.add("logical_path", 1);
        pod.add("modification_time", 1);

        po::variables_map vm;
        po::store(po::command_line_parser(_argc, _argv).options(desc).positional(pod).run(), vm);
        po::notify(vm);

        if (vm.count("help") > 0) {
            std::cout << desc << '\n';
            return 0;
        }

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
                std::cerr << "Error: Logical path does not point to a collection or data object.\n";
                return 1;
            }
        }

        using clock_type    = fs::object_time_type::clock;
        using duration_type = fs::object_time_type::duration;

        fs::object_time_type new_mtime;

        if (vm.count("modification_time") > 0) {
            new_mtime = fs::object_time_type{duration_type{vm["modification_time"].as<rep_type>()}};
        }
        else {
            new_mtime = std::chrono::time_point_cast<duration_type>(clock_type::now());
        }

        try {
            fs::client::last_write_time(conn, logical_path, new_mtime);
        }
        catch (const fs::filesystem_error& e) {
            std::cerr << "Error: " << e.what() << '\n';
            return 1;
        }

        return 0;
    }
} // namespace irods::command

