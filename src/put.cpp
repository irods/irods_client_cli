#include "put.hpp"

#include <irods/rodsClient.h>
#include <irods/connection_pool.hpp>
#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>
#include <irods/filesystem.hpp>

#include <boost/program_options.hpp>

#include <iostream>
#include <string>

namespace fs = irods::experimental::filesystem::client;
namespace io = irods::experimental::io;

namespace po = boost::program_options;

namespace irods::command
{
    int put(int _argc, char* _argv[])
    {
        /*
        po::positional_options_description options;
        options.add("physical_path", 1);
        options.add("logical_path", 2);

        po::variables_map vm;
        po::store(po::command_line_parser(_argc, _argv).positional(options).run(), vm);
        po::notify(vm);

        if (vm.count("physical_path") == 0) {
            std::cerr << "Error: Missing physical path.\n";
            return 1;
        }

        if ("-" != vm["physical_path"].as<std::string>()) {
            std::cerr << "Error: Physical path must be '-'.\n";
            return 1;
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

        irods::connection_pool conn_pool{1, env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 600};
        const auto logical_path = vm["logical_path"].as<std::string>();

        if (!fs::is_data_object(conn_pool.get_connection(), logical_path)) {
            std::cerr << "Error: Logical path does not point to a data object.\n";
            return 1;
        }

        auto conn = conn_pool.get_connection();
        io::client::default_transport dtp{conn};

        if (io::idstream in{dtp, logical_path}; in) {
            std::string line;
            while (std::getline(in, line)) {
                std::cout << line;
            }
        }
        */

        return 0;
    }
} // namespace irods

