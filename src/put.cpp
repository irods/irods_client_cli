#include "put.hpp"

#include <irods/rodsClient.h>
#include <irods/connection_pool.hpp>
#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <iostream>
#include <string>
#include <array>

namespace fs = boost::filesystem;
namespace io = irods::experimental::io;

namespace po = boost::program_options;

namespace irods::command
{
    int put(int _argc, char* _argv[])
    {
        po::options_description desc{"Allowed options"};
        desc.add_options()
            ("help,h", "Produce this message.")
            ("physical_path", po::value<std::string>(), "The physical path to write to.")
            ("logical_path", po::value<std::string>(), "The logical path of a data object.");

        po::positional_options_description pod;
        pod.add("physical_path", 1);
        pod.add("logical_path", 1);

        po::variables_map vm;
        po::store(po::command_line_parser(_argc, _argv).options(desc).positional(pod).run(), vm);
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

        const auto logical_path = vm["logical_path"].as<std::string>();
        irods::connection_pool conn_pool{1, env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 600};

        auto conn = conn_pool.get_connection();
        io::client::default_transport dtp{conn};

        if (io::odstream out{dtp, logical_path}; out) {
            std::array<char, 4 * 1024 * 1024> buffer{};

            while (std::cin && out) {
                std::cin.read(&buffer[0], buffer.size());
                out.write(&buffer[0], std::cin.gcount());
            }
        }
        else {
            std::cerr << "Error: Could not open output stream [path => " << logical_path << "]\n";
        }

        return 0;
    }
} // namespace irods

