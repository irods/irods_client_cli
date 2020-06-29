#include "command.hpp"

#include <irods/rodsClient.h>
#include <irods/irods_default_paths.hpp>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/dll.hpp>

#include <fmt/format.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace po = boost::program_options;
namespace fs = boost::filesystem;

using cli_command_map_type = std::unordered_map<std::string_view, boost::shared_ptr<irods::cli::command>>;

auto is_shared_library(const fs::path& p) -> bool;

auto load_cli_command_modules(const po::variables_map& vm) -> cli_command_map_type;

auto print_version_info() noexcept -> void;
auto print_usage_info(const cli_command_map_type& cli) -> void;

int main(int argc, char* argv[])
{
    po::options_description options{""};
    options.add_options()
        ("help,h", "")
        ("version,v", "")
        ("plugin-home,p", po::value<std::string>(), "")
        ("command", po::value<std::string>(), "")
        ("arguments", po::value<std::vector<std::string>>(), "");

    po::positional_options_description positional_options;
    positional_options.add("command", 1);
    positional_options.add("arguments", -1);

    try {
        po::variables_map vm;
        auto parsed = po::command_line_parser(argc, argv)
            .options(options)
            .positional(positional_options)
            .allow_unregistered()
            .run();

        po::store(parsed, vm);
        po::notify(vm);

        if (vm.count("version")) {
            print_version_info();
            return 0;
        }

        load_client_api_plugins();

        auto cli = load_cli_command_modules(vm);

        if (const auto show_help_text = vm.count("help") > 0; vm.count("command")) {
            const auto command = vm["command"].as<std::string>();
            auto iter = cli.find(command);

            if (auto iter = cli.find(command); std::end(cli) == iter) {
                fmt::print("Invalid command: {}\n", command);
                return 1;
            }

            if (show_help_text) {
                fmt::print("{}\n", iter->second->help_text());
                return 0;
            }

            auto remaining_args = po::collect_unrecognized(parsed.options, po::include_positional);
            remaining_args.erase(std::begin(remaining_args));
            return iter->second->execute(remaining_args);
        }
        else if (show_help_text) {
            print_usage_info(cli);
        }
    }
    catch (const std::exception& e) {
        fmt::print("ERROR: {}\n", e.what());
        return 1;
    }

    return 0;
}

auto is_shared_library(const fs::path& p) -> bool
{
    // TODO
    return true;
}

auto load_cli_command_modules(const po::variables_map& vm) -> cli_command_map_type
{
    cli_command_map_type map;
    fs::path lib_dir;

    if (vm.count("plugin-home")) {
        lib_dir = vm["plugin-home"].as<std::string>();
    }
    else {
        rodsEnv env;
        _getRodsEnv(env);

        if (std::strlen(env.irodsPluginHome) > 0) {
            lib_dir = env.irodsPluginHome;
            lib_dir /= "cli";
        }
        else {
            lib_dir = irods::get_irods_default_plugin_directory();
        }
    }

    for (auto&& e : fs::directory_iterator{lib_dir}) {
        if (is_shared_library(e)) {
            namespace dll = boost::dll;
            auto cli_impl = dll::import<irods::cli::command>(e.path(), "cli_impl", dll::load_mode::append_decorations);
            map.insert_or_assign(cli_impl->name(), cli_impl);
        }
    }

    return map;
}

auto print_version_info() noexcept -> void
{
    fmt::print("irods version {}\n", IRODS_CLI_VERSION); // Defined by CMakeLists.txt
}

auto print_usage_info(const cli_command_map_type& cli) -> void
{
    fmt::print("usage: irods [-v | --version] [-p | --plugin-home <dir>] [-h | --help]\n"
               "usage: irods [-p | --plugin-home <dir>] <command> [<options>] [<args>]\n"
               "\n"
               "These are common iRODS commands used in various situations:\n"
               "\n");

    for (auto&& [name, impl] : cli) {
        fmt::print("{:<10} {}\n", name, impl->description());
    }

    fmt::print("\n");
}

