#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <functional>

#include "get.hpp"
#include "put.hpp"
#include "touch.hpp"

namespace 
{
    using command = std::function<int(int _argc, char**)>;

    const std::unordered_map<std::string, command> commands{
        {"get", irods::command::get},
        {"put", irods::command::put},
        {"touch", irods::command::touch}
    };
} // anonymous namespace

int main(int _argc, char* _argv[])
{
    if (_argc < 2) {
        std::cerr << "Invalid number of arguments. See help.";
        return 1;
    }

    if (auto it = commands.find(_argv[1]); std::end(commands) != it) {
        return (it->second)(_argc - 1, _argv + 1);
    }

    std::cerr << "Unknown command [" << _argv[1] << "]\n";

    return 1;
}

