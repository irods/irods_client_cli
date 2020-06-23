#ifndef IRODS_CLI_COMMAND_HPP
#define IRODS_CLI_COMMAND_HPP

#include <string_view>
#include <vector>

namespace irods::cli
{
    class command
    {
    public:
        virtual ~command() = default;

        virtual auto name() const noexcept -> std::string_view = 0;

        virtual auto description() const noexcept -> std::string_view = 0;

        virtual auto help_text() const noexcept -> std::string_view = 0;

        virtual auto execute(const std::vector<std::string>& args) -> int = 0;
    };
} // namespace irods::cli

#endif // IRODS_CLI_COMMAND_HPP

