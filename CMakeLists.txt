cmake_minimum_required(VERSION 3.7.0 FATAL_ERROR)

find_package(IRODS REQUIRED)

set(CMAKE_C_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang)
set(CMAKE_CXX_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang++)

project(irods_cli)

set(IRODS_CLI_VERSION ${IRODS_VERSION})
set(IRODS_CLI_REVISION 0)
set(IRODS_CLI_COMMANDS_INSTALL_DIR /usr/lib/irods/plugins/cli)

include(${IRODS_TARGETS_PATH})

if (NOT CPACK_PACKAGING_INSTALL_PREFIX)
    set(CPACK_PACKAGING_INSTALL_PREFIX "/" CACHE STRING "Package root path. \"/\" is correct for normal package builds.." FORCE)
    message(STATUS "Setting unspecified CPACK_PACKAGING_INSTALL_PREFIX to '${CPACK_PACKAGING_INSTALL_PREFIX}'. This is the correct setting for normal builds.")
endif()

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
    message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'")
endif()

if (NOT IRODS_EXTERNALS_PACKAGE_ROOT)
    set(IRODS_EXTERNALS_PACKAGE_ROOT "/opt/irods-externals" CACHE STRING "Choose the location of iRODS external packages." FORCE)
    message(STATUS "Setting unspecified IRODS_EXTERNALS_PACKAGE_ROOT to '${IRODS_EXTERNALS_PACKAGE_ROOT}'")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_EXE_LINKER_FLAGS_INIT "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++ -Wl,--export-dynamic -pthread")

# Make ALL symbols in shared libraries hidden by default.
# TODO Verify this behavior.
set_property(GLOBAL PROPERTY CMAKE_CXX_VISIBILITY_PRESET hidden)

set(APP irods)

add_executable(${APP} ${CMAKE_SOURCE_DIR}/src/main.cpp)

set_target_properties(${APP} PROPERTIES CXX_STANDARD ${IRODS_CXX_STANDARD})

target_compile_options(${APP} PRIVATE -Wno-write-strings -nostdinc++)

target_compile_definitions(${APP} PRIVATE IRODS_CLI_VERSION="${IRODS_CLI_VERSION}" ${IRODS_COMPILE_DEFINITIONS})

target_include_directories(${APP} PRIVATE ${CMAKE_SOURCE_DIR}/include
                                          ${IRODS_INCLUDE_DIRS}
                                          ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
                                          ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1
                                          ${IRODS_EXTERNALS_FULLPATH_FMT}/include)

target_link_libraries(${APP} PRIVATE irods_common
                                     irods_plugin_dependencies
                                     irods_client
                                     ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
                                     ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
                                     ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_program_options.so
                                     ${IRODS_EXTERNALS_FULLPATH_FMT}/lib/libfmt.so
                                     dl)

# CLI Commands
add_subdirectory(commands/get)
add_subdirectory(commands/ls)
add_subdirectory(commands/put)
add_subdirectory(commands/touch)
add_subdirectory(commands/cp)

# Installation
install(TARGETS ${APP}
        DESTINATION /usr/local/bin
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_EXECUTE
                    WORLD_READ WORLD_EXECUTE)

