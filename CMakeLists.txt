cmake_minimum_required(VERSION 3.7.0 FATAL_ERROR)

find_package(IRODS REQUIRED)

set(CMAKE_C_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang)
set(CMAKE_CXX_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang++)

project(irods_console)

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

set(CMAKE_COMPILE_COMMANDS TRUE)

set(CMAKE_EXE_LINKER_FLAGS_INIT "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++")

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++ -Wl,--export-dynamic -pthread")

set(APP irods)

add_executable(${APP} ${CMAKE_SOURCE_DIR}/src/main.cpp
                      ${CMAKE_SOURCE_DIR}/src/get.cpp
                      ${CMAKE_SOURCE_DIR}/src/put.cpp)
set_target_properties(${APP} PROPERTIES CXX_STANDARD ${IRODS_CXX_STANDARD})
target_compile_options(${APP} PRIVATE -Wno-write-strings -nostdinc++)
target_compile_definitions(${APP} PRIVATE ${IRODS_COMPILE_DEFINITIONS})
target_include_directories(${APP} PRIVATE ${CMAKE_SOURCE_DIR}/include
                                          ${IRODS_INCLUDE_DIRS}
                                          ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1
                                          ${IRODS_EXTERNALS_FULLPATH_JSON}/include
                                          ${IRODS_EXTERNALS_FULLPATH_BOOST}/include)
target_link_libraries(${APP} PRIVATE irods_common
                                     irods_plugin_dependencies
                                     irods_client
                                     ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
                                     ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
                                     ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_program_options.so
                                     c++abi)

