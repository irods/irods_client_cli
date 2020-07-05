project(irods_cli_rm)

set(CLI_MODULE_NAME irods_cli_rm)

add_library(${CLI_MODULE_NAME} MODULE ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

set_target_properties(${CLI_MODULE_NAME} PROPERTIES CXX_STANDARD ${IRODS_CXX_STANDARD}
                                                    VERSION      0.0.1
                                                    SOVERSION    0)

target_compile_options(${CLI_MODULE_NAME} PRIVATE -Wno-write-strings -nostdinc++)

target_compile_definitions(${CLI_MODULE_NAME} PRIVATE ${IRODS_COMPILE_DEFINITIONS})

target_include_directories(${CLI_MODULE_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include
                                                      ${IRODS_INCLUDE_DIRS}
                                                      ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1
                                                      ${IRODS_EXTERNALS_FULLPATH_JSON}/include
                                                      ${IRODS_EXTERNALS_FULLPATH_BOOST}/include)

target_link_libraries(${CLI_MODULE_NAME} PRIVATE irods_common
                                                 irods_plugin_dependencies
                                                 irods_client
                                                 ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
                                                 ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
                                                 ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_program_options.so)

# Installation
install(TARGETS ${CLI_MODULE_NAME}
        DESTINATION ${IRODS_CLI_COMMANDS_INSTALL_DIR}
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_EXECUTE
                    WORLD_READ WORLD_EXECUTE)

