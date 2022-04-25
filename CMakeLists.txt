cmake_minimum_required(VERSION 3.16)
project(simple-zns-device VERSION 1.0.1 DESCRIPTION "Simple storage interface for ZNS-SSD and SPDK")
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
message("compiler is ${CMAKE_CXX_COMPILER_ID} and name is ${CMAKE_CXX_COMPILER}")

set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(CMAKE_LIB_DIR ${CMAKE_SOURCE_DIR}/lib)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_LIB_DIR})

#default to debug
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
# Set flags
set(BUILD_FLAGS -Wall -Wextra)
if (CMAKE_BUILD_TYPE MATCHES Debug)
    list(APPEND BUILD_FLAGS -g2 -ggdb -O3)
endif()
if (CMAKE_BUILD_TYPE MATCHES Release)
    list(APPEND BUILD_FLAGS -O0)
endif()
option(FSANITIZE "Turn on fsanitize" OFF)
if (FSANITIZE)
    list(APPEND BUILD_FLAGS -fsanitize=address -fsanitize=undefined 
        -fno-sanitize-recover=all -fsanitize=float-divide-by-zero 
        -fsanitize=float-cast-overflow -fno-sanitize=null 
        -fno-sanitize=alignment)
    message("fsanitize is turned on")
endif()

# Sets up SPDK
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/LinkSPDK.cmake")
link_directories("${SPDK_LIB_DIRS}")

# Only set these vars, when it is included in another CMake
if(${CMAKE_PROJECT_NAME} STREQUAL ${PROJECT_NAME})
else()
    set(SPDK_LIB_DIRS "${SPDK_LIB_DIRS}" PARENT_SCOPE)
    set(SPDK_LIBRARY_DEPENDENCIES "${SPDK_LIBRARY_DEPENDENCIES}" PARENT_SCOPE)
    # necessary to prevent issues when the upstream CMakeList.txt uses -Werror.
    string(REPLACE " -Werror" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
endif()

# Use for each library and executable
function(SETUP_SPDK_PROJECT_STRUCTURE project_name)
    set_target_properties(${project_name} PROPERTIES LINKER_LANGUAGE CXX)
    target_include_directories(${project_name} PUBLIC 
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>"
    )
    target_include_directories(${project_name} PRIVATE src)
    target_link_libraries(${project_name} ${SPDK_LIBRARY_DEPENDENCIES})
    set_property(TARGET ${project_name} PROPERTY POSITION_INDEPENDENT_CODE ON)
    target_compile_options(
        ${project_name}
        PUBLIC "${SPDK_CFLAGS};${DPDK_CFLAGS}"
    )
    target_compile_options(
        ${project_name}
        PRIVATE "${BUILD_FLAGS}"
    )
    target_link_options(
        ${project_name}
        PRIVATE "${BUILD_FLAGS}"
    )
endfunction()

set(include_dir "${CMAKE_CURRENT_SOURCE_DIR}/include/szd")
set(src_dir "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(common_sources
    "${include_dir}/szd_namespace.h"
    "${include_dir}/szd.h"
    "${include_dir}/szd_utils.h"
    "${include_dir}/cpp/szd_channel.h"
    "${src_dir}/szd.cpp"
    "${src_dir}/cpp/szd_channel.cpp"
)

# Library
add_library(znsdevice_lib STATIC
    "${common_sources}"
)
setup_spdk_project_structure(znsdevice_lib)

# add_library(znsdevice_lib_shared SHARED
#     "${common_sources}"
# )
#setup_spdk_project_structure(znsdevice_lib_shared)

# Standalone CLI
add_executable(znscli
    tools/znscli.cpp
)
target_link_libraries(znscli znsdevice_lib)
setup_spdk_project_structure(znscli)

# Tests
add_executable(device_initial_test
    tests/device_initial_test.cpp
)
target_link_libraries(device_initial_test znsdevice_lib)
setup_spdk_project_structure(device_initial_test)

enable_testing()
add_test(
    NAME device_initial_test_runner
    COMMAND device_initial_test
)

# Default to "default" location of clang-format, override if necessary
if(DEFINED ENV{CLANG_FORMAT_PATH})
    set(CLANG_FORMAT_PATH "$ENV{CLANG_PATH}")
else(DEFINED ENV{CLANG_FORMAT_PATH})
    set(CLANG_FORMAT_PATH "/usr/bin/clang-format")
endif()
# format (DO NOT GLOB! instead let the formatter glob when necessary)
add_custom_target(
    format
    COMMAND ${CLANG_FORMAT_PATH}
    -i
    "-style=file"
    ${common_sources}
    "./tools/*.cpp"
    COMMENT "Formatting files"
)

# We do not force the installation of Doxygen
option(DOXYGEN "Allow generating documentation" OFF)
if(DOXYGEN)
    find_package(Doxygen
                REQUIRED dot
                OPTIONAL_COMPONENTS mscgen dia)
    if(DOXYGEN_FOUND)
        # set input and output files
        set(DOCS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/docs)
        set(DOXYFILE_IN ${DOCS_DIR}/Doxyfile.in)
        set(DOXYFILE_OUT ${DOCS_DIR}/Doxyfile)
            
        # request to configure the file
        configure_file(${DOXYFILE_IN} ${DOXYFILE_OUT} @ONLY)
        add_custom_target( docs
            COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
            WORKING_DIRECTORY ${DOCS_DIR}
            COMMENT "Generating API documentation with Doxygen"
            VERBATIM )
    else(DOXYGEN_FOUND)
        message("No documentation can be generated, DOXYGEN is not found")
    endif(DOXYGEN_FOUND)
endif(DOXYGEN)
