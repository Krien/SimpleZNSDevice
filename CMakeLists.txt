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
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindSPDK.cmake")
link_directories("${SPDK_LIB_DIRS}")

# Only set these vars, when it is included in another CMake
if(${CMAKE_PROJECT_NAME} STREQUAL ${PROJECT_NAME})
else()
    set(SPDK_LIB_DIRS "${SPDK_LIB_DIRS}" PARENT_SCOPE)
    set(SPDK_LIBRARY_DEPENDENCIES "${SPDK_LIBRARY_DEPENDENCIES}" PARENT_SCOPE)
    # necessary to prevent issues when the upstream CMakeList.txt uses -Werror.
    string(REPLACE " -Werror" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
endif()

# Use for each library and executable, it ensures more homogenous builds.
function(SETUP_SZD_PROJECT_STRUCTURE project_name)
    set_target_properties(${project_name} PROPERTIES LINKER_LANGUAGE CXX)
    target_include_directories(${project_name} PUBLIC 
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>"
    )
    set_property(TARGET ${project_name} PROPERTY POSITION_INDEPENDENT_CODE ON)
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
set(include_files_core
    "${include_dir}/szd_namespace.h"
    "${include_dir}/szd_status_code.h"
    "${include_dir}/szd.h"
)
set(src_files_core
    "${src_dir}/szd_status_code.c"
    "${src_dir}/szd.c"
)
set(include_dir_cpp "${include_dir}/cpp")
set(src_dir_cpp "${src_dir}/cpp")
set(include_files_cpp
    "${include_dir_cpp}/szd_status.h"
    "${include_dir_cpp}/szd_channel.h"
    "${include_dir_cpp}/szd_channel_factory.h"
)
set(src_files_cpp
    "${src_dir_cpp}/szd_status.cpp"
    "${src_dir_cpp}/szd_channel.cpp"
    "${src_dir_cpp}/szd_channel_factory.cpp"
)

set(common_sources
    "${include_files_core}"
    "${src_files_core}"
    "${include_files_cpp}"
    "${src_files_cpp}"
)

# Libraries...
add_library(szd_lib STATIC
    "${common_sources}"
)
target_include_directories(szd_lib PRIVATE "${SPDK_INCLUDE_DIRS}")
target_link_libraries(szd_lib ${SPDK_LIBRARY_DEPENDENCIES})
setup_szd_project_structure(szd_lib)

#TODO: add shared lib
# add_library(szd_lib_shared SHARED
#     "${common_sources}"
# )
#setup_spdk_project_structure(szd_lib_shared)

# Tooling....
set(tool_files)
if (SZD_TOOLS)
    string(REPLACE " " ";" TOOLS ${SZD_TOOLS})
    foreach (tool ${TOOLS})
        add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/tools/${tool}")
        add_executable(${tool} "${${tool}_sources}")
        setup_szd_project_structure(${tool})
        target_link_libraries(${tool} szd_lib)
        LIST(APPEND tool_files "${${tool}_sources}")
    endforeach()
endif()

# Tests...
add_executable(device_initial_test
    tests/device_initial_test.cpp
)
target_link_libraries(device_initial_test szd_lib)
setup_szd_project_structure(device_initial_test)

enable_testing()
add_test(
    NAME device_initial_test_runner
    COMMAND device_initial_test
)

# Formatting...
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
    ${tool_files}
    COMMENT "Formatting files"
)

# Documentation...
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
