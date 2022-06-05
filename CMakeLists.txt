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

# Default to debug
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
# Set flags
set(BUILD_FLAGS -Wall -Wextra -Werror)
if (CMAKE_BUILD_TYPE MATCHES Debug)
    list(APPEND BUILD_FLAGS -g2 -ggdb -O3)
endif()
if (CMAKE_BUILD_TYPE MATCHES Release)
    list(APPEND BUILD_FLAGS -O0)
endif()
option(FSANITIZE "Turn on fsanitize" OFF)
if (FSANITIZE)
    list(APPEND BUILD_FLAGS 
        -fsanitize=address #ASAN
        -fsanitize=undefined  #UBSAN
        -fsanitize=float-divide-by-zero 
        -fsanitize=float-cast-overflow  
        -fno-sanitize-recover=all 
        -fno-sanitize=alignment
    )
    message("fsanitize is turned on")
endif()

# Sets up SPDK
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindSPDK.cmake")
link_directories("${SPDK_LIB_DIRS}")

# Only set these vars, when it is included in another CMake. Helps with linking issues.
if(${CMAKE_PROJECT_NAME} STREQUAL ${PROJECT_NAME})
else()
    set(SPDK_LIB_DIRS "${SPDK_LIB_DIRS}" PARENT_SCOPE)
endif()

# Add your files here
set(szd_core_include_dir "${CMAKE_CURRENT_SOURCE_DIR}/szd/core/include/szd")
set(szd_core_include_files
    "${szd_core_include_dir}/szd_namespace.h"
    "${szd_core_include_dir}/szd_status_code.h"
    "${szd_core_include_dir}/szd.h"
)
list(APPEND szd_all_files szd_core_include_files)
set(szd_core_src_dir "${CMAKE_CURRENT_SOURCE_DIR}/szd/core/src")
set(szd_core_src_files
    "${szd_core_src_dir}/szd_status_code.c"
    "${szd_core_src_dir}/szd.c"
)
list(APPEND szd_all_files szd_core_src_files)

set(szd_cpp_include_dir "${CMAKE_CURRENT_SOURCE_DIR}/szd/cpp/include/szd")
set(szd_cpp_include_files
    "${szd_cpp_include_dir}/szd_status.hpp"
    "${szd_cpp_include_dir}/szd_device.hpp"
    "${szd_cpp_include_dir}/datastructures/szd_buffer.hpp"
    "${szd_cpp_include_dir}/szd_channel.hpp"
    "${szd_cpp_include_dir}/szd_channel_factory.hpp"
    "${szd_cpp_include_dir}/datastructures/szd_log.hpp"
    "${szd_cpp_include_dir}/datastructures/szd_once_log.hpp"
    "${szd_cpp_include_dir}/datastructures/szd_circular_log.hpp"
    "${szd_cpp_include_dir}/datastructures/szd_freezone_list.hpp"
    "${szd_cpp_include_dir}/datastructures/szd_fragmented_log.hpp"
)
list(APPEND szd_all_files szd_cpp_include_files)
set(szd_cpp_src_dir "${CMAKE_CURRENT_SOURCE_DIR}/szd/cpp/src")
set(szd_cpp_src_files
    "${szd_cpp_src_dir}/szd_status.cpp"
    "${szd_cpp_src_dir}/szd_device.cpp"
    "${szd_cpp_src_dir}/datastructures/szd_buffer.cpp"
    "${szd_cpp_src_dir}/szd_channel.cpp"
    "${szd_cpp_src_dir}/szd_channel_factory.cpp"
    "${szd_cpp_src_dir}/datastructures/szd_log.cpp"
    "${szd_cpp_src_dir}/datastructures/szd_once_log.cpp"
    "${szd_cpp_src_dir}/datastructures/szd_circular_log.cpp"
    "${szd_cpp_src_dir}/datastructures/szd_freezone_list.cpp"
    "${szd_cpp_src_dir}/datastructures/szd_fragmented_log.cpp"
)
list(APPEND szd_all_files szd_cpp_src_files)

# Use for each library and executable, it ensures more homogenous builds.
function(SETUP_SZD_PROJECT_STRUCTURE project_name)
    set_target_properties(${project_name} PROPERTIES LINKER_LANGUAGE CXX)
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

# Libraries...
add_library(szd STATIC
    "${szd_core_include_files}"
    "${szd_core_src_files}"
)
target_include_directories(szd PRIVATE "${SPDK_INCLUDE_DIRS}")
target_link_libraries(szd PUBLIC ${SPDK_LIBRARY_DEPENDENCIES})
target_include_directories(szd PUBLIC 
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/szd/core/include>"
    "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/szd/core/${CMAKE_INSTALL_INCLUDEDIR}>"
)
setup_szd_project_structure(szd)

add_library(szd_extended STATIC
    "${szd_core_include_files}"
    "${szd_core_src_files}"
    "${szd_cpp_include_files}"
    "${szd_cpp_src_files}"
)
target_include_directories(szd_extended PRIVATE "${SPDK_INCLUDE_DIRS}")
target_link_libraries(szd_extended PUBLIC ${SPDK_LIBRARY_DEPENDENCIES})
target_include_directories(szd_extended PUBLIC 
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/szd/core/include>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/szd/cpp/include>"
    "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/szd/core/${CMAKE_INSTALL_INCLUDEDIR}>"
    "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/szd/cpp/${CMAKE_INSTALL_INCLUDEDIR}>"
)
setup_szd_project_structure(szd_extended)

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
        if (${tool}_core_only)
            target_link_libraries(${tool} PUBLIC szd)
        else()
            target_link_libraries(${tool} PUBLIC szd_extended)
        endif()
        list(APPEND tool_files "${${tool}_sources}")
        list(APPEND szd_all_files tool_files)
    endforeach()
endif()

# Tests...
option(TESTING "Turn on tests" OFF)
if (TESTING)
    enable_testing()
    # C tests
    set(szd_core_test_dir "${CMAKE_CURRENT_SOURCE_DIR}/szd/core/tests")
    add_executable(szd_full_path_test
        "${szd_core_test_dir}/szd_full_path_test.c"
    )
    list(APPEND szd_all_files "${test_dir}/szd_full_path_test.c")
    target_link_libraries(szd_full_path_test PUBLIC szd)
    setup_szd_project_structure(szd_full_path_test)
    add_test(
        NAME szd_full_path_test_runner
        COMMAND szd_full_path_test
    )
    # Setup GTests
    include(GoogleTest)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
    )
    FetchContent_MakeAvailable(googletest)
    # C++ tests
    set(test_dir_cpp "${CMAKE_CURRENT_SOURCE_DIR}/szd/cpp/tests")
    set(cpp_tests
        "szd_device_test"
        "szd_channel_test"
        "szd_once_log_test"
        "szd_circular_log_test"
        "szd_fragmented_log_test"
    )
    list(APPEND szd_all_files cpp_tests)
    function(add_cpp_test filename dir)
        add_executable("${filename}" "${dir}/${filename}.cpp")
        target_link_libraries("${filename}" PRIVATE szd_extended gtest_main)
        setup_szd_project_structure("${filename}")
        gtest_add_tests(TARGET "${filename}")
        add_test(
            NAME "${filename}_runner"
            COMMAND "${filename}"
        )
    endfunction()
    foreach(cpp_test ${cpp_tests})
        ADD_CPP_TEST("${cpp_test}" "${test_dir_cpp}")
    endforeach()
endif()

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
    ${szd_all_files}
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
