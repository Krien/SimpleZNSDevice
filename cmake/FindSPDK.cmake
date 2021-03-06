# Override to use different location for SPDK.
if(DEFINED $ENV{SPDK_DIR})
    set(SPDK_DIR "$ENV{SPDK_DIR}")
else()
    set(SPDK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/dependencies/spdk")
endif()
message("looking for SPDK in ${SPDK_DIR}")

find_package(PkgConfig REQUIRED)
if(NOT PKG_CONFIG_FOUND)
    message(FATAL_ERROR "pkg-config command not found!" )
endif()

# Needed to ensure that PKG_CONFIG also looks at our SPDK installation.
set(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:${SPDK_DIR}/build/lib/pkgconfig/")
message("Looking for SPDK packages...")
pkg_search_module(SPDK REQUIRED IMPORTED_TARGET spdk_nvme)
pkg_search_module(DPDK REQUIRED IMPORTED_TARGET spdk_env_dpdk)
pkg_search_module(SYS REQUIRED IMPORTED_TARGET spdk_syslibs)

set(SPDK_INCLUDE_DIRS "${SPDK_INCLUDE_DIRS}")
# use ";" otherwise the second and third argument become one?
set(SPDK_LIB_DIRS "${SPDK_LIBRARY_DIRS};${DPDK_LIBRARY_DIRS};${SYS_STATIC_LIBRARY_DIRS}")
set(SPDK_AND_DPDK_LIBRARIES "${SPDK_LIBRARIES}" "${DPDK_LIBRARIES}")
list(REMOVE_DUPLICATES SPDK_AND_DPDK_LIBRARIES)
set(SPDK_LIBRARY_DEPENDENCIES 
    -Wl,--whole-archive -Wl,--as-needed
    "${SPDK_AND_DPDK_LIBRARIES}"
    -Wl,--no-whole-archive
    "${SYS_STATIC_LIBRARIES}" 
    -pthread
)

message("SPDK directory: ${SPDK_DIR}")
