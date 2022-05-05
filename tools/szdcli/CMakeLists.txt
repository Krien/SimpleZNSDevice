cmake_minimum_required(VERSION 3.16)

set(szdcli_sources 
    "${CMAKE_CURRENT_SOURCE_DIR}/szdcli.c"
    PARENT_SCOPE
)
set(szdcli_core_only ON PARENT_SCOPE)
