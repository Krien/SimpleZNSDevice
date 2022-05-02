#!/usr/bin/env bash

function verify_code_style() {
    local rc=0
    make format
    return $rc
}

rc=0
verify_code_style || rc=0

exit $rc
