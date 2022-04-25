#!/usr/bin/env bash
if [ -z ${SPDK_DIR}]
    then bash ../dependencies/spdk/scripts/setup.sh reset
    else bash ${SPDK_DIR}/scripts/setup.sh reset
fi