#!/usr/bin/env bash
if [ -z ${SPDK_DIR}]
    then bash ../dependencies/spdk/scripts/setup.sh
    else bash ${SPDK_DIR}/scripts/setup.sh
fi