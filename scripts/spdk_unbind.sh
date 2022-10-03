#!/bin/bash
set -e

DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR

if [ -z ${SPDK_DIR}]; then 
    bash ../dependencies/spdk/scripts/setup.sh reset
else
    bash ${SPDK_DIR}/scripts/setup.sh reset
fi
