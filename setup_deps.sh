#!/bin/bash

git submodule update --init --recursive
cd dependencies/spdk || exit
./configure
make
