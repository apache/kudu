#!/bin/bash
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# This script is invoked from the Jenkins builds to build the kudu-master
# and kudu-tablet_server binaries with the release build type.
#

set -e
set -x
# We pipe our build output to a log file with tee.
# This bash setting ensures that the script exits if the build fails.
set -o pipefail
ulimit -c unlimited

ROOT=$(readlink -f $(dirname "$BASH_SOURCE")/../..)
cd $ROOT

rm -f build.log

thirdparty/build-if-necessary.sh

export PATH=$(pwd)/thirdparty/installed/bin:$PATH

rm -rf CMakeCache.txt CMakeFiles src/kudu/*/CMakeFiles

cmake . -DCMAKE_BUILD_TYPE=release -DKUDU_LINK=dynamic
make clean

NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)

make -j$NUM_PROCS kudu-master 2>&1 | tee build.log
make -j$NUM_PROCS kudu-tablet_server 2>&1 | tee -a build.log
make -j$NUM_PROCS kudu-ts-cli 2>&1 | tee -a build.log
make -j$NUM_PROCS log-dump 2>&1 | tee -a build.log
