#!/bin/bash
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# This script is invoked from the Jenkins builds to build the kudu-master
# and kudu-tserver binaries with the release build type.
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

cmake . -DNO_TESTS=1 -DCMAKE_BUILD_TYPE=release
make clean
make -j$(nproc) 2>&1 | tee build.log
