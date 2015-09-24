#!/bin/bash
# Copyright 2014 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
