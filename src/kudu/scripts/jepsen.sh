#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# This script is invoked from a Jenkins job to build Kudu C++ components,
# Kudu Java client, kudu-jepsen, and run kudu-jepsen consistency tests
# against the freshly built Kudu binaries.
#
# The following environment variables may be used to customize the operation:
#   JAVA8_HOME          Default: /usr/lib/jvm/java-8-openjdk-amd64
#     Path to the JDK8 installation root.  Expecting to find 'java'
#     in the 'bin' sub-directory.  Java 8 or higher is required for the
#     nebula-clojure-plugin.
#
#   KUDU_MASTER_NODES   Default: ""
#     Set of master nodes for the Kudu cluster to run the Jepsen consistency
#     test. Those should be the same architecture and OS type as the
#     Jenkins machine where this build script is run.
#     NOTE: currently, the Jepsen test can run only at Debian nodes.
#
#   KUDU_TSERVER_NODES  Default: ""
#     Set of tablet server nodes for the Kudu cluster to run the Jepsen
#     consistency test. Those should be the same architecture and OS type
#     as the Jenkins machine where this build script is run.
#     NOTE: currently, the Jepsen test can run only at Debian nodes.
#
#   SSH_KEY             Default: ""
#     The absolute path to the file containing the private SSH key
#     for the root user at the master and tablet server nodes
#     (DB nodes in Jepsen terminology).
#     NOTE: the key should be in plain (i.e. non-encrypted) format.
#

#############################################################################
# Constants
#############################################################################

# Require Kudu binaries to be statically linked with the Kudu-specific libs:
# the kudu-jepsen does not distribute Kudu libraries.
KUDU_LINK=static

SRC_ROOT=$(readlink -f $(dirname $0)/../../..)
THIRDPARTY_BIN=$SRC_ROOT/thirdparty/installed/common/bin
NUM_PROCS=$(getconf _NPROCESSORS_ONLN)

#############################################################################
# Customizable parameters
#############################################################################

BUILD_TYPE=${BUILD_TYPE:-debug}
JAVA8_HOME=${JAVA8_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
KUDU_MASTER_NODES=${KUDU_MASTER_NODES:-}
KUDU_TSERVER_NODES=${KUDU_TSERVER_NODES:-}
SSH_KEY=${SSH_KEY:-}
ITER_NUM=${ITER_NUM:-1}

#############################################################################
# Main
#############################################################################

set -e
set -o pipefail
ulimit -c unlimited

cd $SRC_ROOT

echo
echo "Removing logs from prior runs"
echo "--------------------------------------------------------------------"
rm -rf java/kudu-jepsen/store

echo
echo "Building third-party components"
echo "--------------------------------------------------------------------"
$SRC_ROOT/build-support/enable_devtoolset.sh \
  $SRC_ROOT/thirdparty/build-if-necessary.sh

mkdir -p build/$BUILD_TYPE
pushd build
ln -sf $SRC_ROOT/build/$BUILD_TYPE latest
pushd $BUILD_TYPE

echo
echo "Building Kudu C++ components (no tests)"
echo "--------------------------------------------------------------------"
$SRC_ROOT/build-support/enable_devtoolset.sh $THIRDPARTY_BIN/cmake \
  -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
  -DKUDU_LINK=$KUDU_LINK \
  -DNO_TESTS=1 \
  ../..
make -j$NUM_PROCS 2>&1 | tee build.log

export JAVA_HOME=$JAVA8_HOME
export PATH=$JAVA_HOME/bin:$PATH
set -x

pushd $SRC_ROOT/java
echo
echo "Building Kudu Java packages"
echo "--------------------------------------------------------------------"
./gradlew clean assemble

echo
echo "Building and running kudu-jepsen consistency tests"
echo "--------------------------------------------------------------------"
pushd kudu-jepsen
./gradlew runJepsen \
  -DmasterNodes="$KUDU_MASTER_NODES" \
  -DtserverNodes="$KUDU_TSERVER_NODES" \
  -DsshKeyPath="$SSH_KEY" \
  -DiterNum="$ITER_NUM"
popd
popd

set +x
