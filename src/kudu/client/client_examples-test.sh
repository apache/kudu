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

# This script verifies that the Kudu client library can be installed outside
# the build tree, that the installed headers are sane, and that the example code
# can be built and runs correctly.

OUTPUT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)
SOURCE_ROOT=$(cd "$OUTPUT_DIR/../../.."; pwd)

# Source the common cluster management functions
source "$SOURCE_ROOT/build-support/test-cluster-common.sh"

# Clean up after the test. Must be idempotent.
cleanup() {
  cleanup_cluster
  if [[ -n $LIBRARY_DIR ]] && [[ -d $LIBRARY_DIR ]]; then
      rm -rf "$LIBRARY_DIR"
  fi
}
trap cleanup EXIT

set -e
set -o pipefail
set -x

# Install the client library to a temporary directory.
# Try to detect whether we're building using Ninja or Make.
LIBRARY_DIR=$(mktemp -d -t kudu-examples-test.XXXXXXXXXXXXX)
PREFIX_DIR=$LIBRARY_DIR/usr/local
EXAMPLES_DIR=$PREFIX_DIR/share/doc/kuduClient/examples
pushd "$OUTPUT_DIR/.."
NINJA=$(which ninja 2>/dev/null) || NINJA=""
if [[ -r build.ninja ]] && [[ -n $NINJA ]]; then
  DESTDIR=$LIBRARY_DIR ninja install
else
  # nproc is not available on macOS.
  make -j"$(getconf _NPROCESSORS_ONLN)" DESTDIR="$LIBRARY_DIR" install
fi
popd

# Test that all of the installed headers can be compiled on their own.
# This catches bugs where we've made a mistake in 'include-what-you-use'
# within the library.
#
# The API of the Kudu C++ client is supposed to be compatible with legacy C++
# compilers talking C++98 standard at most, but Kudu uses C++17 internally
# (as of June 2021). An extra flag -std=c++98 is added to catch changes
# incompatible with C++98 in the exported files representing the API of
# the Kudu C++ client.
for include_file in $(find "$LIBRARY_DIR" -name \*.h) ; do
  echo Checking standalone compilation of "$include_file"...
  if ! ${CXX:-g++} \
       -c \
       -o /dev/null \
       -x c++ \
       -std=c++98 \
       -Werror \
       -I"$LIBRARY_DIR/usr/local/include" - \
       < "$include_file" ; then
    set +x
    echo
    echo -----------------------------------------
    echo "$include_file" fails to build on its own.
    echo See log above for details.
    echo -----------------------------------------
    exit 1
  fi
done

# Prefer the cmake on the system path, since we expect our client library
# to be usable with older versions of cmake. But if it isn't there,
# use the one from thirdparty.
CMAKE=$(which cmake || :)
if [[ -z $CMAKE ]]; then
  # TODO: temporary hack which assumes this script is in src/build/<type>/bin
  CMAKE=$OUTPUT_DIR/../../../thirdparty/installed/common/bin/cmake
fi

# Build the client examples using the client library.
# We can just always use Make here, since we're calling cmake ourselves.
pushd "$EXAMPLES_DIR"
CMAKE_PREFIX_PATH=$PREFIX_DIR $CMAKE .
make -j"$(getconf _NPROCESSORS_ONLN)"
popd

# Start the test cluster
start_test_cluster "$OUTPUT_DIR" "client_examples-test"

# Run the examples.
"$EXAMPLES_DIR/example" $LOCALHOST_IP
"$EXAMPLES_DIR/non_unique_primary_key" $LOCALHOST_IP
