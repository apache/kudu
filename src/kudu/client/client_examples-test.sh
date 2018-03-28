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

# Clean up after the test. Must be idempotent.
cleanup() {
  if [[ -n $TS_PID ]]; then
      kill -9 "$TS_PID" || :
      wait "$TS_PID" || :
  fi
  if [[ -n $MASTER_PID ]]; then
      kill -9 "$MASTER_PID" || :
      wait "$MASTER_PID" || :
  fi
  if [[ -n $BASE_DIR ]] && [[ -d $BASE_DIR ]]; then
      rm -rf "$BASE_DIR"
  fi
  if [[ -n $LIBRARY_DIR ]] && [[ -d $LIBRARY_DIR ]]; then
      rm -rf "$LIBRARY_DIR"
  fi
}
trap cleanup EXIT

set -e
set -o pipefail
set -x

wait_for_listen_port() {
  local pid=$1
  local expected_port=$2
  local num_attempts=$3

  local attempt=0
  while true; do
    # The lsof utility does not allow to distinguish between an existing
    # process not listening to the specified port and a non-existing process
    # by its return code. For the fast check let's verify that the process
    # is still running.
    if ! kill -0 "$pid"; then
      return 1
    fi
    local ports
    ports=$(lsof -wbnP -Fn -p "$pid" -a -i 4TCP -a -s TCP:LISTEN | \
            sed -n '/^n/ s/^[^:].*://p')
    for i in $ports; do
      if [[ $i -eq $expected_port ]]; then
        return 0
      fi
    done

    attempt=$((attempt+1))
    if [[ $attempt -ge $num_attempts ]]; then
      break
    fi
    sleep 1
  done

  return 1
}

exit_error() {
  local err_msg="$1"

  set +x
  echo ----------------------------------------------------------------------
  echo ERROR: "$err_msg"
  echo ----------------------------------------------------------------------
  exit 1
}

OUTPUT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)

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
for include_file in $(find "$LIBRARY_DIR" -name \*.h) ; do
  echo Checking standalone compilation of "$include_file"...
  if ! ${CXX:-g++} \
       -o /dev/null \
       -I"$LIBRARY_DIR/usr/local/include" \
       "$include_file" ; then
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

LOCALHOST_IP=127.0.0.1
if [[ $(uname) == Linux ]]; then
  # Pick a unique localhost IP address so this can run in parallel with other
  # tests. This only works on Linux.
  LOCALHOST_IP=127.$((($$ >> 8) & 0xff)).$(($$ & 0xff)).1
fi
echo Using localhost IP $LOCALHOST_IP


# Start master+ts
export TMPDIR=${TMPDIR:-/tmp}
export TEST_TMPDIR=${TEST_TMPDIR:-$TMPDIR/kudutest-$UID}
mkdir -p "$TEST_TMPDIR"
BASE_DIR=$(mktemp -d "$TEST_TMPDIR/client_examples-test.XXXXXXXX")
MASTER_RPC_PORT=7051
mkdir -p "$BASE_DIR/master/logs"
"$OUTPUT_DIR/kudu-master" \
  --unlock_experimental_flags \
  --default_num_replicas=1 \
  --log_dir="$BASE_DIR/master/logs" \
  --fs_wal_dir="$BASE_DIR/master/wals" \
  --fs_data_dirs="$BASE_DIR/master/data" \
  --webserver_interface=$LOCALHOST_IP \
  --webserver_port=0 \
  --rpc_bind_addresses=$LOCALHOST_IP:$MASTER_RPC_PORT &
MASTER_PID=$!

TSERVER_RPC_PORT=7050
mkdir -p "$BASE_DIR/ts/logs"
"$OUTPUT_DIR/kudu-tserver" \
  --unlock_experimental_flags \
  --heartbeat_interval_ms=200 \
  --heartbeat_rpc_timeout_ms=1000 \
  --log_dir="$BASE_DIR/ts/logs" \
  --fs_wal_dir="$BASE_DIR/ts/wals" \
  --fs_data_dirs="$BASE_DIR/ts/data" \
  --rpc_bind_addresses=$LOCALHOST_IP:$TSERVER_RPC_PORT \
  --local_ip_for_outbound_sockets=$LOCALHOST_IP \
  --webserver_interface=$LOCALHOST_IP \
  --webserver_port=0 \
  --tserver_master_addrs=$LOCALHOST_IP:$MASTER_RPC_PORT &
TS_PID=$!

# Make sure that at least it's possible to establish a TCP connection to the
# master's and the tablet server's RPC ports before running the client example
# application.
if ! wait_for_listen_port $MASTER_PID $MASTER_RPC_PORT 30; then
  exit_error "master is not accepting connections"
fi
if ! wait_for_listen_port $TS_PID $TSERVER_RPC_PORT 30; then
  exit_error "tserver is not accepting connections"
fi

# Allow for the tablet server registering with the master: wait for ~10s max.
max_attempts=10
attempt=0
num_tservers=0
while true; do
  if ! num_tservers=$("$OUTPUT_DIR/kudu" tserver list \
      $LOCALHOST_IP:$MASTER_RPC_PORT -format=space | wc -l); then
    exit_error "failed to determine number of registered tservers"
  fi
  if [[ $num_tservers -ge 1 ]]; then
    break
  fi
  attempt=$((attempt+1))
  if [[ $attempt -ge $max_attempts ]]; then
    break
  fi
  sleep 1
done

if [[ $num_tservers -lt 1 ]]; then
  exit_error "tserver has not registered with the master"
fi

# Run the examples.
"$EXAMPLES_DIR/example" $LOCALHOST_IP
