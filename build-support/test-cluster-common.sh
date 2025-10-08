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
# Common functions and cluster management shared by example test scripts.

# Wait for a process to start listening on a specific port.
# Arguments:
#   $1 - PID of the process
#   $2 - Expected port number
#   $3 - Number of attempts to wait
# Returns: 0 if port is listening, 1 if timeout or process died
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
    ports=$(lsof -wnP -Fn -p "$pid" -a -i 4TCP -a -s TCP:LISTEN | \
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

# Print an error message and exit.
# Arguments:
#   $1 - Error message
exit_error() {
  local err_msg="$1"

  set +x
  echo ----------------------------------------------------------------------
  echo ERROR: "$err_msg"
  echo ----------------------------------------------------------------------
  exit 1
}

# Clean up cluster processes.
# Expects MASTER_PID, TS_PID, and BASE_DIR to be set.
cleanup_cluster() {
  if [[ -n $TS_PID ]]; then
      kill -9 "$TS_PID" 2>/dev/null || :
      wait "$TS_PID" 2>/dev/null || :
  fi
  if [[ -n $MASTER_PID ]]; then
      kill -9 "$MASTER_PID" 2>/dev/null || :
      wait "$MASTER_PID" 2>/dev/null || :
  fi
  if [[ -n $BASE_DIR ]] && [[ -d $BASE_DIR ]]; then
      rm -rf "$BASE_DIR"
  fi
}

# Select a unique localhost IP for parallel test execution.
# Sets LOCALHOST_IP variable.
setup_localhost_ip() {
  LOCALHOST_IP=127.0.0.1
  if [[ $(uname) == Linux ]]; then
    # Pick a unique localhost IP address so this can run in parallel with other
    # tests. This only works on Linux.
    LOCALHOST_IP=127.$((($$ >> 8) & 0xff)).$(($$ & 0xff)).1
  fi
  echo Using localhost IP $LOCALHOST_IP
}

# Start a Kudu master and tserver for testing.
# Arguments:
#   $1 - BIN_DIR (where kudu-master, kudu-tserver, and kudu binaries are located)
#   $2 - TEST_NAME (for temp directory naming, e.g. "client_examples-test")
# Sets the following variables:
#   MASTER_PID, TS_PID, BASE_DIR, LOCALHOST_IP, MASTER_RPC_PORT, TSERVER_RPC_PORT
start_test_cluster() {
  local bin_dir=$1
  local test_name=$2

  setup_localhost_ip

  export TMPDIR=${TMPDIR:-/tmp}
  export TEST_TMPDIR=${TEST_TMPDIR:-$TMPDIR/kudutest-$UID}
  mkdir -p "$TEST_TMPDIR"
  BASE_DIR=$(mktemp -d "$TEST_TMPDIR/${test_name}.XXXXXXXX")

  MASTER_RPC_PORT=7051
  mkdir -p "$BASE_DIR/master/logs"
  "$bin_dir/kudu-master" \
    --unlock_experimental_flags \
    --unlock_unsafe_flags \
    --default_num_replicas=1 \
    --log_dir="$BASE_DIR/master/logs" \
    --fs_wal_dir="$BASE_DIR/master/wals" \
    --fs_data_dirs="$BASE_DIR/master/data" \
    --time_source=system_unsync \
    --webserver_interface=$LOCALHOST_IP \
    --webserver_port=0 \
    --rpc_bind_addresses=$LOCALHOST_IP:$MASTER_RPC_PORT &
  MASTER_PID=$!

  TSERVER_RPC_PORT=7050
  mkdir -p "$BASE_DIR/ts/logs"
  "$bin_dir/kudu-tserver" \
    --unlock_experimental_flags \
    --unlock_unsafe_flags \
    --heartbeat_interval_ms=200 \
    --heartbeat_rpc_timeout_ms=1000 \
    --log_dir="$BASE_DIR/ts/logs" \
    --fs_wal_dir="$BASE_DIR/ts/wals" \
    --fs_data_dirs="$BASE_DIR/ts/data" \
    --rpc_bind_addresses=$LOCALHOST_IP:$TSERVER_RPC_PORT \
    --local_ip_for_outbound_sockets=$LOCALHOST_IP \
    --time_source=system_unsync \
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
  local max_attempts=10
  local attempt=0
  local num_tservers=0
  while true; do
    if ! num_tservers=$("$bin_dir/kudu" tserver list \
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

  echo "Test cluster started successfully on $LOCALHOST_IP"
}

