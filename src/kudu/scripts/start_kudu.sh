#!/bin/bash

########################################################################
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
########################################################################

set -e
ulimit -n 2048

function usage() {
cat << EOF
Usage:
start_kudu.sh [flags]
-h, --help         Print help
-m, --num-masters  Number of Kudu Masters to start (default: 1)
-t, --num-tservers Number of Kudu Tablet Servers to start (default: 3)
--rpc-master       RPC port of first Kudu Master; HTTP port is the next number.
                   Subsequent Masters will have following numbers
--rpc-tserver      RPC port of first Kudu Tablet Server; HTTP port is the next
                   number. Subsequent Tablet Servers will have following numbers
--time_source      Time source for Kudu Masters and Tablet Servers
                   (default: system_unsync)
-b, --builddir     Path to the Kudu build directory
EOF
}

NUM_MASTERS=1
NUM_TSERVERS=3
MASTER_RPC_PORT_BASE=8764
TSERVER_RPC_PORT_BASE=9870
TIME_SOURCE=system_unsync
BUILDDIR="$PWD"
echo $(readlink -f $(dirname $0))
while (( "$#" )); do
  case "$1" in
    -h|--help)
      usage
      exit 1
      ;;
    -m|--num-masters)
      NUM_MASTERS=$2
      shift 2
      ;;
    -t|--num-tservers)
      NUM_TSERVERS=$2
      shift 2
      ;;
    --rpc-master)
      MASTER_RPC_PORT_BASE=$2
      shift 2
      ;;
    --rpc-tserver)
      TSERVER_RPC_PORT_BASE=$2
      shift 2
      ;;
    --time_source)
      TIME_SOURCE=$2
      shift 2
      ;;
    -b|--builddir)
      BUILDDIR="$2"
      shift 2
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      usage
      exit 1
      ;;
    *) # positional arguments
      echo "Error: Unsupported argument $1" >&2
      usage
      exit 1
      ;;
  esac
done

WEBSERVER_DOC_ROOT="$BUILDDIR/../../www"
KUDUMASTER="$BUILDDIR/bin/kudu-master"
KUDUTSERVER="$BUILDDIR/bin/kudu-tserver"
echo $KUDUMASTER
echo $KUDUTSERVER
IP=127.0.0.1

[ ! -d "$WEBSERVER_DOC_ROOT" ] && { echo "Cannot find webroot directory $WEBSERVER_DOC_ROOT"; exit 1; }
[ ! -x "$KUDUMASTER" ] && { echo "Cannot find $KUDUMASTER executable";  exit 1; }
[ ! -x "$KUDUTSERVER" ] && { echo "Cannot find $KUDUTSERVER executable";  exit 1; }


# Common steps before starting masters or tablet servers

# 1) Create "data", "wal" and "log" directories for a server before start

function create_dirs_and_set_vars() {
  root_dir="$BUILDDIR/$1"
  dir_data="$root_dir/data"
  dir_wal="$root_dir/wal"
  dir_log="$root_dir/log"

  mkdir -p "$dir_data" "$dir_wal" "$dir_log"
}

# 2) Print information about interface ports

function set_port_vars_and_print() {
  RPC_PORT=$2
  HTTP_PORT=$3
  echo "Starting $1:"
  echo "  RPC  port $RPC_PORT"
  echo "  HTTP port $HTTP_PORT"
}

pids=()

# Start master server function

function start_master() {
  create_dirs_and_set_vars $1
  set_port_vars_and_print $1 $2 $3
  ARGS="$KUDUMASTER"
  if [ $NUM_MASTERS -gt 1 ]; then
    ARGS="$ARGS --master_addresses=$MASTER_ADDRESSES"
  fi
  ARGS="$ARGS --fs_data_dirs=$dir_data"
  ARGS="$ARGS --fs_wal_dir=$dir_wal"
  ARGS="$ARGS --log_dir=$dir_log"
  ARGS="$ARGS --rpc_bind_addresses=$IP:$RPC_PORT"
  ARGS="$ARGS --time_source=$TIME_SOURCE"
  ARGS="$ARGS --webserver_port=$HTTP_PORT"
  ARGS="$ARGS --webserver_interface=$IP"
  ARGS="$ARGS --webserver_doc_root=$WEBSERVER_DOC_ROOT"
  $ARGS &
  pids+=($!)
}

# Start tablet server function

function start_tserver() {
  create_dirs_and_set_vars $1
  set_port_vars_and_print $1 $2 $3
  ARGS="$KUDUTSERVER"
  ARGS="$ARGS --fs_data_dirs=$dir_data"
  ARGS="$ARGS --fs_wal_dir=$dir_wal"
  ARGS="$ARGS --log_dir=$dir_log"
  ARGS="$ARGS --rpc_bind_addresses=$IP:$RPC_PORT"
  ARGS="$ARGS --time_source=$TIME_SOURCE"
  ARGS="$ARGS --webserver_port=$HTTP_PORT"
  ARGS="$ARGS --webserver_interface=$IP"
  ARGS="$ARGS --webserver_doc_root=$WEBSERVER_DOC_ROOT"
  ARGS="$ARGS --tserver_master_addrs=$4"
  $ARGS &
  pids+=($!)
}

# Precompute the comma-separated list of master addresses.
MASTER_ADDRESSES=
for i in $(seq 0 $((NUM_MASTERS - 1))); do
  MASTER_RPC_PORT=$((MASTER_RPC_PORT_BASE + $i * 2))
  ADDR=$IP:$MASTER_RPC_PORT
  if [ $i -ne 0 ]; then
    MASTER_ADDRESSES="${MASTER_ADDRESSES},"
  fi
  MASTER_ADDRESSES="${MASTER_ADDRESSES}${ADDR}"
done

# Start masters
for i in $(seq 0 $((NUM_MASTERS - 1))); do
  MASTER_RPC_PORT=$((MASTER_RPC_PORT_BASE + $i * 2))
  MASTER_HTTP_PORT=$((MASTER_RPC_PORT + 1))
  start_master master-$i $MASTER_RPC_PORT $MASTER_HTTP_PORT
done

# Start tservers
for i in $(seq 0 $((NUM_TSERVERS - 1))); do
  TSERVER_RPC_PORT=$((TSERVER_RPC_PORT_BASE + $i * 2))
  TSERVER_HTTP_PORT=$((TSERVER_RPC_PORT + 1))
  start_tserver tserver-$i $TSERVER_RPC_PORT $TSERVER_HTTP_PORT $MASTER_ADDRESSES
done

# Show status of started processes

ps -wwo args -p ${pids[@]}
