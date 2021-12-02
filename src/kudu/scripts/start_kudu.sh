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
set -o pipefail
ulimit -n 2048

function usage() {
cat << EOF
Usage:
start_kudu.sh [flags]
-h, --help          Print help
-m, --num-masters   Number of Kudu Masters to start (default: 1)
-t, --num-tservers  Number of Kudu Tablet Servers to start (default: 3)
--rpc-master        RPC port of first Kudu Master; HTTP port is the next number.
                    Subsequent Masters will have following numbers
--rpc-tserver       RPC port of first Kudu Tablet Server; HTTP port is the next
                    number. Subsequent Tablet Servers will have following numbers
--time_source       Time source for Kudu Masters and Tablet Servers
                    (default: system_unsync)
-b, --builddir      Path to the Kudu build directory
-c  --clusterdir    Path to place the Kudu masters and tablet servers.
-T, --tserver-flags Extra flags to be used on the tablet servers. Multiple
                    flags can be specified if wrapped in ""s.
-M, --master-flags  Extra flags to be used on the master servers. Multiple
                    flags can be specified if wrapped in ""s.
EOF
}

NUM_MASTERS=1
NUM_TSERVERS=3
MASTER_RPC_PORT_BASE=8764
TSERVER_RPC_PORT_BASE=9870
TIME_SOURCE=system_unsync
BUILDDIR=""
CLUSTER_DIR="$PWD"
EXTRA_TSERVER_FLAGS=""
EXTRA_MASTER_FLAGS=""

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
    -c|--clusterdir)
      CLUSTER_DIR=$2
      shift 2
      ;;
    -T|--tserver-flags)
      EXTRA_TSERVER_FLAGS=$2
      shift 2
      ;;
    -M|--master-flags)
      EXTRA_MASTER_FLAGS=$2
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

if [ -z "$BUILDDIR" ]; then
  echo -n "Assuming that the script was started from the build directory. "
  echo "You can override this with -b|--builddir option."
  BUILDDIR="$PWD"
fi

# If $KUDU_HOME is not set or $KUDU_HOME/www doesn't exists, let's default to $BUILDDIR/../../www
# In case neither is available we'll issue a warning and won't set the --webserver_doc_root flag
# for kudu-master and kudu-tserver
if [ -z "$KUDU_HOME" ] || [ ! -d "$KUDU_HOME/www" ]; then
  WEBSERVER_DOC_ROOT="$BUILDDIR/../../www"
fi

if [ -n "$WEBSERVER_DOC_ROOT" ] && [ ! -d "$WEBSERVER_DOC_ROOT" ]; then
  echo  -n "Cannot find webroot directory $WEBSERVER_DOC_ROOT at "
  echo "\$KUDU_HOME/www or \$BUILDDIR/../../www"
fi

KUDUMASTER="$BUILDDIR/bin/kudu-master"
KUDUTSERVER="$BUILDDIR/bin/kudu-tserver"
echo $KUDUMASTER
echo $KUDUTSERVER
IP=127.0.0.1

[ ! -x "$KUDUMASTER" ] && { echo "Cannot find $KUDUMASTER executable";  exit 1; }
[ ! -x "$KUDUTSERVER" ] && { echo "Cannot find $KUDUTSERVER executable";  exit 1; }


# Common steps before starting masters or tablet servers

# 1) Create "data", "wal" and "log" directories for a server before start

function create_dirs_and_set_vars() {
  root_dir="$CLUSTER_DIR/$1"
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

# Return a flag to set the hard memory limit for the Kudu server processes
# running at the same node. Each of the processes is able to set the hard
# memory limit based on the total amount of memory available, but such a
# provision assumes there is a single Kudu server process running at a node.
# Since there is going to be NUM_TSERVERS kudu-tserver and NUM_MASTERS
# kudu-master processes running, it's necessary to divide the available memory
# among them.
function get_memory_limit_hard_bytes_flag() {
  local num_processes=$1
  local mem_size_bytes=0
  if [[ "$OSTYPE" =~ ^linux ]]; then
    local mem_size_kb=$(grep -E '^MemTotal' /proc/meminfo | awk '{print $2}')
    mem_size_bytes=$((mem_size_kb * 1024))
  elif [[ "$OSTYPE" =~ ^darwin ]]; then
    mem_size_bytes=$(sysctl hw.memsize | awk '{print $2}')
  fi

  # Do not set the limit for a non-recognized OS.
  if [ $mem_size_bytes -eq 0 ]; then
    echo ""
    return
  fi

  # Allocate 80% of all available memory to be used by all the Kudu processes.
  local mem_limit_bytes=$((mem_size_bytes * 4 / 5))
  mem_limit_bytes=$((mem_limit_bytes / num_processes))
  echo "--memory_limit_hard_bytes=$mem_limit_bytes"
}

pids=()

# Start kudu-master process.
function start_master() {
  create_dirs_and_set_vars $1
  set_port_vars_and_print $1 $2 $3
  ARGS="$KUDUMASTER"
  ARGS="$ARGS --master_addresses=$MASTER_ADDRESSES"
  ARGS="$ARGS --fs_data_dirs=$dir_data"
  ARGS="$ARGS --fs_wal_dir=$dir_wal"
  ARGS="$ARGS --log_dir=$dir_log"
  ARGS="$ARGS --rpc_bind_addresses=$IP:$RPC_PORT"
  ARGS="$ARGS --time_source=$TIME_SOURCE"
  ARGS="$ARGS --unlock_unsafe_flags"
  ARGS="$ARGS --webserver_port=$HTTP_PORT"
  ARGS="$ARGS --webserver_interface=$IP"
  if [ -d "$WEBSERVER_DOC_ROOT" ]; then
    ARGS="$ARGS --webserver_doc_root=$WEBSERVER_DOC_ROOT"
  fi
  # NOTE: a kudu-master process doesn't usually consume a lot of memory,
  #       so the memory hard limit isn't set for them; if kudu-master memory
  #       consumption becomes an issue, provide the necessary flags for
  #       kudu-master processing using the --master-flags/-M command line
  #       option
  ARGS="$ARGS $EXTRA_MASTER_FLAGS"
  $ARGS &
  pids+=($!)
}

# Start kudu-tserver process.
function start_tserver() {
  create_dirs_and_set_vars $1
  set_port_vars_and_print $1 $2 $3
  ARGS="$KUDUTSERVER"
  ARGS="$ARGS --fs_data_dirs=$dir_data"
  ARGS="$ARGS --fs_wal_dir=$dir_wal"
  ARGS="$ARGS --log_dir=$dir_log"
  ARGS="$ARGS --rpc_bind_addresses=$IP:$RPC_PORT"
  ARGS="$ARGS --time_source=$TIME_SOURCE"
  ARGS="$ARGS --unlock_unsafe_flags"
  ARGS="$ARGS --webserver_port=$HTTP_PORT"
  ARGS="$ARGS --webserver_interface=$IP"
  ARGS="$ARGS --tserver_master_addrs=$4"
  if [ -d "$WEBSERVER_DOC_ROOT" ]; then
    ARGS="$ARGS --webserver_doc_root=$WEBSERVER_DOC_ROOT"
  fi

  # If applicable, set the memory hard limit.
  local mem_limit_flag=$(get_memory_limit_hard_bytes_flag $NUM_TSERVERS)
  if [ -n $mem_limit_flag ]; then
    ARGS="$ARGS $mem_limit_flag"
  fi
  ARGS="$ARGS $EXTRA_TSERVER_FLAGS"
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

# Show the status of the started processes.
ps -wwo args -p ${pids[@]}
