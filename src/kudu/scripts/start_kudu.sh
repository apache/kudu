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
-h, --help       help
-m, --master     RPC port of kudu master server (HTTP port is next number)
-t, --tserver    RPC port of first kudu tablet server (other servers
                 will have following numbers)
-b, --builddir   path to the kudu build directory
EOF
}

MASTER_RPC_PORT=8764
TSERVER_RPC_PORT_BASE=9870
BUILDDIR="$PWD"
echo $(readlink -f $(dirname $0))
while (( "$#" )); do
  case "$1" in
    -h|--help)
      usage
      exit 1
      ;;
    -m|--master)
      MASTER_RPC_PORT=$2
      shift 2
      ;;
    -t|--tserver)
      TSERVER_RPC_PORT_BASE=$2
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


# Common steps before starting master or tablet server

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
  create_dirs_and_set_vars master
  set_port_vars_and_print master $1 $2
  "$KUDUMASTER" \
    --fs_data_dirs="$dir_data" \
    --fs_wal_dir="$dir_wal" \
    --log_dir="$dir_log" \
    --rpc_bind_addresses=$IP:$RPC_PORT \
    --webserver_port=$HTTP_PORT \
    --webserver_interface=$IP \
    --webserver_doc_root="$WEBSERVER_DOC_ROOT" &
  pids+=($!)
}

# Start tablet server function

function start_tserver() {
  create_dirs_and_set_vars $1
  set_port_vars_and_print $1 $2 $3
  MASTER_RPC_PORT=$4
  "$KUDUTSERVER" \
    --fs_data_dirs="$dir_data" \
    --fs_wal_dir="$dir_wal" \
    --log_dir="$dir_log" \
    --rpc_bind_addresses=$IP:$RPC_PORT \
    --webserver_port=$HTTP_PORT \
    --webserver_interface=$IP \
    --webserver_doc_root="$WEBSERVER_DOC_ROOT" \
    --tserver_master_addrs=$IP:$MASTER_RPC_PORT &
  pids+=($!)
}


# Start master server
MASTER_HTTP_PORT=$(($MASTER_RPC_PORT + 1))
start_master $MASTER_RPC_PORT $MASTER_HTTP_PORT

# Start tablet servers
for i in 0 1 2
do
  TSERVER_RPC_PORT=$((TSERVER_RPC_PORT_BASE + $i * 2))
  TSERVER_HTTP_PORT=$((TSERVER_RPC_PORT_BASE + $i * 2 + 1))
  start_tserver tserver-$i $TSERVER_RPC_PORT $TSERVER_HTTP_PORT $MASTER_RPC_PORT
done

# Show status of started processes

ps -wwo args -p ${pids[@]}
