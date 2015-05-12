#!/bin/bash
##
#
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

set -x

# Time marker for both stderr and stdout
date 1>&2

export KUDU_HOME=${KUDU_HOME:-/usr/lib/kudu}

CMD=$1
LOG_DIR=$2
WAL_DIR=$3
DATA_DIRS=$4
MASTER_FILE=$CONF_DIR/$5
DEFAULT_NUM_REPLICAS=$6
shift 2

echo "KUDU_HOME: $KUDU_HOME"
echo "CONF_DIR: $CONF_DIR"
echo "CMD: $CMD"
echo "LOG_DIR: $LOG_DIR"
echo "WAL_DIR: $WAL_DIR"
echo "DATA_DIRS: $DATA_DIRS"
echo "MASTER_FILE: $MASTER_FILE"
echo "DEFAULT_NUM_REPLICAS: $DEFAULT_NUM_REPLICAS"

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Reads a line in the format "$host:$key=$value", setting those variables.
function readconf {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

if [ -f "$MASTER_FILE" ]; then
  MASTER_IPS=
  for line in $(cat "$MASTER_FILE")
  do
    readconf "$line"
    case $key in
      server.address)
        # Fall back to the host only if there's no defined value.
        if [ -n "$value" ]; then
          actual_value="$value"
        else
          actual_value="$host"
        fi

        # Append to comma-separated MASTER_IPS.
        if [ -n "$MASTER_IPS" ]; then
          MASTER_IPS="${MASTER_IPS},"
        fi
        MASTER_IPS="${MASTER_IPS}${actual_value}"
        ;;
    esac
  done
  log "Found master(s) on $MASTER_IPS"
fi

if [ "$CMD" = "master" ]; then
  # Only pass --master_quorum if there's more than one master.
  #
  # Need to use [[ ]] for regex support.
  if [[ "$MASTER_IPS" =~ , ]]; then
    MASTER_QUORUM="--master_quorum=$MASTER_IPS"
  fi

  exec "$KUDU_HOME/sbin/kudu-master" \
    --log_dir="$LOG_DIR" \
    $MASTER_QUORUM \
    --default_num_replicas=$DEFAULT_NUM_REPLICAS \
    --master_wal_dir="$WAL_DIR" \
    --master_data_dirs="$DATA_DIRS" \
    --flagfile="$CONF_DIR"/kudu-master.gflags
elif [ "$CMD" = "tserver" ]; then
  exec "$KUDU_HOME/sbin/kudu-tablet_server" \
    --log_dir="$LOG_DIR" \
    --tablet_server_master_addrs="$MASTER_IPS" \
    --tablet_server_wal_dir="$WAL_DIR" \
    --tablet_server_data_dirs="$DATA_DIRS" \
    --flagfile="$CONF_DIR"/kudu-ts.gflags
else
  log "Unknown command: $CMD"
  exit 2
fi
