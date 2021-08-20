#!/bin/bash
################################################################################
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
################################################################################
#
# This script follows the pattern described in the docker best practices here:
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#entrypoint
################################################################################

set -e
set -o pipefail

function print_help {
  echo "Supported commands:"
  echo "   master     - Start a Kudu Master"
  echo "   tserver    - Start a Kudu TServer"
  echo "   kudu       - Run the Kudu CLI"
  echo "   help       - print useful information and exit"
  echo ""
  echo "Other commands can be specified to run shell commands."
  echo ""
  echo "Environment variables:"
  echo "KUDU_MASTERS:"
  echo "  Defines the kudu-master and kudu-tserver configured master addresses."
  echo "  Defaults to localhost."
  echo "DATA_DIR:"
  echo "  Defines the root directory to use. Subdirectories are added depending on whether a "
  echo "  Kudu master or a Kudu tablet server is being deployed. Ignored if the FS_WAL_DIR "
  echo "  environment variable is set."
  echo "  NOTE: this variable is deprecated. FS_WAL_DIR should be used instead."
  echo "  Defaults to /var/lib/kudu."
  echo "FS_WAL_DIR:"
  echo "  Defines the WAL directory to use. Takes precedence over the DATA_DIR environment "
  echo "  variable."
  echo "FS_DATA_DIRS:"
  echo "  Defines the data directories to use. If set, the FS_WAL_DIR environment variable "
  echo "  must also be set."
  echo "  Defaults to the value of the FS_WAL_DIR environment variable."
  echo "MASTER_ARGS:"
  echo "  Defines custom arguments passed to kudu-master."
  echo "  Defaults to an empty set."
  echo "  kudu-master is run with the set of arguments built from"
  echo "  DEFAULT_ARGS appended by MASTER_ARGS, so Kudu flags in DEFAULT_ARGS"
  echo "  can be overridden by corresponding flags in MASTER_ARGS."
  echo "TSERVER_ARGS:"
  echo "  Defines custom arguments passed to kudu-tserver."
  echo "  Defaults to an empty set."
  echo "  kudu-tserver is run with the set of arguments built from"
  echo "  DEFAULT_ARGS appended by TSERVER_ARGS, so Kudu flags in DEFAULT_ARGS"
  echo "  can be overridden by corresponding flags in TSERVER_ARGS."
  echo "DEFAULT_ARGS:"
  echo "  Defines a recommended base set of arguments."
}

if [[ -z "$FS_WAL_DIR" && -n "$FS_DATA_DIRS" ]]; then
  echo "If FS_DATA_DIRS is set, FS_WAL_DIR must also be set"
  echo "FS_WAL_DIR: $FS_WAL_DIR"
  echo "FS_DATA_DIRS: $FS_DATA_DIRS"
  exit 1
fi

DATA_DIR=${DATA_DIR:="/var/lib/kudu"}
if [[ -n "$FS_WAL_DIR" ]]; then
  # Use the WAL directory for data if a data directory is not specified.
  WAL_DIR="$FS_WAL_DIR"
  DATA_DIRS=${FS_DATA_DIRS:="$FS_WAL_DIR"}
else
  # If no WAL directory is specified, use a subdirectory in the root directory.
  WAL_DIR="$DATA_DIR/$1"
  DATA_DIRS="$DATA_DIR/$1"
fi

# Define the defaults environment variables.
KUDU_MASTERS=${KUDU_MASTERS:=""}
 # TODO: Remove use_hybrid_clock=false when ntpd is setup.
DEFAULT_ARGS="--fs_wal_dir=$WAL_DIR \
 --fs_data_dirs=$DATA_DIRS \
 --webserver_doc_root=/opt/kudu/www \
 --stderrthreshold=0 \
 --use_hybrid_clock=false"
MASTER_ARGS=${MASTER_ARGS:=""}
TSERVER_ARGS=${TSERVER_ARGS:=""}

# Wait until the master hosts can be resolved.
#
# Without this Kudu will fail with "Name or service not known" errors
# on startup.
#
# Gives a maximum of 5 attempts/seconds to each host. On failure
# falls through without failing to still give Kudu a chance to startup
# or fail on it's own.
function wait_for_master_hosts() {
  IFS=","
  for HOST in $KUDU_MASTERS
  do
    MAX_ATTEMPTS=5
    ATTEMPTS=0
    until `ping -c1 "$HOST" &>/dev/null;` || [[ "$ATTEMPTS" -eq "$MAX_ATTEMPTS" ]]; do
      ATTEMPTS=$((ATTEMPTS + 1))
      sleep 2;
    done
  done
  unset IFS
}

function make_directories() {
  IFS=","
  mkdir -p $WAL_DIR
  for DIR in $DATA_DIRS
  do
    mkdir -p $DIR
  done
  unset IFS
}

# If no arguments are passed, print the help.
if [[ -z "$1" ]]; then
  print_help
  exit 1
fi

# Note: we use "master" and "tserver" here so the kudu-master and kudu-tserver
# binaries can be manually invoked if needed.
if [[ "$1" == "master" ]]; then
  make_directories
  wait_for_master_hosts
  # Supply --master_addresses even if a single master address is specified.
  if [[ -n "$KUDU_MASTERS" ]]; then
    MASTER_ARGS="--master_addresses=$KUDU_MASTERS $MASTER_ARGS"
  fi
  exec kudu master run ${DEFAULT_ARGS} ${MASTER_ARGS}
elif [[ "$1" == "tserver" ]]; then
  make_directories
  wait_for_master_hosts
  if [[ -n "$KUDU_MASTERS" ]]; then
    TSERVER_ARGS="--tserver_master_addrs=$KUDU_MASTERS $TSERVER_ARGS"
  else
    TSERVER_ARGS="--tserver_master_addrs=localhost $TSERVER_ARGS"
  fi
  exec kudu tserver run ${DEFAULT_ARGS} ${TSERVER_ARGS}
elif [[ "$1" == "help" ]]; then
  print_help
  exit 0
fi

# Support calling anything else in the container.
exec "$@"
