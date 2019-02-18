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
  echo "  Defines the root data directory to use."
  echo "  Defaults to /var/lib/kudu."
  echo "MASTER_ARGS:"
  echo "  Defines the arguments passed to kudu-master."
  echo "  Defaults to the value of the DEFAULT_ARGS environment variable."
  echo "TSERVER_ARGS:"
  echo "  Defines the arguments passed to kudu-tserver."
  echo "  Defaults to the value of the DEFAULT_ARGS environment variable."
  echo "DEFAULT_ARGS:"
  echo "  Defines a recommended base set of arguments."
}

# Define the defaults environment variables.
KUDU_MASTERS=${KUDU_MASTERS:=""}
DATA_DIR=${DATA_DIR:="/var/lib/kudu"}
SERVICE_DIR="$DATA_DIR/$1"
 # TODO: Remove use_hybrid_clock=false when ntpd is setup.
DEFAULT_ARGS="--fs_wal_dir=$SERVICE_DIR \
 --webserver_doc_root=/opt/kudu/www \
 --stderrthreshold=0 \
 --use_hybrid_clock=false"
MASTER_ARGS=${MASTER_ARGS:="$DEFAULT_ARGS"}
TSERVER_ARGS=${TSERVER_ARGS:="$DEFAULT_ARGS"}

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
  for HOST in "$KUDU_MASTERS"
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

# If no arguments are passed, print the help.
if [[ -z "$1" ]]; then
  print_help
  exit 1
fi

# Note: we use "master" and "tserver" here so the kudu-master and kudu-tserver
# binaries can be manually invoked if needed.
if [[ "$1" == "master" ]]; then
  mkdir -p "$SERVICE_DIR"
  wait_for_master_hosts
  if [[ -n "$KUDU_MASTERS" ]]; then
    MASTER_ARGS="--master_addresses=$KUDU_MASTERS $MASTER_ARGS"
  fi
  exec kudu master run ${MASTER_ARGS}
elif [[ "$1" == "tserver" ]]; then
  mkdir -p "$SERVICE_DIR"
  wait_for_master_hosts
  if [[ -n "$KUDU_MASTERS" ]]; then
    TSERVER_ARGS="--tserver_master_addrs=$KUDU_MASTERS $TSERVER_ARGS"
  else
    TSERVER_ARGS="--tserver_master_addrs=localhost $TSERVER_ARGS"
  fi
  exec kudu tserver run ${TSERVER_ARGS}
elif [[ "$1" == "help" ]]; then
  print_help
  exit 0
fi

# Support calling anything else in the container.
exec "$@"
