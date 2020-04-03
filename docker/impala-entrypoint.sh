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
  echo "   impala        - Start a single node, Kudu only, Impala cluster"
  echo "   impala-shell  - Start the Impala shell CLI"
  echo "   help          - print useful information and exit"
  echo ""
  echo "Other commands can be specified to run shell commands."
  echo ""
  echo "Environment variables:"
  echo "KUDU_MASTERS:"
  echo "  Defines the kudu-master configured master addresses."
  echo "  Defaults to kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251"
}

# Define the default environment variables.
KUDU_MASTERS=${KUDU_MASTERS:="kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251"}

DATA_DIR="/var/lib/impala"
LOG_DIR="$DATA_DIR/logs"

export JAVA_HOME=$(which javac | xargs readlink -f | sed "s:/bin/javac::")

function start_hive_metastore() {
  # If the derby files do not existm initialize the schema.
  if [ ! -d "${DATA_DIR}/metastore/metastore_db" ]; then
    schematool -dbType derby -initSchema
  fi
  # Starth the Hive Metastore.
  hive --service metastore &
}

function start_statestored() {
  daemon_entrypoint.sh statestored -log_dir=${DATA_DIR}/logs &
}

function start_catalogd() {
  daemon_entrypoint.sh catalogd -log_dir=${DATA_DIR}/logs \
  -abort_on_config_error=false -catalog_topic_mode=minimal \
  -hms_event_polling_interval_s=0 -invalidate_tables_on_memory_pressure=true &
}

function start_impalad() {
  daemon_entrypoint.sh impalad -log_dir=${DATA_DIR}/logs \
  -abort_on_config_error=false -mem_limit_includes_jvm=true \
  -use_local_catalog=true -rpc_use_loopback=true \
  -kudu_master_hosts=${KUDU_MASTERS} &
}

# If no arguments are passed, print the help.
if [[ -z "$1" ]]; then
  print_help
  exit 1
fi

# Note: we use "master" and "tserver" here so the kudu-master and kudu-tserver
# binaries can be manually invoked if needed.
if [[ "$1" == "impala" ]]; then
  mkdir -p $DATA_DIR
  mkdir -p $LOG_DIR
  start_hive_metastore
  start_statestored
  start_catalogd
  start_impalad
  tail -F ${LOG_DIR}/impalad.INFO
elif [[ "$1" == "help" ]]; then
  print_help
  exit 0
fi

# Support calling anything else in the container.
exec "$@"

}