#!/usr/bin/env bash
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

# submit-job.sh — submit the Kudu replication Flink job to the demo cluster.
#
# Usage:
#   bash flink/submit-job.sh              (from examples/flink-replication/ directory)
#   make submit-job                        (from examples/flink-replication/ directory)
#
# Environment overrides (all have defaults from .env):
#   SRC_MASTER              source Kudu master address  (default: src-kudu-master-1:7051)
#   SINK_MASTER             sink Kudu master address    (default: snk-kudu-master-1:7051)
#   DEMO_TABLE              table name                  (default: demo_table)
#   DISCOVERY_INTERVAL_SECONDS   KuduSource diff-scan period  (default: 10)
#   CHECKPOINT_INTERVAL_MS       Flink checkpoint interval     (default: 5000)
#
# The script locates the shadow JAR that was installed by 'make build-jar' into
# the flink/jars/ directory (mounted at /opt/flink/usrlib inside the containers).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."   # ensure we are in examples/flink-replication/

# Load .env defaults (shell env vars take precedence)
if [[ -f ".env" ]]; then
  # Export only lines that look like KEY=VALUE (skip comments and blanks).
  set -o allexport
  # shellcheck source=.env
  source <(grep -E '^[A-Z_]+=.*' .env)
  set +o allexport
fi

# Configuration

SRC_MASTER="${SRC_MASTER:-src-kudu-master-1:7051}"
SINK_MASTER="${SINK_MASTER:-snk-kudu-master-1:7051}"
TABLE="${DEMO_TABLE:-demo_table}"
CHECKPOINTS_DIR="file:///checkpoints"
SAVEPOINTS_DIR="/checkpoints/savepoints"
DISCOVERY_INTERVAL="${DISCOVERY_INTERVAL_SECONDS:-10}"
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL_MS:-5000}"

JAR_GLOB="/opt/flink/usrlib/kudu-replication-*.jar"

# Pre-flight checks

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     Submitting Kudu Replication Flink Job                    ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Source master  : ${SRC_MASTER}"
echo "  Sink master    : ${SINK_MASTER}"
echo "  Table          : ${TABLE}"
echo "  Discovery      : ${DISCOVERY_INTERVAL}s"
echo "  Checkpointing  : ${CHECKPOINT_INTERVAL}ms"
echo "  Checkpoints    : ${CHECKPOINTS_DIR}"
echo "  Savepoints     : ${SAVEPOINTS_DIR}"
echo "  Parallelism    : ${FLINK_PARALLELISM:-default (parallelism.default from flink-conf)}"
echo ""

# Verify the Flink JobManager is reachable.
if ! docker compose exec -T flink-jobmanager \
    bash -c 'echo > /dev/tcp/localhost/8081' 2>/dev/null; then
  echo "ERROR: flink-jobmanager is not reachable on port 8081."
  echo "       Run 'make up' first, then wait for the service to start."
  exit 1
fi

# Locate the shadow JAR inside the flink-jobmanager container.
JAR_PATH=$(docker compose exec -T flink-jobmanager \
  bash -c "ls ${JAR_GLOB} 2>/dev/null | head -1" || true)

if [[ -z "${JAR_PATH}" ]]; then
  echo "ERROR: No kudu-replication JAR found at '${JAR_GLOB}' in flink-jobmanager."
  echo ""
  echo "  Run the following from the examples/flink-replication/ directory:"
  echo "    make build-jar"
  echo ""
  echo "  This builds the shadow JAR and copies it to examples/flink-replication/flink/jars/,"
  echo "  which is mounted into the Flink containers at /opt/flink/usrlib/."
  exit 1
fi

echo "  JAR : ${JAR_PATH}"
echo ""

# Verify the source table exists before submitting

echo "  Checking source table '${TABLE}' on ${SRC_MASTER} ..."
if ! docker compose exec -T src-kudu-master-1 \
    kudu table list "${SRC_MASTER}" 2>/dev/null | grep -qF "${TABLE}"; then
  echo ""
  echo "ERROR: Table '${TABLE}' not found on the source cluster."
  echo "       Start the ingest simulator first so it can create the table:"
  echo "         make start-ingest"
  exit 1
fi

# Detect latest savepoint (written by make stop-job)

SAVEPOINT_PATH=$(docker compose exec -T flink-jobmanager \
  bash -c "ls -td ${SAVEPOINTS_DIR}/savepoint-* 2>/dev/null | head -1" 2>/dev/null | tr -d '\r' || true)

SAVEPOINT_FLAG=""
if [[ -n "${SAVEPOINT_PATH}" ]]; then
  echo "  Savepoint found — resuming from:"
  echo "    ${SAVEPOINT_PATH}"
  SAVEPOINT_FLAG="-s ${SAVEPOINT_PATH}"
else
  echo "  No savepoint found — starting fresh."
fi
echo ""

# Submit the job

echo "  Submitting ..."
echo ""

# ParameterTool requires space-separated args (--key value).
PARALLELISM_FLAG=""
if [[ -n "${FLINK_PARALLELISM:-}" ]]; then
  PARALLELISM_FLAG="-p ${FLINK_PARALLELISM}"
fi

docker compose exec -T flink-jobmanager \
  bash -c "flink run -d ${PARALLELISM_FLAG} ${SAVEPOINT_FLAG} \
    --class org.apache.kudu.replication.ReplicationJob \
    '${JAR_PATH}' \
    --job.sourceMasterAddresses '${SRC_MASTER}' \
    --job.sinkMasterAddresses '${SINK_MASTER}' \
    --job.tableName '${TABLE}' \
    --job.checkpointsDirectory '${CHECKPOINTS_DIR}' \
    --job.createTable true \
    --job.discoveryIntervalSeconds '${DISCOVERY_INTERVAL}' \
    --job.checkpointingIntervalMillis '${CHECKPOINT_INTERVAL}' \
    2>&1"

echo ""
echo "Job submitted successfully."
echo ""
echo "  Monitor  : http://localhost:8081"
echo "  Verify   : make verify"
echo ""
if [[ -n "${SAVEPOINT_FLAG}" ]]; then
  echo "Note: Resumed from savepoint — state (offsets, split assignments) restored."
else
  echo "Note: The first diff scan runs after ${DISCOVERY_INTERVAL}s."
  echo "      Sink row count will begin catching up shortly after."
fi
