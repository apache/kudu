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

# ready.sh — wait for both Kudu clusters and print a readiness summary.
#
# Waits for masters AND tablet servers to be up before printing next steps.
# The demo table itself is created by the ingest simulator on first start.
#
# Usage:
#   bash scripts/ready.sh   (from examples/flink-replication/ directory)
#   Called automatically by: make up

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

SRC_MASTER="src-kudu-master-1:7051"
SINK_MASTER="snk-kudu-master-1:7051"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       Kudu Replication Demo — Environment Setup              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# 1. Wait for masters and tservers
bash "${SCRIPT_DIR}/wait-for-kudu.sh"

# 2. Print cluster status
echo ""
echo "Source cluster (${SRC_MASTER}):"
docker compose exec src-kudu-master-1 kudu master status "${SRC_MASTER}" 2>&1

echo ""
echo "Sink cluster (${SINK_MASTER}):"
docker compose exec snk-kudu-master-1 kudu master status "${SINK_MASTER}" 2>&1

# 3. Next-step guidance
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Infrastructure is ready!  Next steps:                       ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  1. Start the ingest simulator (creates demo_table):         ║"
echo "║       make start-ingest        # INSERT / UPDATE / DELETE    ║"
echo "║                                                              ║"
echo "║  2. Submit the Flink replication job:                        ║"
echo "║       make submit-job                                        ║"
echo "║                                                              ║"
echo "║  3. Verify data is flowing:                                  ║"
echo "║       make verify                                            ║"
echo "║                                                              ║"
echo "║  Web UIs:  make ui                                           ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
