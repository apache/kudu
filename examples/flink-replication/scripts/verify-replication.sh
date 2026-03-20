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

# verify-replication.sh — compare source and sink row counts.
#
# Uses 'kudu table statistics' to read the approximate live-row count from each
# cluster without performing a full scan.
#
# Usage:
#   bash scripts/verify-replication.sh    (from examples/flink-replication/ directory)
#   make verify                            (from examples/flink-replication/ directory)
#
# Environment overrides:
#   DEMO_TABLE    table name to check (default: demo_table)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

SRC_MASTER="src-kudu-master-1:7051"
SINK_MASTER="snk-kudu-master-1:7051"
TABLE="${DEMO_TABLE:-demo_table}"

# Helper: extract live row count from 'kudu table statistics'
# Returns the numeric value, or "N/A" if unavailable.
get_row_count() {
  local service="$1"
  local master="$2"
  local table="$3"

  local output
  output=$(docker compose exec -T "${service}" \
    kudu table statistics "${master}" "${table}" 2>&1) || true

  # CLI prints "live row count: N"
  echo "${output}" | grep -i "live row count" | grep -o '[0-9]*' | tail -1 || echo "N/A"
}

# Table existence check
table_exists() {
  local service="$1"
  local master="$2"
  local table="$3"
  docker compose exec -T "${service}" \
    kudu table list "${master}" 2>/dev/null | grep -qF "${table}"
}

# Main

PADDED=$(printf "%-26s" "${TABLE}")
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║         Replication Verification — ${PADDED}║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Source
echo "Source (${SRC_MASTER}):"
if table_exists "src-kudu-master-1" "${SRC_MASTER}" "${TABLE}"; then
  SRC_COUNT=$(get_row_count "src-kudu-master-1" "${SRC_MASTER}" "${TABLE}")
  echo "  table      : ${TABLE}"
  echo "  rows (approx): ${SRC_COUNT}"
else
  echo "  table '${TABLE}' not found — run: make start-ingest"
fi

echo ""

# Sink
echo "Sink (${SINK_MASTER}):"
if table_exists "snk-kudu-master-1" "${SINK_MASTER}" "${TABLE}"; then
  SINK_COUNT=$(get_row_count "snk-kudu-master-1" "${SINK_MASTER}" "${TABLE}")
  echo "  table      : ${TABLE}"
  echo "  rows (approx): ${SINK_COUNT}"
else
  echo "  table '${TABLE}' not found — run: make submit-job"
fi

echo ""
echo "  Flink dashboard : http://localhost:8081"
echo "  Source Kudu UI  : http://localhost:8051"
echo "  Sink Kudu UI    : http://localhost:18051"
echo ""
