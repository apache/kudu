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

# wait-for-kudu.sh — poll both Kudu master services until they are healthy.
#
# Usage (called from examples/flink-replication/ directory, or via 'make ready'):
#   bash scripts/wait-for-kudu.sh
#
# Environment overrides:
#   MAX_WAIT   - total seconds to wait per master (default: 120)
#   POLL_DELAY - seconds between retries (default: 5)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  # Always operate from the examples/flink-replication/ directory so docker compose finds the right project.
cd "${SCRIPT_DIR}/.."

MAX_WAIT="${MAX_WAIT:-120}"
POLL_DELAY="${POLL_DELAY:-5}"

# wait_for_master SERVICE MASTER_ADDR
#   Polls 'kudu master status <MASTER_ADDR>' inside the named Docker Compose
#   service until it succeeds or MAX_WAIT is exceeded.
wait_for_master() {
  local service="$1"
  local master_addr="$2"
  local elapsed=0

  printf "  %-45s" "master  ${master_addr} ..."

  while ! docker compose exec -T "${service}" \
      kudu master status "${master_addr}" >/dev/null 2>&1; do

    if [[ ${elapsed} -ge ${MAX_WAIT} ]]; then
      echo " TIMED OUT after ${MAX_WAIT}s"
      echo ""
      echo "  Hint: check container logs with:"
      echo "    docker compose logs ${service}"
      return 1
    fi

    sleep "${POLL_DELAY}"
    elapsed=$(( elapsed + POLL_DELAY ))
    printf "."
  done

  echo " up (${elapsed}s)"
}

# wait_for_tservers SERVICE MASTER_ADDR EXPECTED_COUNT
#   Polls 'kudu tserver list <MASTER_ADDR>' until at least EXPECTED_COUNT
#   tablet servers appear in the output.
wait_for_tservers() {
  local service="$1"
  local master_addr="$2"
  local expected="$3"
  local elapsed=0

  printf "  %-45s" "tservers ${master_addr} (need ${expected}) ..."

  while true; do
    local count
    # Skip the header and separator lines; count remaining non-empty lines.
    # 'grep -c' already prints 0 when there are no matches but exits non-zero,
    # so use '|| true' to suppress the exit code without adding extra output.
    count=$(docker compose exec -T "${service}" \
      kudu tserver list "${master_addr}" 2>/dev/null \
      | tail -n +3 | grep -c . || true)

    if [[ "${count}" -ge "${expected}" ]]; then
      echo " up (${elapsed}s) — ${count}/${expected} tservers"
      return 0
    fi

    if [[ ${elapsed} -ge ${MAX_WAIT} ]]; then
      echo " TIMED OUT after ${MAX_WAIT}s (${count}/${expected} tservers registered)"
      echo ""
      echo "  Hint: check container logs with:"
      echo "    docker compose logs ${service}"
      return 1
    fi

    sleep "${POLL_DELAY}"
    elapsed=$(( elapsed + POLL_DELAY ))
    printf "."
  done
}

echo ""
echo "Polling Kudu clusters (timeout: ${MAX_WAIT}s each) ..."
echo ""
echo "  Source cluster:"
wait_for_master  "src-kudu-master-1" "src-kudu-master-1:7051"
wait_for_tservers "src-kudu-master-1" "src-kudu-master-1:7051" 2
echo ""
echo "  Sink cluster:"
wait_for_master  "snk-kudu-master-1" "snk-kudu-master-1:7051"
wait_for_tservers "snk-kudu-master-1" "snk-kudu-master-1:7051" 2

echo ""
echo "Both Kudu clusters are healthy (masters + tservers)."
