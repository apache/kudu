#!/bin/bash
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
# For each requested Python version:
#   0. make clean         -- remove stale build artifacts (run once, up-front)
#   1. Copy python/ to an isolated temp dir (e.g. /tmp/kudu-python-311-XXXXXX)
#   2. Create a venv inside that temp dir
#   3. make requirements  -- install dev dependencies into the venv
#   4. make build         -- compile the Cython extension in-place
#   5. make test          -- run the test suite
#   6. Remove the temp dir
#
# Each version builds in its own temp copy of the source tree, so parallel
# invocations never race on shared generated files (config.pxi, version.py,
# *.cpp, build/).
#
# Usage:
#   ./test-client.sh               # test all supported versions
#   ./test-client.sh 3.12          # test a single version
#   ./test-client.sh 3.11 3.12     # test a subset
#   ./test-client.sh -j            # run all versions in parallel
#   ./test-client.sh -j4           # run up to 4 versions in parallel
#   ./test-client.sh -j 3.11 3.12  # parallel subset (unlimited)
#
# Environment:
#   KUDU_HOME   If unset, inferred from the script location (two levels up)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KUDU_HOME="${KUDU_HOME:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
PYTHON_DIR="${KUDU_HOME}/python"

cd "${PYTHON_DIR}"

source "${SCRIPT_DIR}/versions.sh"

MAX_JOBS=0  # 0 = sequential; >0 = parallel with that concurrency limit
VERSIONS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -j)   MAX_JOBS=$(( ${#SUPPORTED_VERSIONS[@]} )); shift ;;  # unlimited
    -j*)  MAX_JOBS="${1#-j}"; shift ;;                         # -j4 style
    *)    VERSIONS+=("$1"); shift ;;
  esac
done
if [[ ${#VERSIONS[@]} -eq 0 ]]; then
  VERSIONS=("${SUPPORTED_VERSIONS[@]}")
fi

log() { echo "[test-client] $*"; }
die() { echo "[test-client] ERROR: $*" >&2; exit 1; }

if command -v virtualenv >/dev/null 2>&1; then
  VIRTUALENV_CMD=(virtualenv)
elif python3 -m virtualenv --version >/dev/null 2>&1; then
  VIRTUALENV_CMD=(python3 -m virtualenv)
else
  die "virtualenv is required."
fi

run_one() {
  local ver="$1"
  local py_bin="python${ver}"
  # venv name: strip the dot, e.g. 2.7 -> venv_27, 3.11 -> venv_311
  local venv_name="venv_${ver/./}"

  if ! command -v "${py_bin}" >/dev/null 2>&1; then
    log "SKIP ${ver}: ${py_bin} not found on PATH"
    # return 2 distinct from 0 (OK) and 1 (FAIL) so callers can label it correctly
    return 2
  fi
  local py_path
  py_path="$(command -v "${py_bin}")"

  # Each version gets its own copy of the source tree so parallel builds
  # don't race on shared generated files (config.pxi, version.py, *.cpp, build/).
  local work_dir
  work_dir="$(mktemp -d /tmp/kudu-python-${ver}-XXXXXX)"
  cp -aL "${PYTHON_DIR}/." "${work_dir}/"
  cd "${work_dir}"

  log "=== Python ${ver} -> ${work_dir}/${venv_name} ==="

  log "Creating ${venv_name} with: ${VIRTUALENV_CMD[*]} -p ${py_path}"
  "${VIRTUALENV_CMD[@]}" -p "${py_path}" "${venv_name}"

  source "${venv_name}/bin/activate"

  local rc=0

  log "make requirements"
  make requirements || { log "FAIL ${ver}: make requirements"; rc=1; }

  if [[ "${rc}" -eq 0 ]]; then
    log "make build"
    make build || { log "FAIL ${ver}: make build"; rc=1; }
  fi

  if [[ "${rc}" -eq 0 ]]; then
    log "make test"
    make test || { log "FAIL ${ver}: make test"; rc=1; }
  fi

  [[ "${rc}" -eq 0 ]] && log "OK ${ver}"
  deactivate 2>/dev/null || true
  cd "${PYTHON_DIR}"
  rm -rf "${work_dir}"
  return "${rc}"
}

log "KUDU_HOME=${KUDU_HOME}"
log "PYTHON_DIR=${PYTHON_DIR}"
log "Versions: ${VERSIONS[*]}"
log "make clean"
make clean

failed=0
summary=()

if [[ "${MAX_JOBS}" -gt 0 ]]; then
  log "Running ${#VERSIONS[@]} version(s) in parallel (max jobs: ${MAX_JOBS})..."
  WORK_DIR="$(mktemp -d)"
  log "Log dir: ${WORK_DIR}  (tail -f ${WORK_DIR}/<ver>.log to monitor)"
  declare -a pids=()
  running=0

  for ver in "${VERSIONS[@]}"; do
    # Throttle: wait for a slot before spawning the next job.
    while [[ "${running}" -ge "${MAX_JOBS}" ]]; do
      wait -n 2>/dev/null || true
      running=$(( running - 1 ))
    done
    run_one "${ver}" > "${WORK_DIR}/${ver}.log" 2>&1 &
    pids+=($!)
    running=$(( running + 1 ))
  done

  for i in "${!VERSIONS[@]}"; do
    ver="${VERSIONS[$i]}"
    ec=0; wait "${pids[$i]}" || ec=$?
    case "${ec}" in
      0) summary+=("${ver}: OK") ;;
      2) summary+=("${ver}: SKIP") ;;
      *) summary+=("${ver}: FAIL"); failed=1 ;;
    esac
    log "--- Output: Python ${ver} ---"
    cat "${WORK_DIR}/${ver}.log"
  done

  rm -rf "${WORK_DIR}"
else
  for ver in "${VERSIONS[@]}"; do
    ec=0; run_one "${ver}" || ec=$?
    case "${ec}" in
      0) summary+=("${ver}: OK") ;;
      2) summary+=("${ver}: SKIP") ;;
      *) summary+=("${ver}: FAIL"); failed=1 ;;
    esac
  done
fi

echo ""
log "Summary:"
for line in "${summary[@]}"; do
  echo "  ${line}"
done

[[ "${failed}" -eq 0 ]] || exit 1
