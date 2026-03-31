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
#   1. Build an sdist in a clean venv
#   2. Install that sdist in a second clean venv
#   3. Verify the package imports correctly
#
# Usage:
#   ./test-packaging.sh               # test all supported versions
#   ./test-packaging.sh 3.12          # test a single version
#   ./test-packaging.sh 3.11 3.12     # test a subset
#
# Environment:
#   KUDU_HOME   If unset, inferred from the script location (two levels up)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KUDU_HOME="${KUDU_HOME:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
PYTHON_DIR="${KUDU_HOME}/python"

cd "${PYTHON_DIR}"

source "${SCRIPT_DIR}/versions.sh"

if [[ $# -gt 0 ]]; then
  VERSIONS=("$@")
else
  VERSIONS=("${SUPPORTED_VERSIONS[@]}")
fi

log() { echo "[test-packaging] $*"; }
die() { echo "[test-packaging] ERROR: $*" >&2; exit 1; }
ok()  { echo "[test-packaging] OK    $*"; }

if command -v virtualenv >/dev/null 2>&1; then
  VIRTUALENV_CMD=(virtualenv)
elif python3 -m virtualenv --version >/dev/null 2>&1; then
  VIRTUALENV_CMD=(python3 -m virtualenv)
else
  die "virtualenv is required (install: pip install virtualenv)."
fi

failed=0
summary=()

run_one() {
  local ver="$1"
  local py_bin="python${ver}"

  if ! command -v "${py_bin}" >/dev/null 2>&1; then
    log "SKIP ${ver}: ${py_bin} not found on PATH"
    summary+=("${ver}: SKIP (no ${py_bin})")
    return 0
  fi

  log "=== Python ${ver} ==="

  # Build the sdist in a clean venv
  log "Building sdist..."
  local build_venv
  build_venv="$(mktemp -d)"

  "${VIRTUALENV_CMD[@]}" -p "${py_bin}" "${build_venv}" >/dev/null
  source "${build_venv}/bin/activate"

  make requirements
  rm -rf dist/
  python setup.py sdist --quiet

  local sdist
  sdist="$(ls -1t dist/*.tar.gz 2>/dev/null | head -1)"
  if [[ -z "${sdist}" ]]; then
    log "FAIL ${ver}: no .tar.gz produced"
    summary+=("${ver}: FAIL sdist")
    failed=1
    deactivate 2>/dev/null || true
    rm -rf "${build_venv}"
    return 0
  fi
  log "Built: ${sdist}"

  deactivate
  rm -rf "${build_venv}"

  # Install the sdist in a second clean venv
  log "Installing sdist..."
  local install_venv
  install_venv="$(mktemp -d)"

  "${VIRTUALENV_CMD[@]}" -p "${py_bin}" "${install_venv}" >/dev/null
  source "${install_venv}/bin/activate"

  if pip install "${sdist}" && python -c "import kudu; print('kudu', kudu.__version__)"; then
    log "pip list:"
    pip list
    ok "Python ${ver} — sdist round-trip OK."
    summary+=("${ver}: OK")
  else
    log "FAIL ${ver}: install or import failed"
    summary+=("${ver}: FAIL install")
    failed=1
  fi

  deactivate 2>/dev/null || true
  rm -rf "${install_venv}"
}

log "KUDU_HOME=${KUDU_HOME}"
log "PYTHON_DIR=${PYTHON_DIR}"
log "Versions: ${VERSIONS[*]}"

for ver in "${VERSIONS[@]}"; do
  run_one "${ver}"
done

echo ""
log "Summary:"
for line in "${summary[@]}"; do
  echo "  ${line}"
done

[[ "${failed}" -eq 0 ]] || exit 1
