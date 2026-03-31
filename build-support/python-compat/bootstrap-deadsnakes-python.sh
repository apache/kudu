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
# Test infrastructure script: install every Python version supported by the
# Kudu Python client via the deadsnakes PPA, then run a smoke test for each.
#
# Usage:
#   ./bootstrap-deadsnakes-python.sh
#
# Requires: Ubuntu/Debian with apt-get and sudo.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KUDU_HOME="${KUDU_HOME:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
PYTHON_DIR="${KUDU_HOME}/python"

source "${SCRIPT_DIR}/versions.sh"

log() { echo "[bootstrap-deadsnakes] $*"; }
die() { echo "[bootstrap-deadsnakes] ERROR: $*" >&2; exit 1; }

[[ -x "/usr/bin/apt-get" ]] || die "This script requires apt-get (Ubuntu/Debian)."
command -v sudo >/dev/null 2>&1 || die "sudo is required."
command -v virtualenv >/dev/null 2>&1 || die "virtualenv is required."

# One-time setup: deadsnakes PPA + prerequisites
setup_ppa() {
  log "Updating apt index and ensuring prerequisites..."
  sudo apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    software-properties-common ca-certificates curl

  if ! grep -rq "deadsnakes/ppa" /etc/apt/sources.list /etc/apt/sources.list.d/ 2>/dev/null; then
    log "Adding deadsnakes PPA..."
    sudo add-apt-repository -y ppa:deadsnakes/ppa
    sudo apt-get update -y
  else
    log "deadsnakes PPA already present."
  fi
}

# Smoke test: verify the interpreter works and a venv can be created
smoke_test() {
  local ver="$1"
  local py_bin="python${ver}"

  command -v "${py_bin}" >/dev/null 2>&1 || die "${py_bin} not found on PATH after install."

  local work
  work="$(mktemp -d)"
  local venv_dir="${work}/venv"

  log "Creating temp venv with ${py_bin}..."
  virtualenv -p "${py_bin}" "${venv_dir}" >/dev/null

  source "${venv_dir}/bin/activate"
  python -c "import sys; print('OK:', sys.version)"
  python -m pip --version >/dev/null
  deactivate || true
  rm -rf "${work}"
}

setup_ppa

summary_ok=()
summary_skip=()
summary_fail=()

for ver in "${SUPPORTED_VERSIONS[@]}"; do
  log "=== Python ${ver} ==="
  log "Installing: python${ver} python${ver}-dev"
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    "python${ver}" "python${ver}-dev"
  if smoke_test "${ver}"; then
    log "OK Python ${ver} ready."
    summary_ok+=("${ver}: OK")
  else
    summary_fail+=("${ver}: FAIL")
  fi
done

echo ""
log "Summary:"
for line in "${summary_ok[@]+"${summary_ok[@]}"}";    do echo "  ${line}"; done
for line in "${summary_skip[@]+"${summary_skip[@]}"}"; do echo "  ${line}"; done
for line in "${summary_fail[@]+"${summary_fail[@]}"}"; do echo "  ${line}"; done

[[ ${#summary_fail[@]} -eq 0 ]] || exit 1
