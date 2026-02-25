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
# Wrapper to run the swagger verification script via the test harness.

set -euo pipefail

if [ -n "${KUDU_HOME:-}" ]; then
  # For out-of-tree builds or test runners that copy scripts into temp dirs,
  # walking up from the script path cannot find the repo. Allow an explicit
  # override to point at the source root in those cases.
  REPO_ROOT="$KUDU_HOME"
else
  SCRIPT_PATH=$(readlink -f "${BASH_SOURCE[0]}")
  SCRIPT_DIR=$(cd "$(dirname "${SCRIPT_PATH}")" && pwd)
  SEARCH_DIR="$SCRIPT_DIR"
  REPO_ROOT=""

  while [ "$SEARCH_DIR" != "/" ]; do
    if [ -f "$SEARCH_DIR/build-support/verify_swagger_spec.py" ]; then
      REPO_ROOT="$SEARCH_DIR"
      break
    fi
    SEARCH_DIR=$(cd "$SEARCH_DIR/.." && pwd)
  done
fi

if [ -z "$REPO_ROOT" ]; then
  echo "Unable to locate repo root containing build-support/verify_swagger_spec.py"
  exit 1
fi

python3 "$REPO_ROOT/build-support/verify_swagger_spec.py"
