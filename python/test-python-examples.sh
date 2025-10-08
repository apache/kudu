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

# This script verifies that the Python client examples can run correctly
# against a local Kudu cluster. It is similar to client_examples-test.sh
# but for Python examples.
#
# Usage:
#   test-python-examples.sh <python2|python3> [example-script]
#
# If no example script is provided, tests all basic Python examples.
#
# To add a new example:
#   1. Add the script name to the VALID_EXAMPLES array below
#   2. Add a case in the test_example() function with the command to run it

# List of valid example scripts
VALID_EXAMPLES=(
  "basic_example.py"
  "non_unique_primary_key.py"
)

PYTHON_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)

if [[ -z "$KUDU_HOME" ]]; then
  # Try to infer it if not set
  KUDU_HOME=$(cd "$PYTHON_DIR/.."; pwd)
  echo "KUDU_HOME not set, inferring from script location: $KUDU_HOME"
fi

if [[ ! -d "$KUDU_HOME" ]]; then
  exit_error "KUDU_HOME directory does not exist: $KUDU_HOME"
fi

EXAMPLES_DIR="$KUDU_HOME/examples/python"

# Source the common cluster management functions
source "$KUDU_HOME/build-support/test-cluster-common.sh"

BUILD_DIR="$KUDU_HOME/build/latest"
BIN_DIR="$BUILD_DIR/bin"

# Track virtualenv directories for cleanup
VENV_DIRS=()

# Clean up after the test. Must be idempotent.
cleanup() {
  cleanup_cluster
  for venv_dir in "${VENV_DIRS[@]}"; do
    if [[ -n "$venv_dir" ]] && [[ -d "$venv_dir" ]]; then
      rm -rf "$venv_dir"
    fi
  done
}
trap cleanup EXIT

set -e
set -o pipefail
set -x

test_example() {
  local example_script=$1
  local python_version=$2

  case "$example_script" in
    basic_example.py)
      echo "Testing basic_example.py..."
      if ! "$python_version" "$EXAMPLES_DIR/basic-python-example/basic_example.py" \
          --masters $LOCALHOST_IP \
          --ports $MASTER_RPC_PORT ; then
        exit_error "basic_example.py failed with $python_version"
      fi
      ;;
    non_unique_primary_key.py)
      echo "Testing non_unique_primary_key.py..."
      if ! "$python_version" "$EXAMPLES_DIR/basic-python-example/non_unique_primary_key.py" \
          --masters $LOCALHOST_IP \
          --ports $MASTER_RPC_PORT ; then
        exit_error "non_unique_primary_key.py failed with $python_version"
      fi
      ;;
    *)
      exit_error "Unknown example script: $example_script"
      ;;
  esac
}

test_with_python_version() {
  local python_version=$1
  local example_name=$2

  if ! command -v "$python_version" &> /dev/null; then
    echo "WARNING: $python_version not found, skipping $python_version tests"
    return 0
  fi

  # Create a temporary virtualenv for this Python version
  local venv_dir=$(mktemp -d -t kudu-python-examples-test-${python_version}.XXXXXXXXXXXXX)
  VENV_DIRS+=("$venv_dir")
  echo "Creating virtualenv for $python_version in $venv_dir"
  virtualenv -p "$python_version" "$venv_dir"
  source "$venv_dir/bin/activate"

  pip install -r "$PYTHON_DIR/requirements.txt"
  make clean
  make install

  echo "Running Python examples with $python_version..."

  if [[ -z "$example_name" ]]; then
    for example in "${VALID_EXAMPLES[@]}"; do
      test_example "$example" "$python_version"
    done
  else
    test_example "$example_name" "$python_version"
  fi

  echo "All $python_version examples completed successfully!"

  deactivate
}

# Parse command-line arguments
PYTHON_VERSION="$1"
EXAMPLE_NAME="$2"

# Validate Python version (required)
if [[ "$PYTHON_VERSION" != "python2" ]] && [[ "$PYTHON_VERSION" != "python3" ]]; then
  exit_error "Usage: test-python-examples.sh <python2|python3> [example-script]
Valid example scripts: ${VALID_EXAMPLES[*]}"
fi

# Validate example script name (optional)
if [[ -n "$EXAMPLE_NAME" ]]; then
  valid=false
  for example in "${VALID_EXAMPLES[@]}"; do
    if [[ "$EXAMPLE_NAME" == "$example" ]]; then
      valid=true
      break
    fi
  done
  if [[ "$valid" != "true" ]]; then
    exit_error "Invalid example script: $EXAMPLE_NAME. Must be one of: ${VALID_EXAMPLES[*]}"
  fi
fi

start_test_cluster "$BIN_DIR" "python_examples-test"

test_with_python_version "$PYTHON_VERSION" "$EXAMPLE_NAME"

echo "All Python examples tests completed successfully!"