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

# This script verifies that the Java client examples can run correctly
# against a local Kudu cluster.
#
# Usage:
#   test-java-examples.sh [example-name]
#
# If no argument is provided, tests all Java examples.
#
# To add a new example:
#   1. Add the example directory name to the VALID_EXAMPLES array below
#   2. Add a case in the test_java_example() function with the command to run it

# List of valid example names (single source of truth)
VALID_EXAMPLES=(
  "java-example"
  "insert-loadgen"
  "collectl"
)

JAVA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)

# KUDU_HOME should point to the root of the Kudu repository
if [[ -z "$KUDU_HOME" ]]; then
  # Try to infer it if not set
  KUDU_HOME=$(cd "$JAVA_DIR/.."; pwd)
  echo "KUDU_HOME not set, inferring from script location: $KUDU_HOME"
fi

if [[ ! -d "$KUDU_HOME" ]]; then
  exit_error "KUDU_HOME directory does not exist: $KUDU_HOME"
fi

EXAMPLES_DIR="$KUDU_HOME/examples/java"

# Source the common cluster management functions
source "$KUDU_HOME/build-support/test-cluster-common.sh"

BUILD_DIR="$KUDU_HOME/build/latest"
BIN_DIR="$BUILD_DIR/bin"

# Clean up after the test. Must be idempotent.
cleanup() {
  cleanup_cluster
}
trap cleanup EXIT

set -e
set -o pipefail
set -x

if ! command -v mvn &> /dev/null; then
  exit_error "Maven (mvn) not found. Please install Maven to run Java examples tests."
fi

KUDU_VERSION=$(cat "$KUDU_HOME/version.txt")
echo "Using Kudu version: $KUDU_VERSION"

echo "Publishing Kudu Java artifacts to local Maven repository..."
pushd "$JAVA_DIR"
./gradlew publishToMavenLocal
popd

test_java_example() {
  local example_name=$1
  local example_dir="$EXAMPLES_DIR/$example_name"

  echo "Testing Java example: $example_name"

  if [[ ! -d "$example_dir" ]]; then
    echo "WARNING: Example directory not found: $example_dir, skipping"
    return 0
  fi

  pushd "$example_dir"

  echo "Building $example_name with Maven..."
  # Build the example using locally built Kudu
  if ! mvn clean package \
      -Dkudu-version=$KUDU_VERSION \
      -DkuduBinDir=$KUDU_HOME/build/latest/bin \
      -DuseLocalKuduBin=true ; then
    exit_error "Failed to build $example_name"
  fi

  echo "Running $example_name..."
  case "$example_name" in
    java-example)
      if ! java -DkuduMasters=$LOCALHOST_IP:$MASTER_RPC_PORT \
          -jar target/kudu-java-example-*.jar ; then
        exit_error "$example_name failed"
      fi
      ;;
    insert-loadgen)
      # Run insert-loadgen for a limited time
      local test_table="loadgen_test_table"

      # Run in background and kill after 5 seconds
      java -DkuduMasters=$LOCALHOST_IP:$MASTER_RPC_PORT \
          -jar target/kudu-insert-loadgen-*.jar \
          $LOCALHOST_IP:$MASTER_RPC_PORT "$test_table" &
      local loadgen_pid=$!
      sleep 5
      kill $loadgen_pid 2>/dev/null || true
      wait $loadgen_pid 2>/dev/null || true

      local row_count=$("$BIN_DIR/kudu" table scan $LOCALHOST_IP:$MASTER_RPC_PORT "$test_table" 2>/dev/null | wc -l)
      if [[ $row_count -lt 1 ]]; then
        exit_error "insert-loadgen did not insert enough data (row_count=$row_count)"
      fi
      echo "insert-loadgen inserted data successfully (rows: $row_count)"
      ;;
    collectl)
      # Collectl requires external socket input, so just verify it builds
      echo "collectl requires external input, verifying build only"
      ;;
    *)
      echo "Unknown example: $example_name"
      ;;
  esac

  echo "$example_name completed successfully!"
  popd
}

# Parse command-line arguments
EXAMPLE_NAME="$1"

# Validate example name (optional)
if [[ -n "$EXAMPLE_NAME" ]]; then
  local valid=false
  for example in "${VALID_EXAMPLES[@]}"; do
    if [[ "$EXAMPLE_NAME" == "$example" ]]; then
      valid=true
      break
    fi
  done
  if [[ "$valid" != "true" ]]; then
    exit_error "Invalid example name: $EXAMPLE_NAME. Must be one of: ${VALID_EXAMPLES[*]}"
  fi
fi

start_test_cluster "$BIN_DIR" "java_examples-test"

if [[ -z "$EXAMPLE_NAME" ]]; then
  echo "No example specified, testing all Java examples"
  for example in "${VALID_EXAMPLES[@]}"; do
    test_java_example "$example"
  done
else
  test_java_example "$EXAMPLE_NAME"
fi

echo "All Java examples tests completed successfully!"

