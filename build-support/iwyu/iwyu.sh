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

set -e
set -o pipefail

ROOT=$(cd $(dirname $BASH_SOURCE)/../..; pwd)

# Build the list of updated files which are of IWYU interest.
# Since '-e' is set, transform the exit code from the grep accordingly:
# grep returns 1 if no lines were selected.
file_list_tmp=$(git diff --name-only \
    $($ROOT/build-support/get-upstream-commit.sh) | \
    (grep -E '\.(c|cc|h)$' || [ $? -eq 1 ]))
if [ -z "$file_list_tmp" ]; then
  echo "IWYU verification: no updates on related files, declaring success"
  exit 0
fi

IWYU_LOG=$(mktemp -t kudu-iwyu.XXXXXX)
UNFILTERED_IWYU_LOG=${IWYU_LOG}.unfiltered
trap "rm -f $IWYU_LOG $UNFILTERED_IWYU_LOG" EXIT

# Adjust the path for every element in the list. The iwyu_tool.py normalizes
# paths (via realpath) to match the records from the compilation database.
IWYU_FILE_LIST=
for p in $file_list_tmp; do
  IWYU_FILE_LIST="$IWYU_FILE_LIST $ROOT/$p"
done

IWYU_MAPPINGS_PATH="$ROOT/build-support/iwyu/mappings"
IWYU_ARGS="\
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-all.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-all-private.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-extra.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/gflags.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/glog.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/gtest.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/libstdcpp.imp"

if ! PATH="$PATH:$PWD/../../thirdparty/clang-toolchain/bin" \
    python $ROOT/build-support/iwyu/iwyu_tool.py -p . $IWYU_FILE_LIST -- \
    $IWYU_ARGS > $UNFILTERED_IWYU_LOG 2>&1; then
  echo "IWYU verification: failed to run the tool, see below for details"
  cat $UNFILTERED_IWYU_LOG
  exit 2
fi

awk -f $ROOT/build-support/iwyu/iwyu-filter.awk $UNFILTERED_IWYU_LOG | \
    tee $IWYU_LOG
if [ -s "$IWYU_LOG" ]; then
  # The output is not empty: IWYU finds the set of headers to be inconsistent.
  echo "IWYU verification: changelist needs correction, see above for details"
  exit 1
fi

# The output is empty: the changelist looks good.
echo "IWYU verification: the changes look good"
exit 0
