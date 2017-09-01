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

ROOT=$(cd $(dirname $BASH_SOURCE)/..; pwd)

LAST_COMMITTED_REV=$($ROOT/build-support/get-upstream-commit.sh)
if [ -z "$LAST_COMMITTED_REV" ]; then
  echo "ERROR: could not find the last committed upstream changelist"
  exit 1
fi

$ROOT/build-support/clang_tidy_gerrit.py \
  --no-gerrit \
  --rev-range \
  "${LAST_COMMITTED_REV}"
