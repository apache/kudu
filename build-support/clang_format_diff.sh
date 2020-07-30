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

# This script acts as a simple wrapper to run clang-format-diff.phy
# from the installed thirdparty directory, with the appropriate
# clang-format binary passed in.

set -e

ROOT="$(dirname $BASH_SOURCE)/.."
TP="$ROOT/thirdparty/installed/uninstrumented/"
TOOL="$TP/share/clang/clang-format-diff.py"
CLANG_FORMAT="$TP/bin/clang-format"

for path in "$TOOL" "$CLANG_FORMAT" ; do
  if [ ! -x $path ]; then
    >&2 echo $path not found, please build thirdparty first
    exit 1
  fi
done

exec $TOOL -binary "$CLANG_FORMAT" "$@"
