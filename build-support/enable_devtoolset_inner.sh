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
# Executes the provided arguments within the context of a Red Hat devtoolset.
#
# This script should not be used directly; it is called by enable_devtoolset.sh.

set -e

# If ccache was on the PATH and CC/CXX have not already been set, set them to
# devtoolset-3 specific ccache helper scripts (thus enabling ccache).
if which ccache > /dev/null 2>&1 && [ -a ! "$CC" -a ! "$CXX" ]; then
  ROOT=$(cd $(dirname "$BASH_SOURCE") ; pwd)
  export CC="$ROOT/ccache-devtoolset-3/cc"
  export CXX="$ROOT/ccache-devtoolset-3/c++"
fi

# Execute the arguments.
$*
