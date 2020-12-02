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
# Enables the Red Hat devtoolset and executes the arguments on RHEL-based
# systems where the default C++ compiler does not support all features
# necessary to compile Kudu from source (C++17). On other (newer) RHEL-based
# systems and non-RHEL systems, the arguments are executed without changes
# to the environment.
#
# USAGE: ./enable_devtoolset.sh <command> <args>...

set -e
if [[ "$OSTYPE" =~ ^linux ]] && \
   [[ "$(lsb_release -irs)" =~ (CentOS|RedHatEnterpriseServer|OracleServer)[[:space:]]+(6|7)\.[[:digit:]]+ ]]; then
  # Invoke the inner script, which may do some additional customization within
  # the devtoolset.
  ROOT=$(cd $(dirname "$BASH_SOURCE") ; pwd)
  scl enable devtoolset-8 "$ROOT/enable_devtoolset_inner.sh $*"
else
  $@
fi
