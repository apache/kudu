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
# Sourced by the python-compat scripts to provide SUPPORTED_VERSIONS.
# Parses supported Python versions from the version classifiers in python/setup.py.
#
# Requires: KUDU_HOME must be set before sourcing.

# \K resets the match start, so only the X.Y part (e.g. "3.12") is captured.
_versions_sh_parse() {
  grep -oP "Programming Language :: Python :: \K\d+\.\d+" \
    "${KUDU_HOME}/python/setup.py" | sort -V
}

mapfile -t SUPPORTED_VERSIONS < <(_versions_sh_parse)

if [[ ${#SUPPORTED_VERSIONS[@]} -eq 0 ]]; then
  echo "versions.sh: ERROR: no Python version classifiers found in ${KUDU_HOME}/python/setup.py" >&2
  exit 1
fi
