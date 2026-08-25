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

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/vars.sh
source $TP_DIR/prebuilt-utils.sh

if [ -z "$TP_SOURCE_DIR" ]; then
  echo "ERROR: TP_SOURCE_DIR variable is not set, check your scripts" >&2
  exit 1
fi
mkdir -p $TP_SOURCE_DIR
cd $TP_SOURCE_DIR

TP_COMPONENTS="
 glog
 gmock
 gflags
 gperftools
 flatbuffers
 protobuf
 cmake
 snappy
 zlib
 libev
 rapidjson
 squeasel
 mustache
 cpplint
 gcovr
 curl
 crcutil
 libunwind
 llvm
 lz4
 bitshuffle
 trace-viewer
 boost
 breakpad
 sparsehash
 sparsepp
 thrift
 bison
 hive
 hadoop
 yaml
 chrony
 gumbo-parser
 gumbo-query
 postgres
 postgres-jdbc
 ranger
 jwt-cpp
 ranger-kms
 rocksdb
 prometheus
"

# Fetch source/distro archives for all the 3rd-party components, apply custom
# patches, run autoreconf for some, and then run configure or cmake for each
# of them.  Calling autoreconf is necessary to fix hard-coded aclocal versions
# in the configure scripts that ship with a few source archives.
for comp in $TP_COMPONENTS; do
  fetch_and_patch "$comp"
done

echo "---------------"
echo "Thirdparty source code downloaded and configured successfully"
