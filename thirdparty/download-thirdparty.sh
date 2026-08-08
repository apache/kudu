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

# autoreconf calls are necessary to fix hard-coded aclocal versions in the
# configure scripts that ship with the projects.

set -e

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/vars.sh
source $TP_DIR/prebuilt-utils.sh

if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
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


# If not using pre-built thirdparty artifacts, fetch all the source archives
# unconditionally: this simplifies the logic. It is aligned with the most
# common use case: building everything from scratch (if not built yet).
if [ "${USE_PREBUILT_THIRDPARTY:-1}" = "0" ]; then
  for comp in $TP_COMPONENTS; do
    fetch_and_patch $comp
  done
fi

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
