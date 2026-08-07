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

fetch_and_patch \
 $GLOG_ARCHIVE \
 $GLOG_SOURCE \
 $GLOG_PATCHLEVEL \
 "${GLOG_PATCHES[@]}"

fetch_and_patch \
 $GMOCK_ARCHIVE \
 $GMOCK_SOURCE \
 $GMOCK_PATCHLEVEL \
 "${GMOCK_PATCHES[@]}"

fetch_and_patch \
 $GFLAGS_ARCHIVE \
 $GFLAGS_SOURCE \
 $GFLAGS_PATCHLEVEL

fetch_and_patch \
 $GPERFTOOLS_ARCHIVE \
 $GPERFTOOLS_SOURCE \
 $GPERFTOOLS_PATCHLEVEL \
 "${GPERFTOOLS_PATCHES[@]}" \
 "${GPERFTOOLS_EXTRA_COMMANDS[@]}"

fetch_and_patch \
 $FLATBUFFERS_ARCHIVE \
 $FLATBUFFERS_SOURCE \
 $FLATBUFFERS_PATCHLEVEL \
 "${FLATBUFFERS_PATCHES[@]}"

fetch_and_patch \
 $PROTOBUF_ARCHIVE \
 $PROTOBUF_SOURCE \
 $PROTOBUF_PATCHLEVEL \
 "${PROTOBUF_PATCHES[@]}" \
 "${PROTOBUF_EXTRA_COMMANDS[@]}"

fetch_and_patch \
 $CMAKE_ARCHIVE \
 $CMAKE_SOURCE \
 $CMAKE_PATCHLEVEL \
 "${CMAKE_PATCHES[@]}"

fetch_and_patch \
 $SNAPPY_ARCHIVE \
 $SNAPPY_SOURCE \
 $SNAPPY_PATCHLEVEL

fetch_and_patch \
 $ZLIB_ARCHIVE \
 $ZLIB_SOURCE \
 $ZLIB_PATCHLEVEL

fetch_and_patch \
 $LIBEV_ARCHIVE \
 $LIBEV_SOURCE \
 $LIBEV_PATCHLEVEL

fetch_and_patch \
 $RAPIDJSON_ARCHIVE \
 $RAPIDJSON_SOURCE \
 $RAPIDJSON_PATCHLEVEL \
 "${RAPIDJSON_PATCHES[@]}"

fetch_and_patch \
 $SQUEASEL_ARCHIVE \
 $SQUEASEL_SOURCE \
 $SQUEASEL_PATCHLEVEL \
 "${SQUEASEL_PATCHES[@]}"

fetch_and_patch \
 $MUSTACHE_ARCHIVE \
 $MUSTACHE_SOURCE \
 $MUSTACHE_PATCHLEVEL

fetch_and_patch \
 $CPPLINT_ARCHIVE \
 $CPPLINT_SOURCE \
 $CPPLINT_PATCHLEVEL \
 "${CPPLINT_PATCHES[@]}"

fetch_and_patch \
 $GCOVR_ARCHIVE \
 $GCOVR_SOURCE \
 $GCOVR_PATCHLEVEL

fetch_and_patch \
 $CURL_ARCHIVE \
 $CURL_SOURCE \
 $CURL_PATCHLEVEL \
 "${CURL_PATCHES[@]}" \
 "${CURL_EXTRA_COMMANDS[@]}"

fetch_and_patch \
 $CRCUTIL_ARCHIVE \
 $CRCUTIL_SOURCE \
 $CRCUTIL_PATCHLEVEL \
 "${CRCUTIL_PATCHES[@]}"

fetch_and_patch \
 $LIBUNWIND_ARCHIVE \
 $LIBUNWIND_SOURCE \
 $LIBUNWIND_PATCHLEVEL \
 "${LIBUNWIND_PATCHES[@]}"

fetch_and_patch \
 $LLVM_ARCHIVE \
 $LLVM_SOURCE \
 $LLVM_PATCHLEVEL \
 "${LLVM_PATCHES[@]}"

fetch_and_patch \
 $LZ4_ARCHIVE \
 $LZ4_SOURCE \
 $LZ4_PATCHLEVEL

fetch_and_patch \
 $BITSHUFFLE_ARCHIVE \
 $BITSHUFFLE_SOURCE \
 $BITSHUFFLE_PATCHLEVEL

fetch_and_patch \
 $TRACE_VIEWER_ARCHIVE \
 $TRACE_VIEWER_SOURCE \
 $TRACE_VIEWER_PATCHLEVEL

fetch_and_patch \
 $BOOST_ARCHIVE \
 $BOOST_SOURCE \
 $BOOST_PATCHLEVEL

fetch_and_patch \
 $BREAKPAD_ARCHIVE \
 $BREAKPAD_SOURCE \
 $BREAKPAD_PATCHLEVEL \
 "${BREAKPAD_PATCHES[@]}"

fetch_and_patch \
 $SPARSEHASH_ARCHIVE \
 $SPARSEHASH_SOURCE \
 $SPARSEHASH_PATCHLEVEL \
 "${SPARSEHASH_PATCHES[@]}"

fetch_and_patch \
 $SPARSEPP_ARCHIVE \
 $SPARSEPP_SOURCE \
 $SPARSEPP_PATCHLEVEL

fetch_and_patch \
 $THRIFT_ARCHIVE \
 $THRIFT_SOURCE \
 $THRIFT_PATCHLEVEL \
 "${THRIFT_PATCHES[@]}"

fetch_and_patch \
 $BISON_ARCHIVE \
 $BISON_SOURCE \
 $BISON_PATCHLEVEL

fetch_and_patch \
 $HIVE_ARCHIVE \
 $HIVE_SOURCE \
 $HIVE_PATCHLEVEL

fetch_and_patch \
 $HADOOP_ARCHIVE \
 $HADOOP_SOURCE \
 $HADOOP_PATCHLEVEL

fetch_and_patch \
 $YAML_ARCHIVE \
 $YAML_SOURCE \
 $YAML_PATCHLEVEL

fetch_and_patch \
 $CHRONY_ARCHIVE \
 $CHRONY_SOURCE \
 $CHRONY_PATCHLEVEL \
 "${CHRONY_PATCHES[@]}"

fetch_and_patch \
 $GUMBO_PARSER_ARCHIVE \
 $GUMBO_PARSER_SOURCE \
 $GUMBO_PARSER_PATCHLEVEL \
 "${GUMBO_PARSER_PATCHES[@]}" \
 "${GUMBO_PARSER_EXTRA_COMMANDS[@]}"

fetch_and_patch \
 $GUMBO_QUERY_ARCHIVE \
 $GUMBO_QUERY_SOURCE \
 $GUMBO_QUERY_PATCHLEVEL \
 "${GUMBO_QUERY_PATCHES[@]}"

fetch_and_patch \
 $POSTGRES_ARCHIVE \
 $POSTGRES_SOURCE \
 $POSTGRES_PATCHLEVEL \
 "${POSTGRES_PATCHES[@]}"

fetch_and_patch \
 $POSTGRES_JDBC_ARCHIVE \
 $POSTGRES_JDBC_SOURCE \
 $POSTGRES_JDBC_PATCHLEVEL

fetch_and_patch \
 $RANGER_ARCHIVE \
 $RANGER_SOURCE \
 $RANGER_PATCHLEVEL \
 "${RANGER_PATCHES[@]}"

fetch_and_patch \
 $JWT_CPP_ARCHIVE \
 $JWT_CPP_SOURCE \
 $JWT_CPP_PATCHLEVEL

fetch_and_patch \
 $RANGER_KMS_ARCHIVE \
 $RANGER_KMS_SOURCE \
 $RANGER_KMS_PATCHLEVEL

fetch_and_patch \
 $ROCKSDB_ARCHIVE \
 $ROCKSDB_SOURCE \
 $ROCKSDB_PATCHLEVEL \
 "${ROCKSDB_PATCHES[@]}"

fetch_and_patch \
 $PROMETHEUS_ARCHIVE \
 $PROMETHEUS_SOURCE \
 $PROMETHEUS_PATCHLEVEL

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
