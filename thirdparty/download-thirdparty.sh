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
 glog-${GLOG_VERSION}.tar.gz \
 $GLOG_SOURCE \
 $GLOG_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/glog-make-internals-visible.patch" \
 "patch -p1 < $TP_DIR/patches/glog-support-stacktrace-for-aarch64.patch"

fetch_and_patch \
 googletest-release-${GMOCK_VERSION}.tar.gz \
 $GMOCK_SOURCE \
 $GMOCK_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/gmock-update-iwyu-pragma.patch"

fetch_and_patch \
 gflags-${GFLAGS_VERSION}.tar.gz \
 $GFLAGS_SOURCE \
 $GFLAGS_PATCHLEVEL

fetch_and_patch \
 gperftools-${GPERFTOOLS_VERSION}.tar.gz \
 $GPERFTOOLS_SOURCE \
 $GPERFTOOLS_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/gperftools-Replace-namespace-base-with-namespace-tcmalloc.patch" \
 "autoreconf -fvi"

fetch_and_patch \
 flatbuffers-${FLATBUFFERS_VERSION}.tar.gz \
 $FLATBUFFERS_SOURCE \
 $FLATBUFFERS_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/flatbuffers-length-to-size-uint8-ptr.patch"

# NOTE: creating an empty 'third_party/googletest/m4' subdir is a recipe from
# the $PROTOBUF_SOURCE/autogen.sh file:
#
#   The absence of a m4 directory in googletest causes autoreconf to fail when
#   building under the CentOS docker image. It's a warning in regular build on
#   Ubuntu/gLinux as well.
#
fetch_and_patch \
 protobuf-cpp-${PROTOBUF_VERSION}.tar.gz \
 $PROTOBUF_SOURCE \
 $PROTOBUF_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/protobuf-inlined_string_field.patch" \
 "mkdir -p third_party/googletest/m4" \
 "autoreconf -fvi"

# Returns 0 if cmake should be patched to work around this bug [1].
#
# Currently only SLES 12 SP0 is known to be vulnerable, and since the workaround
# hurts cmake performance, we apply it only if absolutely necessary.
#
# 1. https://gitlab.kitware.com/cmake/cmake/issues/15873.
needs_patched_cmake() {
  if [ ! -e /etc/SuSE-release ]; then
    # Not a SUSE distro.
    return 1
  fi
  if ! grep -q "SUSE Linux Enterprise Server 12" /etc/SuSE-release; then
    # Not SLES 12.
    return 1
  fi
  if ! grep -q "PATCHLEVEL = 0" /etc/SuSE-release; then
    # Not SLES 12 SP0.
    return 1
  fi
  return 0
}
CMAKE_PATCHES=""
if needs_patched_cmake; then \
 CMAKE_PATCHES="patch -p1 < $TP_DIR/patches/cmake-issue-15873-dont-use-select.patch"
fi

# cmake-fix-macos-compilation should be removed once cmake is upgraded to version 3.30 or later
fetch_and_patch \
 cmake-${CMAKE_VERSION}.tar.gz \
 $CMAKE_SOURCE \
 $CMAKE_PATCHLEVEL \
 "$CMAKE_PATCHES" \
 "patch -p1 < $TP_DIR/patches/cmake-fix-macos-compilation.patch"

fetch_and_patch \
 snappy-${SNAPPY_VERSION}.tar.gz \
 $SNAPPY_SOURCE \
 $SNAPPY_PATCHLEVEL

fetch_and_patch \
 zlib-${ZLIB_VERSION}.tar.gz \
 $ZLIB_SOURCE \
 $ZLIB_PATCHLEVEL

fetch_and_patch \
 libev-${LIBEV_VERSION}.tar.gz \
 $LIBEV_SOURCE \
 $LIBEV_PATCHLEVEL

fetch_and_patch \
 rapidjson-${RAPIDJSON_VERSION}.zip \
 $RAPIDJSON_SOURCE \
 $RAPIDJSON_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/rapidjson-fix-signed-unsigned-conversion-error.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-assertions-for-clang-warnings.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-avoid-pointer-arithmetic-on-null-pointer.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-document-assignment-operator-00.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-document-assignment-operator-01.patch"

fetch_and_patch \
 squeasel-${SQUEASEL_VERSION}.tar.gz \
 $SQUEASEL_SOURCE \
 $SQUEASEL_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/squeasel-handle-openssl-errors.patch" \
 "patch -p1 < $TP_DIR/patches/squeasel-tls-min-version.patch" \
 "patch -p1 < $TP_DIR/patches/squeasel-support-get-bound-addresses-for-ipv6.patch" \
 "patch -p1 < $TP_DIR/patches/squeasel-tls-openssl10x.patch" \
 "patch -p1 < $TP_DIR/patches/squeasel-ipv6-only-socket-option.patch"

fetch_and_patch \
 mustache-${MUSTACHE_VERSION}.tar.gz \
 $MUSTACHE_SOURCE \
 $MUSTACHE_PATCHLEVEL

fetch_and_patch \
 cpplint-${CPPLINT_VERSION}.tar.gz \
 $CPPLINT_SOURCE \
 $CPPLINT_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/cpplint-libstdcpp-regex.patch"

fetch_and_patch \
 gcovr-${GCOVR_VERSION}.tar.gz \
 $GCOVR_SOURCE \
 $GCOVR_PATCHLEVEL

fetch_and_patch \
 curl-${CURL_VERSION}.tar.gz \
 $CURL_SOURCE \
 $CURL_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/curl-custom-openssl-library.patch" \
 "patch -p1 < $TP_DIR/patches/curl-handle-openssl-errors.patch" \
 "patch -p1 < $TP_DIR/patches/curl-eventfd-double-close.patch" \
 "autoreconf -fvi"

fetch_and_patch \
 crcutil-${CRCUTIL_VERSION}.tar.gz \
 $CRCUTIL_SOURCE \
 $CRCUTIL_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/crcutil-fix-macos-arm64-flags.patch"

fetch_and_patch \
 libunwind-${LIBUNWIND_VERSION}.tar.gz \
 $LIBUNWIND_SOURCE \
 $LIBUNWIND_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/libunwind-trace-cache-destructor.patch"

fetch_and_patch \
 llvm-${LLVM_VERSION}-iwyu-${IWYU_VERSION}.src.tar.gz \
 $LLVM_SOURCE \
 $LLVM_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/llvm-add-iwyu.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-iwyu-718e69875.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-iwyu-0de60d8a2.patch" \
 "patch -d projects -p1 < $TP_DIR/patches/llvm-remove-cyclades-inclusion-in-sanitizer.patch" \
 "patch -p2 < $TP_DIR/patches/llvm-fix-missing-include.patch" \
 "patch -d projects -p1 < $TP_DIR/patches/llvm-Sanitizer-built-against-glibc-2_34-doesnt-work.patch" \
 "patch -d tools -p1 < $TP_DIR/patches/llvm-ignore-flto-values.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-nostdinc-nostdlib-00.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-nostdinc-nostdlib-01.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-nostdinc-nostdlib-02.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-include-llvm-support-signals.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-is-convertible-00.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-is-convertible-01.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-chrono-duration-00.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-chrono-duration-01.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-section-mm-memory-mapper.patch" \
 "patch -p1 < $TP_DIR/patches/llvm-section-mm-extra-methods.patch"

fetch_and_patch \
 lz4-$LZ4_VERSION.tar.gz \
 $LZ4_SOURCE \
 $LZ4_PATCHLEVEL

fetch_and_patch \
 bitshuffle-${BITSHUFFLE_VERSION}.tar.gz \
 $BITSHUFFLE_SOURCE \
 $BITSHUFFLE_PATCHLEVEL

fetch_and_patch \
 kudu-trace-viewer-${TRACE_VIEWER_VERSION}.tar.gz \
 $TRACE_VIEWER_SOURCE \
 $TRACE_VIEWER_PATCHLEVEL

fetch_and_patch \
 boost-${BOOST_VERSION}-cmake.tar.gz \
 $BOOST_SOURCE \
 $BOOST_PATCHLEVEL

fetch_and_patch \
 breakpad-${BREAKPAD_VERSION}.tar.gz \
 $BREAKPAD_SOURCE \
 $BREAKPAD_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/breakpad-add-basic-support-for-dwz-dwarf-extension.patch" \
 "patch -p1 < $TP_DIR/patches/breakpad-syscall-rsp-clobber-fix.patch" \
 "patch -p1 < $TP_DIR/patches/breakpad-SIGSTKSZ-error.patch" \
 "patch -p1 < $TP_DIR/patches/breakpad-fclose.patch" \
 "patch -p1 < $TP_DIR/patches/breakpad-fread.patch" \
 "patch -p1 < $TP_DIR/patches/breakpad-minidump-descriptor.patch" \
 "patch -p1 < $TP_DIR/patches/breakpad-guid-creator.patch" \
 "patch -p1 < $TP_DIR/patches/breakpad-64k-pages-stack-collection.patch"

fetch_and_patch \
 sparsehash-c11-${SPARSEHASH_VERSION}.tar.gz \
 $SPARSEHASH_SOURCE \
 $SPARSEHASH_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/sparsehash-0001-Add-compatibily-for-gcc-4.x-in-traits.patch" \
 "patch -p1 < $TP_DIR/patches/sparsehash-0002-Add-workaround-for-dense_hashtable-move-constructor-.patch"

fetch_and_patch \
 sparsepp-${SPARSEPP_VERSION}.tar.gz \
 $SPARSEPP_SOURCE \
 $SPARSEPP_PATCHLEVEL

fetch_and_patch \
 $THRIFT_NAME.tar.gz \
 $THRIFT_SOURCE \
 $THRIFT_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/thrift-e96bc4015.patch" \
 "patch -p1 < $TP_DIR/patches/thrift-c1457c69f.patch" \
 "patch -p1 < $TP_DIR/patches/thrift-5748bbb6b.patch" \
 "patch -p1 < $TP_DIR/patches/thrift-e3c8c534c.patch"

fetch_and_patch \
 $BISON_NAME.tar.gz \
 $BISON_SOURCE \
 $BISON_PATCHLEVEL
 # This would normally call autoreconf, but it does not succeed with
 # autoreconf 2.69-11 (RHEL 7): "autoreconf: 'configure.ac' or 'configure.in' is required".

fetch_and_patch \
 $HIVE_NAME-stripped.tar.gz \
 $HIVE_SOURCE \
 $HIVE_PATCHLEVEL

fetch_and_patch \
 $HADOOP_NAME-stripped.tar.gz \
 $HADOOP_SOURCE \
 $HADOOP_PATCHLEVEL

fetch_and_patch \
 $YAML_NAME.tar.gz \
 $YAML_SOURCE \
 $YAML_PATCHLEVEL

fetch_and_patch \
 $CHRONY_NAME.tar.gz \
 $CHRONY_SOURCE \
 $CHRONY_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/chrony-reuseport.patch"

fetch_and_patch \
 $GUMBO_PARSER_NAME.tar.gz \
 $GUMBO_PARSER_SOURCE \
 $GUMBO_PARSER_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/gumbo-parser-autoconf-263.patch" \
 "autoreconf -fvi"

fetch_and_patch \
 $GUMBO_QUERY_NAME.tar.gz \
 $GUMBO_QUERY_SOURCE \
 $GUMBO_QUERY_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/gumbo-query-namespace.patch"

fetch_and_patch \
 $POSTGRES_NAME.tar.gz \
 $POSTGRES_SOURCE \
 $POSTGRES_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/postgres-root-can-run-initdb.patch" \
 "patch -p0 < $TP_DIR/patches/postgres-no-check-root.patch" \
 "patch -p1 < $TP_DIR/patches/postgres-fix-strchrnul-macos-check.patch"

fetch_and_patch \
 $POSTGRES_JDBC_NAME.jar \
 $POSTGRES_JDBC_SOURCE \
 $POSTGRES_JDBC_PATCHLEVEL

fetch_and_patch \
 $RANGER_NAME.tar.gz \
 $RANGER_SOURCE \
 $RANGER_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/ranger-fixscripts.patch"

fetch_and_patch \
 $JWT_CPP_NAME.tar.gz \
 $JWT_CPP_SOURCE \
 $JWT_CPP_PATCHLEVEL

fetch_and_patch \
 $RANGER_KMS_NAME.tar.gz \
 $RANGER_KMS_SOURCE \
 $RANGER_KMS_PATCHLEVEL

fetch_and_patch \
 $ROCKSDB_NAME.tar.gz \
 $ROCKSDB_SOURCE \
 $ROCKSDB_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/rocksdb-gcc13.patch"

fetch_and_patch \
 ${PROMETHEUS_NAME}.tar.gz \
 ${PROMETHEUS_SOURCE} \
 ${PROMETHEUS_PATCHLEVEL}

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"

