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

if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
fi

delete_if_wrong_patchlevel() {
  local DIR=$1
  local PATCHLEVEL=$2
  if [ ! -f $DIR/patchlevel-$PATCHLEVEL ]; then
    echo It appears that $DIR is missing the latest local patches.
    echo Removing it so we re-download it.
    rm -Rf $DIR
  fi
}

unzip_to_source() {
  local FILENAME=$1
  local SOURCE=$2
  unzip -q "$FILENAME"
  # Parse out the unzipped top directory
  DIR_NAME=`unzip -qql "$FILENAME" | awk 'NR==1 {print $4}' | sed -e 's|^[/]*\([^/]*\).*|\1|'`
  # If the unzipped directory is the wrong name, move it.
  if [ "$SOURCE" != "$TP_SOURCE_DIR/$DIR_NAME" ]; then
    mv "$TP_SOURCE_DIR/$DIR_NAME" "$SOURCE"
  fi
}

fetch_and_expand() {
  local FILENAME=$1
  local SOURCE=$2
  local URL_PREFIX=$3

  if [ -z "$FILENAME" ]; then
    echo "Error: Must specify file to fetch"
    exit 1
  fi

  if [ -z "$URL_PREFIX" ]; then
    echo "Error: Must specify url prefix to fetch"
    exit 1
  fi

  TAR_CMD=tar
  if [[ "$OSTYPE" == "darwin"* ]] && which gtar &>/dev/null; then
    TAR_CMD=gtar
  fi

  FULL_URL="${URL_PREFIX}/${FILENAME}"

  SUCCESS=0
  # Loop in case we encounter an error.
  for attempt in 1 2 3; do
    if [ -r "$FILENAME" ]; then
      echo "Archive $FILENAME already exists. Not re-downloading archive."
    else
      echo "Fetching $FILENAME from $FULL_URL"
      if ! curl --retry 3 -L -O "$FULL_URL"; then
        echo "Error downloading $FILENAME"
        rm -f "$FILENAME"

        # Pause for a bit before looping in case the server throttled us.
        sleep 5
        continue
      fi
    fi

    echo "Unpacking $FILENAME to $SOURCE"
    if [[ "$FILENAME" =~ \.zip$ ]]; then
      if ! unzip_to_source "$FILENAME" "$SOURCE"; then
        echo "Error unzipping $FILENAME, removing file"
        rm "$FILENAME"
        continue
      fi
    elif [[ "$FILENAME" =~ \.(tar\.gz|tgz)$ ]]; then
      if ! $TAR_CMD xf "$FILENAME"; then
        echo "Error untarring $FILENAME, removing file"
        rm "$FILENAME"
        continue
      fi
    elif [[ "$FILENAME" =~ \.jar$ ]]; then
      mkdir ${FILENAME%.jar}
      cp $FILENAME ${FILENAME%.jar}/
    else
      echo "Error: unknown file format: $FILENAME"
      exit 1
    fi

    SUCCESS=1
    break
  done

  if [ $SUCCESS -ne 1 ]; then
    echo "Error: failed to fetch and unpack $FILENAME"
    exit 1
  fi

  # Allow for not removing previously-downloaded artifacts.
  # Useful on a low-bandwidth connection.
  if [ -z "$NO_REMOVE_THIRDPARTY_ARCHIVES" ]; then
    echo "Removing $FILENAME"
    rm $FILENAME
  fi
  echo
}

fetch_with_url_and_patch() {
  local FILENAME=$1
  local SOURCE=$2
  local PATCH_LEVEL=$3
  local URL_PREFIX=$4
  # Remaining args are expected to be a list of patch commands

  delete_if_wrong_patchlevel $SOURCE $PATCH_LEVEL
  if [ ! -d $SOURCE ]; then
    fetch_and_expand $FILENAME $SOURCE $URL_PREFIX
    pushd $SOURCE
    shift 4
    # Run the patch commands
    for f in "$@"; do
      eval "$f"
    done
    touch patchlevel-$PATCH_LEVEL
    popd
    echo
  fi
}

# Call fetch_with_url_and_patch with the default dependency URL source.
fetch_and_patch() {
  local FILENAME=$1
  local SOURCE=$2
  local PATCH_LEVEL=$3

  shift 3
  fetch_with_url_and_patch \
    $FILENAME \
    $SOURCE \
    $PATCH_LEVEL \
    $DEPENDENCY_URL \
    "$@"
}

mkdir -p $TP_SOURCE_DIR
cd $TP_SOURCE_DIR

GLOG_PATCHLEVEL=2
fetch_and_patch \
 glog-${GLOG_VERSION}.tar.gz \
 $GLOG_SOURCE \
 $GLOG_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/glog-make-internals-visible.patch" \
 "patch -p1 < $TP_DIR/patches/glog-support-stacktrace-for-aarch64.patch"

GMOCK_PATCHLEVEL=1
fetch_and_patch \
 googletest-release-${GMOCK_VERSION}.tar.gz \
 $GMOCK_SOURCE \
 $GMOCK_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/gmock-update-iwyu-pragma.patch"

GFLAGS_PATCHLEVEL=0
fetch_and_patch \
 gflags-${GFLAGS_VERSION}.tar.gz \
 $GFLAGS_SOURCE \
 $GFLAGS_PATCHLEVEL

GPERFTOOLS_PATCHLEVEL=1
fetch_and_patch \
 gperftools-${GPERFTOOLS_VERSION}.tar.gz \
 $GPERFTOOLS_SOURCE \
 $GPERFTOOLS_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/gperftools-Replace-namespace-base-with-namespace-tcmalloc.patch" \
 "autoreconf -fvi"

# NOTE: creating an empty 'third_party/googletest/m4' subdir is a recipe from
# the $PROTOBUF_SOURCE/autogen.sh file:
#
#   The absence of a m4 directory in googletest causes autoreconf to fail when
#   building under the CentOS docker image. It's a warning in regular build on
#   Ubuntu/gLinux as well.
#
PROTOBUF_PATCHLEVEL=0
fetch_and_patch \
 protobuf-cpp-${PROTOBUF_VERSION}.tar.gz \
 $PROTOBUF_SOURCE \
 $PROTOBUF_PATCHLEVEL \
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

CMAKE_PATCHLEVEL=1
CMAKE_PATCHES=""
if needs_patched_cmake; then \
 CMAKE_PATCHES="patch -p1 < $TP_DIR/patches/cmake-issue-15873-dont-use-select.patch"
fi

fetch_and_patch \
 cmake-${CMAKE_VERSION}.tar.gz \
 $CMAKE_SOURCE \
 $CMAKE_PATCHLEVEL \
 "$CMAKE_PATCHES"

SNAPPY_PATCHLEVEL=0
fetch_and_patch \
 snappy-${SNAPPY_VERSION}.tar.gz \
 $SNAPPY_SOURCE \
 $SNAPPY_PATCHLEVEL

ZLIB_PATCHLEVEL=0
fetch_and_patch \
 zlib-${ZLIB_VERSION}.tar.gz \
 $ZLIB_SOURCE \
 $ZLIB_PATCHLEVEL

LIBEV_PATCHLEVEL=0
fetch_and_patch \
 libev-${LIBEV_VERSION}.tar.gz \
 $LIBEV_SOURCE \
 $LIBEV_PATCHLEVEL

RAPIDJSON_PATCHLEVEL=5
fetch_and_patch \
 rapidjson-${RAPIDJSON_VERSION}.zip \
 $RAPIDJSON_SOURCE \
 $RAPIDJSON_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/rapidjson-fix-signed-unsigned-conversion-error.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-assertions-for-clang-warnings.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-avoid-pointer-arithmetic-on-null-pointer.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-document-assignment-operator-00.patch" \
 "patch -p1 < $TP_DIR/patches/rapidjson-document-assignment-operator-01.patch"

SQUEASEL_PATCHLEVEL=2
fetch_and_patch \
 squeasel-${SQUEASEL_VERSION}.tar.gz \
 $SQUEASEL_SOURCE \
 $SQUEASEL_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/squeasel-handle-openssl-errors.patch" \
 "patch -p1 < $TP_DIR/patches/squeasel-tls-min-version.patch"

MUSTACHE_PATCHLEVEL=0
fetch_and_patch \
 mustache-${MUSTACHE_VERSION}.tar.gz \
 $MUSTACHE_SOURCE \
 $MUSTACHE_PATCHLEVEL

GSG_PATCHLEVEL=3
fetch_and_patch \
 google-styleguide-${GSG_VERSION}.tar.gz \
 $GSG_SOURCE \
 $GSG_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/google-styleguide-cpplint.patch"

GCOVR_PATCHLEVEL=0
fetch_and_patch \
 gcovr-${GCOVR_VERSION}.tar.gz \
 $GCOVR_SOURCE \
 $GCOVR_PATCHLEVEL

CURL_PATCHLEVEL=3
fetch_and_patch \
 curl-${CURL_VERSION}.tar.gz \
 $CURL_SOURCE \
 $CURL_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/curl-custom-openssl-library.patch" \
 "patch -p1 < $TP_DIR/patches/curl-handle-openssl-errors.patch" \
 "patch -p1 < $TP_DIR/patches/curl-eventfd-double-close.patch" \
 "autoreconf -fvi"

CRCUTIL_PATCHLEVEL=0
fetch_and_patch \
 crcutil-${CRCUTIL_VERSION}.tar.gz \
 $CRCUTIL_SOURCE \
 $CRCUTIL_PATCHLEVEL

LIBUNWIND_PATCHLEVEL=1
fetch_and_patch \
 libunwind-${LIBUNWIND_VERSION}.tar.gz \
 $LIBUNWIND_SOURCE \
 $LIBUNWIND_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/libunwind-trace-cache-destructor.patch"

PYTHON_PATCHLEVEL=0
fetch_and_patch \
 python-${PYTHON_VERSION}.tar.gz \
 $PYTHON_SOURCE \
 $PYTHON_PATCHLEVEL

LLVM_PATCHLEVEL=8
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
 "patch -p1 < $TP_DIR/patches/llvm-chrono-duration-01.patch"

LZ4_PATCHLEVEL=0
fetch_and_patch \
 lz4-$LZ4_VERSION.tar.gz \
 $LZ4_SOURCE \
 $LZ4_PATCHLEVEL

BITSHUFFLE_PATCHLEVEL=0
fetch_and_patch \
 bitshuffle-${BITSHUFFLE_VERSION}.tar.gz \
 $BITSHUFFLE_SOURCE \
 $BITSHUFFLE_PATCHLEVEL

TRACE_VIEWER_PATCHLEVEL=0
fetch_and_patch \
 kudu-trace-viewer-${TRACE_VIEWER_VERSION}.tar.gz \
 $TRACE_VIEWER_SOURCE \
 $TRACE_VIEWER_PATCHLEVEL

BOOST_PATCHLEVEL=1
fetch_and_patch \
 boost_${BOOST_VERSION}.tar.gz \
 $BOOST_SOURCE \
 $BOOST_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/boost-bootstrap.patch"

BREAKPAD_PATCHLEVEL=7
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
 "patch -p1 < $TP_DIR/patches/breakpad-guid-creator.patch"

SPARSEHASH_PATCHLEVEL=3
fetch_and_patch \
 sparsehash-c11-${SPARSEHASH_VERSION}.tar.gz \
 $SPARSEHASH_SOURCE \
 $SPARSEHASH_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/sparsehash-0001-Add-compatibily-for-gcc-4.x-in-traits.patch" \
 "patch -p1 < $TP_DIR/patches/sparsehash-0002-Add-workaround-for-dense_hashtable-move-constructor-.patch"

SPARSEPP_PATCHLEVEL=0
fetch_and_patch \
 sparsepp-${SPARSEPP_VERSION}.tar.gz \
 $SPARSEPP_SOURCE \
 $SPARSEPP_PATCHLEVEL

THRIFT_PATCHLEVEL=0
fetch_and_patch \
 $THRIFT_NAME.tar.gz \
 $THRIFT_SOURCE \
 $THRIFT_PATCHLEVEL

BISON_PATCHLEVEL=0
fetch_and_patch \
 $BISON_NAME.tar.gz \
 $BISON_SOURCE \
 $BISON_PATCHLEVEL
 # This would normally call autoreconf, but it does not succeed with
 # autoreconf 2.69-11 (RHEL 7): "autoreconf: 'configure.ac' or 'configure.in' is required".

HIVE_PATCHLEVEL=0
fetch_and_patch \
 $HIVE_NAME-stripped.tar.gz \
 $HIVE_SOURCE \
 $HIVE_PATCHLEVEL

HADOOP_PATCHLEVEL=0
fetch_and_patch \
 $HADOOP_NAME-stripped.tar.gz \
 $HADOOP_SOURCE \
 $HADOOP_PATCHLEVEL

YAML_PATCHLEVEL=0
fetch_and_patch \
 $YAML_NAME.tar.gz \
 $YAML_SOURCE \
 $YAML_PATCHLEVEL

CHRONY_PATCHLEVEL=1
fetch_and_patch \
 $CHRONY_NAME.tar.gz \
 $CHRONY_SOURCE \
 $CHRONY_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/chrony-reuseport.patch"

GUMBO_PARSER_PATCHLEVEL=1
fetch_and_patch \
 $GUMBO_PARSER_NAME.tar.gz \
 $GUMBO_PARSER_SOURCE \
 $GUMBO_PARSER_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/gumbo-parser-autoconf-263.patch" \
 "autoreconf -fvi"

GUMBO_QUERY_PATCHLEVEL=1
fetch_and_patch \
 $GUMBO_QUERY_NAME.tar.gz \
 $GUMBO_QUERY_SOURCE \
 $GUMBO_QUERY_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/gumbo-query-namespace.patch"

POSTGRES_PATCHLEVEL=1
fetch_and_patch \
 $POSTGRES_NAME.tar.gz \
 $POSTGRES_SOURCE \
 $POSTGRES_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/postgres-root-can-run-initdb.patch" \
 "patch -p0 < $TP_DIR/patches/postgres-no-check-root.patch"

POSTGRES_JDBC_PATCHLEVEL=0
fetch_and_patch \
 $POSTGRES_JDBC_NAME.jar \
 $POSTGRES_JDBC_SOURCE \
 $POSTGRES_JDBC_PATCHLEVEL

RANGER_PATCHLEVEL=2
fetch_and_patch \
 $RANGER_NAME.tar.gz \
 $RANGER_SOURCE \
 $RANGER_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/ranger-fixscripts.patch"

JWT_CPP_PATCHLEVEL=0
fetch_and_patch \
 $JWT_CPP_NAME.tar.gz \
 $JWT_CPP_SOURCE \
 $JWT_CPP_PATCHLEVEL

RANGER_KMS_PATCHLEVEL=0
fetch_and_patch \
 $RANGER_KMS_NAME.tar.gz \
 $RANGER_KMS_SOURCE \
 $RANGER_KMS_PATCHLEVEL

ROCKSDB_PATCHLEVEL=1
fetch_and_patch \
 $ROCKSDB_NAME.tar.gz \
 $ROCKSDB_SOURCE \
 $ROCKSDB_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/rocksdb-gcc13.patch"

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"

