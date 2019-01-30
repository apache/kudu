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

GLOG_PATCHLEVEL=3
fetch_and_patch \
 glog-${GLOG_VERSION}.tar.gz \
 $GLOG_SOURCE \
 $GLOG_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/glog-issue-198-fix-unused-warnings.patch" \
 "patch -p0 < $TP_DIR/patches/glog-issue-54-dont-build-tests.patch" \
 "patch -p1 < $TP_DIR/patches/glog-fix-symbolization.patch" \
 "autoreconf -fvi"

GMOCK_PATCHLEVEL=0
fetch_and_patch \
 googletest-release-${GMOCK_VERSION}.tar.gz \
 $GMOCK_SOURCE \
 $GMOCK_PATCHLEVEL

GFLAGS_PATCHLEVEL=0
fetch_and_patch \
 gflags-${GFLAGS_VERSION}.tar.gz \
 $GFLAGS_SOURCE \
 $GFLAGS_PATCHLEVEL

GPERFTOOLS_PATCHLEVEL=2
fetch_and_patch \
 gperftools-${GPERFTOOLS_VERSION}.tar.gz \
 $GPERFTOOLS_SOURCE \
 $GPERFTOOLS_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/gperftools-Replace-namespace-base-with-namespace-tcmalloc.patch" \
 "patch -p1 < $TP_DIR/patches/gperftools-unbreak-memz.patch" \
 "autoreconf -fvi"

PROTOBUF_PATCHLEVEL=0
fetch_and_patch \
 protobuf-${PROTOBUF_VERSION}.tar.gz \
 $PROTOBUF_SOURCE \
 $PROTOBUF_PATCHLEVEL \
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
 $SNAPPY_PATCHLEVEL \
 "autoreconf -fvi"

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

RAPIDJSON_PATCHLEVEL=0
fetch_and_patch \
 rapidjson-${RAPIDJSON_VERSION}.zip \
 $RAPIDJSON_SOURCE \
 $RAPIDJSON_PATCHLEVEL

SQUEASEL_PATCHLEVEL=0
fetch_and_patch \
 squeasel-${SQUEASEL_VERSION}.tar.gz \
 $SQUEASEL_SOURCE \
 $SQUEASEL_PATCHLEVEL

MUSTACHE_PATCHLEVEL=0
fetch_and_patch \
 mustache-${MUSTACHE_VERSION}.tar.gz \
 $MUSTACHE_SOURCE \
 $MUSTACHE_PATCHLEVEL

GSG_PATCHLEVEL=2
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

CURL_PATCHLEVEL=0
fetch_and_patch \
 curl-${CURL_VERSION}.tar.gz \
 $CURL_SOURCE \
 $CURL_PATCHLEVEL \
 "autoreconf -fvi"

CRCUTIL_PATCHLEVEL=1
fetch_and_patch \
 crcutil-${CRCUTIL_VERSION}.tar.gz \
 $CRCUTIL_SOURCE \
 $CRCUTIL_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/crcutil-fix-libtoolize-on-osx.patch"

LIBUNWIND_PATCHLEVEL=2
fetch_and_patch \
 libunwind-${LIBUNWIND_VERSION}.tar.gz \
 $LIBUNWIND_SOURCE \
 $LIBUNWIND_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/libunwind-Use-syscall-directly-in-write_validate-to-avoid-ASAN.patch" \
 "patch -p1 < $TP_DIR/patches/libunwind-trace-cache-destructor.patch"

PYTHON_PATCHLEVEL=0
fetch_and_patch \
 python-${PYTHON_VERSION}.tar.gz \
 $PYTHON_SOURCE \
 $PYTHON_PATCHLEVEL

LLVM_PATCHLEVEL=2
fetch_and_patch \
 llvm-${LLVM_VERSION}-iwyu-${IWYU_VERSION}.src.tar.gz \
 $LLVM_SOURCE \
 $LLVM_PATCHLEVEL \
  "patch -p1 < $TP_DIR/patches/llvm-fix-amazon-linux.patch" \
  "patch -p1 < $TP_DIR/patches/llvm-add-iwyu.patch" \
  "patch -p1 < $TP_DIR/patches/llvm-iwyu-nocurses.patch" \
  "patch -p1 < $TP_DIR/patches/llvm-iwyu-include-picker.patch" \
  "patch -d tools/clang/tools/include-what-you-use -p1 < $TP_DIR/patches/llvm-iwyu-llvm-6-compat.patch" \
  "patch -d projects/compiler-rt -p1 < $TP_DIR/patches/llvm-tsan-disable-trace-switching-after-multithreaded-for.patch"

LZ4_PATCHLEVEL=1
fetch_and_patch \
 lz4-lz4-$LZ4_VERSION.tar.gz \
 $LZ4_SOURCE \
 $LZ4_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/lz4-0001-Fix-cmake-build-to-use-gnu-flags-on-clang.patch"

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

NVML_PATCHLEVEL=0
fetch_and_patch \
 nvml-${NVML_VERSION}.tar.gz \
 $NVML_SOURCE \
 $NVML_PATCHLEVEL

BOOST_PATCHLEVEL=1
fetch_and_patch \
 boost_${BOOST_VERSION}.tar.gz \
 $BOOST_SOURCE \
 $BOOST_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/boost-issue-12179-fix-compilation-errors.patch"

# Return 0 if the current system appears to be el6 (either CentOS or proper RHEL)
needs_openssl_workaround() {
  test -f /etc/redhat-release || return 1
  rel="$(cat /etc/redhat-release)"
  pat="(CentOS|Red Hat Enterprise).* release 6.*"
  [[ "$rel" =~ $pat ]]
  return $?
}
if needs_openssl_workaround && [ ! -d "$OPENSSL_WORKAROUND_DIR" ] ; then
  echo Building on el6: installing OpenSSL from CentOS 6.4.
  $TP_DIR/install-openssl-el6-workaround.sh
fi

BREAKPAD_PATCHLEVEL=1
fetch_and_patch \
 breakpad-${BREAKPAD_VERSION}.tar.gz \
 $BREAKPAD_SOURCE \
 $BREAKPAD_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/breakpad-add-basic-support-for-dwz-dwarf-extension.patch"

SPARSEHASH_PATCHLEVEL=2
fetch_and_patch \
 sparsehash-c11-${SPARSEHASH_VERSION}.tar.gz \
 $SPARSEHASH_SOURCE \
 $SPARSEHASH_PATCHLEVEL \
 "patch -p1 < $TP_DIR/patches/sparsehash-0001-Add-compatibily-for-gcc-4.x-in-traits.patch"

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

BISON_PATCHLEVEL=1
fetch_and_patch \
 $BISON_NAME.tar.gz \
 $BISON_SOURCE \
 $BISON_PATCHLEVEL \
 "patch -p0 < $TP_DIR/patches/bison-fix-high-sierra-compilation-issue.patch"
 # Fix compilation issue in macOS High Sierra
 # See: https://github.com/spack/spack/issues/5521
 # This would normally call autoreconf, but it does not succeed with
 # autoreconf 2.69 (RHEL 7): "autoreconf: 'configure.ac' or 'configure.in' is required".

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

SENTRY_PATCHLEVEL=0
fetch_and_patch \
 $SENTRY_NAME.tar.gz \
 $SENTRY_SOURCE \
 $SENTRY_PATCHLEVEL

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
