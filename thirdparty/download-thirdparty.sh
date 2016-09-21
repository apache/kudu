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
cd $TP_DIR

if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
fi

source vars.sh

delete_if_wrong_patchlevel() {
  local DIR=$1
  local PATCHLEVEL=$2
  if [ ! -f $DIR/patchlevel-$PATCHLEVEL ]; then
    echo It appears that $DIR is missing the latest local patches.
    echo Removing it so we re-download it.
    rm -Rf $DIR
  fi
}

fetch_and_expand() {
  local FILENAME=$1
  if [ -z "$FILENAME" ]; then
    echo "Error: Must specify file to fetch"
    exit 1
  fi

  TAR_CMD=tar
  if [[ "$OSTYPE" == "darwin"* ]] && which gtar &>/dev/null; then
    TAR_CMD=gtar
  fi

  FULL_URL="${CLOUDFRONT_URL_PREFIX}/${FILENAME}"
  SUCCESS=0
  # Loop in case we encounter a corrupted archive and we need to re-download it.
  for attempt in 1 2; do
    if [ -r "$FILENAME" ]; then
      echo "Archive $FILENAME already exists. Not re-downloading archive."
    else
      echo "Fetching $FILENAME from $FULL_URL"
      curl -L -O "$FULL_URL"
    fi

    echo "Unpacking $FILENAME"
    if [[ "$FILENAME" =~ \.zip$ ]]; then
      if ! unzip -q "$FILENAME"; then
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

GLOG_PATCHLEVEL=1
delete_if_wrong_patchlevel $GLOG_DIR $GLOG_PATCHLEVEL
if [ ! -d $GLOG_DIR ]; then
  fetch_and_expand glog-${GLOG_VERSION}.tar.gz

  pushd $GLOG_DIR
  patch -p0 < $TP_DIR/patches/glog-issue-198-fix-unused-warnings.patch
  touch patchlevel-$GLOG_PATCHLEVEL
  autoreconf -fvi
  popd
  echo
fi

if [ ! -d $GMOCK_DIR ]; then
  fetch_and_expand gmock-${GMOCK_VERSION}.zip
fi

if [ ! -d $GFLAGS_DIR ]; then
  fetch_and_expand gflags-${GFLAGS_VERSION}.tar.gz
fi

# Check that the gperftools patch has been applied.
# If you add or remove patches, bump the patchlevel below to ensure
# that any new Jenkins builds pick up your patches.
GPERFTOOLS_PATCHLEVEL=3
delete_if_wrong_patchlevel $GPERFTOOLS_DIR $GPERFTOOLS_PATCHLEVEL
if [ ! -d $GPERFTOOLS_DIR ]; then
  fetch_and_expand gperftools-${GPERFTOOLS_VERSION}.tar.gz

  pushd $GPERFTOOLS_DIR
  patch -p1 < $TP_DIR/patches/gperftools-Change-default-TCMALLOC_TRANSFER_NUM_OBJ-to-40.patch
  patch -p1 < $TP_DIR/patches/gperftools-hook-mi_force_unlock-on-OSX-instead-of-pthread_atfork.patch
  patch -p1 < $TP_DIR/patches/gperftools-issue-827-add_get_default_zone_to_osx_libc_override.patch
  touch patchlevel-$GPERFTOOLS_PATCHLEVEL
  autoreconf -fvi
  popd
  echo
fi

if [ ! -d $PROTOBUF_DIR ]; then
  fetch_and_expand protobuf-${PROTOBUF_VERSION}.tar.gz
  pushd $PROTOBUF_DIR
  autoreconf -fvi
  popd
fi

if [ ! -d $CMAKE_DIR ]; then
  fetch_and_expand cmake-${CMAKE_VERSION}.tar.gz
fi

if [ ! -d $SNAPPY_DIR ]; then
  fetch_and_expand snappy-${SNAPPY_VERSION}.tar.gz
  pushd $SNAPPY_DIR
  autoreconf -fvi
  popd
fi

if [ ! -d $ZLIB_DIR ]; then
  fetch_and_expand zlib-${ZLIB_VERSION}.tar.gz
fi

if [ ! -d $LIBEV_DIR ]; then
  fetch_and_expand libev-${LIBEV_VERSION}.tar.gz
fi

if [ ! -d $RAPIDJSON_DIR ]; then
  fetch_and_expand rapidjson-${RAPIDJSON_VERSION}.zip
  mv rapidjson ${RAPIDJSON_DIR}
fi

if [ ! -d $SQUEASEL_DIR ]; then
  fetch_and_expand squeasel-${SQUEASEL_VERSION}.tar.gz
fi

if [ ! -d $GSG_DIR ]; then
  fetch_and_expand google-styleguide-${GSG_VERSION}.tar.gz
fi

if [ ! -d $GCOVR_DIR ]; then
  fetch_and_expand gcovr-${GCOVR_VERSION}.tar.gz
fi

if [ ! -d $CURL_DIR ]; then
  fetch_and_expand curl-${CURL_VERSION}.tar.gz
fi

CRCUTIL_PATCHLEVEL=1
delete_if_wrong_patchlevel $CRCUTIL_DIR $CRCUTIL_PATCHLEVEL
if [ ! -d $CRCUTIL_DIR ]; then
  fetch_and_expand crcutil-${CRCUTIL_VERSION}.tar.gz

  pushd $CRCUTIL_DIR
  patch -p0 < $TP_DIR/patches/crcutil-fix-libtoolize-on-osx.patch
  touch patchlevel-$CRCUTIL_PATCHLEVEL
  popd
  echo
fi

if [ ! -d $LIBUNWIND_DIR ]; then
  fetch_and_expand libunwind-${LIBUNWIND_VERSION}.tar.gz
fi

if [ ! -d $PYTHON_DIR ]; then
  fetch_and_expand python-${PYTHON_VERSION}.tar.gz
fi

LLVM_PATCHLEVEL=2
delete_if_wrong_patchlevel $LLVM_DIR $LLVM_PATCHLEVEL
if [ ! -d $LLVM_DIR ]; then
  fetch_and_expand llvm-${LLVM_VERSION}.src.tar.gz

  pushd $LLVM_DIR
  patch -p1 < $TP_DIR/patches/llvm-fix-amazon-linux.patch
  patch -p1 < $TP_DIR/patches/llvm-devtoolset-toolchain.patch
  touch patchlevel-$LLVM_PATCHLEVEL
  popd
  echo
fi

GCC_PATCHLEVEL=2
delete_if_wrong_patchlevel $GCC_DIR $GCC_PATCHLEVEL
if [[ "$OSTYPE" =~ ^linux ]] && [[ ! -d $GCC_DIR ]]; then
  fetch_and_expand gcc-${GCC_VERSION}.tar.gz
  pushd $GCC_DIR/libstdc++-v3
  patch -p0 < $TP_DIR/patches/libstdcxx-fix-string-dtor.patch
  patch -p0 < $TP_DIR/patches/libstdcxx-fix-tr1-shared-ptr.patch
  cd ..
  touch patchlevel-$GCC_PATCHLEVEL
  popd
  echo
fi

LZ4_PATCHLEVEL=1
delete_if_wrong_patchlevel $LZ4_DIR $LZ4_PATCHLEVEL
if [ ! -d $LZ4_DIR ]; then
  fetch_and_expand lz4-lz4-$LZ4_VERSION.tar.gz
  pushd $LZ4_DIR
  patch -p1 < $TP_DIR/patches/lz4-0001-Fix-cmake-build-to-use-gnu-flags-on-clang.patch
  touch patchlevel-$LZ4_PATCHLEVEL
  popd
  echo
fi

if [ ! -d $BITSHUFFLE_DIR ]; then
  fetch_and_expand bitshuffle-${BITSHUFFLE_VERSION}.tar.gz
fi

if [ ! -d $TRACE_VIEWER_DIR ]; then
  fetch_and_expand kudu-trace-viewer-${TRACE_VIEWER_VERSION}.tar.gz
fi

if [ -n "$OS_LINUX" -a ! -d $NVML_DIR ]; then
  fetch_and_expand nvml-${NVML_VERSION}.tar.gz
fi

BOOST_PATCHLEVEL=1
delete_if_wrong_patchlevel $BOOST_DIR $BOOST_PATCHLEVEL
if [ ! -d $BOOST_DIR ]; then
  fetch_and_expand boost_${BOOST_VERSION}.tar.gz
  pushd $BOOST_DIR
  patch -p0 < $TP_DIR/patches/boost-issue-12179-fix-compilation-errors.patch
  touch patchlevel-$BOOST_PATCHLEVEL
  popd
  echo
fi

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
