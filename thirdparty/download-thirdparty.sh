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

mkdir -p $TP_SOURCE_DIR
cd $TP_SOURCE_DIR

GLOG_PATCHLEVEL=3
delete_if_wrong_patchlevel $GLOG_SOURCE $GLOG_PATCHLEVEL
if [ ! -d $GLOG_SOURCE ]; then
  fetch_and_expand glog-${GLOG_VERSION}.tar.gz

  pushd $GLOG_SOURCE
  patch -p0 < $TP_DIR/patches/glog-issue-198-fix-unused-warnings.patch
  patch -p0 < $TP_DIR/patches/glog-issue-54-dont-build-tests.patch
  patch -p1 < $TP_DIR/patches/glog-cda16b3443e2d6ef88cdbbe10b9a11adea6f33fe.patch
  touch patchlevel-$GLOG_PATCHLEVEL
  autoreconf -fvi
  popd
  echo
fi

if [ ! -d $GMOCK_SOURCE ]; then
  fetch_and_expand gmock-${GMOCK_VERSION}.zip
fi

if [ ! -d $GFLAGS_SOURCE ]; then
  fetch_and_expand gflags-${GFLAGS_VERSION}.tar.gz
fi

GPERFTOOLS_PATCHLEVEL=3
delete_if_wrong_patchlevel $GPERFTOOLS_SOURCE $GPERFTOOLS_PATCHLEVEL
if [ ! -d $GPERFTOOLS_SOURCE ]; then
  fetch_and_expand gperftools-${GPERFTOOLS_VERSION}.tar.gz

  pushd $GPERFTOOLS_SOURCE
  patch -p1 < $TP_DIR/patches/gperftools-Change-default-TCMALLOC_TRANSFER_NUM_OBJ-to-40.patch
  patch -p1 < $TP_DIR/patches/gperftools-hook-mi_force_unlock-on-OSX-instead-of-pthread_atfork.patch
  patch -p1 < $TP_DIR/patches/gperftools-issue-827-add_get_default_zone_to_osx_libc_override.patch
  touch patchlevel-$GPERFTOOLS_PATCHLEVEL
  autoreconf -fvi
  popd
  echo
fi

if [ ! -d $PROTOBUF_SOURCE ]; then
  fetch_and_expand protobuf-${PROTOBUF_VERSION}.tar.gz
  pushd $PROTOBUF_SOURCE
  autoreconf -fvi
  popd
fi

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
delete_if_wrong_patchlevel $CMAKE_SOURCE $CMAKE_PATCHLEVEL
if [ ! -d $CMAKE_SOURCE ]; then
  fetch_and_expand cmake-${CMAKE_VERSION}.tar.gz
  pushd $CMAKE_SOURCE
  if needs_patched_cmake; then
    patch -p1 < $TP_DIR/patches/cmake-issue-15873-dont-use-select.patch
  fi
  # Write the patchlevel file even if the patch was skipped, so as to simplify
  # future patch numbering.
  touch patchlevel-$CMAKE_PATCHLEVEL
  popd
fi

if [ ! -d $SNAPPY_SOURCE ]; then
  fetch_and_expand snappy-${SNAPPY_VERSION}.tar.gz
  pushd $SNAPPY_SOURCE
  autoreconf -fvi
  popd
fi

if [ ! -d $ZLIB_SOURCE ]; then
  fetch_and_expand zlib-${ZLIB_VERSION}.tar.gz
fi

if [ ! -d $LIBEV_SOURCE ]; then
  fetch_and_expand libev-${LIBEV_VERSION}.tar.gz
fi

if [ ! -d $RAPIDJSON_SOURCE ]; then
  fetch_and_expand rapidjson-${RAPIDJSON_VERSION}.zip
  mv rapidjson ${RAPIDJSON_SOURCE}
fi

if [ ! -d $SQUEASEL_SOURCE ]; then
  fetch_and_expand squeasel-${SQUEASEL_VERSION}.tar.gz
fi

if [ ! -d $GSG_SOURCE ]; then
  fetch_and_expand google-styleguide-${GSG_VERSION}.tar.gz
fi

if [ ! -d $GCOVR_SOURCE ]; then
  fetch_and_expand gcovr-${GCOVR_VERSION}.tar.gz
fi

if [ ! -d $CURL_SOURCE ]; then
  fetch_and_expand curl-${CURL_VERSION}.tar.gz
fi

CRCUTIL_PATCHLEVEL=1
delete_if_wrong_patchlevel $CRCUTIL_SOURCE $CRCUTIL_PATCHLEVEL
if [ ! -d $CRCUTIL_SOURCE ]; then
  fetch_and_expand crcutil-${CRCUTIL_VERSION}.tar.gz

  pushd $CRCUTIL_SOURCE
  patch -p0 < $TP_DIR/patches/crcutil-fix-libtoolize-on-osx.patch
  touch patchlevel-$CRCUTIL_PATCHLEVEL
  popd
  echo
fi

if [ ! -d $LIBUNWIND_SOURCE ]; then
  fetch_and_expand libunwind-${LIBUNWIND_VERSION}.tar.gz
fi

if [ ! -d $PYTHON_SOURCE ]; then
  fetch_and_expand python-${PYTHON_VERSION}.tar.gz
fi

LLVM_PATCHLEVEL=2
delete_if_wrong_patchlevel $LLVM_SOURCE $LLVM_PATCHLEVEL
if [ ! -d $LLVM_SOURCE ]; then
  fetch_and_expand llvm-${LLVM_VERSION}.src.tar.gz

  pushd $LLVM_SOURCE
  patch -p1 < $TP_DIR/patches/llvm-fix-amazon-linux.patch
  pushd projects/libcxx
  patch -p1 < $TP_DIR/patches/libc++-fix-std-once-barriers.patch
  popd
  touch patchlevel-$LLVM_PATCHLEVEL
  popd
  echo
fi

LZ4_PATCHLEVEL=1
delete_if_wrong_patchlevel $LZ4_SOURCE $LZ4_PATCHLEVEL
if [ ! -d $LZ4_SOURCE ]; then
  fetch_and_expand lz4-lz4-$LZ4_VERSION.tar.gz
  pushd $LZ4_SOURCE
  patch -p1 < $TP_DIR/patches/lz4-0001-Fix-cmake-build-to-use-gnu-flags-on-clang.patch
  touch patchlevel-$LZ4_PATCHLEVEL
  popd
  echo
fi

if [ ! -d $BITSHUFFLE_SOURCE ]; then
  fetch_and_expand bitshuffle-${BITSHUFFLE_VERSION}.tar.gz
fi

if [ ! -d $TRACE_VIEWER_SOURCE ]; then
  fetch_and_expand kudu-trace-viewer-${TRACE_VIEWER_VERSION}.tar.gz
fi

if [ -n "$OS_LINUX" -a ! -d $NVML_SOURCE ]; then
  fetch_and_expand nvml-${NVML_VERSION}.tar.gz
fi

BOOST_PATCHLEVEL=1
delete_if_wrong_patchlevel $BOOST_SOURCE $BOOST_PATCHLEVEL
if [ ! -d $BOOST_SOURCE ]; then
  fetch_and_expand boost_${BOOST_VERSION}.tar.gz
  pushd $BOOST_SOURCE
  patch -p0 < $TP_DIR/patches/boost-issue-12179-fix-compilation-errors.patch
  touch patchlevel-$BOOST_PATCHLEVEL
  popd
  echo
fi

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

if [ ! -d "$BREAKPAD_SOURCE" ]; then
  fetch_and_expand breakpad-${BREAKPAD_VERSION}.tar.gz
fi

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
