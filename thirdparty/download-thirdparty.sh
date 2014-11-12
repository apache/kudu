#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.
#
# Note regarding the autoreconf calls:
#
# Mac OS 10.8 shipped a clang-based C++ toolchain that provides both GNU libstdc++ and LLVM's
# libc++. The default library used is libstdc++, though one can override that with the
# -stdlib=libc++ option to clang (while compiling and linking). In 10.9, the default policy has
# switched: libc++ is now the default, and to use libstdc++ you need to pass -stdlib=libstdc++
# as a command line option.
#
# This is relevant to Kudu because libc++ does not support tr1; to use tr1 features like tr1/memory
# we must use them from the C++11 namespace (i.e. <memory> instead of <tr1/memory> and -std=c++11
# to clang).
#
# Setting CXXFLAGS=-stdlib=libstdc++ suffices for an autotools-based project, and this is what we do
# in build-thirdparty.sh. However, older versions of autotools will filter out -stdlib=libstdc++
# from a shared library link invocation. This leads to link failures with every std symbol listed
# as "undefined". To fix this, one must regenerate the autotools system for each library on a
# machine with modern brews of autotools. Running "autoconf -fvi" inside the library's directory
# is sufficient. See this link for more information:
#
# http://trac.macports.org/ticket/32982
#
# This is why all the cmake-based projects have their autotools regenerated with "autoreconf -fvi".

set -e

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
cd $TP_DIR

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

  echo "Fetching $FILENAME"
  curl -O "${CLOUDFRONT_URL_PREFIX}/${FILENAME}"

  echo "Unpacking $FILENAME"
  if echo "$FILENAME" | egrep -q '\.zip$'; then
    unzip -q $FILENAME
  elif echo "$FILENAME" | egrep -q '(\.tar\.gz|\.tgz)$'; then
    tar xf $FILENAME
  else
    echo "Error: unknown file format: $FILENAME"
    exit 1
  fi

  echo "Removing $FILENAME"
  rm $FILENAME
  echo
}

GLOG_PATCHLEVEL=1
delete_if_wrong_patchlevel glog-${GLOG_VERSION} $GLOG_PATCHLEVEL
if [ ! -d glog-${GLOG_VERSION} ]; then
  fetch_and_expand glog-${GLOG_VERSION}.tar.gz

  pushd glog-${GLOG_VERSION}
  patch -p0 < $TP_DIR/patches/glog-issue-198-fix-unused-warnings.patch
  touch patchlevel-$GLOG_PATCHLEVEL
  autoreconf -fvi
  popd
  echo
fi

if [ ! -d gmock-${GMOCK_VERSION} ]; then
  fetch_and_expand gmock-${GMOCK_VERSION}.zip
fi

if [ ! -d gflags-${GFLAGS_VERSION} ]; then
  fetch_and_expand gflags-${GFLAGS_VERSION}.zip
  pushd gflags-${GFLAGS_VERSION}
  autoreconf -fvi
  popd
fi

# Check that the gperftools patch has been applied.
# If you add or remove patches, bump the patchlevel below to ensure
# that any new Jenkins builds pick up your patches.
GPERFTOOLS_PATCHLEVEL=1
delete_if_wrong_patchlevel gperftools-${GPERFTOOLS_VERSION} $GPERFTOOLS_PATCHLEVEL

if [ ! -d gperftools-${GPERFTOOLS_VERSION} ]; then
  fetch_and_expand gperftools-${GPERFTOOLS_VERSION}.tar.gz

  pushd gperftools-${GPERFTOOLS_VERSION}
  patch -p1 < $TP_DIR/patches/gperftools-Change-default-TCMALLOC_TRANSFER_NUM_OBJ-to-40.patch
  touch patchlevel-$GPERFTOOLS_PATCHLEVEL
  autoreconf -fvi
  popd
  echo
fi

if [ ! -d protobuf-${PROTOBUF_VERSION} ]; then
  fetch_and_expand protobuf-${PROTOBUF_VERSION}.tar.gz
  pushd protobuf-${PROTOBUF_VERSION}
  autoreconf -fvi
  popd
fi

if [ ! -d cmake-${CMAKE_VERSION} ]; then
  fetch_and_expand cmake-${CMAKE_VERSION}.tar.gz
fi

if [ ! -d snappy-${SNAPPY_VERSION} ]; then
  fetch_and_expand snappy-${SNAPPY_VERSION}.tar.gz
  pushd snappy-${SNAPPY_VERSION}
  autoreconf -fvi
  popd
fi

if [ ! -d zlib-${ZLIB_VERSION} ]; then
  fetch_and_expand zlib-${ZLIB_VERSION}.tar.gz
fi

if [ ! -d libev-${LIBEV_VERSION} ]; then
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
  fetch_and_expand google-styleguide-r${GSG_REVISION}.tar.gz
fi

if [ ! -d $GCOVR_DIR ]; then
  fetch_and_expand gcovr-${GCOVR_VERSION}.tar.gz
fi

if [ ! -d $CURL_DIR ]; then
  fetch_and_expand curl-${CURL_VERSION}.tar.gz
fi

CRCUTIL_PATCHLEVEL=1
delete_if_wrong_patchlevel crcutil-${CRCUTIL_VERSION} $CRCUTIL_PATCHLEVEL
if [ ! -d $CRCUTIL_DIR ]; then
  fetch_and_expand crcutil-${CRCUTIL_VERSION}.tar.gz

  pushd crcutil-${CRCUTIL_VERSION}
  patch -p0 < $TP_DIR/patches/crcutil-fix-libtoolize-on-osx.patch
  touch patchlevel-$CRCUTIL_PATCHLEVEL
  popd
  echo
fi

if [ ! -d $LIBUNWIND_DIR ]; then
  fetch_and_expand libunwind-${LIBUNWIND_VERSION}.tar.gz
fi

if [ ! -d $LLVM_DIR ]; then
  fetch_and_expand llvm-${LLVM_VERSION}.src.tar.gz
fi

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
