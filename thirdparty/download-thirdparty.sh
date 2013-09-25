#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.

set -x
set -e

TP_DIR=$(readlink -f $(dirname $BASH_SOURCE))
cd $TP_DIR

source vars.sh

TARBALL="cyrus-sasl-${CYRUS_SASL_VERSION}.tar.gz"
if [ ! -f "$TARBALL" ]; then
  echo "Fetching cyrus-sasl"
  wget ftp://ftp.cyrusimap.org/cyrus-sasl/$TARBALL
fi
if [ -d cyrus-sasl-${CYRUS_SASL_VERSION} ]; then
  # jenkins / rhel has problems if we don't build from pristine every time
  rm -rf cyrus-sasl-${CYRUS_SASL_VERSION}
fi
tar xvzf $TARBALL
unset TARBALL

if [ ! -d gtest-${GTEST_VERSION} ]; then
  echo "Fetching gtest"
  wget -c http://googletest.googlecode.com/files/gtest-${GTEST_VERSION}.zip
  unzip gtest-${GTEST_VERSION}.zip
  rm gtest-${GTEST_VERSION}.zip
fi

if [ ! -d glog-${GLOG_VERSION} ]; then
  echo "Fetching glog"
  wget -c http://google-glog.googlecode.com/files/glog-${GLOG_VERSION}.tar.gz
  tar xzf glog-${GLOG_VERSION}.tar.gz
  rm glog-${GLOG_VERSION}.tar.gz
fi

if [ ! -d gflags-${GFLAGS_VERSION} ]; then
  echo "Fetching gflags"
  wget -c http://gflags.googlecode.com/files/gflags-${GFLAGS_VERSION}.zip
  unzip gflags-${GFLAGS_VERSION}.zip
  rm gflags-${GFLAGS_VERSION}.zip
fi

# Check that the gperftools patch has been applied.
# If you add or remove patches, bump the patchlevel below to ensure
# that any new Jenkins builds pick up your patches.
GPERFTOOLS_PATCHLEVEL=1
if [ ! -f gperftools-${GPERFTOOLS_VERSION}/patchlevel-$GPERFTOOLS_PATCHLEVEL ]; then
  echo It appears that the gperftools version we have is missing
  echo the latest local patches. Removing it so we re-download it.
  rm -Rf gperftools-${GPERFTOOLS_VERSION}
fi

if [ ! -d gperftools-${GPERFTOOLS_VERSION} ]; then
  echo "Fetching gperftools"
  wget -c http://gperftools.googlecode.com/files/gperftools-${GPERFTOOLS_VERSION}.tar.gz
  tar xzf gperftools-${GPERFTOOLS_VERSION}.tar.gz
  rm gperftools-${GPERFTOOLS_VERSION}.tar.gz
  pushd gperftools-${GPERFTOOLS_VERSION}
  patch -p1 < $TP_DIR/patches/gperftools-issue-560-Revert-issue-481.patch
  touch patchlevel-$GPERFTOOLS_PATCHLEVEL
  popd
fi

if [ ! -d protobuf-${PROTOBUF_VERSION} ]; then
  echo "Fetching protobuf"
  wget -c http://protobuf.googlecode.com/files/protobuf-${PROTOBUF_VERSION}.tar.gz
  tar xzf protobuf-${PROTOBUF_VERSION}.tar.gz
  rm protobuf-${PROTOBUF_VERSION}.tar.gz
fi

if [ ! -d cmake-${CMAKE_VERSION} ]; then
  echo "Fetching cmake"
  wget -c http://www.cmake.org/files/v2.8/cmake-${CMAKE_VERSION}.tar.gz
  tar xzf cmake-${CMAKE_VERSION}.tar.gz
  rm cmake-${CMAKE_VERSION}.tar.gz
fi

if [ ! -d snappy-${SNAPPY_VERSION} ]; then
  echo "Fetching snappy"
  wget -c http://snappy.googlecode.com/files/snappy-${SNAPPY_VERSION}.tar.gz
  tar xzf snappy-${SNAPPY_VERSION}.tar.gz
  rm snappy-${SNAPPY_VERSION}.tar.gz
fi

if [ ! -d zlib-${ZLIB_VERSION} ]; then
  echo "Fetching zlib"
  wget -c http://zlib.net/zlib-${ZLIB_VERSION}.tar.gz
  tar xzf zlib-${ZLIB_VERSION}.tar.gz
  rm zlib-${ZLIB_VERSION}.tar.gz
fi

if [ ! -d libev-${LIBEV_VERSION} ]; then
  echo "Fetching libev"
  wget http://dist.schmorp.de/libev/libev-${LIBEV_VERSION}.tar.gz
  tar xvzf libev-${LIBEV_VERSION}.tar.gz
  rm libev-${LIBEV_VERSION}.tar.gz
fi

if [ ! -d $RAPIDJSON_DIR ]; then
  echo "Fetching rapidjson"
  wget https://rapidjson.googlecode.com/files/rapidjson-${RAPIDJSON_VERSION}.zip
  unzip rapidjson-${RAPIDJSON_VERSION}.zip
  mv rapidjson ${RAPIDJSON_DIR}
  rm rapidjson-${RAPIDJSON_VERSION}.zip
fi

if [ ! -d $SQUEASEL_DIR ]; then
  echo "Fetching squeasel"
  # Ideally we'd fetch from our github repo directly, but our version of
  # GitHub Enterprise doesn't support this feature yet (see HD-7469).
  wget -O squeasel-${SQUEASEL_VERSION}.tar.gz http://cloudera-todd.s3.amazonaws.com/squeasel-${SQUEASEL_VERSION}.tar.gz
  tar xzf squeasel-$SQUEASEL_VERSION.tar.gz
  rm squeasel-$SQUEASEL_VERSION.tar.gz
fi

if [ ! -d $GSG_DIR ]; then
  echo "Fetching google style guide"
  svn export --force -r ${GSG_REVISION} http://google-styleguide.googlecode.com/svn/trunk/ ${GSG_DIR}.tmp
  mv ${GSG_DIR}.tmp ${GSG_DIR}
fi

if [ ! -d $GCOVR_DIR ]; then
  echo "Fetching gcovr"
  wget -O gcovr-${GCOVR_VERSION}.tar.gz https://github.com/gcovr/gcovr/archive/3.0.tar.gz
  tar xzf gcovr-${GCOVR_VERSION}.tar.gz
  rm gcovr-${GCOVR_VERSION}.tar.gz
fi

if [ ! -d $CURL_DIR ]; then
  echo "Fetching curl"
  wget http://curl.download.nextag.com/download/curl-${CURL_VERSION}.tar.gz
  tar xzf curl-${CURL_VERSION}.tar.gz
  rm curl-${CURL_VERSION}.tar.gz
fi

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
