#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.

set -x
set -e

TP_DIR=$(readlink -f $(dirname $BASH_SOURCE))
cd $TP_DIR

source vars.sh

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

if [ ! -d gperftools-${GPERFTOOLS_VERSION} ]; then
  echo "Fetching gperftools"
  wget -c http://gperftools.googlecode.com/files/gperftools-${GPERFTOOLS_VERSION}.tar.gz
  tar xzf gperftools-${GPERFTOOLS_VERSION}.tar.gz
  rm gperftools-${GPERFTOOLS_VERSION}.tar.gz
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

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
