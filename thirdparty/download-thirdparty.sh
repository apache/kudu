#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.

set -x
set -e

TP_DIR=$(readlink -f $(dirname $BASH_SOURCE))
cd $TP_DIR

source vars.sh

echo "Fetching gtest"
wget http://googletest.googlecode.com/files/gtest-${GTEST_VERSION}.zip
unzip gtest-${GTEST_VERSION}.zip
rm gtest-${GTEST_VERSION}.zip

echo "Fetching glog"
wget http://google-glog.googlecode.com/files/glog-${GLOG_VERSION}.tar.gz
tar xzf glog-${GLOG_VERSION}.tar.gz
rm glog-${GLOG_VERSION}.tar.gz

echo "Fetching gflags"
wget http://gflags.googlecode.com/files/gflags-${GFLAGS_VERSION}.zip
unzip gflags-${GFLAGS_VERSION}.zip
rm gflags-${GFLAGS_VERSION}.zip

echo "Fetching gperftools"
wget http://gperftools.googlecode.com/files/gperftools-${GPERFTOOLS_VERSION}.tar.gz
tar xzf gperftools-${GPERFTOOLS_VERSION}.tar.gz
rm gperftools-${GPERFTOOLS_VERSION}.tar.gz

echo "Fetching protobuf"
wget http://protobuf.googlecode.com/files/protobuf-${PROTOBUF_VERSION}.tar.gz
tar xzf protobuf-${PROTOBUF_VERSION}.tar.gz
rm protobuf-${PROTOBUF_VERSION}.tar.gz

echo "Fetching cmake"
wget http://www.cmake.org/files/v2.8/cmake-${CMAKE_VERSION}.tar.gz
tar xzf cmake-${CMAKE_VERSION}.tar.gz
rm cmake-${CMAKE_VERSION}.tar.gz

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
