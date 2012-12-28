#!/bin/bash
# Copyright (c) 2012, Cloudera, inc.

set -x
set -e

TP_DIR=$(readlink -f $(dirname $BASH_SOURCE))
PREFIX=$TP_DIR/installed

GFLAGS_VERSION=1.5
GFLAGS_DIR=$TP_DIR/gflags-$GFLAGS_VERSION

GLOG_VERSION=0.3.1
GLOG_DIR=$TP_DIR/glog-$GLOG_VERSION

GPERFTOOLS_VERSION=2.0
GPERFTOOLS_DIR=$TP_DIR/gperftools-$GPERFTOOLS_VERSION

GTEST_VERSION=1.6.0
GTEST_DIR=$TP_DIR/gtest-$GTEST_VERSION

##############################

# build gflags
cd $GFLAGS_DIR
./configure --with-pic --prefix=$PREFIX
make -j4 install

# build glog
cd $GLOG_DIR
./configure --with-pic --prefix=$PREFIX --with-gflags=$PREFIX
make -j4 install

# build gperftools
cd $GPERFTOOLS_DIR
./configure --enable-frame-pointers --with-pic --prefix=$PREFIX
make -j4 install

# build gtest
cd $GTEST_DIR
cmake .
make -j4