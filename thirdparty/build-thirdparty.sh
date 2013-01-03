#!/bin/bash
# Copyright (c) 2012, Cloudera, inc.

set -x
set -e

TP_DIR=$(readlink -f $(dirname $BASH_SOURCE))

source $TP_DIR/vars.sh

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

# build protobuf
cd $PROTOBUF_DIR
./configure --with-pic --disable-shared --prefix=$PREFIX
make -j4 install


echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
