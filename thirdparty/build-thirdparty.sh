#!/bin/bash
# Copyright (c) 2012, Cloudera, inc.

set -x
set -e

TP_DIR=$(readlink -f $(dirname $BASH_SOURCE))

source $TP_DIR/vars.sh

##############################

# On some systems, autotools installs libraries to lib64 rather than lib.  Fix
# this by setting up lib64 as a symlink to lib.  We have to do this step first
# to handle cases where one third-party library depends on another.
mkdir -p "$TP_DIR/installed/lib"
ln -s lib "$TP_DIR/installed/lib64"

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

# build cmake
cd $CMAKE_DIR
./bootstrap --prefix=$PREFIX --parallel=8
make -j
make install

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
