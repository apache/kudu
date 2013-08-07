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
mkdir -p "$PREFIX/lib"
ln -sf lib "$PREFIX/lib64"

# use the compiled tools
export PATH=$PREFIX/bin:$PATH

# build cmake
cd $CMAKE_DIR
./bootstrap --prefix=$PREFIX --parallel=8
make -j
make install

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

# build snappy
cd $SNAPPY_DIR
./configure --with-pic --prefix=$PREFIX
make -j4 install

# build zlib
cd $ZLIB_DIR
./configure --prefix=$PREFIX
make -j4 install

# build lz4
cd $LZ4_DIR
$PREFIX/bin/cmake -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX $LZ4_DIR
make -j4 install

## build libev
cd $LIBEV_DIR
./configure --with-pic --disable-shared --prefix=$PREFIX
make -j4 install

## build cyrus-sasl
cd $CYRUS_SASL_DIR
[ -r Makefile ] && make distclean # (Jenkins was complaining about CFLAGS changes)
# Disable everything except those protocols needed -- currently just Kerberos.
# Sasl does not have a --with-pic configuration.
CFLAGS="-fPIC -DPIC" CXXFLAGS="-fPIC -DPIC" ./configure \
  --disable-digest --disable-sql --disable-cram --disable-ldap --disable-otp \
  --enable-static --enable-staticdlopen --with-dblib=none --without-des\
  --prefix=$PREFIX
make clean
make # no -j4 ... concurrent build probs on RHEL?
make install

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
