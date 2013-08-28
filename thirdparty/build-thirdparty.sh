#!/bin/bash
# Copyright (c) 2012, Cloudera, inc.

set -x
set -e
TP_DIR=$(readlink -f $(dirname $BASH_SOURCE))

source $TP_DIR/vars.sh

################################################################################

if [ "$#" = "0" ]; then
  F_ALL=1
else
  # Allow passing specific libs to build on the command line
  for arg in "$*"; do
    case $arg in
      "cmake")      F_CMAKE=1 ;;
      "cyrus-sasl") F_CYRUS_SASL=1 ;;
      "gflags")     F_GFLAGS=1 ;;
      "glog")       F_GLOG=1 ;;
      "gperftools") F_GPERFTOOLS=1 ;;
      "gtest")      F_GTEST=1 ;;
      "libev")      F_LIBEV=1 ;;
      "lz4")        F_LZ4=1 ;;
      "protobuf")   F_PROTOBUF=1 ;;
      "rapidjson")  F_RAPIDJSON=1 ;;
      "snappy")     F_SNAPPY=1 ;;
      "zlib")       F_ZLIB=1 ;;
      "mongoose")   F_MONGOOSE=1 ;;
      "gsg")        F_GSG=1 ;;
      *)            echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

mkdir -p "$PREFIX/include"
mkdir -p "$PREFIX/lib"

# On some systems, autotools installs libraries to lib64 rather than lib.  Fix
# this by setting up lib64 as a symlink to lib.  We have to do this step first
# to handle cases where one third-party library depends on another.
ln -sf lib "$PREFIX/lib64"

# use the compiled tools
export PATH=$PREFIX/bin:$PATH

# build cmake
if [ -n "$F_ALL" -o -n "$F_CMAKE" ]; then
  cd $CMAKE_DIR
  ./bootstrap --prefix=$PREFIX --parallel=8
  make -j
  make install
fi

# build gflags
if [ -n "$F_ALL" -o -n "$F_GFLAGS" ]; then
  cd $GFLAGS_DIR
  ./configure --with-pic --prefix=$PREFIX
  make -j4 install
fi

# build glog
if [ -n "$F_ALL" -o -n "$F_GLOG" ]; then
  cd $GLOG_DIR
  ./configure --with-pic --prefix=$PREFIX --with-gflags=$PREFIX
  make -j4 install
fi

# build gperftools
if [ -n "$F_ALL" -o -n "$F_GPERFTOOLS" ]; then
  cd $GPERFTOOLS_DIR
  ./configure --enable-frame-pointers --with-pic --prefix=$PREFIX
  make -j4 install
fi

# build gtest
if [ -n "$F_ALL" -o -n "$F_GTEST" ]; then
  cd $GTEST_DIR
  cmake .
  make -j4
fi

# build protobuf
if [ -n "$F_ALL" -o -n "$F_PROTOBUF" ]; then
  cd $PROTOBUF_DIR
  ./configure --with-pic --disable-shared --prefix=$PREFIX
  make -j4 install
fi

# build snappy
if [ -n "$F_ALL" -o -n "$F_SNAPPY" ]; then
  cd $SNAPPY_DIR
  ./configure --with-pic --prefix=$PREFIX
  make -j4 install
fi

# build zlib
if [ -n "$F_ALL" -o -n "$F_ZLIB" ]; then
  cd $ZLIB_DIR
  ./configure --prefix=$PREFIX
  make -j4 install
fi

# build lz4
if [ -n "$F_ALL" -o -n "$F_LZ4" ]; then
  cd $LZ4_DIR
  $PREFIX/bin/cmake -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX $LZ4_DIR
  make -j4 install
fi

## build libev
if [ -n "$F_ALL" -o -n "$F_LIBEV" ]; then
  cd $LIBEV_DIR
  ./configure --with-pic --disable-shared --prefix=$PREFIX
  make -j4 install
fi

## build cyrus-sasl
if [ -n "$F_ALL" -o -n "$F_CYRUS_SASL" ]; then
  cd $CYRUS_SASL_DIR
  [ -r Makefile ] && make distclean # (Jenkins was complaining about CFLAGS changes)
  # Disable everything except those protocols needed.
  # SASL does not have a --with-pic configuration, so we'd normally pass CLAGS="-DPIC".
  # The gssapi plugin requires -DPIC to link, otherwise it can't find some symbols.
  # Unfortunately, there seem to be some compatibility issues with RHEL6 that still need to be
  # resolved when cyrus is built with -DPIC. Apparently, it's unable to load auxprop plugins
  # from the system, particularly sasldb. That failure to load the sasldb plugin causes errors
  # with plain authentication (anonymous is also affected even without -DPIC, but we have a
  # hack in sasl_server.cc to work around that case). So... disabling PIC and gssapi for now.
  ./configure --disable-gssapi \
    --disable-digest --disable-sql --disable-cram --disable-ldap --disable-otp \
    --enable-static --enable-staticdlopen --with-dblib=none --without-des \
    --prefix=$PREFIX
  make clean
  make # no -j4 ... concurrent build probs on RHEL?
  make install
fi

# Build rapidjson
if [ -n "$F_ALL" -o -n "$F_RAPIDJSON" ]; then
  # rapidjson is actually a header-only library, so our "build" is really
  # just installing it into our prefix
  cd $RAPIDJSON_DIR
  rsync -av --delete $RAPIDJSON_DIR/include/rapidjson/ $PREFIX/include/rapidjson/
fi

# Build mongoose
if [ -n "$F_ALL" -o -n "$F_MONGOOSE" ]; then
  # Mongoose's Makefile builds a standalone web server, whereas we just want
  # a static lib
  cd $MONGOOSE_DIR
  ${CC:-gcc} -O3 -DNDEBUG -DNO_SSL_DL -c mongoose.c
  ar rs libmongoose.a mongoose.o
  cp libmongoose.a $PREFIX/lib/
  cp mongoose.h $PREFIX/include/
fi

# Copy cpplint tool into bin directory
if [ -n "$F_ALL" -o -n "$F_GSG" ]; then
  cp $GSG_DIR/cpplint/cpplint.py $PREFIX/bin/cpplint.py
fi

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
