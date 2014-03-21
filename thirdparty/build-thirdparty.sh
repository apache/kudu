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
      "squeasel")   F_SQUEASEL=1 ;;
      "gsg")        F_GSG=1 ;;
      "gcovr")      F_GCOVR=1 ;;
      "curl")       F_CURL=1 ;;
      *)            echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

# Determine how many parallel jobs to use for make based on the number of cores
PARALLEL=$(grep -c processor /proc/cpuinfo)


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
  make -j$PARALLEL
  make install
fi

# build gflags
if [ -n "$F_ALL" -o -n "$F_GFLAGS" ]; then
  cd $GFLAGS_DIR
  ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build glog
if [ -n "$F_ALL" -o -n "$F_GLOG" ]; then
  cd $GLOG_DIR
  ./configure --with-pic --prefix=$PREFIX --with-gflags=$PREFIX
  make -j$PARALLEL install
fi

# build gperftools
if [ -n "$F_ALL" -o -n "$F_GPERFTOOLS" ]; then
  cd $GPERFTOOLS_DIR
  ./configure --enable-frame-pointers --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build gtest
if [ -n "$F_ALL" -o -n "$F_GTEST" ]; then
  cd $GTEST_DIR
  CXXFLAGS=-fPIC cmake .
  make -j$PARALLEL
fi

# build protobuf
if [ -n "$F_ALL" -o -n "$F_PROTOBUF" ]; then
  cd $PROTOBUF_DIR
  ./configure --with-pic --enable-shared --enable-static --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build snappy
if [ -n "$F_ALL" -o -n "$F_SNAPPY" ]; then
  cd $SNAPPY_DIR
  ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build zlib
if [ -n "$F_ALL" -o -n "$F_ZLIB" ]; then
  cd $ZLIB_DIR
  ./configure --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build lz4
if [ -n "$F_ALL" -o -n "$F_LZ4" ]; then
  cd $LZ4_DIR
  CFLAGS=-fPIC $PREFIX/bin/cmake -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX $LZ4_DIR
  make -j$PARALLEL install
fi

## build libev
if [ -n "$F_ALL" -o -n "$F_LIBEV" ]; then
  cd $LIBEV_DIR
  ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# Build rapidjson
if [ -n "$F_ALL" -o -n "$F_RAPIDJSON" ]; then
  # rapidjson is actually a header-only library, so our "build" is really
  # just installing it into our prefix
  cd $RAPIDJSON_DIR
  rsync -av --delete $RAPIDJSON_DIR/include/rapidjson/ $PREFIX/include/rapidjson/
fi

# Build squeasel
if [ -n "$F_ALL" -o -n "$F_SQUEASEL" ]; then
  # Mongoose's Makefile builds a standalone web server, whereas we just want
  # a static lib
  cd $SQUEASEL_DIR
  ${CC:-gcc} -O3 -DNDEBUG -DNO_SSL_DL -fPIC -c squeasel.c
  ar rs libsqueasel.a squeasel.o
  cp libsqueasel.a $PREFIX/lib/
  cp squeasel.h $PREFIX/include/
fi

# Build curl
if [ -n "$F_ALL" -o -n "$F_CURL" ]; then
  # Configure for a very minimal install - basically only HTTP,
  # since we only use this for testing our own HTTP endpoints
  # at this point in time.
  cd $CURL_DIR
  ./configure --prefix=$PREFIX \
    --disable-ftp \
    --disable-file \
    --disable-ldap \
    --disable-ldaps \
    --disable-rtsp \
    --disable-proxy \
    --disable-dict \
    --disable-telnet \
    --disable-tftp \
    --disable-pop3 \
    --disable-imap \
    --disable-smtp \
    --disable-gopher \
    --disable-manual \
    --without-rtmp \
    --disable-ipv6
  make -j$PARALLEL
  make install
fi

# Copy boost_uuid into the include directory.
# This is a header-only library which isn't present in some older versions of
# boost (eg the one on el6). So, we check it in and put it in our own include
# directory.
rsync -a $TP_DIR/boost_uuid/boost/ $PREFIX/include/boost/

# Copy cpplint tool into bin directory
if [ -n "$F_ALL" -o -n "$F_GSG" ]; then
  cp $GSG_DIR/cpplint/cpplint.py $PREFIX/bin/cpplint.py
fi

# Copy gcovr tool into bin directory
if [ -n "$F_ALL" -o -n "$F_GCOVR" ]; then
  cp -a $GCOVR_DIR/scripts/gcovr $PREFIX/bin/gcovr
fi

# Remove any old thirdparty deps which hung around from previous versions
rm -f $PREFIX/lib/libsasl*
rm -Rf $PREFIX/lib/sasl2

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
