#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# build-definitions.sh provides functions to build thirdparty dependencies.
# These functions do not take positional arguments, but individual builds may
# be influenced by setting environment variables:
#
# * PREFIX - the install destination directory.
# * MODE_SUFFIX - an optional suffix to add to each build directory's name.
# * EXTRA_CFLAGS - additional flags to pass to the C compiler.
# * EXTRA_CXXFLAGS - additional flags to pass to the C++ compiler.
# * EXTRA_LDFLAGS - additional flags to pass to the linker.
# * EXTRA_LIBS - additional libraries to link.
#
# build-definitions.sh is meant to be sourced from build-thirdparty.sh, and
# relies on environment variables defined there and in vars.sh.

# Save the current build environment.
save_env() {
  _PREFIX=${PREFIX}
  _MODE_SUFFIX=${MODE_SUFFIX}
  _EXTRA_CFLAGS=${EXTRA_CFLAGS}
  _EXTRA_CXXFLAGS=${EXTRA_CXXFLAGS}
  _EXTRA_LDFLAGS=${EXTRA_LDFLAGS}
  _EXTRA_LIBS=${EXTRA_LIBS}
}

# Restore the most recently saved build environment.
restore_env() {
  PREFIX=${_PREFIX}
  MODE_SUFFIX=${_MODE_SUFFIX}
  EXTRA_CFLAGS=${_EXTRA_CFLAGS}
  EXTRA_CXXFLAGS=${_EXTRA_CXXFLAGS}
  EXTRA_LDFLAGS=${_EXTRA_LDFLAGS}
  EXTRA_LIBS=${_EXTRA_LIBS}
}

build_cmake() {
  CMAKE_BDIR=$TP_BUILD_DIR/$CMAKE_NAME$MODE_SUFFIX
  mkdir -p $CMAKE_BDIR
  pushd $CMAKE_BDIR
  $CMAKE_SOURCE/bootstrap \
    --prefix=$PREFIX \
    --parallel=$PARALLEL
  make -j$PARALLEL
  make install
  popd
}

build_llvm() {

  # Build Python if necessary.
  if [[ $(python2.7 -V 2>&1) =~ "Python 2.7." ]]; then
    PYTHON_EXECUTABLE=$(which python2.7)
  elif [[ $(python -V 2>&1) =~ "Python 2.7." ]]; then
    PYTHON_EXECUTABLE=$(which python)
  else
    PYTHON_BDIR=$TP_BUILD_DIR/$PYTHON_NAME$MODE_SUFFIX
    mkdir -p $PYTHON_BDIR
    pushd $PYTHON_BDIR
    $PYTHON_SOURCE/configure --prefix=$PREFIX
    make -j$PARALLEL
    PYTHON_EXECUTABLE="$PYTHON_BDIR/python"
    popd
  fi

  LLVM_BDIR=$TP_BUILD_DIR/llvm-$LLVM_VERSION$MODE_SUFFIX
  mkdir -p $LLVM_BDIR
  pushd $LLVM_BDIR

  # Rebuild the CMake cache every time.
  rm -Rf CMakeCache.txt CMakeFiles/

  # The LLVM build can fail if a different version is already installed
  # in the install prefix. It will try to link against that version instead
  # of the one being built.
  rm -Rf $PREFIX/include/{llvm*,clang*} \
         $PREFIX/lib/lib{LLVM,LTO,clang}* \
         $PREFIX/lib/clang/ \
         $PREFIX/lib/cmake/{llvm,clang}

  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DLLVM_TARGETS_TO_BUILD=X86 \
    -DLLVM_ENABLE_RTTI=ON \
    -DLLVM_TOOL_LIBCXX_BUILD=OFF \
    -DLLVM_TOOL_LIBCXXABI_BUILD=OFF \
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS" \
    -DPYTHON_EXECUTABLE=$PYTHON_EXECUTABLE \
    $LLVM_SOURCE

  make -j$PARALLEL install

  # Create a link from Clang to thirdparty/clang-toolchain. This path is used
  # for compiling Kudu with sanitizers. The link can't point to the Clang
  # installed in the prefix directory, since this confuses CMake into believing
  # the thirdparty prefix directory is the system-wide prefix, and it omits the
  # thirdparty prefix directory from the rpath of built binaries.
  ln -sfn $LLVM_BDIR $TP_DIR/clang-toolchain
  popd
}

build_libstdcxx() {
  GCC_BDIR=$TP_BUILD_DIR/$GCC_NAME$MODE_SUFFIX

  # Remove the GCC build directory to remove cached build configuration.
  rm -rf $GCC_BDIR

  mkdir -p $GCC_BDIR
  pushd $GCC_BDIR
  CFLAGS=$EXTRA_CFLAGS \
    CXXFLAGS=$EXTRA_CXXFLAGS \
    $GCC_SOURCE/libstdc++-v3/configure \
    --enable-multilib=no \
    --prefix="$PREFIX"

  # On Ubuntu distros (tested on 14.04 and 16.04), the configure script has a
  # nasty habit of disabling TLS support when -fsanitize=thread is used. This
  # appears to be an interaction between TSAN and the GCC_CHECK_TLS m4 macro
  # used by configure. It doesn't manifest on el6 because the devtoolset
  # causes an early conftest to fail, which passes the macro's smell test.
  #
  # This is a silly hack to force TLS support back on, but it's only temporary,
  # as we're about to replace all of this with libc++.
  sed -ie 's|/\* #undef HAVE_TLS \*/|#define HAVE_TLS 1|' config.h

  make -j$PARALLEL install
  popd
}

build_gflags() {
  GFLAGS_BDIR=$TP_BUILD_DIR/$GFLAGS_NAME$MODE_SUFFIX
  mkdir -p $GFLAGS_BDIR
  pushd $GFLAGS_BDIR
  rm -rf CMakeCache.txt CMakeFiles/
  CXXFLAGS="$EXTRA_CFLAGS $EXTRA_CXXFLAGS $EXTRA_LDFLAGS $EXTRA_LIBS" \
    cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DBUILD_SHARED_LIBS=On \
    -DBUILD_STATIC_LIBS=On \
    $GFLAGS_SOURCE
  make -j$PARALLEL install
  popd
}

build_libunwind() {
  LIBUNWIND_BDIR=$TP_BUILD_DIR/$LIBUNWIND_NAME$MODE_SUFFIX
  mkdir -p $LIBUNWIND_BDIR
  pushd $LIBUNWIND_BDIR
  # Disable minidebuginfo, which depends on liblzma, until/unless we decide to
  # add liblzma to thirdparty.
  $LIBUNWIND_SOURCE/configure \
    --disable-minidebuginfo \
    --with-pic \
    --prefix=$PREFIX
  make -j$PARALLEL install
  popd
}

build_glog() {
  GLOG_BDIR=$TP_BUILD_DIR/$GLOG_NAME$MODE_SUFFIX
  mkdir -p $GLOG_BDIR
  pushd $GLOG_BDIR
  CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $GLOG_SOURCE/configure \
    --with-pic \
    --prefix=$PREFIX \
    --with-gflags=$PREFIX
  make -j$PARALLEL install
  popd
}

build_gperftools() {
  GPERFTOOLS_BDIR=$TP_BUILD_DIR/$GPERFTOOLS_NAME$MODE_SUFFIX
  mkdir -p $GPERFTOOLS_BDIR
  pushd $GPERFTOOLS_BDIR
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $GPERFTOOLS_SOURCE/configure \
    --enable-frame-pointers \
    --enable-heap-checker \
    --with-pic \
    --prefix=$PREFIX
  make -j$PARALLEL install
  popd
}

build_gmock() {
  GMOCK_SHARED_BDIR=$TP_BUILD_DIR/$GMOCK_NAME.shared$MODE_SUFFIX
  GMOCK_STATIC_BDIR=$TP_BUILD_DIR/$GMOCK_NAME.static$MODE_SUFFIX
  for SHARED in ON OFF; do
    if [ $SHARED = "ON" ]; then
      GMOCK_BDIR=$GMOCK_SHARED_BDIR
    else
      GMOCK_BDIR=$GMOCK_STATIC_BDIR
    fi
    mkdir -p $GMOCK_BDIR
    pushd $GMOCK_BDIR
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="$EXTRA_CXXFLAGS $EXTRA_LDFLAGS $EXTRA_LIBS" \
      cmake \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_POSITION_INDEPENDENT_CODE=On \
      -DBUILD_SHARED_LIBS=$SHARED \
      $GMOCK_SOURCE
    make -j$PARALLEL
    popd
  done
  echo Installing gmock...
  cp -a $GMOCK_SHARED_BDIR/libgmock.$DYLIB_SUFFIX $PREFIX/lib/
  cp -a $GMOCK_STATIC_BDIR/libgmock.a $PREFIX/lib/
  rsync -av $GMOCK_SOURCE/include/ $PREFIX/include/
  rsync -av $GMOCK_SOURCE/gtest/include/ $PREFIX/include/
}

build_protobuf() {
  PROTOBUF_BDIR=$TP_BUILD_DIR/$PROTOBUF_NAME$MODE_SUFFIX
  mkdir -p $PROTOBUF_BDIR
  pushd $PROTOBUF_BDIR
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $PROTOBUF_SOURCE/configure \
    --with-pic \
    --enable-shared \
    --enable-static \
    --prefix=$PREFIX
  make -j$PARALLEL install
  popd
}

build_snappy() {
  SNAPPY_BDIR=$TP_BUILD_DIR/$SNAPPY_NAME$MODE_SUFFIX
  mkdir -p $SNAPPY_BDIR
  pushd $SNAPPY_BDIR
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $SNAPPY_SOURCE/configure \
    --with-pic \
    --prefix=$PREFIX
  make -j$PARALLEL install
  popd
}

build_zlib() {
  ZLIB_BDIR=$TP_BUILD_DIR/$ZLIB_NAME$MODE_SUFFIX
  mkdir -p $ZLIB_BDIR
  pushd $ZLIB_BDIR

  # It doesn't appear possible to isolate source and build directories, so just
  # prepopulate the latter using the former.
  rsync -av --delete $ZLIB_SOURCE/ .

  CFLAGS="$EXTRA_CFLAGS -fPIC" \
    ./configure \
    --prefix=$PREFIX
  make -j$PARALLEL install
  popd
}

build_lz4() {
  LZ4_BDIR=$TP_BUILD_DIR/$LZ4_NAME$MODE_SUFFIX
  mkdir -p $LZ4_BDIR
  pushd $LZ4_BDIR
  CFLAGS="$EXTRA_CFLAGS" \
    cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DBUILD_TOOLS=0 \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    $LZ4_SOURCE/cmake_unofficial
  make -j$PARALLEL install
  popd
}

build_bitshuffle() {
  BITSHUFFLE_BDIR=$TP_BUILD_DIR/$BITSHUFFLE_NAME$MODE_SUFFIX
  mkdir -p $BITSHUFFLE_BDIR
  pushd $BITSHUFFLE_BDIR
  # bitshuffle depends on lz4, therefore set the flag I$PREFIX/include
  ${CC:-gcc} $EXTRA_CFLAGS -std=c99 -I$PREFIX/include -O3 -DNDEBUG -fPIC -c \
    "$BITSHUFFLE_SOURCE/src/bitshuffle_core.c" \
    "$BITSHUFFLE_SOURCE/src/bitshuffle.c" \
    "$BITSHUFFLE_SOURCE/src/iochain.c"
  ar rs bitshuffle.a bitshuffle_core.o bitshuffle.o iochain.o
  cp bitshuffle.a $PREFIX/lib/
  cp $BITSHUFFLE_SOURCE/src/bitshuffle.h $PREFIX/include/bitshuffle.h
  cp $BITSHUFFLE_SOURCE/src/bitshuffle_core.h $PREFIX/include/bitshuffle_core.h
  popd
}

build_libev() {
  LIBEV_BDIR=$TP_BUILD_DIR/$LIBEV_NAME$MODE_SUFFIX
  mkdir -p $LIBEV_BDIR
  pushd $LIBEV_BDIR
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    $LIBEV_SOURCE/configure \
    --with-pic \
    --prefix=$PREFIX
  make -j$PARALLEL install
  popd
}

build_rapidjson() {
  # just installing it into our prefix
  pushd $RAPIDJSON_SOURCE
  rsync -av --delete $RAPIDJSON_SOURCE/include/rapidjson/ $PREFIX/include/rapidjson/
  popd
}

build_squeasel() {
  # Mongoose's Makefile builds a standalone web server, whereas we just want
  # a static lib
  SQUEASEL_BDIR=$TP_BUILD_DIR/$SQUEASEL_NAME$MODE_SUFFIX
  mkdir -p $SQUEASEL_BDIR
  pushd $SQUEASEL_BDIR
  ${CC:-gcc} $EXTRA_CFLAGS -std=c99 -O3 -DNDEBUG -fPIC -c "$SQUEASEL_SOURCE/squeasel.c"
  ar rs libsqueasel.a squeasel.o
  cp libsqueasel.a $PREFIX/lib/
  cp $SQUEASEL_SOURCE/squeasel.h $PREFIX/include/
  popd
}

build_curl() {
  # Configure for a very minimal install - basically only HTTP, since we only
  # use this for testing our own HTTP endpoints at this point in time.
  CURL_BDIR=$TP_BUILD_DIR/$CURL_NAME$MODE_SUFFIX
  mkdir -p $CURL_BDIR
  pushd $CURL_BDIR
  $CURL_SOURCE/configure \
    --prefix=$PREFIX \
    --disable-dict \
    --disable-file \
    --disable-ftp \
    --disable-gopher \
    --disable-imap \
    --disable-ipv6 \
    --disable-ldap \
    --disable-ldaps \
    --disable-manual \
    --disable-pop3 \
    --disable-rtsp \
    --disable-smtp \
    --disable-telnet \
    --disable-tftp \
    --without-librtmp \
    --without-ssl
  make -j$PARALLEL install
  popd
}

build_crcutil() {
  CRCUTIL_BDIR=$TP_BUILD_DIR/$CRCUTIL_NAME$MODE_SUFFIX
  mkdir -p $CRCUTIL_BDIR
  pushd $CRCUTIL_BDIR

  # The autogen.sh script makes it difficult to isolate source and build
  # directories, so just prepopulate the latter using the former.
  rsync -av --delete $CRCUTIL_SOURCE/ .
  ./autogen.sh

  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    ./configure \
    --prefix=$PREFIX
  make -j$PARALLEL install
  popd
}

build_cpplint() {
  # Copy cpplint tool into bin directory
  cp $GSG_SOURCE/cpplint/cpplint.py $PREFIX/bin/cpplint.py
}

build_gcovr() {
  # Copy gcovr tool into bin directory
  cp -a $GCOVR_SOURCE/scripts/gcovr $PREFIX/bin/gcovr
}

build_trace_viewer() {
  echo Installing trace-viewer into the www directory
  cp -a $TRACE_VIEWER_SOURCE/* $TP_DIR/../www/
}

build_nvml() {
  NVML_BDIR=$TP_BUILD_DIR/$NVML_NAME$MODE_SUFFIX
  mkdir -p $NVML_BDIR
  pushd $NVML_BDIR

  # It doesn't appear possible to isolate source and build directories, so just
  # prepopulate the latter using the former.
  rsync -av --delete $NVML_SOURCE/ .
  cd src/

  # The embedded jemalloc build doesn't pick up the EXTRA_CFLAGS environment
  # variable, so we have to stick our flags into this config file.
  if ! grep -q -e "$EXTRA_CFLAGS" jemalloc/jemalloc.cfg ; then
    perl -p -i -e "s,(EXTRA_CFLAGS=\"),\$1$EXTRA_CFLAGS ," jemalloc/jemalloc.cfg
  fi
  for LIB in libvmem libpmem libpmemobj; do
    EXTRA_CFLAGS="$EXTRA_CFLAGS" make -j$PARALLEL $LIB DEBUG=0
    # NVML doesn't allow configuring PREFIX -- it always installs into
    # DESTDIR/usr/lib. Additionally, the 'install' target builds all of
    # the NVML libraries, even though we only need the three libraries above.
    # So, we manually install the built artifacts.
    cp -a $NVML_BDIR/src/include/$LIB.h $PREFIX/include
    cp -a $NVML_BDIR/src/nondebug/$LIB.{so*,a} $PREFIX/lib
  done
  popd
}

build_boost() {
  # This is a header-only installation of Boost.
  rsync -a --delete $BOOST_SOURCE/boost $PREFIX/include
}
