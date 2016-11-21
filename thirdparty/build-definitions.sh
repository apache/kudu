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

# Some versions of libtool are vulnerable to a bug[1] wherein clang's -stdlib
# parameter isn't passed through to the link command line. This causes the
# libtool to link the shared object against libstdc++ instead of libc++.
#
# The bug was only fixed in version 2.4.2 of libtool and el6 carries version
# 2.2.6, so we work around it here.
#
# 1. https://debbugs.gnu.org/db/10/10579.html
fixup_libtool() {
  if [[ ! "$EXTRA_CXXFLAGS" =~ "-stdlib=libc++" ]]; then
    echo "libtool does not need to be fixed up: not using libc++"
    return
  fi
  if [ ! -f libtool ]; then
    echo "libtool not found"
    exit 1
  fi
  if ! grep -q -e 'postdeps=.*-lstdc++' libtool; then
    echo "libtool does not need to be fixed up: already configured for libc++"
    return
  fi

  # Modify a line like:
  #
  #   postdeps="-lfoo -lstdc++ -lbar -lstdc++ -lbaz"
  #
  # To become:
  #
  #   postdeps="-lfoo -lc++ -lbar -lc++ -lbaz"
  sed -i.before_fixup -e '/postdeps=/s/-lstdc++/-lc++/g' libtool
  echo "libtool has been fixed up"
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

build_libcxxabi() {
  LIBCXXABI_BDIR=$TP_BUILD_DIR/llvm-$LLVM_VERSION.libcxxabi$MODE_SUFFIX
  mkdir -p $LIBCXXABI_BDIR
  pushd $LIBCXXABI_BDIR
  rm -Rf CMakeCache.txt CMakeFiles/
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS $EXTRA_LDFLAGS" \
    -DLLVM_PATH=$LLVM_SOURCE \
    $LLVM_SOURCE/projects/libcxxabi
  make -j$PARALLEL install
  popd
}

build_libcxx() {
  local BUILD_TYPE=$1
  case $BUILD_TYPE in
    "tsan")
      SANITIZER_TYPE=Thread
      ;;
    *)
      echo "Unknown build type: $BUILD_TYPE"
      exit 1
      ;;
  esac

  LIBCXX_BDIR=$TP_BUILD_DIR/llvm-$LLVM_VERSION.libcxx$MODE_SUFFIX
  mkdir -p $LIBCXX_BDIR
  pushd $LIBCXX_BDIR
  rm -Rf CMakeCache.txt CMakeFiles/
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS $EXTRA_LDFLAGS" \
    -DLLVM_PATH=$LLVM_SOURCE \
    -DLIBCXX_CXX_ABI=libcxxabi \
    -DLIBCXX_CXX_ABI_INCLUDE_PATHS=$LLVM_SOURCE/projects/libcxxabi/include \
    -DLLVM_USE_SANITIZER=$SANITIZER_TYPE \
    $LLVM_SOURCE/projects/libcxx
  make -j$PARALLEL install
  popd
}

build_or_find_python() {
  if [ -n "$PYTHON_EXECUTABLE" ]; then
    return
  fi

  # Build Python only if necessary.
  if [[ $(python2.7 -V 2>&1) =~ "Python 2.7." ]]; then
    PYTHON_EXECUTABLE=$(which python2.7)
  elif [[ $(python -V 2>&1) =~ "Python 2.7." ]]; then
    PYTHON_EXECUTABLE=$(which python)
  else
    PYTHON_BDIR=$TP_BUILD_DIR/$PYTHON_NAME$MODE_SUFFIX
    mkdir -p $PYTHON_BDIR
    pushd $PYTHON_BDIR
    $PYTHON_SOURCE/configure
    make -j$PARALLEL
    PYTHON_EXECUTABLE="$PYTHON_BDIR/python"
    popd
  fi
}

build_llvm() {
  local TOOLS_ARGS=
  local BUILD_TYPE=$1

  build_or_find_python

  # Always disabled; these subprojects are built standalone.
  TOOLS_ARGS="$TOOLS_ARGS -DLLVM_TOOL_LIBCXX_BUILD=OFF"
  TOOLS_ARGS="$TOOLS_ARGS -DLLVM_TOOL_LIBCXXABI_BUILD=OFF"

  case $BUILD_TYPE in
    "normal")
      # Default build: core LLVM libraries, clang, compiler-rt, and all tools.
      ;;
    "tsan")
      # Build just the core LLVM libraries, dependent on libc++.
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_ENABLE_LIBCXX=ON"
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_INCLUDE_TOOLS=OFF"
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_TOOL_COMPILER_RT_BUILD=OFF"

      # Configure for TSAN.
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_USE_SANITIZER=Thread"
      ;;
    *)
      echo "Unknown LLVM build type: $BUILD_TYPE"
      exit 1
      ;;
  esac

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
    -DLLVM_INCLUDE_DOCS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_UTILS=OFF \
    -DLLVM_TARGETS_TO_BUILD=X86 \
    -DLLVM_ENABLE_RTTI=ON \
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS $EXTRA_LDFLAGS" \
    -DPYTHON_EXECUTABLE=$PYTHON_EXECUTABLE \
    $TOOLS_ARGS \
    $LLVM_SOURCE

  make -j$PARALLEL install

  if [[ "$BUILD_TYPE" == "normal" ]]; then
    # Create a link from Clang to thirdparty/clang-toolchain. This path is used
    # for all Clang invocations. The link can't point to the Clang installed in
    # the prefix directory, since this confuses CMake into believing the
    # thirdparty prefix directory is the system-wide prefix, and it omits the
    # thirdparty prefix directory from the rpath of built binaries.
    ln -sfn $LLVM_BDIR $TP_DIR/clang-toolchain
  fi
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

  # glog depends on libunwind and gflags.
  #
  # Specifying -Wl,-rpath has different default behavior on GNU binutils ld vs.
  # the GNU gold linker. ld sets RPATH (due to defaulting to --disable-new-dtags)
  # and gold sets RUNPATH (due to defaulting to --enable-new-dtags). At the time
  # of this writing, contrary to the way RPATH is treated, when RUNPATH is
  # specified on a binary, glibc doesn't respect it for transitive (non-direct)
  # library dependencies (see https://sourceware.org/bugzilla/show_bug.cgi?id=13945).
  # So we must set RUNPATH for all deps-of-deps on the dep libraries themselves.
  #
  # This comment applies both here and the locations elsewhere in this script
  # where we add something to -Wl,-rpath.
  CXXFLAGS="$EXTRA_CXXFLAGS -I$PREFIX/include" \
    LDFLAGS="$EXTRA_LDFLAGS -L$PREFIX/lib -Wl,-rpath,$PREFIX/lib" \
    LIBS="$EXTRA_LIBS" \
    $GLOG_SOURCE/configure \
    --with-pic \
    --prefix=$PREFIX \
    --with-gflags=$PREFIX
  fixup_libtool
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
  fixup_libtool
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
  fixup_libtool
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
  fixup_libtool
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

  # This library has significant optimizations when built with -mavx2. However,
  # we still need to support non-AVX2-capable hardware. So, we build it twice,
  # once with the flag and once without, and use some linker tricks to
  # suffix the AVX2 symbols with '_avx2'. OSX doesn't have objcopy, so we only
  # do this trick on Linux.
  if [ -n "$OS_LINUX" ]; then
    arches="default avx2"
  else
    arches="default"
  fi
  to_link=""
  for arch in $arches ; do
    arch_flag=""
    if [ "$arch" == "avx2" ]; then
      arch_flag="-mavx2"
    fi
    tmp_obj=bitshuffle_${arch}_tmp.o
    dst_obj=bitshuffle_${arch}.o
    ${CC:-gcc} $EXTRA_CFLAGS $arch_flag -std=c99 -I$PREFIX/include -O3 -DNDEBUG -fPIC -c \
      "$BITSHUFFLE_SOURCE/src/bitshuffle_core.c" \
      "$BITSHUFFLE_SOURCE/src/bitshuffle.c" \
      "$BITSHUFFLE_SOURCE/src/iochain.c"
    # Merge the object files together to produce a combined .o file.
    ld -r -o $tmp_obj bitshuffle_core.o bitshuffle.o iochain.o
    # For the AVX2 symbols, suffix them.
    if [ "$arch" == "avx2" ]; then
      # Create a mapping file with '<old_sym> <suffixed_sym>' on each line.
      nm --defined-only --extern-only $tmp_obj | while read addr type sym ; do
        echo ${sym} ${sym}_${arch}
      done > renames.txt
      objcopy --redefine-syms=renames.txt $tmp_obj $dst_obj
    else
      mv $tmp_obj $dst_obj
    fi
    to_link="$to_link $dst_obj"
  done

  rm bitshuffle.a
  ar rs bitshuffle.a $to_link
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
  fixup_libtool
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
    # Disable -Werror; it prevents jemalloc from building via clang.
    #
    # Add PREFIX/lib to the rpath; libpmemobj depends on libpmem at runtime.
    EXTRA_CFLAGS="$EXTRA_CFLAGS -Wno-error" \
      EXTRA_LDFLAGS="$EXTRA_LDFLAGS -Wl,-rpath,$PREFIX/lib" \
      make -j$PARALLEL $LIB DEBUG=0

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
