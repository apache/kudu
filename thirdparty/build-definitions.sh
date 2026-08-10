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
# * PREFIX - location of already-installed dependencies for particular
#            dependency group
# * INSTALL_DESTDIR - the directory where 3rd-party components are installed:
#                     $KUDU_HOME/thirdparty/installed; paths for -I/-L flags
#                     are built by prepending $INSTALL_DESTDIR to $PREFIX
# * MODE_SUFFIX - an optional suffix to add to each build directory's name.
# * EXTRA_CFLAGS - additional flags to pass to the C compiler.
# * EXTRA_CXXFLAGS - additional flags to pass to the C++ compiler.
# * EXTRA_LDFLAGS - additional flags to pass to the linker.
# * EXTRA_LIBS - additional libraries to link.
# * EXTRA_CMAKE_FLAGS - extra flags to pass to cmake
#
# build-definitions.sh is meant to be sourced from build-thirdparty.sh, and
# relies on environment variables defined there and in vars.sh.

# Save the current build environment.
save_env() {
  _PREFIX=${PREFIX}
  _INSTALL_DESTDIR=${INSTALL_DESTDIR}
  _MODE_SUFFIX=${MODE_SUFFIX}
  _EXTRA_CFLAGS=${EXTRA_CFLAGS}
  _EXTRA_CXXFLAGS=${EXTRA_CXXFLAGS}
  _EXTRA_LDFLAGS=${EXTRA_LDFLAGS}
  _EXTRA_LIBS=${EXTRA_LIBS}
}

# Restore the most recently saved build environment.
restore_env() {
  PREFIX=${_PREFIX}
  INSTALL_DESTDIR=${_INSTALL_DESTDIR}
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
  if [[ ! "$EXTRA_LDFLAGS" =~ "-stdlib=libc++" ]]; then
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
  # It would be nice to use system curl if it is available to shorten build
  # times, but with curl v8.13.0 and newer some of the CURL_NETRC_OPTION
  # enumerators were made constants of type long [1], and compiling
  # cmake v3.25.3 fails when trying to assign those constants to strings.
  #
  # [1] https://github.com/curl/curl/commit/2ec00372a
  local use_system_curl="--no-system-curl"

  # TODO(aserbin): switch to using system curl when migrating to newer
  #                cmake version that's able to build with curl v8.13.0
  #                and newer
  #if pkg-config --exists libcurl; then
  #  use_system_curl="--system-curl"
  #fi

  local comp_name=$CMAKE_NAME$MODE_SUFFIX
  CMAKE_BDIR=$TP_BUILD_DIR/$comp_name
  mkdir -p $CMAKE_BDIR
  pushd $CMAKE_BDIR
  $CMAKE_SOURCE/bootstrap \
    --prefix=$PREFIX \
    $use_system_curl \
    --parallel=$PARALLEL -- \
    -DBUILD_TESTING=OFF
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
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
    $EXTRA_CMAKE_FLAGS \
    $LLVM_SOURCE/projects/libcxxabi
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_libcxx() {
  local BUILD_TYPE=$1
  case $BUILD_TYPE in
    "normal")
      SANITIZER_ARG=
      ;;
    "tsan")
      SANITIZER_ARG="-DLLVM_USE_SANITIZER=Thread"
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
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$EXTRA_LDFLAGS" \
    -DCMAKE_MODULE_LINKER_FLAGS="$EXTRA_LDFLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$EXTRA_LDFLAGS" \
    -DLLVM_PATH=$LLVM_SOURCE \
    -DLIBCXX_CXX_ABI=libcxxabi \
    -DLIBCXX_CXX_ABI_INCLUDE_PATHS=$LLVM_SOURCE/projects/libcxxabi/include \
    -DLIBCXX_CXX_ABI_LIBRARY_PATH=$PREFIX/lib \
    $SANITIZER_ARG \
    $EXTRA_CMAKE_FLAGS \
    $LLVM_SOURCE/projects/libcxx
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_llvm() {
  local TOOLS_ARGS=
  local BUILD_TYPE=$1

  if [ -z "$PYTHON_EXECUTABLE" ]; then
    if [[ $(python3 -V 2>&1) =~ "Python 3." ]]; then
      PYTHON_EXECUTABLE=$(which python3)
    elif [[ $(python -V 2>&1) =~ "Python 3." ||
            $(python -V 2>&1) =~ "Python 2.7." ]]; then
      PYTHON_EXECUTABLE=$(which python)
    elif [[ $(python2 -V 2>&1) =~ "Python 2.7." ]]; then
      PYTHON_EXECUTABLE=$(which python2)
    else
      echo "Python not found"
      exit 1
    fi
  fi

  # Always disabled; these subprojects are built standalone.
  TOOLS_ARGS="$TOOLS_ARGS -DLLVM_TOOL_LIBCXX_BUILD=OFF"
  TOOLS_ARGS="$TOOLS_ARGS -DLLVM_TOOL_LIBCXXABI_BUILD=OFF"

  # Disable some builds we don't care about.
  for arg in \
      CLANG_ENABLE_ARCMT \
      CLANG_TOOL_ARCMT_TEST_BUILD \
      CLANG_TOOL_C_ARCMT_TEST_BUILD \
      CLANG_TOOL_C_INDEX_TEST_BUILD \
      CLANG_TOOL_CLANG_CHECK_BUILD \
      CLANG_TOOL_CLANG_DIFF_BUILD \
      CLANG_TOOL_CLANG_FORMAT_VS_BUILD \
      CLANG_TOOL_CLANG_FUZZER_BUILD \
      CLANG_TOOL_CLANG_IMPORT_TEST_BUILD \
      CLANG_TOOL_CLANG_OFFLOAD_BUNDLER_BUILD \
      CLANG_TOOL_CLANG_REFACTOR_BUILD \
      CLANG_TOOL_CLANG_RENAME_BUILD \
      CLANG_TOOL_DIAGTOOL_BUILD \
      COMPILER_RT_BUILD_LIBFUZZER \
      COMPILER_RT_INCLUDE_TESTS \
      LLVM_BUILD_BENCHMARKS \
      LLVM_ENABLE_OCAMLDOC \
      LLVM_INCLUDE_BENCHMARKS \
      LLVM_INCLUDE_GO_TESTS \
      LLVM_POLLY_BUILD \
      LLVM_TOOL_BUGPOINT_BUILD \
      LLVM_TOOL_BUGPOINT_PASSES_BUILD \
      LLVM_TOOL_DSYMUTIL_BUILD \
      LLVM_TOOL_LLI_BUILD \
      LLVM_TOOL_LLVM_AS_FUZZER_BUILD \
      LLVM_TOOL_LLVM_BCANALYZER_BUILD \
      LLVM_TOOL_LLVM_CAT_BUILD \
      LLVM_TOOL_LLVM_CFI_VERIFY_BUILD \
      LLVM_TOOL_LLVM_C_TEST_BUILD \
      LLVM_TOOL_LLVM_CVTRES_BUILD \
      LLVM_TOOL_LLVM_CXXDUMP_BUILD \
      LLVM_TOOL_LLVM_CXXFILT_BUILD \
      LLVM_TOOL_LLVM_DIFF_BUILD \
      LLVM_TOOL_LLVM_DIS_BUILD \
      LLVM_TOOL_LLVM_DWARFDUMP_BUILD \
      LLVM_TOOL_LLVM_DWP_BUILD \
      LLVM_TOOL_LLVM_EXTRACT_BUILD \
      LLVM_TOOL_LLVM_GO_BUILD \
      LLVM_TOOL_LLVM_ISEL_FUZZER_BUILD \
      LLVM_TOOL_LLVM_JITLISTENER_BUILD \
      LLVM_TOOL_LLVM_MC_ASSEMBLE_FUZZER_BUILD \
      LLVM_TOOL_LLVM_MC_BUILD \
      LLVM_TOOL_LLVM_MC_DISASSEMBLE_FUZZER_BUILD \
      LLVM_TOOL_LLVM_MODEXTRACT_BUILD \
      LLVM_TOOL_LLVM_MT_BUILD \
      LLVM_TOOL_LLVM_NM_BUILD \
      LLVM_TOOL_LLVM_OBJCOPY_BUILD \
      LLVM_TOOL_LLVM_OBJDUMP_BUILD \
      LLVM_TOOL_LLVM_OPT_FUZZER_BUILD \
      LLVM_TOOL_LLVM_OPT_REPORT_BUILD \
      LLVM_TOOL_LLVM_PDBUTIL_BUILD \
      LLVM_TOOL_LLVM_PROFDATA_BUILD \
      LLVM_TOOL_LLVM_RC_BUILD \
      LLVM_TOOL_LLVM_READOBJ_BUILD \
      LLVM_TOOL_LLVM_RTDYLD_BUILD \
      LLVM_TOOL_LLVM_SHLIB_BUILD \
      LLVM_TOOL_LLVM_SIZE_BUILD \
      LLVM_TOOL_LLVM_SPECIAL_CASE_LIST_FUZZER_BUILD \
      LLVM_TOOL_LLVM_SPLIT_BUILD \
      LLVM_TOOL_LLVM_STRESS_BUILD \
      LLVM_TOOL_LLVM_STRINGS_BUILD \
      LLVM_TOOL_OBJ2YAML_BUILD \
      LLVM_TOOL_OPT_BUILD \
      LLVM_TOOL_OPT_VIEWER_BUILD \
      LLVM_TOOL_VERIFY_USELISTORDER_BUILD \
      LLVM_TOOL_XCODE_TOOLCHAIN_BUILD \
      LLVM_TOOL_YAML2OBJ_BUILD \
      ; do
    TOOLS_ARGS="$TOOLS_ARGS -D${arg}=OFF"
  done

  CLANG_CXXFLAGS="$EXTRA_CXXFLAGS"
  CLANG_LDFLAGS="$EXTRA_LDFLAGS"
  case $BUILD_TYPE in
    "normal")
      # Default build: core LLVM libraries, clang, compiler-rt, and all tools.

      # Set the gcc prefix based on the gcc that's on the path. Otherwise,
      # clang will auto-detect the newest devtoolset that is installed even
      # if it's not the current one on the path. This results in link errors
      # like:
      #   /opt/rh/devtoolset-3/root/usr/bin/ld:
      #     /opt/rh/devtoolset-6/root/usr/lib/gcc/x86_64-redhat-linux/6.2.1/crtbeginS.o:
      #     unrecognized relocation (0x2a) in section `.text'
      # ...as it tries to use the 'ld' from devtoolset-3 (on the path) to link libraries
      # coming from devtoolset-6.
      GCC_INSTALL_PREFIX=$(gcc -v 2>&1 | grep -o -- '--prefix=[^ ]*' | cut -f2 -d=)
      if [ -n "$GCC_INSTALL_PREFIX" ]; then
        TOOLS_ARGS="$TOOLS_ARGS -DGCC_INSTALL_PREFIX=$GCC_INSTALL_PREFIX"
      fi

      if [ -n "$OS_OSX" ]; then
        # Xcode 12.2 fails to build the sanitizers and they are not needed.
        # We disable them as a workaround to the build issues.
        # Disable the sanitizers and xray to prevent sanitizer_common compilation.
        # See https://github.com/llvm-mirror/compiler-rt/blob/749af53928a31afa3111f27cc41fd15849d86667/lib/CMakeLists.txt#L11-L14
        TOOLS_ARGS="$TOOLS_ARGS -DCOMPILER_RT_BUILD_SANITIZERS=OFF"
        TOOLS_ARGS="$TOOLS_ARGS -DCOMPILER_RT_BUILD_XRAY=OFF"

        # Disable Apple platforms the we do not support.
        TOOLS_ARGS="$TOOLS_ARGS -DCOMPILER_RT_ENABLE_IOS=OFF"
        TOOLS_ARGS="$TOOLS_ARGS -DCOMPILER_RT_ENABLE_WATCHOS=OFF"
        TOOLS_ARGS="$TOOLS_ARGS -DCOMPILER_RT_ENABLE_TVOS=OFF"
      fi

      ;;
    "tsan")
      # Build just the core LLVM libraries, dependent on libc++.
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_ENABLE_LIBCXX=ON"
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_INCLUDE_TOOLS=OFF"
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_TOOL_COMPILER_RT_BUILD=OFF"

      # Configure for TSAN.
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_USE_SANITIZER=Thread"
      # Use the 'tblgen' from the non-TSAN build when building the TSAN
      # build, since it runs much faster.
      TOOLS_ARGS="$TOOLS_ARGS -DLLVM_TABLEGEN=$PREFIX_DEPS/bin/llvm-tblgen"
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
    -DLLVM_TEMPORARILY_ALLOW_OLD_TOOLCHAIN=ON \
    -DLLVM_INCLUDE_DOCS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_UTILS=OFF \
    -DLLVM_TARGETS_TO_BUILD=host \
    -DLLVM_ENABLE_RTTI=ON \
    -DCMAKE_CXX_FLAGS="$CLANG_CXXFLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$CLANG_LDFLAGS" \
    -DCMAKE_MODULE_LINKER_FLAGS="$CLANG_LDFLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$CLANG_LDFLAGS" \
    -DPYTHON_EXECUTABLE=$PYTHON_EXECUTABLE \
    $TOOLS_ARGS \
    $EXTRA_CMAKE_FLAGS \
    $LLVM_SOURCE

  # Retry the build a few times. Thanks to an LLVM bug[1], the build can fail
  # sporadically when using certain values of $PARALLEL.
  #
  # 1. https://bugs.llvm.org/show_bug.cgi?id=26054
  set +e
  attempt_number=1
  max_attempts=3
  while true; do
    make -j$PARALLEL $EXTRA_MAKEFLAGS
    exit_status=$?
    if [[ $exit_status -eq 0 ]]; then
      break
    elif [[ $attempt_number -lt $max_attempts ]]; then
      echo "LLVM build failed, retrying"
      attempt_number=$((attempt_number + 1))
    else
      echo "LLVM build failed $max_attempts times, aborting"
      exit $exit_status
    fi
  done
  set -e
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_gflags() {
  GFLAGS_BDIR=$TP_BUILD_DIR/$GFLAGS_NAME$MODE_SUFFIX
  mkdir -p $GFLAGS_BDIR
  pushd $GFLAGS_BDIR
  rm -rf CMakeCache.txt CMakeFiles/
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DCMAKE_MODULE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DBUILD_SHARED_LIBS=On \
    -DBUILD_STATIC_LIBS=On \
    -DREGISTER_INSTALL_PREFIX=Off \
    $EXTRA_CMAKE_FLAGS \
    $GFLAGS_SOURCE
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_libunwind() {
  local comp_name=$LIBUNWIND_NAME$MODE_SUFFIX
  LIBUNWIND_BDIR=$TP_BUILD_DIR/$comp_name
  mkdir -p $LIBUNWIND_BDIR
  pushd $LIBUNWIND_BDIR
  # Disable minidebuginfo, which depends on liblzma, until/unless we decide to
  # add liblzma to thirdparty.
  CFLAGS="$EXTRA_CFLAGS -I$PREFIX/include" \
    $LIBUNWIND_SOURCE/configure \
    --disable-minidebuginfo \
    --with-pic \
    --prefix=$PREFIX
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_glog() {
  GLOG_SHARED_BDIR=$TP_BUILD_DIR/$GLOG_NAME.shared$MODE_SUFFIX
  GLOG_STATIC_BDIR=$TP_BUILD_DIR/$GLOG_NAME.static$MODE_SUFFIX
  for SHARED in ON OFF; do
    if [ $SHARED = "ON" ]; then
      GLOG_BDIR=$GLOG_SHARED_BDIR
    else
      GLOG_BDIR=$GLOG_STATIC_BDIR
    fi
    mkdir -p $GLOG_BDIR
    pushd $GLOG_BDIR
    rm -rf CMakeCache.txt CMakeFiles/

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
    cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS -I$PREFIX/include" \
      -DCMAKE_EXE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS -Wl,-rpath,$PREFIX/lib" \
      -DCMAKE_MODULE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS -Wl,-rpath,$PREFIX/lib" \
      -DCMAKE_SHARED_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS -Wl,-rpath,$PREFIX/lib" \
      -DBUILD_SHARED_LIBS=$SHARED \
      -DBUILD_TESTING=OFF \
      -DWITH_TLS=OFF \
      $EXTRA_CMAKE_FLAGS \
      $GLOG_SOURCE
    make -j$PARALLEL $EXTRA_MAKEFLAGS
    make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
    popd
  done
}

build_gperftools() {
  local cfg_options="\
    --with-pic\
    --prefix=$PREFIX\
    --enable-emergency-malloc"

  local cfg_cflags="$EXTRA_CFLAGS"
  local cfg_cxxflags="$EXTRA_CXXFLAGS"
  local cfg_ldflags="$EXTRA_LDFLAGS"
  local cfg_libs="$EXTRA_LIBS"

  # On Linux, build perftools with libunwind support to have an option
  # to collect stacktraces using libunwind in addition to the libgcc-
  # and the frame pointers-based methods (if available for particular
  # architecture). For more details on the stacktrace capturing options,
  # see [1].
  #
  # [1] https://github.com/gperftools/gperftools/wiki/gperftools'-stacktrace-capturing-methods-and-their-issues
  if [ -n "$OS_LINUX" ]; then
    cfg_options="$cfg_options --enable-libunwind"
    cfg_cflags="$cfg_cflags -I$PREFIX/include"
    cfg_cxxflags="$cfg_cxxflags -I$PREFIX/include"
    cfg_ldflags="$cfg_ldflags -L$PREFIX/lib -Wl,-rpath,$PREFIX/lib"
  fi

  GPERFTOOLS_BDIR=$TP_BUILD_DIR/$GPERFTOOLS_NAME$MODE_SUFFIX
  mkdir -p $GPERFTOOLS_BDIR
  pushd $GPERFTOOLS_BDIR
  CFLAGS="$cfg_cflags" \
    CXXFLAGS="$cfg_cxxflags" \
    LDFLAGS="$cfg_ldflags" \
    LIBS="$cfg_libs" \
    $GPERFTOOLS_SOURCE/configure $cfg_options
  fixup_libtool
  # KUDU-3776: It seems there is an inconsistency in tracking transitive
  #            dependencies for the 'install' make target, and some races
  #            might happen when building in parallel with 'make -jN install'.
  #            Without digging too deep into the Makefile.in and friends,
  #            a simple solution is to run the 'install' target after the
  #            default target has already been built. It's consistent with
  #            the recommended way of compiling the package per instructions
  #            in the top-level 'INSTALL' file, section 'Basic Installation'.
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  # Add tcmalloc_guard.h because TCMallocGuard is useful for fine-grained
  # control over the initialization of libtcmalloc internals, and that's
  # helpful in the context of addressing KUDU-2439 and KUDU-3635.
  install -m 0444 $GPERFTOOLS_SOURCE/src/tcmalloc_guard.h $INSTALL_DESTDIR$PREFIX/include/gperftools
  popd
}

build_gmock() {
  # Build both gmock and gtest
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
    cmake \
      -DCMAKE_POSITION_INDEPENDENT_CODE=On \
      -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS" \
      -DCMAKE_EXE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
      -DCMAKE_MODULE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
      -DCMAKE_SHARED_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
      -DBUILD_SHARED_LIBS=$SHARED \
      $EXTRA_CMAKE_FLAGS \
      $GMOCK_SOURCE
    make -j$PARALLEL $EXTRA_MAKEFLAGS
    popd
  done

  # Install gmock/gtest libraries and headers manually instead of using make
  # install. Make install results in libraries with a malformed lib name on
  # macOS.
  if [ -n "$OS_LINUX" ]; then
    local ver_suffix=${DYLIB_SUFFIX}.${GMOCK_VERSION}
  else
    # on macOS the naming of versioned libraries differ from what it's on Linux
    local ver_suffix=${GMOCK_VERSION}.${DYLIB_SUFFIX}
  fi
  echo Installing gmock and gtest...
  mkdir -p $INSTALL_DESTDIR$PREFIX/lib $INSTALL_DESTDIR$PREFIX/include
  cp -a $GMOCK_SHARED_BDIR/lib/libgmock.$ver_suffix $INSTALL_DESTDIR$PREFIX/lib/
  cp -a $GMOCK_SHARED_BDIR/lib/libgmock.$DYLIB_SUFFIX $INSTALL_DESTDIR$PREFIX/lib/
  cp -a $GMOCK_STATIC_BDIR/lib/libgmock.a $INSTALL_DESTDIR$PREFIX/lib/
  cp -a $GMOCK_SHARED_BDIR/lib/libgtest.$ver_suffix $INSTALL_DESTDIR$PREFIX/lib/
  cp -a $GMOCK_SHARED_BDIR/lib/libgtest.$DYLIB_SUFFIX $INSTALL_DESTDIR$PREFIX/lib/
  cp -a $GMOCK_STATIC_BDIR/lib/libgtest.a $INSTALL_DESTDIR$PREFIX/lib/
  rsync -av $GMOCK_SOURCE/googlemock/include/ $INSTALL_DESTDIR$PREFIX/include/
  rsync -av $GMOCK_SOURCE/googletest/include/ $INSTALL_DESTDIR$PREFIX/include/
}

build_flatbuffers() {
  FLATBUFFERS_BDIR=$TP_BUILD_DIR/$FLATBUFFERS_NAME$MODE_SUFFIX
  mkdir -p $FLATBUFFERS_BDIR
  pushd $FLATBUFFERS_BDIR
  rm -rf CMakeCache.txt CMakeFiles/
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DCMAKE_MODULE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DFLATBUFFERS_BUILD_SHAREDLIB=On \
    -DFLATBUFFERS_BUILD_TESTS=Off \
    -DFLATBUFFERS_CPP_STD=17 \
    $EXTRA_CMAKE_FLAGS \
    $FLATBUFFERS_SOURCE
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_protobuf() {
  local comp_name=$PROTOBUF_NAME$MODE_SUFFIX
  PROTOBUF_BDIR=$TP_BUILD_DIR/$comp_name
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
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_snappy() {
  SNAPPY_SHARED_BDIR=$TP_BUILD_DIR/$SNAPPY_NAME.shared$MODE_SUFFIX
  SNAPPY_STATIC_BDIR=$TP_BUILD_DIR/$SNAPPY_NAME.static$MODE_SUFFIX
  for SHARED in ON OFF; do
    if [ $SHARED = "ON" ]; then
      SNAPPY_BDIR=$SNAPPY_SHARED_BDIR
    else
      SNAPPY_BDIR=$SNAPPY_STATIC_BDIR
    fi
    mkdir -p $SNAPPY_BDIR
    pushd $SNAPPY_BDIR
    rm -Rf CMakeCache.txt CMakeFiles/
    CFLAGS="$EXTRA_CFLAGS -fPIC" \
      CXXFLAGS="$EXTRA_CXXFLAGS -fPIC" \
      LDFLAGS="$EXTRA_LDFLAGS" \
      cmake \
      -DCMAKE_BUILD_TYPE=release \
      -DBUILD_SHARED_LIBS=$SHARED \
      -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
      -DSNAPPY_BUILD_TESTS=OFF \
      $EXTRA_CMAKE_FLAGS \
      $SNAPPY_SOURCE
    make -j$PARALLEL $EXTRA_MAKEFLAGS
    make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
    popd
  done
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
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_lz4() {
  LZ4_BDIR=$TP_BUILD_DIR/$LZ4_NAME$MODE_SUFFIX
  mkdir -p $LZ4_BDIR
  pushd $LZ4_BDIR
  rm -Rf CMakeCache.txt CMakeFiles/
  CFLAGS="$EXTRA_CFLAGS" \
    cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DBUILD_STATIC_LIBS=On \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    $EXTRA_CMAKE_FLAGS \
    $LZ4_SOURCE/build/cmake
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

# TODO(aserbin): introduce features as AVX2 into the name of pre-built tarballs
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
    AVX2_SUPPORT=$(echo | ${CC:-gcc} -mavx2 -dM -E - | awk '$2 == "__AVX2__" { print $3 }')
  fi
  if [ -n "$AVX2_SUPPORT" ]; then
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

  rm -f bitshuffle.a
  ar rs bitshuffle.a $to_link
  mkdir -p $INSTALL_DESTDIR$PREFIX/lib $INSTALL_DESTDIR$PREFIX/include
  cp bitshuffle.a $INSTALL_DESTDIR$PREFIX/lib/
  cp $BITSHUFFLE_SOURCE/src/bitshuffle.h $INSTALL_DESTDIR$PREFIX/include/bitshuffle.h
  cp $BITSHUFFLE_SOURCE/src/bitshuffle_core.h $INSTALL_DESTDIR$PREFIX/include/bitshuffle_core.h
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
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_rapidjson() {
  # just installing it into our prefix
  pushd $RAPIDJSON_SOURCE
  mkdir -p $INSTALL_DESTDIR$PREFIX/include/rapidjson
  rsync -av --delete $RAPIDJSON_SOURCE/include/rapidjson/ $INSTALL_DESTDIR$PREFIX/include/rapidjson/
  popd
}

build_squeasel() {
  # Mongoose's Makefile builds a standalone web server, whereas we just want
  # a static lib
  SQUEASEL_BDIR=$TP_BUILD_DIR/$SQUEASEL_NAME$MODE_SUFFIX
  mkdir -p $SQUEASEL_BDIR
  pushd $SQUEASEL_BDIR
  CFLAGS="$EXTRA_CFLAGS \
    -DALLOW_UNSAFE_HTTP_METHODS \
    -DUSE_IPV6"
  ${CC:-gcc} $CFLAGS $OPENSSL_CFLAGS $OPENSSL_LDFLAGS -std=c99 -O3 -DNDEBUG -fPIC -c "$SQUEASEL_SOURCE/squeasel.c"
  ar rs libsqueasel.a squeasel.o
  mkdir -p $INSTALL_DESTDIR$PREFIX/lib $INSTALL_DESTDIR$PREFIX/include
  cp libsqueasel.a $INSTALL_DESTDIR$PREFIX/lib/
  cp $SQUEASEL_SOURCE/squeasel.h $INSTALL_DESTDIR$PREFIX/include/
  popd
}

build_mustache() {
  MUSTACHE_BDIR=$TP_BUILD_DIR/$MUSTACHE_NAME$MODE_SUFFIX
  mkdir -p $MUSTACHE_BDIR
  pushd $MUSTACHE_BDIR
  # We add $PREFIX/include for boost and $PREFIX_COMMON/include for rapidjson.
  ${CXX:-g++} \
    -std=c++17 $EXTRA_CXXFLAGS \
    -I$PREFIX/include -I$PREFIX_COMMON/include \
    -O3 -DNDEBUG -fPIC -c "$MUSTACHE_SOURCE/mustache.cc"
  ar rs libmustache.a mustache.o
  mkdir -p $INSTALL_DESTDIR$PREFIX/lib $INSTALL_DESTDIR$PREFIX/include
  cp libmustache.a $INSTALL_DESTDIR$PREFIX/lib/
  cp $MUSTACHE_SOURCE/mustache.h $INSTALL_DESTDIR$PREFIX/include/
  popd
}

build_curl() {
  CURL_BDIR=$TP_BUILD_DIR/$CURL_NAME$MODE_SUFFIX
  mkdir -p $CURL_BDIR
  pushd $CURL_BDIR

  # curl's configure script expects krb5-config to be in /usr/bin. If that's not
  # the case (looking at you, SLES12's /usr/lib/mit/bin/krb5-config), we need to
  # pass the right location via the KRB5CONFIG environment variable.
  #
  # TODO(adar): there's gotta be a way to do this without using export/unset.
  KRB5CONFIG_LOCATION=$(which krb5-config 2>/dev/null || :)
  if [ -n "$KRB5CONFIG_LOCATION" -a "$KRB5CONFIG_LOCATION" != "/usr/bin/krb5-config" ]; then
    export KRB5CONFIG=$KRB5CONFIG_LOCATION
  fi
  # In the case the special SLES location is not on the PATH but exists and we haven't
  # found another viable KRB5CONFIG_LOCATION, use the special SLES location.
  SLES_KRB5CONFIG_LOCATION="/usr/lib/mit/bin/krb5-config"
  if [ -z "$KRB5CONFIG_LOCATION" -a -f "$SLES_KRB5CONFIG_LOCATION" ]; then
    export KRB5CONFIG=$SLES_KRB5CONFIG_LOCATION
  fi

  # In the scope of using libcurl in Kudu tests and other simple scenarios,
  # not so much functionality is needed as of now, so configure for a fairly
  # minimal install. For testing, we need HTTP/HTTPS with GSSAPI support
  # (GSSAPI is needed for SPNEGO testing).
  #
  # NOTE: curl shows a message asking for CPPFLAGS to be used for include
  #       directories, not CFLAGS.
  #
  CFLAGS="$EXTRA_CFLAGS" \
    CPPFLAGS="$EXTRA_CPPFLAGS $OPENSSL_CFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS $OPENSSL_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $CURL_SOURCE/configure \
    --prefix=$PREFIX \
    --disable-alt-svc \
    --disable-aws \
    --disable-cookies \
    --disable-dateparse \
    --disable-dict \
    --disable-docs \
    --disable-doh \
    --disable-file \
    --disable-form-api  \
    --disable-ftp \
    --disable-headers-api \
    --disable-hsts \
    --disable-httpsrr \
    --disable-gopher \
    --disable-imap \
    --disable-ipfs \
    --disable-ldap \
    --disable-ldaps \
    --disable-libcurl-option \
    --disable-manual \
    --disable-mime \
    --disable-mqtt \
    --disable-netrc \
    --disable-ntlm \
    --disable-pop3 \
    --disable-progress-meter \
    --disable-rtsp \
    --disable-sha512-256 \
    --disable-smb \
    --disable-smtp \
    --disable-sspi \
    --disable-telnet \
    --disable-tftp \
    --disable-tls-srp \
    --disable-unix-sockets \
    --disable-websockets \
    --enable-basic-auth \
    --enable-bearer-auth \
    --enable-digest-auth \
    --enable-http-auth \
    --enable-ipv6 \
    --enable-kerberos-auth \
    --enable-negotiate-auth \
    --without-apple-idn \
    --without-brotli \
    --without-fish-functions-dir \
    --without-libidn2 \
    --without-libpsl \
    --without-librtmp \
    --without-libssh \
    --without-libssh2 \
    --without-libuv \
    --without-msh3 \
    --without-nghttp2 \
    --without-nghttp3 \
    --without-ngtcp2 \
    --without-openssl-quic \
    --without-quiche \
    --without-zsh-functions-dir \
    --without-zstd \
    --with-gssapi \
    --with-openssl
  unset KRB5CONFIG
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
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
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_breakpad() {
  BREAKPAD_BDIR=$TP_BUILD_DIR/$BREAKPAD_NAME$MODE_SUFFIX
  mkdir -p $BREAKPAD_BDIR
  pushd $BREAKPAD_BDIR

  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $BREAKPAD_SOURCE/configure --prefix=$PREFIX
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install

  # Here we run a script to munge breakpad #include statements in-place. Sadly,
  # breakpad does not prefix its header file paths. Until we can solve that
  # issue upstream we use this "hack" to add breakpad/ as a prefix for all the
  # breakpad-related includes in the breakpad header files ourselves. See also
  # https://bugs.chromium.org/p/google-breakpad/issues/detail?id=721
  local breakpad_include_dir=${INSTALL_DESTDIR}${PREFIX}/include/breakpad
  pushd "$breakpad_include_dir"
  find . -type f | xargs grep -l "#include" | \
    xargs perl -p -i -e '@pre = qw(client common google_breakpad processor third_party); for $p (@pre) { s{#include "$p/}{#include "breakpad/$p/}; }'
  popd

  popd
}

build_cpplint() {
  # Copy cpplint tool into bin directory
  local dst_dir=$INSTALL_DESTDIR$PREFIX/bin
  mkdir -p $dst_dir
  cp $CPPLINT_SOURCE/cpplint.py $dst_dir
}

build_gcovr() {
  # Copy gcovr tool into bin directory
  local dst_dir=$INSTALL_DESTDIR$PREFIX/bin
  mkdir -p $dst_dir
  cp -a $GCOVR_SOURCE/scripts/gcovr $dst_dir
}

# The trace viewer naturally comes as pre-built in its source tarball.
build_trace_viewer() {
  local dst_dir=$INSTALL_DESTDIR$PREFIX/share/trace-viewer
  mkdir -p $dst_dir
  cp -a $TRACE_VIEWER_SOURCE/tracing.* $dst_dir
}

build_boost() {
  BOOST_BDIR=$TP_BUILD_DIR/boost-$BOOST_VERSION$MODE_SUFFIX
  mkdir -p $BOOST_BDIR
  pushd $BOOST_BDIR
  rm -Rf CMakeCache.txt CMakeFiles/

  # A bit of customization for compile time:
  #   * compile with PIC
  #   * set nanosecond resolution for Impala compatibility
  local BOOST_CUSTOM_FLAGS="\
    -fPIC \
    -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG"
  BOOST_CFLAGS="$EXTRA_CFLAGS $BOOST_CUSTOM_FLAGS"
  BOOST_CXXFLAGS="$EXTRA_CXXFLAGS $BOOST_CUSTOM_FLAGS"
  BOOST_LDFLAGS="$EXTRA_LDFLAGS"

  # Only the 'date_time' Boost library is used in binary form by Kudu,
  # the rest of the Boost usage in Kudu is header-only. Also, some other
  # Boost libraries are used (header-only) by a few 3rd-party components
  # (e.g., Thrift, mustache). The list below isn't exhaustive, mentioning
  # only top-level Boost libraries that implicitly bring in other libraries
  # by dependency:
  #
  #   date_time:      Kudu Thrift
  #   heap:           Kudu
  #   locale:              Thrift
  #   serialization:  Kudu
  #   signals2:       Kudu
  #   uuid:           Kudu Thrift
  #
  # Even for header-only usage, it's necessary to explicitly specify the
  # libraries to install their header files when building Boost with cmake,
  # at least in 1.91.0.
  local BOOST_LIBS="date_time;heap;locale;serialization;signals2;uuid"

  # Build the 'locale' Boost library with the ICU support explicitly disabled:
  # otherwise, cmake automatically enables the ICU support if it finds ICU
  # libraries installed during the configuration phase. Since ICU isn't a
  # documented requirement for building Kudu, that might lead to unexpected
  # difference in the result libraries when building on different machines.
  CFLAGS="$BOOST_CFLAGS" \
  CXXFLAGS="$BOOST_CXXFLAGS" \
  LDFLAGS="$BOOST_LDFLAGS" \
    cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DBOOST_INCLUDE_LIBRARIES="$BOOST_LIBS" \
    -DBOOST_LOCALE_ENABLE_ICU=OFF \
    $EXTRA_CMAKE_FLAGS \
    $BOOST_SOURCE

  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_sparsehash() {
  # This library is header-only, so we just copy the headers
  pushd $SPARSEHASH_SOURCE
  mkdir -p $INSTALL_DESTDIR$PREFIX/include/sparsehash
  rsync -av --delete sparsehash/ $INSTALL_DESTDIR$PREFIX/include/sparsehash/
  popd
}

build_sparsepp() {
  # This library is header-only, so we just copy the headers
  pushd $SPARSEPP_SOURCE
  mkdir -p $INSTALL_DESTDIR$PREFIX/include/sparsepp
  rsync -av --delete sparsepp/ $INSTALL_DESTDIR$PREFIX/include/sparsepp/
  popd
}

build_thrift() {
  THRIFT_BDIR=$TP_BUILD_DIR/$THRIFT_NAME$MODE_SUFFIX
  mkdir -p $THRIFT_BDIR
  pushd $THRIFT_BDIR
  rm -Rf CMakeCache.txt CMakeFiles/

  # Thrift depends on bison.
  #
  # Configure for a very minimal install: we need only the Thrift compiler and
  # C++ client libraries with TSL/SSL support (explicitly added WITH_OPENSSL=ON
  # to enable the latter).
  # Thrift requires at least C++11 when compiled on Linux against libc++
  # (see cxxfunctional.h), but Kudu requires at least C++17-compatible
  # compiler anyway: added -DCMAKE_CXX_STANDARD=17 and -std=c++17 for clarity.
  CFLAGS="$EXTRA_CFLAGS -fPIC" \
  CXXFLAGS="$EXTRA_CXXFLAGS -fPIC -std=c++17" \
  LDFLAGS="$EXTRA_LDFLAGS" \
  LIBS="$EXTRA_LIBS" \
    cmake \
    -DBOOST_ROOT=$PREFIX \
    -DBUILD_AS3=OFF \
    -DBUILD_C_GLIB=OFF \
    -DBUILD_COMPILER=ON \
    -DBUILD_CPP=ON \
    -DBUILD_JAVA=OFF \
    -DBUILD_JAVASCRIPT=OFF \
    -DBUILD_NODEJS=OFF \
    -DBUILD_PYTHON=OFF \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_TESTING=OFF \
    -DBUILD_TUTORIALS=OFF \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_CXX_STANDARD=17 \
    -DWITH_LIBEVENT=OFF \
    -DWITH_OPENSSL=ON \
    -DWITH_QT5=OFF \
    -DWITH_ZLIB=OFF \
    $EXTRA_CMAKE_FLAGS \
    $THRIFT_SOURCE

  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd

  # Install fb303.thrift into the share directory.
  local fb303_dir="$INSTALL_DESTDIR$PREFIX/share/fb303/if"
  mkdir -p $fb303_dir
  cp $THRIFT_SOURCE/contrib/fb303/if/fb303.thrift $fb303_dir
}

build_bison() {
  BISON_BDIR=$TP_BUILD_DIR/$BISON_NAME$MODE_SUFFIX
  mkdir -p $BISON_BDIR
  pushd $BISON_BDIR
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $BISON_SOURCE/configure \
    --prefix=$PREFIX
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_yaml() {
  YAML_SHARED_BDIR=$TP_BUILD_DIR/$YAML_NAME.shared$MODE_SUFFIX
  YAML_STATIC_BDIR=$TP_BUILD_DIR/$YAML_NAME.static$MODE_SUFFIX
  for SHARED in ON OFF; do
    if [ $SHARED = "ON" ]; then
      YAML_BDIR=$YAML_SHARED_BDIR
    else
      YAML_BDIR=$YAML_STATIC_BDIR
    fi
    mkdir -p $YAML_BDIR
    pushd $YAML_BDIR
    rm -rf CMakeCache.txt CMakeFiles/
    CFLAGS="$EXTRA_CFLAGS -fPIC" \
      CXXFLAGS="$EXTRA_CXXFLAGS -fPIC" \
      LDFLAGS="$EXTRA_LDFLAGS" \
      LIBS="$EXTRA_LIBS" \
      cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DYAML_CPP_BUILD_TESTS=OFF \
      -DYAML_CPP_BUILD_TOOLS=OFF \
      -DBUILD_SHARED_LIBS=$SHARED \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      $EXTRA_CMAKE_FLAGS \
      $YAML_SOURCE
    make -j$PARALLEL $EXTRA_MAKEFLAGS
    make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
    popd
  done
}

build_chrony() {
  local comp_name=$CHRONY_NAME$MODE_SUFFIX
  CHRONY_BDIR=$TP_BUILD_DIR/$comp_name
  mkdir -p $CHRONY_BDIR
  pushd $CHRONY_BDIR

  # The configure script for chrony doesn't follow the common policy of
  # the autogen tools (probably, it's manually written from scratch).
  # It's not possible to configure and build chrony in a separate directory;
  # it's necessary to do so in the source directory itself.
  rsync -av --delete $CHRONY_SOURCE/ .

  # In the scope of using chrony in Kudu test framework, it's better to have
  # leaner binaries for chronyd and chronyc, stripping off everything but
  # essential functionality.
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    ./configure \
    --prefix=$PREFIX \
    --sysconfdir=$PREFIX/etc \
    --localstatedir=$PREFIX/var \
    --enable-debug \
    --disable-ipv6 \
    --disable-pps \
    --disable-privdrop \
    --disable-readline \
    --without-editline \
    --without-nettle \
    --without-nss \
    --without-tomcrypt \
    --without-libcap \
    --without-seccomp \
    --disable-forcednsretry \
    --disable-sechash

  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_gumbo_parser() {
  # Although it's a C library its tests are written in C++.
  local comp_name=$GUMBO_PARSER_NAME$MODE_SUFFIX
  GUMBO_PARSER_BDIR=$TP_BUILD_DIR/$comp_name
  mkdir -p $GUMBO_PARSER_BDIR
  pushd $GUMBO_PARSER_BDIR
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    LIBS="$EXTRA_LIBS" \
    $GUMBO_PARSER_SOURCE/configure \
    --prefix=$PREFIX
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_gumbo_query() {
  local comp_name=$GUMBO_QUERY_NAME$MODE_SUFFIX
  GUMBO_QUERY_BDIR=$TP_BUILD_DIR/$comp_name
  mkdir -p $GUMBO_QUERY_BDIR
  pushd $GUMBO_QUERY_BDIR
  rm -Rf CMakeCache.txt CMakeFiles/
  cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_CXX_FLAGS="$EXTRA_CXXFLAGS -I$PREFIX/include" \
    -DCMAKE_EXE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DCMAKE_MODULE_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS -L$PREFIX/lib -Wl,-rpath,$PREFIX/lib" \
    $EXTRA_CMAKE_FLAGS \
    $GUMBO_QUERY_SOURCE
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_postgres() {
  POSTGRES_BDIR=$TP_BUILD_DIR/$POSTGRES_NAME$MODE_SUFFIX
  mkdir -p $POSTGRES_BDIR
  pushd $POSTGRES_BDIR

  # We don't need readline, zlib and icu, so let's simplify build. Also, do not
  # embed the hint for the linker based on the absolute prefix-based path,
  # switching to relative paths instead (see [1] and [2] for details). The
  # latter allows for more flexibility when deploying Postgres and its utility
  # binaries, so there isn't a need to override the compiled-in linker hints
  # using LD_LIBRARY_PATH -- that's useful for dist-test and alike.
  #
  # [1] https://stackoverflow.com/questions/70931415/
  # [2] https://stackoverflow.com/questions/38058041/
  CFLAGS="$EXTRA_CFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS" \
    $POSTGRES_SOURCE/configure \
    --prefix=$PREFIX \
    --without-icu \
    --without-readline \
    --without-zlib \
    --disable-rpath

  LD_RUN_PATH='$ORIGIN/../lib' make -j$PARALLEL $EXTRA_MAKEFLAGS
  LD_RUN_PATH='$ORIGIN/../lib' make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  # Remove the extensions (pgxs) sub-directory since it's not needed
  # but it's confusing to dist-test in certain directory layouts.
  rm -rf $INSTALL_DESTDIR$PREFIX/lib/postgresql/pgxs
  popd
}

build_postgres_jdbc() {
  local dest_dir=$INSTALL_DESTDIR$PREFIX/opt/jdbc
  mkdir -p $dest_dir
  cp -a $POSTGRES_JDBC_SOURCE/$POSTGRES_JDBC_NAME.jar $dest_dir/postgresql.jar
}

build_hadoop() {
  local dest_dir=$INSTALL_DESTDIR$PREFIX/opt/hadoop
  mkdir -p $dest_dir
  pushd $HADOOP_SOURCE
  tar cf - . | tar xf - -C $dest_dir
  popd
}

build_hive() {
  local dest_dir=$INSTALL_DESTDIR$PREFIX/opt/hive
  mkdir -p $dest_dir
  pushd $HIVE_SOURCE
  tar cf - . | tar xf - -C $dest_dir
  popd
}

build_ranger() {
  local dest_dir=$INSTALL_DESTDIR$PREFIX/opt/ranger
  mkdir -p $dest_dir

  pushd $RANGER_SOURCE
  tar cf - . | tar xf - -C $dest_dir
  popd

  pushd $dest_dir
  # Remove any hadoop jars included in the Ranger package to avoid unexpected
  # runtime behavior due to different versions of hadoop jars.
  rm -f lib/hadoop-[a-z-]*.jar
  popd

  # Symlink conf.dist directory to conf.
  pushd $dest_dir/ews/webapp/WEB-INF/classes
  ln -nsf conf.dist conf
  popd
}

build_ranger_kms() {
  local dest_dir=$INSTALL_DESTDIR$PREFIX/opt/ranger-kms
  mkdir -p $dest_dir

  pushd $RANGER_KMS_SOURCE
  tar cf - . | tar xf - -C $dest_dir
  popd

  pushd $dest_dir
  # Remove any hadoop jars included in the Ranger package to avoid unexpected
  # runtime behavior due to different versions of hadoop jars.
  rm -f lib/hadoop-[a-z-]*.jar
  popd

  # Symlink conf.dist directory to conf.
  pushd $dest_dir/ews/webapp/WEB-INF/classes
  ln -nsf conf.dist conf
  popd
}

build_jwt_cpp() {
  JWT_CPP_BUILD_DIR=$TP_BUILD_DIR/$JWT_CPP_NAME$MODE_SUFFIX
  mkdir -p $JWT_CPP_BUILD_DIR
  pushd $JWT_CPP_BUILD_DIR
  CFLAGS="$EXTRA_CFLAGS" \
    CXXFLAGS="$EXTRA_CXXFLAGS $OPENSSL_CFLAGS" \
    LDFLAGS="$EXTRA_LDFLAGS $OPENSSL_LDFLAGS" \
    cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DJWT_BUILD_EXAMPLES=OFF \
    $JWT_CPP_SOURCE
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

build_prometheus() {
  local dest_dir=$INSTALL_DESTDIR$PREFIX/opt/prometheus
  mkdir -p $dest_dir

  # Prometheus is a Go binary and does not require compilation.
  pushd $PROMETHEUS_SOURCE
  tar cf - . | tar xf - -C $dest_dir
  popd
}

build_rocksdb() {
  ROCKSDB_BUILD_DIR=$TP_BUILD_DIR/$ROCKSDB_NAME$MODE_SUFFIX
  mkdir -p $ROCKSDB_BUILD_DIR
  pushd $ROCKSDB_BUILD_DIR
  rm -Rf CMakeCache.txt CMakeFiles/
  CFLAGS="$EXTRA_CFLAGS -fPIC" \
    CXXFLAGS="$EXTRA_CXXFLAGS -fPIC" \
    cmake \
    -DROCKSDB_BUILD_SHARED=ON \
    -DPORTABLE=$PORTABLE \
    -DWITH_SNAPPY=ON \
    -Dsnappy_ROOT_DIR=$PREFIX \
    -DUSE_RTTI=ON \
    -DFAIL_ON_WARNINGS=OFF \
    -DWITH_BENCHMARK_TOOLS=OFF \
    -DWITH_CORE_TOOLS=OFF \
    -DWITH_TOOLS=OFF \
    -DWITH_TRACE_TOOLS=OFF \
    -DWITH_JNI=OFF \
    -DWITH_LIBURING=OFF \
    -DWITH_ZSTD=OFF \
    -DWITH_BZ2=OFF \
    -DWITH_LZ4=OFF \
    -DWITH_TESTS=OFF \
    -DWITH_GFLAGS=OFF \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_SHARED_LINKER_FLAGS="$EXTRA_LDFLAGS $EXTRA_LIBS -Wl,-rpath,$PREFIX/lib" \
    $ROCKSDB_SOURCE
  make -j$PARALLEL $EXTRA_MAKEFLAGS
  make DESTDIR=$INSTALL_DESTDIR $EXTRA_MAKEFLAGS install
  popd
}

source $TP_DIR/prebuilt-utils.sh
