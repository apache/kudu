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

# build-thirdparty.sh builds and installs thirdparty dependencies into prefix
# directories within the thirdparty directory. Three prefix directories are
# used, corresponding to build type:
#
#   * /thirdparty/installed/common - prefix directory for libraries and binary tools
#                                    common to all build types, e.g. CMake, C dependencies.
#   * /thirdparty/installed/uninstrumented - prefix directory for libraries built with
#                                            normal options (no sanitizer instrumentation).
#   * /thirdparty/installed/tsan - prefix directory for libraries built
#                                  with thread sanitizer instrumentation.
#
# Environment variables which can be set when calling build-thirdparty.sh:
#   * EXTRA_CFLAGS - additional flags passed to the C compiler.
#   * EXTRA_CXXFLAGS - additional flags passed to the C++ compiler.
#   * EXTRA_LDFLAGS - additional flags passed to the linker.
#   * EXTRA_LIBS - additional libraries to link.

set -ex

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/vars.sh
source $TP_DIR/build-definitions.sh

# Before doing anything, run the pre-flight check for missing dependencies.
# This avoids the most common issues people have with building (if they don't
# read the docs)
$TP_DIR/preflight.py

for PREFIX_DIR in $PREFIX_COMMON $PREFIX_DEPS $PREFIX_DEPS_TSAN; do
  mkdir -p $PREFIX_DIR/lib
  mkdir -p $PREFIX_DIR/include

  # On some systems, autotools installs libraries to lib64 rather than lib.  Fix
  # this by setting up lib64 as a symlink to lib.  We have to do this step first
  # to handle cases where one third-party library depends on another.
  ln -sf "$PREFIX_DIR/lib" "$PREFIX_DIR/lib64"
done

# We use -O2 instead of -O3 for thirdparty since benchmarks indicate
# that the benefits of a smaller code size outweight the benefits of
# more inlining.
#
# We also enable -fno-omit-frame-pointer so that profiling tools which
# use frame-pointer based stack unwinding can function correctly.
EXTRA_CFLAGS="$CFLAGS $EXTRA_CFLAGS -fno-omit-frame-pointer"
EXTRA_CXXFLAGS="$CXXFLAGS $EXTRA_CXXFLAGS -I${PREFIX_COMMON}/include -fno-omit-frame-pointer -O2"
EXTRA_LDFLAGS="$LDFLAGS $EXTRA_LDFLAGS -L${PREFIX_COMMON}/lib"
EXTRA_LIBS="$LIBS $EXTRA_LIBS"

if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
  PARALLEL=$(grep -c processor /proc/cpuinfo)

  # Explicitly disable the new gcc5 ABI. Until clang supports abi tags [1],
  # Kudu's generated code (which always uses clang) must be built against the
  # old ABI. There's no recourse for using both ABIs in the same process; gcc's
  # advice [2] is to build everything against the old ABI.
  #
  # 1. https://llvm.org/bugs/show_bug.cgi?id=23529
  # 2. https://gcc.gnu.org/onlinedocs/libstdc++/manual/using_dual_abi.html
  EXTRA_CXXFLAGS="$EXTRA_CXXFLAGS -D_GLIBCXX_USE_CXX11_ABI=0"
  DYLIB_SUFFIX="so"

  # Enable TSAN builds on Linux.
  F_TSAN=1
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS_OSX=1
  DYLIB_SUFFIX="dylib"
  PARALLEL=$(sysctl -n hw.ncpu)

  # Kudu builds with C++11, which on OS X requires using libc++ as the standard
  # library implementation. Some of the dependencies do not compile against
  # libc++ by default, so we specify it explicitly.
  EXTRA_CXXFLAGS="$EXTRA_CXXFLAGS -stdlib=libc++"
  EXTRA_LDFLAGS="$EXTRA_LDFLAGS -stdlib=libc++"
  EXTRA_LIBS="$EXTRA_LIBS -lc++ -lc++abi"
else
  echo Unsupported platform $OSTYPE
  exit 1
fi

################################################################################

if [ "$#" = "0" ]; then
  F_ALL=1
else
  # Allow passing specific libs to build on the command line
  for arg in $*; do
    case $arg in
      "cmake")      F_CMAKE=1 ;;
      "gflags")     F_GFLAGS=1 ;;
      "glog")       F_GLOG=1 ;;
      "gmock")      F_GMOCK=1 ;;
      "gperftools") F_GPERFTOOLS=1 ;;
      "libev")      F_LIBEV=1 ;;
      "lz4")        F_LZ4=1 ;;
      "bitshuffle") F_BITSHUFFLE=1;;
      "protobuf")   F_PROTOBUF=1 ;;
      "rapidjson")  F_RAPIDJSON=1 ;;
      "snappy")     F_SNAPPY=1 ;;
      "zlib")       F_ZLIB=1 ;;
      "squeasel")   F_SQUEASEL=1 ;;
      "gsg")        F_GSG=1 ;;
      "gcovr")      F_GCOVR=1 ;;
      "curl")       F_CURL=1 ;;
      "crcutil")    F_CRCUTIL=1 ;;
      "libunwind")  F_LIBUNWIND=1 ;;
      "llvm")       F_LLVM=1 ;;
      "trace-viewer") F_TRACE_VIEWER=1 ;;
      "nvml")       F_NVML=1 ;;
      "boost")      F_BOOST=1 ;;
      *)            echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

### Build common tools and libraries

PREFIX=$PREFIX_COMMON
MODE_SUFFIX=""

# Add tools to path
export PATH=$PREFIX/bin:$PATH

if [ -n "$F_ALL" -o -n "$F_CMAKE" ]; then
  build_cmake
fi

save_env

# Enable debug symbols so that stacktraces and linenumbers are available at
# runtime. CMake is compiled without debug symbols since it is a compile-time
# only tool.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

# Specifying -Wl,-rpath has different default behavior on GNU binutils ld vs.
# the GNU gold linker. ld sets RPATH (due to defaulting to --disable-new-dtags)
# and gold sets RUNPATH (due to defaulting to --enable-new-dtags). At the time
# of this writing, contrary to the way RPATH is treated, when RUNPATH is
# specified on a binary, glibc doesn't respect it for transitive (non-direct)
# library dependencies (see https://sourceware.org/bugzilla/show_bug.cgi?id=13945).
# So we must set RUNPATH for all deps-of-deps on the dep libraries themselves.
# This applies both here and the locations elsewhere in this script where we
# add $PREFIX/lib to -Wl,-rpath.
EXTRA_LDFLAGS="-Wl,-rpath,$PREFIX/lib $EXTRA_LDFLAGS"

if [ -n "$OS_LINUX" ] && [ -n "$F_ALL" -o -n "$F_LIBUNWIND" ]; then
  build_libunwind
fi

if [ -n "$F_ALL" -o -n "$F_ZLIB" ]; then
  build_zlib
fi

if [ -n "$F_ALL" -o -n "$F_LZ4" ]; then
  build_lz4
fi

if [ -n "$F_ALL" -o -n "$F_BITSHUFFLE" ]; then
  build_bitshuffle
fi

if [ -n "$F_ALL" -o -n "$F_LIBEV" ]; then
  build_libev
fi

if [ -n "$F_ALL" -o -n "$F_RAPIDJSON" ]; then
  build_rapidjson
fi

if [ -n "$F_ALL" -o -n "$F_SQUEASEL" ]; then
  build_squeasel
fi

if [ -n "$F_ALL" -o -n "$F_CURL" ]; then
  build_curl
fi

if [ -n "$F_ALL" -o -n "$F_GSG" ]; then
  build_cpplint
fi

if [ -n "$F_ALL" -o -n "$F_GCOVR" ]; then
  build_gcovr
fi

if [ -n "$F_ALL" -o -n "$F_TRACE_VIEWER" ]; then
  build_trace_viewer
fi

if [ -n "$OS_LINUX" ] && [ -n "$F_ALL" -o -n "$F_NVML" ]; then
  build_nvml
fi

if [ -n "$F_ALL" -o -n "$F_BOOST" ]; then
  build_boost
fi

restore_env

### Build C++ dependencies

PREFIX=$PREFIX_DEPS
MODE_SUFFIX=""

save_env

EXTRA_LDFLAGS="-Wl,-rpath,$PREFIX/lib $EXTRA_LDFLAGS"

if [ -n "$F_ALL" -o -n "$F_LLVM" ]; then
  build_llvm normal
fi

# Enable debug symbols so that stacktraces and linenumbers are available at
# runtime. LLVM is compiled without debug symbols as they take up more than
# 20GiB of disk space.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

if [ -n "$F_ALL" -o -n "$F_GFLAGS" ]; then
  build_gflags
fi

if [ -n "$F_ALL" -o -n "$F_GLOG" ]; then
  build_glog
fi

if [ -n "$F_ALL" -o -n "$F_GPERFTOOLS" ]; then
  build_gperftools
fi

if [ -n "$F_ALL" -o -n "$F_GMOCK" ]; then
  build_gmock
fi

if [ -n "$F_ALL" -o -n "$F_PROTOBUF" ]; then
  build_protobuf
fi

if [ -n "$F_ALL" -o -n "$F_SNAPPY" ]; then
  build_snappy
fi

if [ -n "$F_ALL" -o -n "$F_CRCUTIL" ]; then
  build_crcutil
fi

restore_env

## Build C++ dependencies with TSAN instrumentation

if [ -n "$F_TSAN" ]; then

  # Achieving good results with TSAN requires that:
  # 1. The C++ standard library should be instrumented with TSAN.
  # 2. Dependencies which internally use threads or synchronization be
  #    instrumented with TSAN.
  # 3. As a corollary to 1, the C++ standard library requires that all shared
  #    objects linked into an executable be built against the same version of
  #    the C++ standard library version.
  #
  # At the very least, we must build our own C++ standard library. We use libc++
  # because it's easy to build with clang, which has better TSAN support than gcc.
  #
  # To satisfy all of the above requirements, we first build libc++ instrumented
  # with TSAN, then build a second copy of every C++ dependency against that
  # libc++. Later on in the build process, Kudu is also built against libc++.
  #
  # Special flags for TSAN builds:
  #   * -fsanitize=thread -  enable the thread sanitizer during compilation.
  #   * -L ... - add the instrumented libc++ to the library search paths.
  #   * -isystem ... - Add libc++ headers to the system header search paths.
  #   * -nostdinc++ - Do not automatically link the system C++ standard library.
  #   * -Wl,-rpath,... - Add instrumented libc++ location to the rpath so that
  #                      it can be found at runtime.

  if which ccache >/dev/null ; then
    CLANG="$TP_DIR/../build-support/ccache-clang/clang"
    CLANGXX="$TP_DIR/../build-support/ccache-clang/clang++"
  else
    CLANG="$TP_DIR/clang-toolchain/bin/clang"
    CLANGXX="$TP_DIR/clang-toolchain/bin/clang++"
  fi
  export CC=$CLANG
  export CXX=$CLANGXX

  PREFIX=$PREFIX_DEPS_TSAN
  MODE_SUFFIX=".tsan"

  save_env

  # Build libc++abi first as it is a dependency for libc++. Its build has no
  # built-in support for sanitizers, so we build it regularly.
  if [ -n "$F_ALL" -o -n "$F_LLVM" ]; then
    build_libcxxabi
  fi

  # The libc++ build needs to be able to find libc++abi.
  EXTRA_CXXFLAGS="-L$PREFIX/lib $EXTRA_CXXFLAGS"
  EXTRA_LDFLAGS="-Wl,-rpath,$PREFIX/lib $EXTRA_LDFLAGS"

  # Build libc++ with TSAN enabled.
  if [ -n "$F_ALL" -o -n "$F_LLVM" ]; then
    build_libcxx tsan
  fi

  # Build the rest of the dependencies against the TSAN-instrumented libc++
  # instead of the system's C++ standard library.
  EXTRA_CXXFLAGS="-nostdinc++ $EXTRA_CXXFLAGS"
  EXTRA_CXXFLAGS="-stdlib=libc++ $EXTRA_CXXFLAGS"
  EXTRA_CXXFLAGS="-isystem $PREFIX/include/c++/v1 $EXTRA_CXXFLAGS"

  # Build the rest of the dependencies with TSAN instrumentation.
  EXTRA_CFLAGS="-fsanitize=thread $EXTRA_CFLAGS"
  EXTRA_CXXFLAGS="-fsanitize=thread $EXTRA_CXXFLAGS"
  EXTRA_CXXFLAGS="-DTHREAD_SANITIZER $EXTRA_CXXFLAGS"

  if [ -n "$F_ALL" -o -n "$F_LLVM" ]; then
    build_llvm tsan
  fi

  # Enable debug symbols so that stacktraces and linenumbers are available at
  # runtime. LLVM is compiled without debug symbols because the LLVM debug symbols
  # take up more than 20GiB of disk space.
  EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
  EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

  if [ -n "$F_ALL" -o -n "$F_PROTOBUF" ]; then
    build_protobuf
  fi

  if [ -n "$F_ALL" -o -n "$F_GFLAGS" ]; then
    build_gflags
  fi

  if [ -n "$F_ALL" -o -n "$F_GLOG" ]; then
    build_glog
  fi

  if [ -n "$F_ALL" -o -n "$F_GPERFTOOLS" ]; then
    build_gperftools
  fi

  if [ -n "$F_ALL" -o -n "$F_GMOCK" ]; then
    build_gmock
  fi

  if [ -n "$F_ALL" -o -n "$F_SNAPPY" ]; then
    build_snappy
  fi

  if [ -n "$F_ALL" -o -n "$F_CRCUTIL" ]; then
    build_crcutil
  fi

  restore_env
fi

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
