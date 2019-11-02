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
#   * EXTRA_MAKEFLAGS - additional flags passed to make.
#   * PARALLEL - parallelism to use when compiling (defaults to number of cores).

set -ex

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/vars.sh
source $TP_DIR/build-definitions.sh

# Before doing anything, run the pre-flight check for missing dependencies.
# This avoids the most common issues people have with building (if they don't
# read the docs)
$TP_DIR/preflight.py

################################################################################

if [ "$#" = "0" ]; then
  ARGS_TO_PRINT="common uninstrumented tsan"

  F_COMMON=1
  F_UNINSTRUMENTED=1
  F_TSAN=1
else
  ARGS_TO_PRINT="$*"
  REQUESTED_EXPLICIT_DEPENDENCIES=1

  # Parse the command line for specific dependencies or dependency groups.
  for arg in $*; do
    case $arg in
      # Dependency groups.
      "common")         F_COMMON=1 ;;
      "uninstrumented") F_UNINSTRUMENTED=1 ;;
      "tsan")           F_TSAN=1 ;;

      # Dependencies.
      "cmake")        F_CMAKE=1 ;;
      "gflags")       F_GFLAGS=1 ;;
      "glog")         F_GLOG=1 ;;
      "gmock")        F_GMOCK=1 ;;
      "gperftools")   F_GPERFTOOLS=1 ;;
      "libev")        F_LIBEV=1 ;;
      "lz4")          F_LZ4=1 ;;
      "bitshuffle")   F_BITSHUFFLE=1 ;;
      "protobuf")     F_PROTOBUF=1 ;;
      "rapidjson")    F_RAPIDJSON=1 ;;
      "snappy")       F_SNAPPY=1 ;;
      "zlib")         F_ZLIB=1 ;;
      "squeasel")     F_SQUEASEL=1 ;;
      "mustache")     F_MUSTACHE=1 ;;
      "gsg")          F_GSG=1 ;;
      "gcovr")        F_GCOVR=1 ;;
      "curl")         F_CURL=1 ;;
      "crcutil")      F_CRCUTIL=1 ;;
      "libunwind")    F_LIBUNWIND=1 ;;
      "llvm")         F_LLVM=1 ;;
      "trace-viewer") F_TRACE_VIEWER=1 ;;
      "boost")        F_BOOST=1 ;;
      "breakpad")     F_BREAKPAD=1 ;;
      "sparsehash")   F_SPARSEHASH=1 ;;
      "sparsepp")     F_SPARSEPP=1 ;;
      "thrift")       F_THRIFT=1 ;;
      "bison")        F_BISON=1 ;;
      "hadoop")       F_HADOOP=1 ;;
      "hive")         F_HIVE=1 ;;
      "sentry")       F_SENTRY=1 ;;
      "yaml")         F_YAML=1 ;;
      *)              echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

finish() {
  # Run the post-flight checks.
  $TP_DIR/postflight.py

  echo "---------------------"
  echo "Thirdparty dependencies '$ARGS_TO_PRINT' built and installed successfully"
  exit 0
}

for PREFIX_DIR in $PREFIX_COMMON $PREFIX_DEPS $PREFIX_DEPS_TSAN; do
  mkdir -p $PREFIX_DIR/include

  # PREFIX_COMMON is for header-only libraries.
  if [ "$PREFIX_DIR" != "$PREFIX_COMMON" ]; then
    mkdir -p $PREFIX_DIR/lib

    # On some systems, autotools installs libraries to lib64 rather than lib.  Fix
    # this by setting up lib64 as a symlink to lib.  We have to do this step first
    # to handle cases where one third-party library depends on another.
    ln -nsf "$PREFIX_DIR/lib" "$PREFIX_DIR/lib64"
  fi
done

# Incorporate the value of these standard compilation environment variables into
# the EXTRA_* environment variables.
EXTRA_CFLAGS="$CFLAGS $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="$CXXFLAGS $EXTRA_CXXFLAGS"
EXTRA_LDFLAGS="$LDFLAGS $EXTRA_LDFLAGS"
EXTRA_LIBS="$LIBS $EXTRA_LIBS"

# We use -O2 instead of -O3 for thirdparty since benchmarks indicate
# that the benefits of a smaller code size outweight the benefits of
# more inlining.
#
# We also enable -fno-omit-frame-pointer so that profiling tools which
# use frame-pointer based stack unwinding can function correctly.
EXTRA_CFLAGS="-fno-omit-frame-pointer $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-fno-omit-frame-pointer -O2 $EXTRA_CXXFLAGS"

if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
  DYLIB_SUFFIX="so"
  PARALLEL=${PARALLEL:-$(grep -c processor /proc/cpuinfo)}

  if [ -d "$OPENSSL_WORKAROUND_DIR" ]; then
    # If the el6 workaround openssl is present, we must build dependencies
    # against that version of openssl, not the system version, because at test
    # runtime we use the workaround openssl.
    OPENSSL_CFLAGS="-I$OPENSSL_WORKAROUND_DIR/usr/include"
    OPENSSL_LDFLAGS="-L$OPENSSL_WORKAROUND_DIR/usr/lib64 -Wl,-rpath,$OPENSSL_WORKAROUND_DIR/usr/lib64"
  fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS_OSX=1
  DYLIB_SUFFIX="dylib"
  PARALLEL=${PARALLEL:-$(sysctl -n hw.ncpu)}

  # Kudu builds with C++11, which on OS X requires using libc++ as the standard
  # library implementation. Some of the dependencies do not compile against
  # libc++ by default, so we specify it explicitly.
  EXTRA_CXXFLAGS="$EXTRA_CXXFLAGS -stdlib=libc++"
  EXTRA_LDFLAGS="$EXTRA_LDFLAGS -stdlib=libc++"
  EXTRA_LIBS="$EXTRA_LIBS -lc++ -lc++abi"

  # Build against the Macports or Homebrew OpenSSL versions, in order to match
  # the Kudu build.
  if ! OPENSSL_CFLAGS=$(pkg-config --cflags openssl); then
    # If OpenSSL is built via Homebrew, pkg-config does not report on cflags.
    homebrew_openssl_dir=/usr/local/opt/openssl
    if [ -d $homebrew_openssl_dir ]; then
      OPENSSL_CFLAGS="-I$homebrew_openssl_dir/include"
      OPENSSL_LDFLAGS="-L$homebrew_openssl_dir/lib"
    fi
  fi

  # TSAN doesn't work on macOS. If it was explicitly asked for, respond with an
  # error. Otherwise, just disable it silently.
  if [ -n "$F_TSAN" ]; then
    if [ -n "$REQUESTED_EXPLICIT_DEPENDENCIES" ]; then
      echo TSAN does not work on macOS
      exit 1
    else
      unset F_TSAN
    fi
  fi
else
  echo Unsupported platform $OSTYPE
  exit 1
fi

### Detect and enable 'ninja' instead of 'make' for faster builds.
if which ninja-build > /dev/null ; then
  NINJA=ninja-build
  EXTRA_CMAKE_FLAGS=-GNinja
elif which ninja > /dev/null ; then
  NINJA=ninja
  EXTRA_CMAKE_FLAGS=-GNinja
fi

### Build common tools and header-only libraries

PREFIX=$PREFIX_COMMON
MODE_SUFFIX=""

# Add tools to path
export PATH=$PREFIX/bin:$PATH

if [ -n "$F_COMMON" -o -n "$F_CMAKE" ]; then
  build_cmake
fi

if [ -n "$F_COMMON" -o -n "$F_RAPIDJSON" ]; then
  build_rapidjson
fi

if [ -n "$F_COMMON" -o -n "$F_GSG" ]; then
  build_cpplint
fi

if [ -n "$F_COMMON" -o -n "$F_GCOVR" ]; then
  build_gcovr
fi

if [ -n "$F_COMMON" -o -n "$F_TRACE_VIEWER" ]; then
  build_trace_viewer
fi

if [ -n "$F_COMMON" -o -n "$F_SPARSEHASH" ]; then
  build_sparsehash
fi

if [ -n "$F_COMMON" -o -n "$F_SPARSEPP" ]; then
  build_sparsepp
fi

if [ -n "$F_COMMON" -o -n "$F_BISON" ]; then
  build_bison
fi

# Install Hadoop, Hive, and Sentry by symlinking their source directories (which
# are pre-built) into $PREFIX/opt.
if [ -n "$F_COMMON" -o -n "$F_HADOOP" ]; then
  mkdir -p $PREFIX/opt
  ln -nsf $HADOOP_SOURCE $PREFIX/opt/hadoop
fi

if [ -n "$F_COMMON" -o -n "$F_HIVE" ]; then
  mkdir -p $PREFIX/opt
  ln -nsf $HIVE_SOURCE $PREFIX/opt/hive
fi

if [ -n "$F_COMMON" -o -n "$F_SENTRY" ]; then
  mkdir -p $PREFIX/opt
  # Remove any hadoop jars included in the Sentry package to avoid unexpected
  # runtime behavior, due to different versions of hadoop jars are loaded
  # (one from Kudu's third-party dependency, the other from the Sentry package).
  rm -rf $SENTRY_SOURCE/lib/hadoop-[a-z-]*.jar
  ln -nsf $SENTRY_SOURCE $PREFIX/opt/sentry
fi

### Build C dependencies without instrumentation

PREFIX=$PREFIX_DEPS
MODE_SUFFIX=""

save_env

# Enable debug symbols so that stacktraces and linenumbers are available at runtime.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

if [ -n "$OS_LINUX" ] && [ -n "$F_UNINSTRUMENTED" -o -n "$F_LIBUNWIND" ]; then
  build_libunwind
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_ZLIB" ]; then
  build_zlib
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_LZ4" ]; then
  build_lz4
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_BITSHUFFLE" ]; then
  build_bitshuffle
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_LIBEV" ]; then
  build_libev
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_SQUEASEL" ]; then
  build_squeasel
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CURL" ]; then
  build_curl
fi

restore_env

### Build C++ dependencies without instrumentation

# Clang is used by all builds so it is part of the 'common' library group even
# though its LLVM libraries are installed to $PREFIX_DEPS.
if [ -n "$F_COMMON" -o -n "$F_LLVM" ]; then
  build_llvm normal
fi

save_env

# Enable debug symbols so that stacktraces and linenumbers are available at
# runtime. LLVM is compiled without debug symbols as they take up more than
# 20GiB of disk space.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_GFLAGS" ]; then
  build_gflags
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_GLOG" ]; then
  build_glog
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_GPERFTOOLS" ]; then
  build_gperftools
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_GMOCK" ]; then
  build_gmock
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_PROTOBUF" ]; then
  build_protobuf
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_SNAPPY" ]; then
  build_snappy
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CRCUTIL" ]; then
  build_crcutil
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_BOOST" ]; then
  build_boost normal
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_MUSTACHE" ]; then
  build_mustache
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_BREAKPAD" ]; then
  build_breakpad
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_THRIFT" ]; then
  build_thrift
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_YAML" ]; then
  build_yaml
fi

restore_env

# If we're on macOS best to exit here, otherwise single dependency builds will try to
# build the tsan version of the dependency (and fail).

if [[ "$OSTYPE" == "darwin"* ]]; then
  echo "Not building tsan dependencies on macOS."
  finish
fi

### Build dependencies with TSAN instrumentation

# Achieving good results with TSAN requires that:
# 1. The C++ standard library should be instrumented with TSAN.
# 2. Dependencies which internally use threads or synchronization be
#    instrumented with TSAN.
# 3. As a corollary to 1, the C++ standard library requires that all shared
#    objects linked into an executable be built against the same version of the
#    C++ standard library version.
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
#   * -Wl,-rpath,... - Add instrumented libc++ location to the rpath so that it
#                      can be found at runtime.

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

# Build the C (non-C++) dependencies with TSAN instrumentation.
EXTRA_CFLAGS="-fsanitize=thread $EXTRA_CFLAGS"

# Enable debug symbols so that stacktraces and linenumbers are available at runtime.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

if [ -n "$OS_LINUX" ] && [ -n "$F_TSAN" -o -n "$F_LIBUNWIND" ]; then
  build_libunwind
fi

if [ -n "$F_TSAN" -o -n "$F_ZLIB" ]; then
  build_zlib
fi

if [ -n "$F_TSAN" -o -n "$F_LZ4" ]; then
  build_lz4
fi

if [ -n "$F_TSAN" -o -n "$F_BITSHUFFLE" ]; then
  build_bitshuffle
fi

if [ -n "$F_TSAN" -o -n "$F_LIBEV" ]; then
  build_libev
fi

if [ -n "$F_TSAN" -o -n "$F_SQUEASEL" ]; then
  build_squeasel
fi

if [ -n "$F_TSAN" -o -n "$F_CURL" ]; then
  build_curl
fi

restore_env

### Build C++ dependencies with TSAN instrumentation

# Build libc++abi first as it is a dependency for libc++. Its build has no
# built-in support for sanitizers, so we build it regularly.
if [ -n "$F_TSAN" -o -n "$F_LLVM" ]; then
  build_libcxxabi
fi

save_env

# Build libc++ with TSAN enabled.
if [ -n "$F_TSAN" -o -n "$F_LLVM" ]; then
  build_libcxx tsan
fi

# Build the rest of the dependencies against the TSAN-instrumented libc++
# instead of the system's C++ standard library.
EXTRA_CXXFLAGS="-isystem $PREFIX/include/c++/v1 $EXTRA_CXXFLAGS"
EXTRA_LDFLAGS="-L$PREFIX/lib $EXTRA_LDFLAGS"
EXTRA_LDFLAGS="-Wl,-rpath,$PREFIX/lib $EXTRA_LDFLAGS"

# Build the rest of the dependencies with TSAN instrumentation.
EXTRA_CFLAGS="-fsanitize=thread $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-fsanitize=thread $EXTRA_CXXFLAGS"
EXTRA_CXXFLAGS="-DTHREAD_SANITIZER $EXTRA_CXXFLAGS"

if [ -n "$F_TSAN" -o -n "$F_LLVM" ]; then
  build_llvm tsan
fi

# LLVM is told to use libc++ explicitly and thus doesn't need these, but the
# rest of the dependencies need them.
#
# Note: -nostdinc++ is necessary to prevent C++ headers from using #include_next
# to chain the host's C++ headers. However, using it means we need to also use
# -Qunused-arguments because clang raises an unused argument warning when it
# detects -nostdinc++ on a link line, and there's no way to prevent that when
# passing -nostdinc++ to cmake via -DCMAKE_CXX_FLAGS [1].
#
# 1. https://gitlab.kitware.com/cmake/cmake/issues/12652
EXTRA_CXXFLAGS="-Qunused-arguments -nostdinc++ $EXTRA_CXXFLAGS"
EXTRA_LDFLAGS="-stdlib=libc++ $EXTRA_LDFLAGS"

# Enable debug symbols so that stacktraces and linenumbers are available at
# runtime. LLVM is compiled without debug symbols because the LLVM debug symbols
# take up more than 20GiB of disk space.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

if [ -n "$F_TSAN" -o -n "$F_PROTOBUF" ]; then
  build_protobuf
fi

if [ -n "$F_TSAN" -o -n "$F_GFLAGS" ]; then
  build_gflags
fi

if [ -n "$F_TSAN" -o -n "$F_GLOG" ]; then
  build_glog
fi

if [ -n "$F_TSAN" -o -n "$F_GMOCK" ]; then
  build_gmock
fi

if [ -n "$F_TSAN" -o -n "$F_SNAPPY" ]; then
  build_snappy
fi

if [ -n "$F_TSAN" -o -n "$F_CRCUTIL" ]; then
  build_crcutil
fi

if [ -n "$F_TSAN" -o -n "$F_BOOST" ]; then
  build_boost tsan
fi

if [ -n "$F_TSAN" -o -n "$F_MUSTACHE" ]; then
  build_mustache
fi

if [ -n "$F_TSAN" -o -n "$F_BREAKPAD" ]; then
  build_breakpad
fi

if [ -n "$F_TSAN" -o -n "$F_THRIFT" ]; then
  build_thrift
fi

if [ -n "$F_TSAN" -o -n "$F_YAML" ]; then
  build_yaml
fi

restore_env

finish
