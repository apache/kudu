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
#   * PORTABLE - whether to build portable libraries, otherwise build native libraries. Portable
#                libraries may cause a slight performance degradation, it's recommend to disable
#                portable option if there is no port requirements. (defaults to ON).
#   * USE_PREBUILT_THIRDPARTY - when set to 0, build 3rd-party components from
#                               source instead of using archived pre-built
#                               artifacts, when they are available (default: 1).
#                               NOTE: different prefix is used for pre-built
#                                     and build-locally-from-source artifacts
#   * REBUILD_PREBUILT_THIRDPARTY - when set to 1, always build every 3rd-party
#                                   component from scratch, even if pre-built
#                                   archive is available either locally or
#                                   in the designated S3 bucket (default: 0)

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
      "client_only")    F_CLIENT_ONLY=1 ;;
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
      "cpplint")      F_CPPLINT=1 ;;
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
      "yaml")         F_YAML=1 ;;
      "chrony")       F_CHRONY=1 ;;
      "gumbo-parser") F_GUMBO_PARSER=1 ;;
      "gumbo-query")  F_GUMBO_QUERY=1 ;;
      "postgres")     F_POSTGRES=1 ;;
      "postgres-jdbc")F_POSTGRES_JDBC=1 ;;
      "ranger")       F_RANGER=1 ;;
      "jwt-cpp")      F_JWT_CPP=1 ;;
      "ranger-kms")   F_RANGER_KMS=1 ;;
      "rocksdb")      F_ROCKSDB=1 ;;
      "flatbuffers")  F_FLATBUFFERS=1 ;;
      "prometheus")   F_PROMETHEUS=1 ;;
      *)              echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

if [ "${USE_PREBUILT_THIRDPARTY:-1}" != "0" ]; then
  if [ $UID -eq 0 ]; then
    # If running as a super-user, prepare the prefix directories for pre-built
    # components: create necessary directories to match the pre-defined
    # prefix paths, so symbolic links to be created in these directories after
    # expanding prebuilt archives will point to the actual location of the
    # pre-built artifacts under TP_INSTALL_DIR.
    for PREFIX_DIR in $PREFIX_COMMON $PREFIX_DEPS $PREFIX_DEPS_TSAN; do
      mkdir -p $(dirname $PREFIX_DIR)
    done
  else
    # If running as a regular user, check for the presence of the pre-defined
    # prefix and permissions granted to the user on the prefix directory.
    # If the directory doesn't exist or the necessary permissions aren't
    # granted, prompt the user to prepare the pre-defined prefix directories
    # and grant access to these, so it will be possible to create symbolic
    # links in them after expanding prebuilt archives, pointing the links
    # to the actual location of the pre-built artifacts under TP_INSTALL_DIR.
    set +x
    for PREFIX_DIR in $PREFIX_COMMON $PREFIX_DEPS $PREFIX_DEPS_TSAN; do
      prefix_dir=$(dirname $PREFIX_DIR)
      if [ ! -d "$prefix_dir" ]; then
        echo ""
        echo ""
        echo ""
        echo "  Please create $prefix_dir directory and grant write permissions"
        echo "  to this user (UID $UID) on the directory to allow the build"
        echo "  process creating symbolic links in the directory, pointing"
        echo "  to the actual location of the thirdparty artifacts under the"
        echo "  $TP_INSTALL_DIR directory"
        echo ""
        echo ""
        echo ""
        exit 2
      fi
      if [ ! -r "$prefix_dir" -o ! -w "$prefix_dir" -o ! -x "$prefix_dir" ]; then
        echo ""
        echo ""
        echo ""
        echo "  Please grant read, write, and execute permissions to this user"
        echo "  (UID $UID) on the $prefix_dir directory to allow the build"
        echo "  process creating and using symbolic links in the directory."
        echo "  The symbolic links will be pointing to the actual location "
        echo "  of the thirdparty artifacts under the"
        echo "  $TP_INSTALL_DIR directory"
        echo ""
        echo ""
        echo ""
        exit 2
      fi
    done
    set -x
  fi
fi

finish() {
  # Run the post-flight checks.
  local postflight_args=
  if [ -n "$F_TSAN" ]; then
    postflight_args="$postflight_args --tsan"
  fi
  $TP_DIR/postflight.py $postflight_args

  echo "---------------------"
  echo "Thirdparty dependencies '$ARGS_TO_PRINT' built and installed successfully"
  exit 0
}

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
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS_OSX=1
  DYLIB_SUFFIX="dylib"
  PARALLEL=${PARALLEL:-$(sysctl -n hw.ncpu)}

  # Kudu builds with C++17, which on OS X requires using libc++ as the standard
  # library implementation. Some of the dependencies do not compile against
  # libc++ by default, so we specify it explicitly.
  EXTRA_CXXFLAGS="$EXTRA_CXXFLAGS -stdlib=libc++"
  EXTRA_LDFLAGS="$EXTRA_LDFLAGS -stdlib=libc++"
  EXTRA_LIBS="$EXTRA_LIBS -lc++ -lc++abi"

  # Build against the Macports or Homebrew OpenSSL versions, in order to match
  # the Kudu build.
  if ! OPENSSL_CFLAGS=$(pkg-config --cflags openssl); then
    # If OpenSSL is built via Homebrew, pkg-config does not report on cflags.
    homebrew_openssl_dirs=(/usr/local/opt/openssl /opt/homebrew/opt/openssl@1.1)
    for homebrew_openssl_dir in "${homebrew_openssl_dirs[@]}"; do
      if [ -d $homebrew_openssl_dir ]; then
        OPENSSL_CFLAGS="-I$homebrew_openssl_dir/include"
        OPENSSL_LDFLAGS="-L$homebrew_openssl_dir/lib"
      fi
    done
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

### Build portable libraries by default.
PORTABLE=${PORTABLE:-"ON"}

### Build common tools and header-only libraries

PREFIX=$PREFIX_COMMON
MODE_SUFFIX=""

# Add tools to path
export PATH=$PREFIX/bin:$PATH

if [ -n "$F_COMMON" -o -n "$F_CLIENT_ONLY" -o -n "$F_CMAKE" ]; then
  fetch_prebuilt_or_build cmake common
fi

if [ -n "$F_COMMON" -o -n "$F_CLIENT_ONLY" -o -n "$F_RAPIDJSON" ]; then
  fetch_prebuilt_or_build rapidjson common
fi

if [ -n "$F_COMMON" -o -n "$F_CPPLINT" ]; then
  fetch_prebuilt_or_build cpplint common
fi

if [ -n "$F_COMMON" -o -n "$F_GCOVR" ]; then
  fetch_prebuilt_or_build gcovr common
fi

if [ -n "$F_COMMON" -o -n "$F_TRACE_VIEWER" ]; then
  build_trace_viewer
fi

if [ -n "$F_COMMON" -o -n "$F_CLIENT_ONLY" -o -n "$F_SPARSEHASH" ]; then
  fetch_prebuilt_or_build sparsehash common
fi

if [ -n "$F_COMMON" -o -n "$F_SPARSEPP" ]; then
  fetch_prebuilt_or_build sparsepp common
fi

if [ -n "$F_COMMON" -o -n "$F_BISON" ]; then
  fetch_prebuilt_or_build bison common
fi

if [ -n "$F_COMMON" -o -n "$F_CHRONY" ]; then
  fetch_prebuilt_or_build chrony common
fi

if [ -n "$F_COMMON" -o -n "$F_POSTGRES" ]; then
  fetch_prebuilt_or_build postgres common
fi

if [ -n "$F_COMMON" -o -n "$F_POSTGRES_JDBC" ]; then
  fetch_prebuilt_or_build postgres_jdbc common
fi

if [ -n "$F_COMMON" -o -n "$F_HADOOP" ]; then
  fetch_prebuilt_or_build hadoop common
fi

if [ -n "$F_COMMON" -o -n "$F_HIVE" ]; then
  fetch_prebuilt_or_build hive common
fi

if [ -n "$F_COMMON" -o -n "$F_RANGER" ]; then
  fetch_prebuilt_or_build ranger common
fi

if [ -n "$F_COMMON" -o -n "$F_RANGER_KMS" ]; then
  fetch_prebuilt_or_build ranger_kms common
fi

# Actual Kudu binaries only use the header-only part
if [ -n "$F_COMMON" -o -n "$F_CLIENT_ONLY" -o -n "$F_FLATBUFFERS" ]; then
  fetch_prebuilt_or_build flatbuffers common
fi

if [ -n "$F_COMMON" -o -n "$F_PROMETHEUS" ]; then
  fetch_prebuilt_or_build prometheus common
fi

### Build C dependencies without instrumentation

PREFIX=$PREFIX_DEPS
MODE_SUFFIX=""

save_env

# Enable debug symbols so that stacktraces and linenumbers are available at runtime.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_ZLIB" ]; then
  fetch_prebuilt_or_build zlib uninstrumented
fi

# Put this after zlib to allow ARM builds to pick up compressed .debug_info support
if [ -n "$OS_LINUX" ] && \
    [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_LIBUNWIND" ]; then
  fetch_prebuilt_or_build libunwind uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_LZ4" ]; then
  fetch_prebuilt_or_build lz4 uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_BITSHUFFLE" ]; then
  fetch_prebuilt_or_build bitshuffle uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_LIBEV" ]; then
  fetch_prebuilt_or_build libev uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_SQUEASEL" ]; then
  fetch_prebuilt_or_build squeasel uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CURL" ]; then
  fetch_prebuilt_or_build curl uninstrumented
fi

restore_env

### Build C++ dependencies without instrumentation

# Clang is used by all builds so it is part of the 'common' library group even
# though its LLVM libraries are installed to $PREFIX_DEPS.
if [ -n "$F_COMMON" -o -n "$F_LLVM" ]; then
  fetch_prebuilt_or_build llvm uninstrumented
fi

# From this point forward, clang is available for us to use if needed.
if which ccache >/dev/null ; then
  CLANG="$TP_DIR/../build-support/ccache-clang/clang"
  CLANGXX="$TP_DIR/../build-support/ccache-clang/clang++"
else
  CLANG="$TP_DIR/clang-toolchain/bin/clang"
  CLANGXX="$TP_DIR/clang-toolchain/bin/clang++"
fi

save_env

# Enable debug symbols so that stacktraces and linenumbers are available at
# runtime. LLVM is compiled without debug symbols as they take up more than
# 20GiB of disk space.
EXTRA_CFLAGS="-g $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-g $EXTRA_CXXFLAGS"

# Build libc++abi first as it is a dependency for libc++.
if [ -n "$F_UNINSTRUMENTED" -o -n "$F_LLVM" ]; then
  fetch_prebuilt_or_build libcxxabi uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_LLVM" ]; then
  fetch_prebuilt_or_build libcxx uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_GFLAGS" ]; then
  fetch_prebuilt_or_build gflags uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_GLOG" ]; then
  fetch_prebuilt_or_build glog uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_GPERFTOOLS" ]; then
  fetch_prebuilt_or_build gperftools uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_GMOCK" ]; then
  fetch_prebuilt_or_build gmock uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_PROTOBUF" ]; then
  fetch_prebuilt_or_build protobuf uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_SNAPPY" ]; then
  fetch_prebuilt_or_build snappy uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_CRCUTIL" ]; then
  fetch_prebuilt_or_build crcutil uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_CLIENT_ONLY" -o -n "$F_BOOST" ]; then
  fetch_prebuilt_or_build boost uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_MUSTACHE" ]; then
  fetch_prebuilt_or_build mustache uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_BREAKPAD" ]; then
  fetch_prebuilt_or_build breakpad uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_THRIFT" ]; then
  fetch_prebuilt_or_build thrift uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_YAML" ]; then
  fetch_prebuilt_or_build yaml uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_GUMBO_PARSER" ]; then
  fetch_prebuilt_or_build gumbo-parser uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_GUMBO_QUERY" ]; then
  fetch_prebuilt_or_build gumbo-query uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_JWT_CPP" ]; then
  fetch_prebuilt_or_build jwt-cpp uninstrumented
fi

if [ -n "$F_UNINSTRUMENTED" -o -n "$F_ROCKSDB" ]; then
  fetch_prebuilt_or_build rocksdb uninstrumented
fi

restore_env

# If we're on macOS best to exit here, otherwise single dependency builds
# will try to build the tsan version of the dependency and fail.

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

if [ -n "$F_TSAN" -o -n "$F_ZLIB" ]; then
  fetch_prebuilt_or_build zlib tsan
fi

if [ -n "$OS_LINUX" ] && [ -n "$F_TSAN" -o -n "$F_LIBUNWIND" ]; then
  fetch_prebuilt_or_build libunwind tsan
fi

if [ -n "$F_TSAN" -o -n "$F_LZ4" ]; then
  fetch_prebuilt_or_build lz4 tsan
fi

if [ -n "$F_TSAN" -o -n "$F_BITSHUFFLE" ]; then
  fetch_prebuilt_or_build bitshuffle tsan
fi

if [ -n "$F_TSAN" -o -n "$F_LIBEV" ]; then
  fetch_prebuilt_or_build libev tsan
fi

if [ -n "$F_TSAN" -o -n "$F_SQUEASEL" ]; then
  fetch_prebuilt_or_build squeasel tsan
fi

if [ -n "$F_TSAN" -o -n "$F_CURL" ]; then
  fetch_prebuilt_or_build curl tsan
fi

restore_env

### Build C++ dependencies with TSAN instrumentation

# Build libc++abi first as it is a dependency for libc++. Its build has no
# built-in support for sanitizers, so we build it regularly.
if [ -n "$F_TSAN" -o -n "$F_LLVM" ]; then
  fetch_prebuilt_or_build libcxxabi tsan
fi

save_env

# Build libc++ with TSAN enabled.
if [ -n "$F_TSAN" -o -n "$F_LLVM" ]; then
  fetch_prebuilt_or_build libcxx tsan
fi

# Build the rest of the dependencies against the TSAN-instrumented libc++
# instead of the system's C++ standard library.
EXTRA_CXXFLAGS="-isystem $PREFIX/include/c++/v1 -nostdinc++ $EXTRA_CXXFLAGS"
EXTRA_LDFLAGS="-L$PREFIX/lib $EXTRA_LDFLAGS"
EXTRA_LDFLAGS="-Wl,-rpath,$PREFIX/lib $EXTRA_LDFLAGS"

# Build the rest of the dependencies with TSAN instrumentation.
EXTRA_CFLAGS="-fsanitize=thread $EXTRA_CFLAGS"
EXTRA_CXXFLAGS="-fsanitize=thread $EXTRA_CXXFLAGS"
EXTRA_CXXFLAGS="-DTHREAD_SANITIZER $EXTRA_CXXFLAGS"

if [ -n "$F_TSAN" -o -n "$F_LLVM" ]; then
  fetch_prebuilt_or_build llvm tsan
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
  fetch_prebuilt_or_build protobuf tsan
fi

if [ -n "$F_TSAN" -o -n "$F_GFLAGS" ]; then
  fetch_prebuilt_or_build gflags tsan
fi

if [ -n "$F_TSAN" -o -n "$F_GLOG" ]; then
  fetch_prebuilt_or_build glog tsan
fi

if [ -n "$F_TSAN" -o -n "$F_GMOCK" ]; then
  fetch_prebuilt_or_build gmock tsan
fi

if [ -n "$F_TSAN" -o -n "$F_SNAPPY" ]; then
  fetch_prebuilt_or_build snappy tsan
fi

if [ -n "$F_TSAN" -o -n "$F_CRCUTIL" ]; then
  fetch_prebuilt_or_build crcutil tsan
fi

if [ -n "$F_TSAN" -o -n "$F_BOOST" ]; then
  fetch_prebuilt_or_build boost tsan
fi

if [ -n "$F_TSAN" -o -n "$F_MUSTACHE" ]; then
  fetch_prebuilt_or_build mustache tsan
fi

if [ -n "$F_TSAN" -o -n "$F_BREAKPAD" ]; then
  fetch_prebuilt_or_build breakpad tsan
fi

if [ -n "$F_TSAN" -o -n "$F_THRIFT" ]; then
  fetch_prebuilt_or_build thrift tsan
fi

if [ -n "$F_TSAN" -o -n "$F_YAML" ]; then
  fetch_prebuilt_or_build yaml tsan
fi

if [ -n "$F_TSAN" -o -n "$F_GUMBO_PARSER" ]; then
  fetch_prebuilt_or_build gumbo-parser tsan
fi

if [ -n "$F_TSAN" -o -n "$F_GUMBO_QUERY" ]; then
  fetch_prebuilt_or_build gumbo-query tsan
fi

if [ -n "$F_TSAN" -o -n "$F_JWT_CPP" ]; then
  fetch_prebuilt_or_build jwt-cpp tsan
fi

if [ -n "$F_TSAN" -o -n "$F_ROCKSDB" ]; then
  fetch_prebuilt_or_build rocksdb tsan
fi

restore_env

finish
