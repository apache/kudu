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

if [ -z "$TP_DIR" ]; then
   echo "TP_DIR variable not set, check your scripts"
   exit 1
fi

TP_SOURCE_DIR="$TP_DIR/src"
TP_BUILD_DIR="$TP_DIR/build"
TP_INSTALL_DIR="$TP_DIR/installed"
TP_STAGING_DIR="$TP_DIR/staging"

# This URL corresponds to the CloudFront Distribution for the S3
# bucket cloudera-thirdparty-libs which is directly accessible at
# http://cloudera-thirdparty-libs.s3.amazonaws.com/
CLOUDFRONT_URL_PREFIX=https://d3dr9sfxru4sde.cloudfront.net

# Third party dependency downloading URL, default to the CloudFront
# Distribution URL.
DEPENDENCY_URL=${DEPENDENCY_URL:-$CLOUDFRONT_URL_PREFIX}

# Pre-built 3rd-party archives are stored in the same S3 bucket as the
# source archives by default. See prebuilt-utils.sh for naming conventions, etc.
PREBUILT_THIRDPARTY_URL=${PREBUILT_THIRDPARTY_URL:-$DEPENDENCY_URL/prebuilt}

# Differentiate the prefix, targeting either locally-compiled-from-source or
# prebuilt 3rd-party components. If switching from one to the other,
# it's necessary to clean up and start from scratch to have the same prefix
# across all the 3rd-party components installed. To clean up,
# remove the following sub-directories and files in $KUDU_HOME/thirdparty:
#
#   * build
#   * installed
#   * src
#   * clang-toolchain
#   * .build-hash.common
#   * .build-hash.tsan
#   * .build-hash.uninstrumented
#
if [ "${USE_PREBUILT_THIRDPARTY:-1}" = "0" ]; then
  PREFIX_COMMON=$TP_DIR/installed/common
  PREFIX_DEPS=$TP_DIR/installed/uninstrumented
  PREFIX_DEPS_TSAN=$TP_DIR/installed/tsan
else
  PREFIX_COMMON=/opt/kudu/thirdparty/common
  PREFIX_DEPS=/opt/kudu/thirdparty/uninstrumented
  PREFIX_DEPS_TSAN=/opt/kudu/thirdparty/tsan
fi

#
# The following variables are mandatory for every 3rd-party component:
#
# * <comp>_VERSION
#   Upstream version of the component: usually it's a sequence of numbers
#   composed by semantic versioning rules, but it can be a git hash
#   if no explicit upstream releases exist.
#
# * <comp>_PATCHLEVEL
#   Kudu-specific patch level, a number (in decimal representation).
#   By convention, it usually starts with 0 for a new upstream version
#   of a component, and then it should not decrease while staying with that
#   upstream version. It should be incremented by one every time a new patch
#   or a set of patches is added, or when corresponding build_<comp> function
#   in build-definitions.sh is updated.
#
# * <comp>_NAME
#   Name of the component that includes upstream version but doesn't include
#   Kudu patch level.
#
# * <comp>_SOURCE
#   Full path to the sub-directory in $TP_SOURCE_DIR when the component's
#   source archive is expanded to.  Usually, it's $TP_SOURCE_DIR/$<comp>_NAME
#
# * <comp>_ARCHIVE
#   Name of the source archive downloadable from the dedicated S3 bucket.
#   Usually, it's something similar $<comp>_NAME.tar.gz, where supported
#   archive types now are tar with different compressors (gzip, bzip2, xz)
#   and ZIP.
#
# The following variables are optional for a 3rd-party component:
#
# * <comp>_PATCHES
#   Bash array: a set of patch/update commands to amend the upstream source
#   archive with custom patches maintained by the Kudu project.
#
# * <comp>_EXTRA_COMMANDS
#   Bash array: a set of commands to run after expanding and patching the
#   source archive. In most cases, it's an invocation of the autoreconf utility.
#   Calling autoreconf sometimes is necessary to fix hard-coded aclocal
#   versions in 'configure' scripts that ship with upstream source archives.
#
# * <comp>_ANY_ARCH
#   Set to 1 to indicate that pre-built artifacts for this component fit
#   any architecture (e.g., x86_64, aarch64, etc.)
#
# * <comp>_ANY_OS
#   Set to 1 to indicate that pre-built artifacts for this component fit
#   any OS.
#
# * <comp>_ANY_OS_VERSION
#   Set to 1 to indicate that pre-built artifacts for this component fit
#   any version of the OS that the artifacts are built for. There is no need
#   to set <comp>_ANY_OS_VERSION if <comp>_ANY_OS is set to 1 already.
#
# * <comp>_ANY_TOOLCHAIN
#   Set to 1 to indicate that pre-built artifacts for this component do not
#   depend on the toolchain used to build them or build toolchain isn't
#   relevant to the production of the artifacts.  There is no need to set
#   <comp>_ANY_TOOLCHAIN if <comp>_ANY_ARCH is set to 1 already.
#
# * <comp>_SRC_URL
#   Set to custom URL from where to fetch the component's source tarball
#   instead of DEPENDENCY_URL (e.g., file:///tmp/3rdparty). This is useful
#   when introducing a new component, upgrading an existing one, experimenting
#   with changes in the source archive before publishing the source archive
#   in the dedicated S3 bucket accessible via $CLOUDFRONT_URL_PREFIX URL.
#
GFLAGS_VERSION=2.2.2
GFLAGS_PATCHLEVEL=0
GFLAGS_NAME=gflags-$GFLAGS_VERSION
GFLAGS_SOURCE=$TP_SOURCE_DIR/$GFLAGS_NAME
GFLAGS_ARCHIVE=$GFLAGS_NAME.tar.gz

GLOG_VERSION=0.6.0
GLOG_PATCHLEVEL=2
GLOG_NAME=glog-$GLOG_VERSION
GLOG_SOURCE=$TP_SOURCE_DIR/$GLOG_NAME
GLOG_ARCHIVE=$GLOG_NAME.tar.gz
GLOG_PATCHES=(
 "patch -p1 < $TP_DIR/patches/glog-make-internals-visible.patch"
 "patch -p1 < $TP_DIR/patches/glog-support-stacktrace-for-aarch64.patch"
)

GMOCK_VERSION=1.12.1
GMOCK_PATCHLEVEL=1
GMOCK_NAME=googletest-release-$GMOCK_VERSION
GMOCK_SOURCE=$TP_SOURCE_DIR/$GMOCK_NAME
GMOCK_ARCHIVE=$GMOCK_NAME.tar.gz
GMOCK_PATCHES=(
 "patch -p0 < $TP_DIR/patches/gmock-update-iwyu-pragma.patch"
)

GPERFTOOLS_VERSION=2.13
GPERFTOOLS_PATCHLEVEL=1
GPERFTOOLS_NAME=gperftools-$GPERFTOOLS_VERSION
GPERFTOOLS_SOURCE=$TP_SOURCE_DIR/$GPERFTOOLS_NAME
GPERFTOOLS_ARCHIVE=$GPERFTOOLS_NAME.tar.gz
GPERFTOOLS_PATCHES=(
 "patch -p1 < $TP_DIR/patches/gperftools-Replace-namespace-base-with-namespace-tcmalloc.patch"
)
GPERFTOOLS_EXTRA_COMMANDS=(
 "autoreconf -fvi"
)

FLATBUFFERS_VERSION=25.2.10
FLATBUFFERS_PATCHLEVEL=1
FLATBUFFERS_NAME=flatbuffers-$FLATBUFFERS_VERSION
FLATBUFFERS_SOURCE=$TP_SOURCE_DIR/$FLATBUFFERS_NAME
FLATBUFFERS_ARCHIVE=$FLATBUFFERS_NAME.tar.gz
FLATBUFFERS_PATCHES=(
 "patch -p1 < $TP_DIR/patches/flatbuffers-length-to-size-uint8-ptr.patch"
)

# NOTE: creating an empty 'third_party/googletest/m4' subdir is a recipe from
# the $PROTOBUF_SOURCE/autogen.sh file:
#
#   The absence of a m4 directory in googletest causes autoreconf to fail when
#   building under the CentOS docker image. It's a warning in regular build on
#   Ubuntu/gLinux as well.
#
PROTOBUF_VERSION=3.21.9
PROTOBUF_PATCHLEVEL=1
PROTOBUF_NAME=protobuf-$PROTOBUF_VERSION
PROTOBUF_SOURCE=$TP_SOURCE_DIR/$PROTOBUF_NAME
PROTOBUF_ARCHIVE=protobuf-cpp-$PROTOBUF_VERSION.tar.gz
PROTOBUF_PATCHES=(
 "patch -p1 < $TP_DIR/patches/protobuf-inlined_string_field.patch"
)
PROTOBUF_EXTRA_COMMANDS=(
 "mkdir -p third_party/googletest/m4"
 "autoreconf -fvi"
)

# Returns 0 if cmake should be patched to work around this bug [1].
#
# Currently only SLES 12 SP0 is known to be vulnerable, and since the workaround
# hurts cmake performance, we apply it only if absolutely necessary.
#
# 1. https://gitlab.kitware.com/cmake/cmake/issues/15873.
needs_patched_cmake() {
  if [ ! -e /etc/SuSE-release ]; then
    # Not a SUSE distro.
    return 1
  fi
  if ! grep -q "SUSE Linux Enterprise Server 12" /etc/SuSE-release; then
    # Not SLES 12.
    return 1
  fi
  if ! grep -q "PATCHLEVEL = 0" /etc/SuSE-release; then
    # Not SLES 12 SP0.
    return 1
  fi
  return 0
}
CMAKE_SLES_PATCH=""
if needs_patched_cmake; then
 CMAKE_SLES_PATCH="patch -p1 < $TP_DIR/patches/cmake-issue-15873-dont-use-select.patch"
fi

# cmake-fix-macos-compilation should be removed once cmake is upgraded to version 3.30 or later
# Note: CMake gets patched on SLES12SP0. When changing the CMake version, please check if
# cmake-issue-15873-dont-use-select.patch needs to be updated.
CMAKE_VERSION=3.25.3
CMAKE_PATCHLEVEL=2
CMAKE_NAME=cmake-$CMAKE_VERSION
CMAKE_SOURCE=$TP_SOURCE_DIR/$CMAKE_NAME
CMAKE_ARCHIVE=$CMAKE_NAME.tar.gz
CMAKE_PATCHES=(
 "$CMAKE_SLES_PATCH"
 "patch -p1 < $TP_DIR/patches/cmake-fix-macos-compilation.patch"
)

SNAPPY_VERSION=1.1.8
SNAPPY_PATCHLEVEL=0
SNAPPY_NAME=snappy-$SNAPPY_VERSION
SNAPPY_SOURCE=$TP_SOURCE_DIR/$SNAPPY_NAME
SNAPPY_ARCHIVE=$SNAPPY_NAME.tar.gz

LZ4_VERSION=1.10.0
LZ4_PATCHLEVEL=0
LZ4_NAME=lz4-$LZ4_VERSION
LZ4_SOURCE=$TP_SOURCE_DIR/$LZ4_NAME
LZ4_ARCHIVE=$LZ4_NAME.tar.gz

# from https://github.com/kiyo-masui/bitshuffle
BITSHUFFLE_VERSION=0.3.5
BITSHUFFLE_PATCHLEVEL=0
BITSHUFFLE_NAME=bitshuffle-$BITSHUFFLE_VERSION
BITSHUFFLE_SOURCE=$TP_SOURCE_DIR/$BITSHUFFLE_NAME
BITSHUFFLE_ARCHIVE=$BITSHUFFLE_NAME.tar.gz

ZLIB_VERSION=1.3.1
ZLIB_PATCHLEVEL=0
ZLIB_NAME=zlib-$ZLIB_VERSION
ZLIB_SOURCE=$TP_SOURCE_DIR/$ZLIB_NAME
ZLIB_ARCHIVE=$ZLIB_NAME.tar.gz

LIBEV_VERSION=4.33
LIBEV_PATCHLEVEL=0
LIBEV_NAME=libev-$LIBEV_VERSION
LIBEV_SOURCE=$TP_SOURCE_DIR/$LIBEV_NAME
LIBEV_ARCHIVE=$LIBEV_NAME.tar.gz

RAPIDJSON_VERSION=1.1.0
RAPIDJSON_PATCHLEVEL=5
RAPIDJSON_NAME=rapidjson-$RAPIDJSON_VERSION
RAPIDJSON_SOURCE=$TP_SOURCE_DIR/$RAPIDJSON_NAME
RAPIDJSON_ARCHIVE=$RAPIDJSON_NAME.zip
RAPIDJSON_ANY_ARCH=1
RAPIDJSON_ANY_OS=1
RAPIDJSON_PATCHES=(
 "patch -p1 < $TP_DIR/patches/rapidjson-fix-signed-unsigned-conversion-error.patch"
 "patch -p1 < $TP_DIR/patches/rapidjson-assertions-for-clang-warnings.patch"
 "patch -p1 < $TP_DIR/patches/rapidjson-avoid-pointer-arithmetic-on-null-pointer.patch"
 "patch -p1 < $TP_DIR/patches/rapidjson-document-assignment-operator-00.patch"
 "patch -p1 < $TP_DIR/patches/rapidjson-document-assignment-operator-01.patch"
)

# Hash of the squeasel git revision to use.
# (from http://github.com/cloudera/squeasel)
#
# To re-build this tarball use the following in the squeasel repo:
#  export NAME=squeasel-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SQUEASEL_VERSION=d83cf6d9af0e2c98c16467a6a035ae0d7ca21cb1
SQUEASEL_PATCHLEVEL=5
SQUEASEL_NAME=squeasel-$SQUEASEL_VERSION
SQUEASEL_SOURCE=$TP_SOURCE_DIR/$SQUEASEL_NAME
SQUEASEL_ARCHIVE=$SQUEASEL_NAME.tar.gz
SQUEASEL_PATCHES=(
 "patch -p1 < $TP_DIR/patches/squeasel-handle-openssl-errors.patch"
 "patch -p1 < $TP_DIR/patches/squeasel-tls-min-version.patch"
 "patch -p1 < $TP_DIR/patches/squeasel-support-get-bound-addresses-for-ipv6.patch"
 "patch -p1 < $TP_DIR/patches/squeasel-tls-openssl10x.patch"
 "patch -p1 < $TP_DIR/patches/squeasel-ipv6-only-socket-option.patch"
)

# Hash of the mustache git revision to use.
# (from https://github.com/henryr/cpp-mustache)
#
# To re-build this tarball use the following in the mustache repo:
#  export NAME=mustache-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
MUSTACHE_VERSION=b290952d8eb93d085214d8c8c9eab8559df9f606
MUSTACHE_PATCHLEVEL=0
MUSTACHE_NAME=mustache-$MUSTACHE_VERSION
MUSTACHE_SOURCE=$TP_SOURCE_DIR/$MUSTACHE_NAME
MUSTACHE_ARCHIVE=$MUSTACHE_NAME.tar.gz

# git release/revision of cpplint https://github.com/cpplint/cpplint
# (used to be a part of google styleguide https://github.com/google/styleguide)
#
# $ git clone https://github.com/cpplint/cpplint.git
# $ cd cpplint
# $ git tag -l    # to see available tags/snapshots
# $ git checkout 1.6.1  # checkout the sources of the chosen tag/snapshot
# $ git archive --prefix=cpplint-1.6.1/ -o /tmp/cpplint-1.6.1.tar.gz HEAD
CPPLINT_VERSION=1.6.1
CPPLINT_PATCHLEVEL=1
CPPLINT_NAME=cpplint-$CPPLINT_VERSION
CPPLINT_SOURCE=$TP_SOURCE_DIR/$CPPLINT_NAME
CPPLINT_ARCHIVE=$CPPLINT_NAME.tar.gz
CPPLINT_ANY_ARCH=1
CPPLINT_ANY_OS=1
CPPLINT_PATCHES=(
 "patch -p1 < $TP_DIR/patches/cpplint-libstdcpp-regex.patch"
)

GCOVR_VERSION=3.0
GCOVR_PATCHLEVEL=0
GCOVR_NAME=gcovr-$GCOVR_VERSION
GCOVR_SOURCE=$TP_SOURCE_DIR/$GCOVR_NAME
GCOVR_ARCHIVE=$GCOVR_NAME.tar.gz
GCOVR_ANY_ARCH=1
GCOVR_ANY_OS=1

CURL_VERSION=8.11.1
CURL_PATCHLEVEL=3
CURL_NAME=curl-$CURL_VERSION
CURL_SOURCE=$TP_SOURCE_DIR/$CURL_NAME
CURL_ARCHIVE=$CURL_NAME.tar.gz
CURL_PATCHES=(
 "patch -p1 < $TP_DIR/patches/curl-custom-openssl-library.patch"
 "patch -p1 < $TP_DIR/patches/curl-handle-openssl-errors.patch"
 "patch -p1 < $TP_DIR/patches/curl-eventfd-double-close.patch"
)
CURL_EXTRA_COMMANDS=(
 "autoreconf -fvi"
)

# Hash of the crcutil git revision to use.
# (from http://github.com/cloudera/crcutil)
#
# To re-build this tarball use the following in the crcutil repo:
#  export NAME=crcutil-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
CRCUTIL_VERSION=0437b1a99cf8a29910579ac440e48bb2385021b1
CRCUTIL_PATCHLEVEL=1
CRCUTIL_NAME=crcutil-$CRCUTIL_VERSION
CRCUTIL_SOURCE=$TP_SOURCE_DIR/$CRCUTIL_NAME
CRCUTIL_ARCHIVE=$CRCUTIL_NAME.tar.gz
CRCUTIL_PATCHES=(
 "patch -p1 < $TP_DIR/patches/crcutil-fix-macos-arm64-flags.patch"
)

LIBUNWIND_VERSION=1.8.3
LIBUNWIND_PATCHLEVEL=1
LIBUNWIND_NAME=libunwind-$LIBUNWIND_VERSION
LIBUNWIND_SOURCE=$TP_SOURCE_DIR/$LIBUNWIND_NAME
LIBUNWIND_ARCHIVE=$LIBUNWIND_NAME.tar.gz
LIBUNWIND_PATCHES=(
 "patch -p1 < $TP_DIR/patches/libunwind-trace-cache-destructor.patch"
)

# See package-llvm.sh for details on the LLVM tarball.
# The include-what-you-use is built along with LLVM in its source tree.
IWYU_VERSION=0.15
LLVM_VERSION=11.0.0
LLVM_PATCHLEVEL=12
LLVM_NAME=llvm-$LLVM_VERSION.src
LLVM_SOURCE=$TP_SOURCE_DIR/$LLVM_NAME
LLVM_ARCHIVE=llvm-$LLVM_VERSION-iwyu-$IWYU_VERSION.src.tar.gz
LLVM_PATCHES=(
 "patch -p1 < $TP_DIR/patches/llvm-add-iwyu.patch"
 "patch -p1 < $TP_DIR/patches/llvm-iwyu-718e69875.patch"
 "patch -p1 < $TP_DIR/patches/llvm-iwyu-0de60d8a2.patch"
 "patch -d projects -p1 < $TP_DIR/patches/llvm-remove-cyclades-inclusion-in-sanitizer.patch"
 "patch -p2 < $TP_DIR/patches/llvm-fix-missing-include.patch"
 "patch -d projects -p1 < $TP_DIR/patches/llvm-Sanitizer-built-against-glibc-2_34-doesnt-work.patch"
 "patch -d tools -p1 < $TP_DIR/patches/llvm-ignore-flto-values.patch"
 "patch -p1 < $TP_DIR/patches/llvm-nostdinc-nostdlib-00.patch"
 "patch -p1 < $TP_DIR/patches/llvm-nostdinc-nostdlib-01.patch"
 "patch -p1 < $TP_DIR/patches/llvm-nostdinc-nostdlib-02.patch"
 "patch -p1 < $TP_DIR/patches/llvm-include-llvm-support-signals.patch"
 "patch -p1 < $TP_DIR/patches/llvm-is-convertible-00.patch"
 "patch -p1 < $TP_DIR/patches/llvm-is-convertible-01.patch"
 "patch -p1 < $TP_DIR/patches/llvm-chrono-duration-00.patch"
 "patch -p1 < $TP_DIR/patches/llvm-chrono-duration-01.patch"
 "patch -p1 < $TP_DIR/patches/llvm-section-mm-memory-mapper.patch"
 "patch -p1 < $TP_DIR/patches/llvm-section-mm-extra-methods.patch"
 "patch -p2 < $TP_DIR/patches/llvm-gcc15-fix-missing-cstdint-include.patch"
)

# All libcxxabi's variables are pointing to LLVM's: libcxxabi is built
# from the same LLVM sources, but in a different build directory.
LIBCXXABI_VERSION=$LLVM_VERSION
LIBCXXABI_PATCHLEVEL=$LLVM_PATCHLEVEL
LIBCXXABI_ARCHIVE=$LLVM_ARCHIVE
LIBCXXABI_NAME=$LLVM_NAME
LIBCXXABI_SOURCE=$LLVM_SOURCE

# All libcxx's variables are pointing to LLVM's: libcxxabi is built
# from the same LLVM sources, but in a different build directory.
LIBCXX_VERSION=$LLVM_VERSION
LIBCXX_PATCHLEVEL=$LLVM_PATCHLEVEL
LIBCXX_ARCHIVE=$LLVM_ARCHIVE
LIBCXX_NAME=$LLVM_NAME
LIBCXX_SOURCE=$LLVM_SOURCE

# Our trace-viewer repository is separate since it's quite large and
# shouldn't change frequently. We upload the built artifacts (HTML/JS)
# when we need to roll to a new revision.
#
# The source can be found in the 'kudu' branch of https://github.com/cloudera/catapult
# and built with "tracing/kudu-build.sh" included within the repository.
TRACE_VIEWER_VERSION=99efe2f56191867ba7bb602c7c227dea6d576d2f
TRACE_VIEWER_PATCHLEVEL=0
TRACE_VIEWER_NAME=kudu-trace-viewer-$TRACE_VIEWER_VERSION
TRACE_VIEWER_SOURCE=$TP_SOURCE_DIR/$TRACE_VIEWER_NAME
TRACE_VIEWER_ARCHIVE=$TRACE_VIEWER_NAME.tar.gz
TRACE_VIEWER_ANY_ARCH=1
TRACE_VIEWER_ANY_OS=1

# Since 1.91.0 version, the distro file for the Boost library is a git archive,
# not a regular/legacy source distribution archive which targets b2-based build
# and available at https://archives.boost.org/release. The git archive
# allows for building the Boost library with standard cmake and GNU make tools,
# so it's possible to install the result artifacts into a staging area with
# a pre-defined PREFIX using the DESTDIR approach. This is important for
# pre-built 3rd-party components since they have to have a pre-defined prefix
# independent of the layout of the local Kudu workspace.
#
# At the time of writing this, it's possible to download git archives
# for the Boost library from https://github.com/boostorg/boost/releases page.
# For 1.91.0 it's sourced from:
#   https://github.com/boostorg/boost/releases/download/boost-1.91.0-1/boost-1.91.0-1-cmake.tar.gz
# In some cases, the original archive might require repackaging to conform to
# the layout convention for $KUDU_HOME/thirdparty/{build,src} directories.
# For example, it's done so for 1.91.0 release to remove the extra '-1' suffix.
#
# References:
#   https://www.boost.org/doc/user-guide/getting-started.html
#   https://github.com/boostorg/cmake
#   https://www.boost.org/doc/user-guide/building-with-cmake.html
BOOST_VERSION=1.91.0
BOOST_PATCHLEVEL=0
BOOST_NAME=boost-$BOOST_VERSION
BOOST_SOURCE=$TP_SOURCE_DIR/$BOOST_NAME
BOOST_ARCHIVE=$BOOST_NAME-cmake.tar.gz

# The breakpad source artifact is created using the script found in
# scripts/make-breakpad-src-archive.sh
BREAKPAD_VERSION=9eac2058b70615519b2c4d8c6bdbfca1bd079e39
BREAKPAD_PATCHLEVEL=8
BREAKPAD_NAME=breakpad-$BREAKPAD_VERSION
BREAKPAD_SOURCE=$TP_SOURCE_DIR/$BREAKPAD_NAME
BREAKPAD_ARCHIVE=$BREAKPAD_NAME.tar.gz
BREAKPAD_PATCHES=(
 "patch -p1 < $TP_DIR/patches/breakpad-add-basic-support-for-dwz-dwarf-extension.patch"
 "patch -p1 < $TP_DIR/patches/breakpad-syscall-rsp-clobber-fix.patch"
 "patch -p1 < $TP_DIR/patches/breakpad-SIGSTKSZ-error.patch"
 "patch -p1 < $TP_DIR/patches/breakpad-fclose.patch"
 "patch -p1 < $TP_DIR/patches/breakpad-fread.patch"
 "patch -p1 < $TP_DIR/patches/breakpad-minidump-descriptor.patch"
 "patch -p1 < $TP_DIR/patches/breakpad-guid-creator.patch"
 "patch -p1 < $TP_DIR/patches/breakpad-64k-pages-stack-collection.patch"
)

# Hash of the sparsehash-c11 git revision to use.
# (from http://github.com/sparsehash/sparsehash-c11)
#
# To re-build this tarball use the following in the sparsehash-c11 repo:
#  export NAME=sparsehash-c11-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SPARSEHASH_VERSION=cf0bffaa456f23bc4174462a789b90f8b6f5f42f
SPARSEHASH_PATCHLEVEL=3
SPARSEHASH_NAME=sparsehash-c11-$SPARSEHASH_VERSION
SPARSEHASH_SOURCE=$TP_SOURCE_DIR/$SPARSEHASH_NAME
SPARSEHASH_ARCHIVE=$SPARSEHASH_NAME.tar.gz
SPARSEHASH_ANY_ARCH=1
SPARSEHASH_ANY_OS=1
SPARSEHASH_PATCHES=(
 "patch -p1 < $TP_DIR/patches/sparsehash-0001-Add-compatibily-for-gcc-4.x-in-traits.patch"
 "patch -p1 < $TP_DIR/patches/sparsehash-0002-Add-workaround-for-dense_hashtable-move-constructor-.patch"
)

SPARSEPP_VERSION=1.22
SPARSEPP_PATCHLEVEL=0
SPARSEPP_NAME=sparsepp-$SPARSEPP_VERSION
SPARSEPP_SOURCE=$TP_SOURCE_DIR/$SPARSEPP_NAME
SPARSEPP_ARCHIVE=$SPARSEPP_NAME.tar.gz
SPARSEPP_ANY_ARCH=1
SPARSEPP_ANY_OS=1

THRIFT_VERSION=0.23.0
THRIFT_PATCHLEVEL=1
THRIFT_NAME=thrift-$THRIFT_VERSION
THRIFT_SOURCE=$TP_SOURCE_DIR/$THRIFT_NAME
THRIFT_ARCHIVE=$THRIFT_NAME.tar.gz
THRIFT_PATCHES=(
 "patch -p1 < $TP_DIR/patches/thrift-e96bc4015.patch"
 "patch -p1 < $TP_DIR/patches/thrift-c1457c69f.patch"
 "patch -p1 < $TP_DIR/patches/thrift-5748bbb6b.patch"
 "patch -p1 < $TP_DIR/patches/thrift-e3c8c534c.patch"
)

# This would normally call autoreconf, but it does not succeed with autoreconf
# 2.69-11 (RHEL 7): "autoreconf: 'configure.ac' or 'configure.in' is required".
BISON_VERSION=3.8.2
BISON_PATCHLEVEL=0
BISON_NAME=bison-$BISON_VERSION
BISON_SOURCE=$TP_SOURCE_DIR/$BISON_NAME
BISON_ARCHIVE=$BISON_NAME.tar.gz

# Note: The Hive release binary tarball is stripped of unnecessary jars before
# being uploaded. See thirdparty/package-hive.sh for details.
# ./thirdparty/package-hive.sh -d -r -v 3.1.2 apache-hive-3.1.2-bin
HIVE_VERSION=3.1.2
HIVE_PATCHLEVEL=0
HIVE_NAME=hive-$HIVE_VERSION
HIVE_SOURCE=$TP_SOURCE_DIR/$HIVE_NAME
HIVE_ARCHIVE=$HIVE_NAME-stripped.tar.gz
HIVE_ANY_ARCH=1
HIVE_ANY_OS=1

# Note: The Hadoop release tarball is stripped of unnecessary jars before being
# uploaded. See thirdparty/package-hadoop.sh for details.
HADOOP_VERSION=3.4.1
HADOOP_PATCHLEVEL=0
HADOOP_NAME=hadoop-$HADOOP_VERSION
HADOOP_SOURCE=$TP_SOURCE_DIR/$HADOOP_NAME
HADOOP_ARCHIVE=$HADOOP_NAME-stripped.tar.gz
HADOOP_ANY_ARCH=1
HADOOP_ANY_OS=1

YAML_VERSION=0.8.0
YAML_PATCHLEVEL=1
YAML_NAME=yaml-cpp-yaml-cpp-$YAML_VERSION
YAML_SOURCE=$TP_SOURCE_DIR/$YAML_NAME
YAML_ARCHIVE=$YAML_NAME.tar.gz
YAML_PATCHES=(
 "patch -p1 < $TP_DIR/patches/yaml-fix-missing-cstdint-for-GCC15.patch"
)

CHRONY_VERSION=4.6.1
CHRONY_PATCHLEVEL=1
CHRONY_NAME=chrony-$CHRONY_VERSION
CHRONY_SOURCE=$TP_SOURCE_DIR/$CHRONY_NAME
CHRONY_ARCHIVE=$CHRONY_NAME.tar.gz
CHRONY_PATCHES=(
 "patch -p1 < $TP_DIR/patches/chrony-reuseport.patch"
)

# Hash of the gumbo-parser git revision to use.
# (from https://github.com/google/gumbo-parser)
#
# To re-build this tarball use the following in the sparsepp repo:
#  export NAME=gumbo-parser-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
GUMBO_PARSER_VERSION=aa91b27b02c0c80c482e24348a457ed7c3c088e0
GUMBO_PARSER_PATCHLEVEL=1
GUMBO_PARSER_NAME=gumbo-parser-$GUMBO_PARSER_VERSION
GUMBO_PARSER_SOURCE=$TP_SOURCE_DIR/$GUMBO_PARSER_NAME
GUMBO_PARSER_ARCHIVE=$GUMBO_PARSER_NAME.tar.gz
GUMBO_PARSER_PATCHES=(
 "patch -p1 < $TP_DIR/patches/gumbo-parser-autoconf-263.patch"
)
GUMBO_PARSER_EXTRA_COMMANDS=(
 "autoreconf -fvi"
)

# Hash of the gumbo-query git revision to use.
# (from https://github.com/lazytiger/gumbo-query)
#
# To re-build this tarball use the following in the sparsepp repo:
#  export NAME=gumbo-query-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
GUMBO_QUERY_VERSION=c9f10880b645afccf4fbcd11d2f62a7c01222d2e
GUMBO_QUERY_PATCHLEVEL=1
GUMBO_QUERY_NAME=gumbo-query-$GUMBO_QUERY_VERSION
GUMBO_QUERY_SOURCE=$TP_SOURCE_DIR/$GUMBO_QUERY_NAME
GUMBO_QUERY_ARCHIVE=$GUMBO_QUERY_NAME.tar.gz
GUMBO_QUERY_PATCHES=(
 "patch -p1 < $TP_DIR/patches/gumbo-query-namespace.patch"
)

POSTGRES_VERSION=17.2
POSTGRES_PATCHLEVEL=2
POSTGRES_NAME=postgresql-$POSTGRES_VERSION
POSTGRES_SOURCE=$TP_SOURCE_DIR/$POSTGRES_NAME
POSTGRES_ARCHIVE=$POSTGRES_NAME.tar.gz
POSTGRES_PATCHES=(
 "patch -p0 < $TP_DIR/patches/postgres-root-can-run-initdb.patch"
 "patch -p0 < $TP_DIR/patches/postgres-no-check-root.patch"
 "patch -p1 < $TP_DIR/patches/postgres-fix-strchrnul-macos-check.patch"
)

POSTGRES_JDBC_VERSION=42.7.4
POSTGRES_JDBC_PATCHLEVEL=0
POSTGRES_JDBC_NAME=postgresql-$POSTGRES_JDBC_VERSION
POSTGRES_JDBC_SOURCE=$TP_SOURCE_DIR/$POSTGRES_JDBC_NAME
POSTGRES_JDBC_ARCHIVE=$POSTGRES_JDBC_NAME.jar
POSTGRES_JDBC_ANY_ARCH=1
POSTGRES_JDBC_ANY_OS=1

# If you need to rebuild the tarball for a specific hash instead of a release,
# run the following commands:
# mvn versions:set -DnewVersion=$(git rev-parse HEAD)
# mvn versions:update-child-modules
# mvn package -DskipTests
RANGER_VERSION=2.6.0
RANGER_PATCHLEVEL=2
RANGER_NAME=ranger-$RANGER_VERSION-admin
RANGER_SOURCE=$TP_SOURCE_DIR/$RANGER_NAME
RANGER_ARCHIVE=$RANGER_NAME.tar.gz
RANGER_ANY_ARCH=1
RANGER_ANY_OS=1
RANGER_PATCHES=(
 "patch -p0 < $TP_DIR/patches/ranger-fixscripts.patch"
)

RANGER_KMS_VERSION=2.6.0 # this probably should match the ranger version
RANGER_KMS_PATCHLEVEL=0
RANGER_KMS_NAME=ranger-$RANGER_KMS_VERSION-kms
RANGER_KMS_SOURCE=$TP_SOURCE_DIR/$RANGER_KMS_NAME
RANGER_KMS_ARCHIVE=$RANGER_KMS_NAME.tar.gz
RANGER_KMS_ANY_ARCH=1
RANGER_KMS_ANY_OS=1

JWT_CPP_VERSION=3bd600762a70faccc7ec1c2dacb999cba6c6ef5e
JWT_CPP_PATCHLEVEL=0
JWT_CPP_NAME=jwt-cpp-$JWT_CPP_VERSION
JWT_CPP_SOURCE=$TP_SOURCE_DIR/$JWT_CPP_NAME
JWT_CPP_ARCHIVE=$JWT_CPP_NAME.tar.gz
JWT_CPP_ANY_ARCH=1
JWT_CPP_ANY_OS=1

ROCKSDB_VERSION=7.7.3
ROCKSDB_PATCHLEVEL=2
ROCKSDB_NAME=rocksdb-$ROCKSDB_VERSION
ROCKSDB_SOURCE=$TP_SOURCE_DIR/$ROCKSDB_NAME
ROCKSDB_ARCHIVE=$ROCKSDB_NAME.tar.gz
ROCKSDB_PATCHES=(
 "patch -p1 < $TP_DIR/patches/rocksdb-gcc13.patch"
 "patch -p1 < $TP_DIR/patches/rocksdb-gcc15-part1.patch"
 "patch -p1 < $TP_DIR/patches/rocksdb-gcc15-part2.patch"
 "patch -p1 < $TP_DIR/patches/rocksdb-gcc15-part3.patch"
)

# Prometheus is downloaded as a prebuilt binary from the S3 bucket.
# OS and arch are mapped to the Prometheus release naming convention.
#
# To update to a new version:
#   1. Download the tarballs for all supported OS/arch combinations from
#      https://prometheus.io/download/
#      (os: linux, darwin; arch: amd64, arm64 — four tarballs total)
#   2. Upload each tarball to the S3 bucket:
#      s3cmd put -P prometheus-<version>.<os>-<arch>.tar.gz \
#        s3://cloudera-thirdparty-libs/
#   3. Update PROMETHEUS_VERSION below.
PROMETHEUS_VERSION=3.11.2
PROMETHEUS_PATCHLEVEL=0
PROMETHEUS_OS=$(uname -s | tr '[:upper:]' '[:lower:]')
PROMETHEUS_ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
PROMETHEUS_NAME=prometheus-$PROMETHEUS_VERSION.$PROMETHEUS_OS-$PROMETHEUS_ARCH
PROMETHEUS_SOURCE=$TP_SOURCE_DIR/$PROMETHEUS_NAME
PROMETHEUS_ARCHIVE=$PROMETHEUS_NAME.tar.gz
PROMETHEUS_ANY_OS_VERSION=1
PROMETHEUS_ANY_TOOLCHAIN=1
