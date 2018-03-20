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

# This URL corresponds to the CloudFront Distribution for the S3
# bucket cloudera-thirdparty-libs which is directly accessible at
# http://cloudera-thirdparty-libs.s3.amazonaws.com/
CLOUDFRONT_URL_PREFIX=http://d3dr9sfxru4sde.cloudfront.net

PREFIX_COMMON=$TP_DIR/installed/common
PREFIX_DEPS=$TP_DIR/installed/uninstrumented
PREFIX_DEPS_TSAN=$TP_DIR/installed/tsan

GFLAGS_VERSION=2.2.0
GFLAGS_NAME=gflags-$GFLAGS_VERSION
GFLAGS_SOURCE=$TP_SOURCE_DIR/$GFLAGS_NAME

GLOG_VERSION=0.3.5
GLOG_NAME=glog-$GLOG_VERSION
GLOG_SOURCE=$TP_SOURCE_DIR/$GLOG_NAME

GMOCK_VERSION=1.8.0
GMOCK_NAME=googletest-release-$GMOCK_VERSION
GMOCK_SOURCE=$TP_SOURCE_DIR/$GMOCK_NAME

GPERFTOOLS_VERSION=2.6.90
GPERFTOOLS_NAME=gperftools-$GPERFTOOLS_VERSION
GPERFTOOLS_SOURCE=$TP_SOURCE_DIR/$GPERFTOOLS_NAME

PROTOBUF_VERSION=3.4.1
PROTOBUF_NAME=protobuf-$PROTOBUF_VERSION
PROTOBUF_SOURCE=$TP_SOURCE_DIR/$PROTOBUF_NAME

# Note: CMake gets patched on SLES12SP0. When changing the CMake version, please check if
# cmake-issue-15873-dont-use-select.patch needs to be updated.
CMAKE_VERSION=3.9.0
CMAKE_NAME=cmake-$CMAKE_VERSION
CMAKE_SOURCE=$TP_SOURCE_DIR/$CMAKE_NAME

SNAPPY_VERSION=1.1.4
SNAPPY_NAME=snappy-$SNAPPY_VERSION
SNAPPY_SOURCE=$TP_SOURCE_DIR/$SNAPPY_NAME

LZ4_VERSION=r130
LZ4_NAME=lz4-lz4-$LZ4_VERSION
LZ4_SOURCE=$TP_SOURCE_DIR/$LZ4_NAME

# from https://github.com/kiyo-masui/bitshuffle
# Hash of git: 55f9b4caec73fa21d13947cacea1295926781440
BITSHUFFLE_VERSION=55f9b4c
BITSHUFFLE_NAME=bitshuffle-$BITSHUFFLE_VERSION
BITSHUFFLE_SOURCE=$TP_SOURCE_DIR/$BITSHUFFLE_NAME

ZLIB_VERSION=1.2.8
ZLIB_NAME=zlib-$ZLIB_VERSION
ZLIB_SOURCE=$TP_SOURCE_DIR/$ZLIB_NAME

LIBEV_VERSION=4.20
LIBEV_NAME=libev-$LIBEV_VERSION
LIBEV_SOURCE=$TP_SOURCE_DIR/$LIBEV_NAME

RAPIDJSON_VERSION=0.11
RAPIDJSON_NAME=rapidjson-$RAPIDJSON_VERSION
RAPIDJSON_SOURCE=$TP_SOURCE_DIR/$RAPIDJSON_NAME

# Hash of the squeasel git revision to use.
# (from http://github.com/cloudera/squeasel)
#
# To re-build this tarball use the following in the squeasel repo:
#  export NAME=squeasel-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SQUEASEL_VERSION=9335b81317a6451d5a37c5dc7ec088eecbf68c82
SQUEASEL_NAME=squeasel-$SQUEASEL_VERSION
SQUEASEL_SOURCE=$TP_SOURCE_DIR/$SQUEASEL_NAME

# Hash of the mustache git revision to use.
# (from https://github.com/henryr/cpp-mustache)
#
# To re-build this tarball use the following in the mustache repo:
#  export NAME=mustache-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
MUSTACHE_VERSION=87a592e8aa04497764c533acd6e887618ca7b8a8
MUSTACHE_NAME=mustache-$MUSTACHE_VERSION
MUSTACHE_SOURCE=$TP_SOURCE_DIR/$MUSTACHE_NAME

# git revision of google style guide:
# https://github.com/google/styleguide
# git archive --prefix=google-styleguide-$(git rev-parse HEAD)/ -o /tmp/google-styleguide-$(git rev-parse HEAD).tgz HEAD
GSG_VERSION=7a179d1ac2e08a5cc1622bec900d1e0452776713
GSG_NAME=google-styleguide-$GSG_VERSION
GSG_SOURCE=$TP_SOURCE_DIR/$GSG_NAME

GCOVR_VERSION=3.0
GCOVR_NAME=gcovr-$GCOVR_VERSION
GCOVR_SOURCE=$TP_SOURCE_DIR/$GCOVR_NAME

CURL_VERSION=7.32.0
CURL_NAME=curl-$CURL_VERSION
CURL_SOURCE=$TP_SOURCE_DIR/$CURL_NAME

# Hash of the crcutil git revision to use.
# (from http://github.com/adembo/crcutil)
#
# To re-build this tarball use the following in the crcutil repo:
#  export NAME=crcutil-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
CRCUTIL_VERSION=42148a6df6986a257ab21c80f8eca2e54544ac4d
CRCUTIL_NAME=crcutil-$CRCUTIL_VERSION
CRCUTIL_SOURCE=$TP_SOURCE_DIR/$CRCUTIL_NAME

LIBUNWIND_VERSION=1.3-rc1
LIBUNWIND_NAME=libunwind-$LIBUNWIND_VERSION
LIBUNWIND_SOURCE=$TP_SOURCE_DIR/$LIBUNWIND_NAME

# See package-llvm.sh for details on the LLVM tarball.
LLVM_VERSION=4.0.0
LLVM_NAME=llvm-$LLVM_VERSION.src
LLVM_SOURCE=$TP_SOURCE_DIR/$LLVM_NAME

# The include-what-you-use is built along with LLVM in its source tree.
IWYU_VERSION=0.8

# Python 2.7 is required to build LLVM 3.6+. It is only built and installed if
# the system Python version is not 2.7.
PYTHON_VERSION=2.7.13
PYTHON_NAME=python-$PYTHON_VERSION
PYTHON_SOURCE=$TP_SOURCE_DIR/$PYTHON_NAME

# Our trace-viewer repository is separate since it's quite large and
# shouldn't change frequently. We upload the built artifacts (HTML/JS)
# when we need to roll to a new revision.
#
# The source can be found in the 'kudu' branch of https://github.com/cloudera/catapult
# and built with "tracing/kudu-build.sh" included within the repository.
TRACE_VIEWER_VERSION=21d76f8350fea2da2aa25cb6fd512703497d0c11
TRACE_VIEWER_NAME=kudu-trace-viewer-$TRACE_VIEWER_VERSION
TRACE_VIEWER_SOURCE=$TP_SOURCE_DIR/$TRACE_VIEWER_NAME

NVML_VERSION=1.1
NVML_NAME=nvml-$NVML_VERSION
NVML_SOURCE=$TP_SOURCE_DIR/$NVML_NAME

BOOST_VERSION=1_61_0
BOOST_NAME=boost_$BOOST_VERSION
BOOST_SOURCE=$TP_SOURCE_DIR/$BOOST_NAME

OPENSSL_WORKAROUND_DIR="$TP_DIR/installed/openssl-el6-workaround"

# The breakpad source artifact is created using the script found in
# scripts/make-breakpad-src-archive.sh
BREAKPAD_VERSION=f78d953511606348173911ae0b62572ebec1bbc4
BREAKPAD_NAME=breakpad-$BREAKPAD_VERSION
BREAKPAD_SOURCE=$TP_SOURCE_DIR/$BREAKPAD_NAME

# Hash of the sparsehash-c11 git revision to use.
# (from http://github.com/sparsehash/sparsehash-c11)
#
# To re-build this tarball use the following in the sparsehash-c11 repo:
#  export NAME=sparsehash-c11-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SPARSEHASH_VERSION=47a55825ca3b35eab1ca22b7ab82b9544e32a9af
SPARSEHASH_NAME=sparsehash-c11-$SPARSEHASH_VERSION
SPARSEHASH_SOURCE=$TP_SOURCE_DIR/$SPARSEHASH_NAME

# Hash of the sparsepp git revision to use.
# (from https://github.com/greg7mdp/sparsepp)
#
# To re-build this tarball use the following in the sparsepp repo:
#  export NAME=sparsepp-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SPARSEPP_VERSION=824860bb76893d163efbcff330734b9f62eecb17
SPARSEPP_NAME=sparsepp-$SPARSEPP_VERSION
SPARSEPP_SOURCE=$TP_SOURCE_DIR/$SPARSEPP_NAME

THRIFT_VERSION=0.11.0
THRIFT_NAME=thrift-$THRIFT_VERSION
THRIFT_SOURCE=$TP_SOURCE_DIR/$THRIFT_NAME

BISON_VERSION=3.0.4
BISON_NAME=bison-$BISON_VERSION
BISON_SOURCE=$TP_SOURCE_DIR/$BISON_NAME

# TODO(dan): bump to a release version once HIVE-17747 is published. The SHA
# below is the current head of branch-2.
# Note: The Hive release binary tarball is stripped of unnecessary jars before
# being uploaded. See thirdparty/package-hive.sh for details.
HIVE_VERSION=6189dbdea543939e977f97170f9a37389f46eb29
HIVE_NAME=apache-hive-$HIVE_VERSION-bin
HIVE_SOURCE=$TP_SOURCE_DIR/$HIVE_NAME

# Note: The Hadoop release tarball is stripped of unnecessary jars before being
# uploaded. See thirdparty/package-hadoop.sh for details.
HADOOP_VERSION=2.8.2
HADOOP_NAME=hadoop-$HADOOP_VERSION
HADOOP_SOURCE=$TP_SOURCE_DIR/$HADOOP_NAME
