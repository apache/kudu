# Copyright 2013 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PREFIX=$TP_DIR/installed

# This URL corresponds to the CloudFront Distribution for the S3
# bucket cloudera-thirdparty-libs which is directly accessible at
# http://cloudera-thirdparty-libs.s3.amazonaws.com/
CLOUDFRONT_URL_PREFIX=http://d3dr9sfxru4sde.cloudfront.net

GFLAGS_VERSION=1.5
GFLAGS_DIR=$TP_DIR/gflags-$GFLAGS_VERSION

GLOG_VERSION=0.3.4
GLOG_DIR=$TP_DIR/glog-$GLOG_VERSION

GMOCK_VERSION=1.7.0
GMOCK_DIR=$TP_DIR/gmock-$GMOCK_VERSION

GPERFTOOLS_VERSION=2.2.1
GPERFTOOLS_DIR=$TP_DIR/gperftools-$GPERFTOOLS_VERSION

PROTOBUF_VERSION=2.6.1
PROTOBUF_DIR=$TP_DIR/protobuf-$PROTOBUF_VERSION

CMAKE_VERSION=3.2.3
CMAKE_DIR=$TP_DIR/cmake-${CMAKE_VERSION}

SNAPPY_VERSION=1.1.0
SNAPPY_DIR=$TP_DIR/snappy-$SNAPPY_VERSION

LZ4_VERSION=r130
LZ4_DIR=$TP_DIR/lz4-lz4-$LZ4_VERSION

# from https://github.com/kiyo-masui/bitshuffle
# Hash of git: c5c928fe7d4bc5b9391748a8dd29de5a89c3c94a
BITSHUFFLE_VERSION=c5c928f
BITSHUFFLE_DIR=$TP_DIR/bitshuffle-${BITSHUFFLE_VERSION}

ZLIB_VERSION=1.2.8
ZLIB_DIR=$TP_DIR/zlib-$ZLIB_VERSION

LIBEV_VERSION=4.20
LIBEV_DIR=$TP_DIR/libev-$LIBEV_VERSION

RAPIDJSON_VERSION=0.11
RAPIDJSON_DIR=$TP_DIR/rapidjson-${RAPIDJSON_VERSION}

# Hash of the squeasel git revision to use.
# (from http://github.com/cloudera/squeasel)
#
# To re-build this tarball use the following in the squeasel repo:
#  export NAME=squeasel-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
#
# File a HD ticket for access to the cloudera-dev AWS instance to push to S3.
SQUEASEL_VERSION=fe28fe62b80c2e8a5e6a7d23e58369309958dc26
SQUEASEL_DIR=$TP_DIR/squeasel-${SQUEASEL_VERSION}

# SVN revision of google style guide:
# https://code.google.com/p/google-styleguide/source/list
GSG_REVISION=134
GSG_DIR=$TP_DIR/google-styleguide-r${GSG_REVISION}

GCOVR_VERSION=3.0
GCOVR_DIR=$TP_DIR/gcovr-${GCOVR_VERSION}

CURL_VERSION=7.32.0
CURL_DIR=$TP_DIR/curl-${CURL_VERSION}

# Hash of the crcutil git revision to use.
# (from http://github.mtv.cloudera.com/CDH/crcutil)
#
# To re-build this tarball use the following in the crcutil repo:
#  export NAME=crcutil-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
CRCUTIL_VERSION=440ba7babeff77ffad992df3a10c767f184e946e
CRCUTIL_DIR=$TP_DIR/crcutil-${CRCUTIL_VERSION}

LIBUNWIND_VERSION=1.1a
LIBUNWIND_DIR=$TP_DIR/libunwind-${LIBUNWIND_VERSION}

# Our llvm tarball includes clang, extra clang tools, and compiler-rt.
#
# See http://clang.llvm.org/get_started.html for details on how they're laid
# out in the llvm tarball.
LLVM_VERSION=3.7.0
LLVM_DIR=$TP_DIR/llvm-${LLVM_VERSION}.src
LLVM_BUILD=$TP_DIR/llvm-${LLVM_VERSION}.build

# Python 2.7 is required to build LLVM 3.6+. We install it if the system Python
# version is wrong.
PYTHON_VERSION=2.7.10
PYTHON_DIR=$TP_DIR/python-${PYTHON_VERSION}

GCC_VERSION=4.9.3
GCC_DIR=${TP_DIR}/gcc-${GCC_VERSION}
GCC_BUILD=${GCC_DIR}.build

# Our trace-viewer repository is separate since it's quite large and
# shouldn't change frequently. We upload the built artifacts (HTML/JS)
# when we need to roll to a new revision.
#
# The source can be found at https://github.com/cloudera/kudu-trace-viewer
# and built with "kudu-build.sh" included within the repository.
TRACE_VIEWER_VERSION=45f6525d8aa498be53e4137fb73a9e9e036ce91d
TRACE_VIEWER_DIR=$TP_DIR/kudu-trace-viewer-${TRACE_VIEWER_VERSION}

NVML_VERSION=0.3
NVML_DIR=$TP_DIR/nvml-$NVML_VERSION
