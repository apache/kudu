# Copyright (c) 2013, Cloudera, inc.

PREFIX=$TP_DIR/installed

# This URL corresponds to the CloudFront Distribution for the S3
# bucket cloudera-thirdparty-libs which is directly accessible at
# http://cloudera-thirdparty-libs.s3.amazonaws.com/
CLOUDFRONT_URL_PREFIX=http://d3dr9sfxru4sde.cloudfront.net

GFLAGS_VERSION=1.5
GFLAGS_DIR=$TP_DIR/gflags-$GFLAGS_VERSION

GLOG_VERSION=0.3.3
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

LIBEV_VERSION=4.15
LIBEV_DIR=$TP_DIR/libev-$LIBEV_VERSION

RAPIDJSON_VERSION=0.11
RAPIDJSON_DIR=$TP_DIR/rapidjson-${RAPIDJSON_VERSION}

# Hash of the squeasel git revision to use.
# (from http://github.mtv.cloudera.com/CDH/squeasel)
#
# To re-build this tarball use the following in the squeasel repo:
#  export NAME=squeasel-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
#
# File a HD ticket for access to the cloudera-dev AWS instance to push to S3.
SQUEASEL_VERSION=5b330b5791ea69fc0e65c7e7751f27437dae1e7d
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
LLVM_VERSION=3.4.2
LLVM_DIR=$TP_DIR/llvm-${LLVM_VERSION}.src
LLVM_BUILD=$TP_DIR/llvm-${LLVM_VERSION}.build

# We have a separate clang package which we use for sanitizer builds. We're
# stuck on llvm 3.4.2 to link against (because later versions require C++11)
# but it's fine to use a much more recent version as a compiler.
#
# The binary packages we use here are built by the impala-deps repository:
# http://github.mtv.cloudera.com/mgrund/impala-deps
CLANG_TOOLCHAIN_VERSION=233105
CLANG_TOOLCHAIN_DIR=clang-$CLANG_TOOLCHAIN_VERSION

# Our trace-viewer repository is separate since it's quite large and
# shouldn't change frequently. We upload the built artifacts (HTML/JS)
# when we need to roll to a new revision.
#
# The source can be found at https://github.com/cloudera/kudu-trace-viewer
# and built with "kudu-build.sh" included within the repository.
TRACE_VIEWER_VERSION=f0f132051837fba4e57c8a7603331aa9c90151f4
TRACE_VIEWER_DIR=$TP_DIR/kudu-trace-viewer-${TRACE_VIEWER_VERSION}

NVML_VERSION=0.2
NVML_DIR=$TP_DIR/nvml-$NVML_VERSION
