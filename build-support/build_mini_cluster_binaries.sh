#!/bin/bash
################################################################################
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
################################################################################
# This script generates optimized, dynamically-linked, stripped, fully
# relocatable Kudu server binaries for easy use by integration tests using a
# mini cluster. The resulting binaries should never be deployed to run an
# actual Kudu service, whether in production or development, because all
# security dependencies are copied from the build system and will not be
# updated if the operating system on the runtime host is patched.
################################################################################
set -e

SOURCE_ROOT=$(cd $(dirname $0)/..; pwd)
BUILD_ROOT=$SOURCE_ROOT/build/minicluster
TARGETS="kudu kudu-tserver kudu-master"

cd $SOURCE_ROOT
if [ -n "$NO_REBUILD_THIRDPARTY" ]; then
  echo Skipping thirdparty because NO_REBUILD_THIRDPARTY is not empty
else
  echo Building thirdparty... >&2
  $SOURCE_ROOT/build-support/enable_devtoolset.sh \
    ./thirdparty/build-if-necessary.sh
fi

mkdir -p $BUILD_ROOT
cd $BUILD_ROOT

MACOS=""
# TODO(mpercy): What's a better way to detect macOS?
if [ $(uname) == "Darwin" ]; then
  MACOS=1
fi

EXTRA_CMAKE_FLAGS=""
if [ -n "$MACOS" ]; then
  # TODO(mpercy): Consider using pkg-config to support building with MacPorts.
  EXTRA_CMAKE_FLAGS="$EXTRA_CMAKE_FLAGS -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl"
  # TODO(mpercy): Is it even possible to build Kudu with gcc/g++ on macOS?
  export CC=clang
  export CXX=clang++
fi

rm -rf CMakeCache.txt CMakeFiles

# We want a fast build with a small total output size, so we need to build in
# release mode with dynamic linking so that all of the target executables can
# use the same shared objects for their dependencies.
echo Configuring Kudu... >&2
$SOURCE_ROOT/build-support/enable_devtoolset.sh \
  $SOURCE_ROOT/thirdparty/installed/common/bin/cmake ../.. \
  -DNO_TESTS=1 -DCMAKE_BUILD_TYPE=RELEASE -DKUDU_LINK=dynamic $EXTRA_CMAKE_FLAGS

echo Building Kudu... >&2
NUM_PROCS=$(getconf _NPROCESSORS_ONLN)
make -j$NUM_PROCS $TARGETS

# Relocate the binaries.
$SOURCE_ROOT/build-support/relocate_binaries_for_mini_cluster.py $BUILD_ROOT $TARGETS

ARTIFACT_NAME=$(ls -d kudu-binary* | sed 's#/##' | head -1)

# Strip everything to minimize the size of the tarball we generate.
echo Stripping symbols...
for file in $ARTIFACT_NAME/bin/*; do
  strip $file
done

# Stripping libraries on macOS is tricky, so skip it for now.
# TODO(mpercy): Deal with detecting signed libraries and indirect symbol table
# entries on macOS.
if [ -z "$MACOS" ]; then
  for file in $ARTIFACT_NAME/lib/*; do
    strip $file
  done
fi

# Include the basic legal files.
for file in LICENSE.txt NOTICE.txt; do
  cp -p $SOURCE_ROOT/$file $ARTIFACT_NAME/
done

# Include the web UI template files.
cp -Rp $SOURCE_ROOT/www $ARTIFACT_NAME/

# Include a basic warning.
cat <<EOF > $ARTIFACT_NAME/README.txt
This archive contains Kudu binaries for use in a "mini cluster" environment for
TESTING ONLY.

The binaries in this archive should never be deployed to run an actual Kudu
service, whether in production or development, because all security
dependencies are copied from the build system and will not be updated if the
operating system on the runtime host is patched.
EOF

echo Creating archive...
ARTIFACT_FILE=$ARTIFACT_NAME.jar
jar cf $ARTIFACT_FILE $ARTIFACT_NAME/
echo "Binary test artifact: $(pwd)/$ARTIFACT_FILE"
