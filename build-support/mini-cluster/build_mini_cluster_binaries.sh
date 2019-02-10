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
#
# Mini cluster test binaries
# -------------------------
#
# This script generates optimized, dynamically-linked, stripped, fully
# relocatable Kudu server binaries for easy use by integration tests using a
# mini cluster. The resulting binaries should never be deployed to run an
# actual Kudu service, whether in production or development, because all
# security dependencies are copied from the build system and will not be
# updated if the operating system on the runtime host is patched.
#
# Properties file
# ---------------
#
# The JAR that is generated contains a properties file in META-INF that allows
# it to be located by Java code at runtime if the JAR is on the classpath.
# The location of that file in the archive is:
#
# META-INF/apache-kudu-test-binary.properties
#
# The following properties are supported:
#
# * format.version: The format of the artifact, in case we need to extend it
#   later and be able to tell the difference between the archive formats.
# * artifact.version: The version of the release (or snapshot, i.e.
#   1.9.0-SNAPSHOT) in order for the format to support support multiple
#   versions on the classpath or to allow a client to be able to request a
#   specific release version.
# * artifact.prefix: The directory name at the root of the JAR under which the
#   binary files can be found (similar to ./configure --prefix in autoconf).
# * artifact.os: The operating system name using the same convention as
#   https://github.com/trustin/os-maven-plugin for ${os.detected.name}.
#   Practically speaking for Kudu at the time of writing, that is either
#   'linux' or 'osx'.
# * artifact.arch: The target system architecture using the same convention as
#   os-maven-plugin for ${os.detected.arch}. Practically speaking for Kudu, at
#   the time of writing this will always be set to x86_64.
#
# Example:
#
# $ cat META-INF/apache-kudu-test-binary.properties
# format.version=1
# artifact.version=1.8.0
# artifact.prefix=kudu-binary-1.8.0-linux-x86_64
# artifact.os=linux
# artifact.arch=x86_64
#
################################################################################
set -e

SOURCE_ROOT=$(cd $(dirname $0)/../..; pwd)
BUILD_ROOT=$SOURCE_ROOT/build/mini-cluster
MINI_CLUSTER_SRCDIR=$SOURCE_ROOT/build-support/mini-cluster
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
$MINI_CLUSTER_SRCDIR/relocate_binaries_for_mini_cluster.py $BUILD_ROOT $TARGETS

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
  find $ARTIFACT_NAME/lib -type f -exec strip {} \;
fi

# Generate the properties file that allows us to find this archive on the
# classpath at runtime. See above for the specification.
PROP_DIR="META-INF"
PROP_FILE="$PROP_DIR/apache-kudu-test-binary.properties"
FORMAT_VERSION=1
ARTIFACT_VERSION=$(cat $SOURCE_ROOT/version.txt)
ARTIFACT_OS="linux"
if [ $MACOS ]; then
  ARTIFACT_OS="osx"
fi
ARTIFACT_ARCH=$(uname -m)
rm -rf $PROP_DIR
mkdir -p $PROP_DIR
cat <<EOF > $PROP_FILE
format.version=$FORMAT_VERSION
artifact.os=$ARTIFACT_OS
artifact.arch=$ARTIFACT_ARCH
artifact.prefix=$ARTIFACT_NAME
artifact.version=$ARTIFACT_VERSION
EOF

# Include the basic legal files.
# Create a platform-specific NOTICE file.
JAR_NOTICE=$MINI_CLUSTER_SRCDIR/NOTICE-BINARY-JAR-LINUX.txt
if [ $MACOS ]; then
  JAR_NOTICE=$MINI_CLUSTER_SRCDIR/NOTICE-BINARY-JAR-OSX.txt
fi
cat $SOURCE_ROOT/NOTICE.txt \
    $JAR_NOTICE \
    > $ARTIFACT_NAME/NOTICE.txt

# Create a platform-specific LICENSE file.
JAR_LICENSE=$MINI_CLUSTER_SRCDIR/LICENSE-BINARY-JAR-LINUX.txt
if [ $MACOS ]; then
  JAR_LICENSE=$MINI_CLUSTER_SRCDIR/LICENSE-BINARY-JAR-OSX.txt
fi
cat $SOURCE_ROOT/LICENSE.txt \
    $SOURCE_ROOT/thirdparty/LICENSE.txt \
    $JAR_LICENSE \
    > $ARTIFACT_NAME/LICENSE.txt

# Include the web UI template files.
cp -Rp $SOURCE_ROOT/www $ARTIFACT_NAME/

# Include a basic warning.
cat <<EOF > $ARTIFACT_NAME/README.txt
This archive contains Kudu binaries for use in a "mini cluster" environment for
TESTING ONLY.

The binaries in this archive should never be deployed to run an actual Kudu
service, whether in production or development, because many security-related
dependencies are copied from the build system and will not be patched when the
operating system on the runtime host is patched.
EOF

echo "Running license check on artifact..."
$SOURCE_ROOT/build-support/mini-cluster/check-license.pl $ARTIFACT_NAME

echo Creating archive...
ARTIFACT_FILE=$ARTIFACT_NAME.jar
jar cf $ARTIFACT_FILE $PROP_DIR/ $ARTIFACT_NAME/
echo "Binary test artifact: $(pwd)/$ARTIFACT_FILE"
