#!/bin/bash
########################################################################
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
#
# This script generates site documentation and Javadocs.
########################################################################
set -e

BUILD_TYPE=release
SOURCE_ROOT=$(cd $(dirname $0)/../../..; pwd)
BUILD_ROOT="$SOURCE_ROOT/build/$BUILD_TYPE"
SITE_OUTPUT_DIR="$BUILD_ROOT/site"

OPT_DOXYGEN=1 # By default, build doxygen docs.
OPT_JAVADOC=1 # By default, build javadocs.
OPT_FORCE='' # By default, don't overwrite the destination directory.

usage() {
  echo "Usage: $0 [--no-doxygen] [--no-javadoc] [--force]"
  echo "Specify --no-doxygen to skip generation of the C++ client API docs"
  echo "Specify --no-javadoc to skip generation of the Java API docs"
  echo "Specify --force to overwrite the destination directory, if it exists"
  exit 1
}

if [ $# -gt 0 ]; then
  for arg in $*; do
    case $arg in
      "--no-doxygen")  OPT_DOXYGEN='' ;;
      "--no-javadoc")  OPT_JAVADOC='' ;;
      "--force")       OPT_FORCE=1 ;;
      "--help")        usage ;;
      "-h")            usage ;;
      *)               echo "$0: Unknown command-line option: $arg"; usage ;;
    esac
  done
fi

if [ -n "$OPT_DOXYGEN" ] && ! which doxygen >/dev/null 2>&1; then
  echo "Error: doxygen not found."
  usage
fi

set -x

cd "$SOURCE_ROOT"

# Build Kudu thirdparty
$SOURCE_ROOT/build-support/enable_devtoolset.sh $SOURCE_ROOT/thirdparty/build-if-necessary.sh
echo "Successfully built third-party dependencies."

# Build the binaries so we can auto-generate the command-line references
mkdir -p "$BUILD_ROOT"
cd "$BUILD_ROOT"
rm -rf CMakeCache CMakeFiles/
$SOURCE_ROOT/build-support/enable_devtoolset.sh \
    $SOURCE_ROOT/thirdparty/installed/common/bin/cmake \
    -DNO_TESTS=1 \
    -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
    $SOURCE_ROOT
MAKE_TARGETS="kudu kudu-tserver kudu-master"
if [ -n "$OPT_DOXYGEN" ]; then
  MAKE_TARGETS="$MAKE_TARGETS doxygen"
fi
make -j$(getconf _NPROCESSORS_ONLN) $MAKE_TARGETS

# Check out the gh-pages repo into $SITE_OUTPUT_DIR
if [ -d "$SITE_OUTPUT_DIR" -a -n "$OPT_FORCE" ]; then
  rm -rf "$SITE_OUTPUT_DIR"
fi
git clone -q $(git config --get remote.apache.url) --reference $SOURCE_ROOT -b gh-pages --depth 1 "$SITE_OUTPUT_DIR"

# Build the docs using the styles from the Jekyll site
rm -Rf "$SITE_OUTPUT_DIR/docs"
$SOURCE_ROOT/docs/support/scripts/make_docs.sh --build_root $BUILD_ROOT --site "$SITE_OUTPUT_DIR"
if [ -f "$SITE_OUTPUT_DIR/docs/index.html" ]; then
  echo "Successfully built docs."
else
  echo "Docs failed to build."
  exit 1
fi

if [ -n "$OPT_JAVADOC" ]; then
  JAVADOC_SUBDIR="apidocs"
  cd "$SOURCE_ROOT/java"
  mvn clean install -DskipTests
  if mvn clean javadoc:aggregate | tee /dev/stdout | fgrep -q "Javadoc Warnings"; then
    echo "There are Javadoc warnings. Please fix them."
    exit 1
  fi

  if [ -f "$SOURCE_ROOT/java/target/site/$JAVADOC_SUBDIR/index.html" ]; then
    echo "Successfully built Javadocs."
  else
    echo "Javadocs failed to build."
    exit 1
  fi

  rm -Rf "$SITE_OUTPUT_DIR/$JAVADOC_SUBDIR"
  cp -au "$SOURCE_ROOT/java/target/site/$JAVADOC_SUBDIR" "$SITE_OUTPUT_DIR/"
fi

if [ -n "$OPT_DOXYGEN" ]; then
  CPP_CLIENT_API_SUBDIR="cpp-client-api"
  rm -Rf "$SITE_OUTPUT_DIR/$CPP_CLIENT_API_SUBDIR"
  cp -a "$BUILD_ROOT/docs/doxygen/client_api/html" "$SITE_OUTPUT_DIR/$CPP_CLIENT_API_SUBDIR"
fi

SITE_SUBDIRS="docs"
if [ -n "$OPT_JAVADOC" ]; then
  SITE_SUBDIRS="$SITE_SUBDIRS $JAVADOC_SUBDIR"
fi
if [ -n "$OPT_DOXYGEN" ]; then
  SITE_SUBDIRS="$SITE_SUBDIRS $CPP_CLIENT_API_SUBDIR"
fi

cd "$SITE_OUTPUT_DIR"
SITE_ARCHIVE="$SITE_OUTPUT_DIR/website_archive.zip"
zip -rq "$SITE_ARCHIVE" $SITE_SUBDIRS

echo "Generated web site at $SITE_OUTPUT_DIR"
echo "Docs zip generated at $SITE_ARCHIVE"
echo "To view live site locally, run: (cd $SITE_OUTPUT_DIR && ./site_tool jekyll serve)"
