#!/bin/bash
########################################################################
# Copyright 2015 Cloudera, Inc.
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
#
# This script generates site documentation and Javadocs.
#
# Usage: make_site.sh <output_dir>
########################################################################
set -e

REL_OUTPUT_DIR=$1
if [ -z "$REL_OUTPUT_DIR" ]; then
  echo "usage: $0 <output_dir>"
  exit 1
fi

# Ensure a clean build.
mkdir -p "$REL_OUTPUT_DIR"
ABS_OUTPUT_DIR=$(cd "$REL_OUTPUT_DIR"; pwd)
SITE_OUTPUT_DIR="$ABS_OUTPUT_DIR/kudu-site"
if [ -d "$SITE_OUTPUT_DIR" ]; then
  echo "Error: Please remove the old site directory before running this script: $SITE_OUTPUT_DIR"
  exit 1
fi
mkdir "$SITE_OUTPUT_DIR"

ROOT=$(cd $(dirname $0)/../../..; pwd)
set -x

cd "$ROOT"

# Build Kudu thirdparty
./thirdparty/build-if-necessary.sh
echo "Successfully built third-party dependencies."

# Build the binaries so we can auto-generate the command-line references
rm -rf CMakeCache.txt CMakeFiles
./thirdparty/installed/bin/cmake -DNO_TESTS=1 .
make -j$(getconf _NPROCESSORS_ONLN)
rm -rf CMakeCache.txt CMakeFiles

# Check out the gh-pages repo into $SITE_OUTPUT_DIR
git clone -q $(git config --get remote.origin.url) --reference $(pwd) -b gh-pages --depth 1 "$SITE_OUTPUT_DIR"

# Build the docs using the styles from the Jekyll site
./docs/support/scripts/make_docs.sh --site "$SITE_OUTPUT_DIR"
if [ -f "$SITE_OUTPUT_DIR/docs/index.html" ]; then
  echo "Successfully built docs."
else
  echo "Docs failed to build."
  exit 1
fi

cd "$ROOT/java"
mvn clean install -DskipTests
mvn clean javadoc:aggregate

if [ -f "$ROOT/java/target/site/apidocs/index.html" ]; then
  echo "Successfully built Javadocs."
else
  echo "Javadocs failed to build."
  exit 1
fi

cp -au "$ROOT/java/target/site/apidocs" "$SITE_OUTPUT_DIR/"

cd "$SITE_OUTPUT_DIR"
SITE_ARCHIVE="$SITE_OUTPUT_DIR/website_archive.zip"
zip -rq "$SITE_ARCHIVE" docs apidocs

echo "Generated web site at $SITE_OUTPUT_DIR"
echo "Docs zip generated at $SITE_ARCHIVE"
