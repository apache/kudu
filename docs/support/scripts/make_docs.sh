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
# This script runs after Kudu binaries are built. It does:
# 1. For each binary, run $binary --helpxml and save the output to an XML file
# 2. For each generated XML file, run it through xsltproc two times:
#    a. Once to capture "supported" (stable) flags
#    b. Once to capture "unsupported" (evolving or experimental) flags
# 3. For each generated Asciidoc file, include it in either configuration_reference.adoc
#    or configuration_reference_unsupported.adoc
#
# Usage: make_docs.sh <output_dir>
########################################################################
set -e

usage() {
  echo usage: "$0 --build_root <path> --output_subdir <relative path> [--site <path to gh-pages checkout>]"
}

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  SED="sed -i -e"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  echo "WARNING: The docs MUST NOT be built on Mac for publishing purposes.
  Press 'y' to continue"
  read -n 1
  if [[ "$REPLY" != "y" ]]; then
    exit 1;
  fi
  SED="sed -i~ -e"
else
  echo "Unsupported OS"
  exit 1
fi

while [[ $# > 0 ]] ; do
    arg=$1
    case $arg in
      --help)
        usage
        exit 1
        ;;
      --build_root)
        BUILD_ROOT=$2
        shift
        shift
        ;;
      --site|-s)
        SITE=$2
        if [ -z "$SITE" ]; then
          usage
          exit 1
        fi
        shift
        shift
        if [ ! -d "$SITE"/.git ] || [ ! -d "$SITE/_layouts/" ]; then
          echo "path $SITE doesn't appear to be the root of a git checkout "
          echo "of the Kudu site. Expected to find .git/ and _layouts/ directories"
          exit 1
        fi
        SITE=$(cd $SITE && pwd)
        ;;
      --output_subdir|-o)
        OUTPUT_SUBDIR=$2
        if [ -z "$OUTPUT_SUBDIR" ]; then
          usage
          exit 1
        fi
        shift
        shift
        ;;
      --no-jekyll)
        NO_JEKYLL=1
        shift
        ;;
      *)
        echo unknown argument: $arg
        exit 1
    esac
done

if [ -z "$BUILD_ROOT" ]; then
  usage
  exit 1
fi

if [ -z "$SITE" ]; then
  OUTPUT_DIR=$BUILD_ROOT/$OUTPUT_SUBDIR
else
  OUTPUT_DIR=$SITE/$OUTPUT_SUBDIR
fi

GEN_DOC_DIR=$BUILD_ROOT/gen-docs
SOURCE_ROOT=$(cd $(dirname $0)/../../..; pwd)

if ! which ruby > /dev/null; then
  echo "ruby must be installed in order to build the docs."
  echo 1
fi

DOCS_SCRIPTS="$SOURCE_ROOT/docs/support/scripts"

# We must set GEM_PATH because bundler depends on it to find its own libraries.
export GEM_PATH="$BUILD_ROOT/gems"
echo GEM_PATH=$GEM_PATH

export PATH="$GEM_PATH/bin:$PATH"
echo PATH="$GEM_PATH/bin:$PATH"

BUNDLE="$GEM_PATH/bin/bundle"

echo "Locally installing ruby gems needed to build docs."
if [ ! -x "$BUNDLE" ]; then
  set -x
  gem install --no-document  -q --install-dir "$GEM_PATH" bundler
  set +x
fi

set -x
cd "$BUILD_ROOT"
cp $DOCS_SCRIPTS/Gemfile .
cp $DOCS_SCRIPTS/Gemfile.lock .
$BUNDLE install --no-color --path "$GEM_PATH"
set +x

# We need the xsltproc package.
for requirement in "xsltproc"; do
  if ! which $requirement > /dev/null; then
    echo "$requirement is required, but cannot be found. Make sure it is in your path."
    exit 1
  fi
done

mkdir -p "$OUTPUT_DIR" "$GEN_DOC_DIR"

# Create config flag references for each of the binaries below
binaries=("kudu-master" \
          "kudu-tserver")

for binary in ${binaries[@]}; do
  (
    # Reset environment to avoid affecting the default flag values.
    for var in $(env | awk -F= '{print $1}' | egrep -i 'KUDU|GLOG'); do
      echo "unset $var"
      eval "unset $var"
    done

    echo "Running $binary --helpxml"
    # Create the help XML file.
    # This command exits with a nonzero value.
    $BUILD_ROOT/bin/$binary --helpxml > ${GEN_DOC_DIR}/$(basename $binary).xml || true

    echo "Running $binary --dump_metrics_xml"
    # Create the metrics XML file.
    $BUILD_ROOT/bin/$binary --dump_metrics_xml > ${GEN_DOC_DIR}/$(basename $binary)-metrics.xml
  )

  # Create the supported config reference
  xsltproc \
    --stringparam binary $binary \
    --stringparam support-level supported \
    -o $GEN_DOC_DIR/${binary}_configuration_reference.adoc \
      $SOURCE_ROOT/docs/support/xsl/gflags_to_asciidoc.xsl \
    ${GEN_DOC_DIR}/${binary}.xml
  INCLUSIONS_SUPPORTED+="include::${binary}_configuration_reference.adoc[leveloffset=+1]\\"$'\n'

  # Create the unsupported config reference
  xsltproc \
    --stringparam binary $binary \
    --stringparam support-level unsupported \
    -o $GEN_DOC_DIR/${binary}_configuration_reference_unsupported.adoc \
      $SOURCE_ROOT/docs/support/xsl/gflags_to_asciidoc.xsl \
    ${GEN_DOC_DIR}/${binary}.xml
  INCLUSIONS_UNSUPPORTED+="include::${binary}_configuration_reference_unsupported.adoc[leveloffset=+1]\\"$'\n'

  # Create the metrics reference
  xsltproc \
    --stringparam binary $binary \
    -o $GEN_DOC_DIR/${binary}_metrics_reference.adoc \
      $SOURCE_ROOT/docs/support/xsl/metrics_to_asciidoc.xsl \
    ${GEN_DOC_DIR}/${binary}-metrics.xml
  INCLUSIONS_METRICS+="include::${binary}_metrics_reference.adoc[leveloffset=+1]\\"$'\n'
done

# Add the includes to the configuration reference files, replacing the template lines
cp $SOURCE_ROOT/docs/configuration_reference* $GEN_DOC_DIR/
$SED "s#@@CONFIGURATION_REFERENCE@@#${INCLUSIONS_SUPPORTED}#" ${GEN_DOC_DIR}/configuration_reference.adoc
$SED "s#@@CONFIGURATION_REFERENCE@@#${INCLUSIONS_UNSUPPORTED}#" ${GEN_DOC_DIR}/configuration_reference_unsupported.adoc

# Add the includes to the metrics reference files, replacing the template lines
cp $SOURCE_ROOT/docs/metrics_reference.adoc $GEN_DOC_DIR/
$SED "s#@@METRICS_REFERENCE@@#${INCLUSIONS_METRICS}#" ${GEN_DOC_DIR}/metrics_reference.adoc

# Create tool references
echo "Running kudu --helpxml"
(
  # Reset environment to avoid affecting the default flag values.
  for var in $(env | awk -F= '{print $1}' | egrep -i 'KUDU|GLOG'); do
    echo "unset $var"
    eval "unset $var"
  done

  # Create the XML file.
  # This command exits with a nonzero value.
  $BUILD_ROOT/bin/kudu --helpxml > ${GEN_DOC_DIR}/kudu.xml || true
)

# Create the command line tool reference
xsltproc \
-o $GEN_DOC_DIR/command_line_tools.adoc \
  $SOURCE_ROOT/docs/support/xsl/tool_to_asciidoc.xsl \
${GEN_DOC_DIR}/kudu.xml

# Add the includes to the cli tools reference files, replacing the template lines
cp $SOURCE_ROOT/docs/command_line_tools_reference.adoc $GEN_DOC_DIR/
$SED "s#@@TOOLS_REFERENCE@@#include::command_line_tools.adoc[leveloffset=+1]\\"$'\n#' ${GEN_DOC_DIR}/command_line_tools_reference.adoc

# If we're generating the web site, pass the template which causes us
# to generate Jekyll templates instead of full HTML.
if [ -n "$SITE" ]; then
    TEMPLATE_FLAG="-T $SOURCE_ROOT/docs/support/jekyll-templates"
else
    TEMPLATE_FLAG=""
fi

bundle exec asciidoctor -d book $TEMPLATE_FLAG \
    $SOURCE_ROOT/docs/*.adoc ${GEN_DOC_DIR}/*.adoc -D "$OUTPUT_DIR"

mkdir -p "$OUTPUT_DIR/images"
cp $SOURCE_ROOT/docs/images/* "$OUTPUT_DIR/images/"


echo
echo ----------------------------
echo "Docs built in $OUTPUT_DIR"

# If we're building the site, try to run Jekyll for them to make
# it a bit easier to quickly preview the results.
if [ -n "$SITE" ] && [ -z "$NO_JEKYLL" ]; then
  # We need to generate a config file which fakes the "github.url" property
  # so that relative links within the site work.
  BASE_URL="file://$SITE/_site/"
  TMP_CONFIG=$(mktemp -t config.XXXXXX)
  # Rename the config file. The template adds the random section
  # to the end of the file to be compatible with Mac OS. However,
  # Jekyll expects the file to end in yml.
  mv "$TMP_CONFIG" "$TMP_CONFIG.yml"
  TMP_CONFIG="$TMP_CONFIG.yml"
  trap "rm $TMP_CONFIG" EXIT
  printf "github:\n  url: %s" "$BASE_URL" > $TMP_CONFIG

  # Now rebuild the site itself.
  echo Attempting to re-build via Jekyll...
  bundle exec jekyll build --source "$SITE" --config "$TMP_CONFIG"

  # Output the URL so it's easy to click on from the terminal.
  echo ----------------------
  echo Rebuild successful. View your site at
  echo $BASE_URL/index.html
  echo ----------------------
fi
