#!/bin/bash

########################################################################
# Copyright (c) 2015 Cloudera, Inc.
# Confidential Cloudera Information: Covered by NDA.
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

THIS_DIR=$(dirname $BASH_SOURCE)
ROOT=$(cd $THIS_DIR ; cd ../../.. ; pwd)

GEN_DOC_DIR=$ROOT/build/gen-docs
OUTPUT_DIR=$ROOT/build/docs

usage() {
  echo usage: "$0 [--site <path to gh-pages checkout>]"
}

# We need asciidoctor and xsltproc binaries
for requirement in "asciidoctor" "xsltproc"; do
  which $requirement > /dev/null
  if [ $? -ne 0 ]; then
    echo "$requirement is required, but cannot be found. Make sure it is in your path."
    exit 1
  fi
done

while [[ $# > 0 ]] ; do
    arg=$1
    case $arg in
      --help)
        usage
        exit 1
        ;;
      --site|-s)
        SITE=$2
        if [ -z "$SITE" ]; then
          usage
          exit 1
        fi
        shift
        shift
        if [ ! -d "$SITE"/.git ] || [ ! -d "$SITE/docs/" ]; then
          echo "path $SITE doesn't appear to be the root of a git checkout "
          echo "of the Kudu site. Expected to find .git/ and docs/ directories"
          exit 1
        fi
        SITE=$(cd $SITE && pwd)
        OUTPUT_DIR=$SITE/docs
        ;;
      *)
        echo unknown argument: $arg
        exit 1
    esac
done

mkdir -p "$OUTPUT_DIR" "$GEN_DOC_DIR"

# Create config flag references for each of the binaries below
binaries=("cfile-dump" \
          "kudu-admin" \
          "kudu-fs_dump" \
          "kudu-fs_list" \
          "kudu-ksck" \
          "kudu-master" \
          "kudu-pbc-dump" \
          "kudu-tserver" \
          "kudu-ts-cli" \
          "log-dump")

for binary in ${binaries[@]}; do
  echo "Running $(basename $binary) --helpxml"

  # Create the XML file
  $ROOT/build/latest/$binary --helpxml  > ${GEN_DOC_DIR}/$(basename $binary).xml

  # Create the supported config reference
  xsltproc \
    --stringparam binary $binary \
    --stringparam support-level stable \
    -o $GEN_DOC_DIR/${binary}_configuration_reference.adoc \
      $ROOT/docs/support/xsl/gflags_to_asciidoc.xsl \
    ${GEN_DOC_DIR}/$binary.xml
  INCLUSIONS_SUPPORTED+="include::${binary}_configuration_reference.adoc[leveloffset=+1]\n"

  # Create the unsupported config reference
  xsltproc \
    --stringparam binary $binary \
    --stringparam support-level unsupported \
    -o $GEN_DOC_DIR/${binary}_configuration_reference_unsupported.adoc \
      $ROOT/docs/support/xsl/gflags_to_asciidoc.xsl \
    ${GEN_DOC_DIR}/$binary.xml
  INCLUSIONS_UNSUPPORTED+="include::${binary}_configuration_reference_unsupported.adoc[leveloffset=+1]\n"
done

# Add the includes to the configuration reference files, replacing the template lines
cp $ROOT/docs/configuration_reference* $GEN_DOC_DIR/
sed -i "s#@@CONFIGURATION_REFERENCE@@#${INCLUSIONS_SUPPORTED}#" ${GEN_DOC_DIR}/configuration_reference.adoc
sed -i "s#@@CONFIGURATION_REFERENCE@@#${INCLUSIONS_UNSUPPORTED}#" ${GEN_DOC_DIR}/configuration_reference_unsupported.adoc

# If we're generating the web site, pass the template which causes us
# to generate Jekyll templates instead of full HTML.
if [ -n "$SITE" ]; then
    TEMPLATE_FLAG="-T $ROOT/docs/support/jekyll-templates"
else
    TEMPLATE_FLAG=""
fi

asciidoctor -d book $TEMPLATE_FLAG $ROOT/docs/*.adoc ${GEN_DOC_DIR}/*.adoc -D "$OUTPUT_DIR"
RESULT=$?
if [ $RESULT -ne 0 ]; then
  exit $RESULT
fi

echo
echo ----------------------------
echo "Docs built in $OUTPUT_DIR."

# If we're building the site, try to run Jekyll for them to make
# it a bit easier to quickly preview the results.
if [ -n "$SITE" ]; then
  # We need to generate a config file which fakes the "github.url" property
  # so that relative links within the site work.
  BASE_URL="file://$SITE/_site/"
  TMP_CONFIG=$(mktemp --suffix=.yml)
  trap "rm $TMP_CONFIG" EXIT
  printf "github:\n  url: %s" "$BASE_URL" > $TMP_CONFIG

  # Now rebuild the site itself.
  echo Attempting to re-build via Jekyll...
  cd $SITE
  jekyll build --config $TMP_CONFIG
  jekyll_result=$?
  if [ $jekyll_result -eq 0 ]; then
    # Output the URL so it's easy to click on from the terminal.
    echo ----------------------
    echo Rebuild successful. View your site at
    echo $BASE_URL/index.html
    echo ----------------------
  fi
fi

