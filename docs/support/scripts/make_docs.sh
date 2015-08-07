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

OUTPUT_DIR="$1"
BUILD_DIR="$OUTPUT_DIR/.."
# We need asciidoctor and xsltproc binaries
for requirement in "asciidoctor" "xsltproc"; do
  which $requirement > /dev/null
  if [ $? -ne 0 ]; then
    echo "$requirement is required, but cannot be found. Make sure it is in your path."
  fi
done

mkdir -p "$BUILD_DIR"

cp -r docs/*.adoc docs/images "$BUILD_DIR/"

# Create config flag references for each of the binaries below
binaries=("cfile-dump" \
          "kudu-admin" \
          "kudu-fs_dump" \
          "kudu-fs_list" \
          "kudu-ksck" \
          "kudu-master" \
          "kudu-pbc-dump" \
          "kudu-tablet_server" \
          "kudu-ts-cli" \
          "log-dump")

for binary in ${binaries[@]}; do
  echo "Running $(basename $binary) --helpxml"

  # Create the XML file
  build/latest/$binary --helpxml  > ${BUILD_DIR}/$(basename $binary).xml

  # Create the supported config reference
  xsltproc \
    --stringparam binary $binary \
    --stringparam support-level stable \
    -o build/docs/${binary}_configuration_reference.adoc \
    docs/support/xsl/gflags_to_asciidoc.xsl \
    ${BUILD_DIR}/$binary.xml
  INCLUSIONS_SUPPORTED+="include::${binary}_configuration_reference.adoc[leveloffset=+1]\n"

  # Create the unsupported config reference
  xsltproc \
    --stringparam binary $binary \
    --stringparam support-level unsupported \
    -o build/docs/${binary}_configuration_reference_unsupported.adoc \
    docs/support/xsl/gflags_to_asciidoc.xsl \
    ${BUILD_DIR}/$binary.xml
  INCLUSIONS_UNSUPPORTED+="include::${binary}_configuration_reference_unsupported.adoc[leveloffset=+1]\n"
done

# Add the includes to the configuration reference files, replacing the template lines
sed -i "s#@@CONFIGURATION_REFERENCE@@#${INCLUSIONS_SUPPORTED}#" ${BUILD_DIR}/configuration_reference.adoc
sed -i "s#@@CONFIGURATION_REFERENCE@@#${INCLUSIONS_UNSUPPORTED}#" ${BUILD_DIR}/configuration_reference_unsupported.adoc
asciidoctor -d book ${BUILD_DIR}/*.adoc -D "$OUTPUT_DIR"
RESULT=$?
if [ $RESULT -eq 0 ]; then
  echo "Docs built in $OUTPUT_DIR."
else
  exit $RESULT
fi

