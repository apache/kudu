#!/bin/bash

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

# Packages Hive for inclusion in the Kudu thirdparty build.
#
# Strips jars unnecessary for running the Hive Metastore. This makes the download
# smaller, and improves HMS startup time.
#
# Summary:
# 1. Download and unpack the Hive binary tarball (optional)
# 2. Strip out unnecessary jars
# 3. Repackage the tarball (optional)

set -ex

function usage() {
  cat <<EOF
  Usage:
    ${0} -h | --help
    ${0} [-d] [-r] <ARTIFACT>
    -h | --help - This message
    -d | --download - Optional
      Download and unpack the binary tarball.
    -r | --repackage - Optional
      Repackage the tarball.
    -v | --version VERSION - Optional
      The Hive version.
    <ARTIFACT> - Required
      The artifact name.
EOF
}

OPTS=`getopt -o hdrv: -l "help,download,repackage,version:" -n 'parse-options' -- "$@"`
OPTS_RESULT=$?
if [ ${OPTS_RESULT} != 0 ]; then
  echo "Failed parsing options." >&2
  exit ${OPTS_RESULT}
fi

while true; do
  case "$1" in
    -d | --download) DOWNLOAD=True; shift;;
    -r | --repackage) REPACKAGE=True; shift;;
    -v | --version) VERSION=$2; shift; shift;;
    -h | --help) usage; exit;;
    --) shift; break;;
    *) break ;;
  esac
done

ARTIFACT="${1}"

if [ -z "${ARTIFACT}" ]; then
  usage
  exit
fi

if [ -n "${DOWNLOAD}" ]; then
  curl --retry 3 -L -O https://archive.apache.org/dist/hive/hive-$VERSION/$ARTIFACT.tar.gz
  tar xf $ARTIFACT.tar.gz
fi

PROJECTS="accumulo"
PROJECTS="$PROJECTS aether"
PROJECTS="$PROJECTS avatica"
PROJECTS="$PROJECTS avro"
PROJECTS="$PROJECTS calcite"
PROJECTS="$PROJECTS curator"
PROJECTS="$PROJECTS druid"
PROJECTS="$PROJECTS ecj"
PROJECTS="$PROJECTS fastutil"
PROJECTS="$PROJECTS groovy"
PROJECTS="$PROJECTS hbase"
PROJECTS="$PROJECTS htrace"
PROJECTS="$PROJECTS icu4j"
PROJECTS="$PROJECTS jcodings"
PROJECTS="$PROJECTS jersey"
PROJECTS="$PROJECTS jetty"
PROJECTS="$PROJECTS jsp"
PROJECTS="$PROJECTS maven"
PROJECTS="$PROJECTS parquet"
PROJECTS="$PROJECTS slider"
PROJECTS="$PROJECTS snappy"
PROJECTS="$PROJECTS zookeeper"

for PROJECT in $PROJECTS; do
  rm -f $ARTIFACT/lib/*$PROJECT*.jar
done

rm -rf $ARTIFACT/auxlib
rm -rf $ARTIFACT/hcatalog/bin
rm -rf $ARTIFACT/hcatalog/etc
rm -rf $ARTIFACT/hcatalog/libexec
rm -rf $ARTIFACT/hcatalog/sbin
rm -rf $ARTIFACT/hcatalog/share/doc
rm -rf $ARTIFACT/hcatalog/share/webhcat
rm -rf $ARTIFACT/jdbc
rm -rf $ARTIFACT/lib/php
rm -rf $ARTIFACT/lib/python
rm -rf $ARTIFACT/lib/py

if [ -n "${REPACKAGE}" ]; then
  # Remove the 'apache-' prefix and '-bin' suffix in order to normalize the
  # tarball with Hadoop and the rest of our thirdparty dependencies.
  mv $ARTIFACT hive-$VERSION
  tar czf hive-$VERSION-stripped.tar.gz hive-$VERSION
fi
