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

# Packages Hadoop for inclusion in the Kudu thirdparty build.
#
# Strips jars and unecessary documentation. This makes the download smaller, and
# improves HMS startup time.
#
# Summary:
# 1. Download and unpack the Hadoop binary tarball (optional)
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
    <ARTIFACT> - Required
      The artifact name.
EOF
}

OPTS=`getopt -o hdr -l "help,download,repackage" -n 'parse-options' -- "$@"`
OPTS_RESULT=$?
if [ ${OPTS_RESULT} != 0 ]; then
  echo "Failed parsing options." >&2
  exit ${OPTS_RESULT}
fi

while true; do
  case "$1" in
    -d | --download) DOWNLOAD=True; shift;;
    -r | --repackage) REPACKAGE=True; shift;;
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
  curl --retry 3 -L -O https://archive.apache.org/dist/hadoop/common/$ARTIFACT/$ARTIFACT.tar.gz
  tar xf $ARTIFACT.tar.gz
fi

DIRS="client"
DIRS="$DIRS common/jdiff"
DIRS="$DIRS common/sources"
DIRS="$DIRS common/webapps"
DIRS="$DIRS hdfs/sources"
DIRS="$DIRS httpfs"
DIRS="$DIRS kms"
DIRS="$DIRS mapreduce/lib-examples"
DIRS="$DIRS mapreduce/sources"
DIRS="$DIRS tools"

for DIR in $DIRS; do
  rm -rf $ARTIFACT/share/hadoop/$DIR
done

for PROJECT in avro commons-math3 snappy; do
  rm -f $ARTIFACT/share/hadoop/common/lib/*$PROJECT*.jar
  rm -f $ARTIFACT/share/hadoop/hdfs/lib/*$PROJECT*.jar
done

rm -f $ARTIFACT/share/hadoop/yarn/*.jar
rm -rf $ARTIFACT/share/hadoop/yarn/lib
rm -rf $ARTIFACT/share/hadoop/yarn/sources
rm -rf $ARTIFACT/share/hadoop/yarn/timelineservice
rm -rf $ARTIFACT/share/doc

if [ -n "${REPACKAGE}" ]; then
  tar czf $ARTIFACT-stripped.tar.gz $ARTIFACT
fi
