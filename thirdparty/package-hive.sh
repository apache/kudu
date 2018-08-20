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
# Strips jars unecessary for running the Hive Metastore. This makes the download
# smaller, and improves HMS startup time.
#
# Summary:
# 1. Download and unpack the Hive binary tarball
# 2. Strip out unnecessary jars
# 3. Repackage the tarball
#
# Usage:
#  $ env VERSION=2.3.1 thirdparty/package-hive.sh

set -eux

ARTIFACT=apache-hive-$VERSION-bin

curl --retry 3 -L -O https://archive.apache.org/dist/hive/hive-$VERSION/$ARTIFACT.tar.gz
tar xf $ARTIFACT.tar.gz

for PROJECT in accumulo aether avatica calcite curator druid groovy hbase icu4j jetty jsp maven parquet zookeeper; do
  rm $ARTIFACT/lib/*$PROJECT*.jar
done

rm -rf $ARTIFACT/hcatalog
rm -rf $ARTIFACT/jdbc
rm -rf $ARTIFACT/lib/php
rm -rf $ARTIFACT/lib/python
rm -rf $ARTIFACT/lib/py

# Remove the 'apache-' prefix and '-bin' suffix in order to normalize the
# tarball with Hadoop and the rest of our thirdparty dependencies.
mv $ARTIFACT hive-$VERSION
tar czf hive-$VERSION-stripped.tar.gz hive-$VERSION
