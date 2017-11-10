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
# 1. Download and unpack the Hadoop binary tarball
# 2. Strip out unnecessary jars
# 3. Repackage the tarball
#
# Usage:
#  $ env VERSION=2.8.2 thirdparty/package-hadoop.sh

set -eux

ARTIFACT=hadoop-$VERSION

wget https://archive.apache.org/dist/hadoop/common/$ARTIFACT/$ARTIFACT.tar.gz
tar xf $ARTIFACT.tar.gz

for DIR in common/jdiff httpfs kms tools yarn; do
  rm -rf $ARTIFACT/share/hadoop/$DIR
done

rm -rf $ARTIFACT/share/doc

tar czf $ARTIFACT-stripped.tar.gz $ARTIFACT
