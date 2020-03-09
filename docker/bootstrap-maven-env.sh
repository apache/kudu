#!/bin/bash
##########################################################
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
# This script handles bootstrapping maven for
# the Apache Kudu impala build docker image.
#
##########################################################

set -xe
set -o pipefail

MVN_VERSION="3.6.3"
MVN_CHECKSUM="c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0"
wget -nv \
  https://www-us.apache.org/dist/maven/maven-3/$MVN_VERSION/binaries/apache-maven-$MVN_VERSION-bin.tar.gz
sha512sum -c - <<< "$MVN_CHECKSUM apache-maven-$MVN_VERSION-bin.tar.gz"
tar -C /usr/local/ -xf apache-maven-$MVN_VERSION-bin.tar.gz
ln -s /usr/local/apache-maven-$MVN_VERSION/bin/mvn /usr/local/bin
rm -f apache-maven-$MVN_VERSION-bin.tar.gz
