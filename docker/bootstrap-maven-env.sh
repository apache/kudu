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

MVN_VERSION="3.9.9"
MVN_CHECKSUM="a555254d6b53d267965a3404ecb14e53c3827c09c3b94b5678835887ab404556bfaf78dcfe03ba76fa2508649dca8531c74bca4d5846513522404d48e8c4ac8b"
wget -nv \
  https://downloads.apache.org/maven/maven-3/$MVN_VERSION/binaries/apache-maven-$MVN_VERSION-bin.tar.gz
sha512sum -c - <<< "$MVN_CHECKSUM apache-maven-$MVN_VERSION-bin.tar.gz"
tar -C /usr/local/ -xf apache-maven-$MVN_VERSION-bin.tar.gz
ln -s /usr/local/apache-maven-$MVN_VERSION/bin/mvn /usr/local/bin
rm -f apache-maven-$MVN_VERSION-bin.tar.gz
