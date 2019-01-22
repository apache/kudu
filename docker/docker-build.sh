#!/bin/bash -xe
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
# This script handles the coordination of building all of
# the Apache Kudu docker images.
#
##########################################################

ROOT=$(cd $(dirname "$BASH_SOURCE")/.. ; pwd)

# Tested options:
#   centos:6
#   centos:7
#   debian:jessie
#   ubuntu:trusty
#   ubuntu:xenial
#   ubuntu:bionic
BASE_OS=${BASE_OS:=ubuntu:xenial}

VERSION=`cat $ROOT/version.txt`
VCS_REF=`git rev-parse --short HEAD`

BUILD_ARGS=(
  --build-arg BASE_OS="$BASE_OS"
  --build-arg DOCKERFILE="docker/Dockerfile"
  --build-arg MAINTAINER="Apache Kudu <dev@kudu.apache.org>"
  --build-arg URL="https://kudu.apache.org"
  --build-arg VERSION=$VERSION
  --build-arg VCS_REF=$VCS_REF
  --build-arg VCS_TYPE="git"
  --build-arg VCS_URL="https://gitbox.apache.org/repos/asf/kudu.git"
)

docker build "${BUILD_ARGS[@]}" -f $ROOT/docker/Dockerfile --target kudu-base -t kudu-base $ROOT
docker build "${BUILD_ARGS[@]}" -f $ROOT/docker/Dockerfile --target kudu-thirdparty -t kudu-thirdparty $ROOT
docker build "${BUILD_ARGS[@]}" -f $ROOT/docker/Dockerfile --target kudu-build -t kudu-build $ROOT
docker build "${BUILD_ARGS[@]}" -f $ROOT/docker/Dockerfile --target kudu-runtime -t kudu-runtime $ROOT
docker build "${BUILD_ARGS[@]}" -f $ROOT/docker/Dockerfile --target kudu-master -t kudu-master $ROOT
docker build "${BUILD_ARGS[@]}" -f $ROOT/docker/Dockerfile --target kudu-tserver -t kudu-tserver $ROOT