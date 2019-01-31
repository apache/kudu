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
##########################################################
#
# This script handles the coordination of building all of
# the Apache Kudu docker images.
#
# The format for tagging images is:
#   kudu:[image-type]-[version]-[operating-system]
#
# The image type is the name of any target image. However,
# the default image `kudu` will not include the image type
# in the tag because it is redundant with the repository
# name and intended to be the image used by most users.
# This allows us to support the common syntax of
# `kudu:latest` for normal users.
#
# The version tag for release images will be the full semantic
# version. An additional tag with just the minor version will
# also be created to allow for roll-forward semantics of
# maintenance releases. Tags with just the major version are not
# created because automatically rolling forward to a major version
# is not recommended. Images created for a snapshot version
# will use the short git hash in place of the version.
# An additional latest version can be used for the latest build.
#
# The operating system is described with the version name.
# If the operating system version is numeric, the operating
# system name will be included. For example, `centos:6` would
# be `centos6` while `ubuntu:trusty` would be `trusty`.
# An additional tag without the operating system included
# will be generated when the default operating system is used.
#
# Environment variables may be used to customize operation:
#   BASE_OS: Default: ubuntu:xenial
#     The base image to use.
#
#   REPOSITORY: Default: apache/kudu
#     The repository string to use when tagging the image.
#
#   TAG_LATEST: Default: 1
#     If set to 1, adds a tag using `-latest` along with the
#     versioned tag.
#
#   DOCKER_CACHE_FROM:
#      Optional images passed to the `docker build` commands
#      via the `--cache-from` option. This option tells docker
#      images to consider as cache sources.
##########################################################
set -ex

ROOT=$(cd $(dirname "$BASH_SOURCE")/.. ; pwd)

# Tested options:
#   centos:6
#   centos:7
#   debian:jessie
#   debian:stretch
#   ubuntu:trusty
#   ubuntu:xenial
#   ubuntu:bionic
DEFAULTS_OS="ubuntu:xenial"
BASE_OS=${BASE_OS:="$DEFAULTS_OS"}
REPOSITORY=${REPOSITORY:="apache/kudu"}
TAG_LATEST=${TAG_LATEST:=1}
DOCKER_CACHE_FROM=${DOCKER_CACHE_FROM:=""}
TARGETS=("base" "thirdparty" "build" "kudu")

VERSION=$(cat $ROOT/version.txt)
VCS_REF=$(git rev-parse --short HEAD)

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

if [[ -n "$DOCKER_CACHE_FROM" ]]; then
  BUILD_ARGS+=(--cache-from "$DOCKER_CACHE_FROM")
fi

# Create the OS_TAG.
OS_NAME=$(echo "$BASE_OS" | cut -d':' -f1)
OS_VERSION=$(echo "$BASE_OS" | cut -d':' -f2)
if [[ "$OS_VERSION" == [[:digit:]]* ]]; then
  OS_TAG="$OS_NAME$OS_VERSION"
else
  OS_TAG="$OS_VERSION"
fi

# Create the VERSION_TAG.
if [[ "$VERSION" == *-SNAPSHOT ]]; then
  IS_RELEASE_VERSION=0
  VERSION_TAG="$VCS_REF"
else
  IS_RELEASE_VERSION=1
  VERSION_TAG="$VERSION"
  MINOR_VERSION_TAG=$(echo "$VERSION" | sed "s/.[^.]*$//")
fi

# Constructs a tag, excluding the OS_TAG if it is empty.
# Additionally ignores the target when it is the default target "kudu".
# Examples:
#   get_tag "kudu" "latest" ""        = apache/kudu:latest
#   get_tag "base" "1.8.0" ""         = apache/kudu:base-1.8.0
#   get_tag "base" "1.8.0" "centos6"  = apache/kudu:base-1.8.0-centos6
function get_tag() {
  local TARGET_TAG=$1
  local VERSION_TAG=$2
  local OS_TAG=$3
  local TAG=""
  # Only include the target if this isn't the default.
  if [[ "$TARGET_TAG" != "kudu" ]]; then
    local TAG="$TARGET_TAG-"
  fi
  local TAG="$TAG$VERSION_TAG-"
  if [[ -n "$OS_TAG" ]]; then
    local TAG="$TAG$OS_TAG-"
  fi
  # Remove the last character to eliminate the extra '-'.
  local TAG=${TAG%?}
  echo "$REPOSITORY:$TAG"
}

for TARGET in "${TARGETS[@]}"; do
  FULL_TAG=$(get_tag "$TARGET" "$VERSION_TAG" "$OS_TAG")

  # Build the target and tag with the full tag.
  docker build "${BUILD_ARGS[@]}" -f $ROOT/docker/Dockerfile \
    --target "$TARGET" -t "$FULL_TAG" ${ROOT}

  # If this is the default OS, also tag it without the OS-specific tag.
  if [[ "$BASE_OS" == "$DEFAULTS_OS" ]]; then
    docker tag "$FULL_TAG" "$(get_tag "$TARGET" "$VERSION_TAG" "")"
  fi

  # Add the minor version tag if this is a release version.
  if [[ "$IS_RELEASE_VERSION" == "1" ]]; then
    docker tag "$FULL_TAG" "$(get_tag "$TARGET" "$MINOR_VERSION_TAG" "$OS_TAG")"
    # Add the default OS tag.
    if [[ "$BASE_OS" == "$DEFAULTS_OS" ]]; then
      docker tag "$FULL_TAG" "$(get_tag "$TARGET" "$MINOR_VERSION_TAG" "")"
    fi
  fi

  # Add the latest version tags.
  if [[ "$TAG_LATEST" == "1" ]]; then
    docker tag "$FULL_TAG" "$(get_tag "$TARGET" "latest" "$OS_TAG")"
    # Add the default OS tag.
    if [[ "$BASE_OS" == "$DEFAULTS_OS" ]]; then
      docker tag "$FULL_TAG" "$(get_tag "$TARGET" "latest" "")"
    fi
  fi
done
