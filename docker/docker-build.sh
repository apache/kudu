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
#
#   BASES: Default: "ubuntu:xenial"
#     A csv string with the list of base operating systems to build with.
#
#   TARGETS: Default: "kudu"
#     A csv string with the list of targets to build and tag.
#     These targets are defined in the Dockerfile.
#     Dependent targets of a passed image will be build, but not
#     tagged. Note that if a target is not tagged it is subject
#     removal by dockers system and image pruning.
#
#   REPOSITORY: Default: "apache/kudu"
#     The repository string to use when tagging the image.
#
#   PUBLISH: Default: "0"
#     If set to 1, the tagged images will be pushed to the docker repository.
#     Only release versions can be published.
#
#   TAG_LATEST: Default: "1"
#     If set to 1, adds a tag using `-latest` along with the
#     versioned tag.
#
#   TAG_HASH: Default: "0"
#     If set to 1, keeps the tags using the short git hash as
#     the version for non-release builds. Leaving this as 0
#     ensures the tags containing the short git hash are removed
#     which keeps the `docker images` list cleaner when only the
#     latest image is relevant.
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
DEFAULT_OS="ubuntu:xenial"
BASES=${BASES:="$DEFAULT_OS"}
TARGETS=${TARGETS:="kudu"}
REPOSITORY=${REPOSITORY:="apache/kudu"}
PUBLISH=${PUBLISH:=0}
TAG_LATEST=${TAG_LATEST:=1}
TAG_HASH=${TAG_HASH:=0}
DOCKER_CACHE_FROM=${DOCKER_CACHE_FROM:=""}

VERSION=$(cat "$ROOT/version.txt")
VCS_REF=$(git rev-parse --short HEAD || echo "")

# Create the VERSION_TAG.
if [[ "$VERSION" == *-SNAPSHOT ]]; then
  if [[ "$VCS_REF" == "" ]]; then
      echo "ERROR: Snapshot builds need to be built in a Git working directory"
      exit 1
  fi

  IS_RELEASE_VERSION=0
  VERSION_TAG="$VCS_REF"
else
  IS_RELEASE_VERSION=1
  VERSION_TAG="$VERSION"
  MINOR_VERSION_TAG=$(echo "$VERSION" | sed "s/.[^.]*$//")
fi

# Constructs an OS tag based on the passed BASE_IMAGE.
# The operating system is described with the version name.
# If the operating system version is numeric, the version
# will also be appended.
function get_os_tag() {
  local BASE_IMAGE=$1
  local OS_NAME=$(echo "$BASE_IMAGE" | cut -d':' -f1)
  local OS_VERSION=$(echo "$BASE_IMAGE" | cut -d':' -f2)
  if [[ "$OS_VERSION" == [[:digit:]]* ]]; then
    echo "$OS_NAME$OS_VERSION"
  else
    echo "$OS_VERSION"
  fi
}

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

TAGS=()
for BASE_OS in $(echo "$BASES" | tr ',' '\n'); do
  # Generate the arguments to pass to the docker build.
  BUILD_ARGS=(
    --build-arg BASE_OS="$BASE_OS"
    --build-arg DOCKERFILE="docker/Dockerfile"
    --build-arg MAINTAINER="Apache Kudu <dev@kudu.apache.org>"
    --build-arg URL="https://kudu.apache.org"
    --build-arg VERSION="$VERSION"
    --build-arg VCS_REF="$VCS_REF"
    --build-arg VCS_TYPE="git"
    --build-arg VCS_URL="https://gitbox.apache.org/repos/asf/kudu.git"
  )
  if [[ -n "$DOCKER_CACHE_FROM" ]]; then
    BUILD_ARGS+=(--cache-from "$DOCKER_CACHE_FROM")
  fi
  OS_TAG=$(get_os_tag "$BASE_OS")

  for TARGET in $(echo "$TARGETS" | tr ',' '\n'); do
    FULL_TAG=$(get_tag "$TARGET" "$VERSION_TAG" "$OS_TAG")

    # Build the target and tag with the full tag.
    docker build "${BUILD_ARGS[@]}" -f "$ROOT/docker/Dockerfile" \
      --target "$TARGET" -t "$FULL_TAG" ${ROOT}
    TAGS+=("$FULL_TAG")

    # If this is the default OS, also tag it without the OS-specific tag.
    if [[ "$BASE_OS" == "$DEFAULT_OS" ]]; then
      DEFAULT_OS_TAG=$(get_tag "$TARGET" "$VERSION_TAG" "")
      docker tag "$FULL_TAG" "$DEFAULT_OS_TAG"
      TAGS+=("$DEFAULT_OS_TAG")
    fi

    # Add the minor version tag if this is a release version.
    if [[ "$IS_RELEASE_VERSION" == "1" ]]; then
      MINOR_TAG=$(get_tag "$TARGET" "$MINOR_VERSION_TAG" "$OS_TAG")
      docker tag "$FULL_TAG" "$MINOR_TAG"
      TAGS+=("$MINOR_TAG")

      # Add the default OS tag.
      if [[ "$BASE_OS" == "$DEFAULT_OS" ]]; then
        MINOR_DEFAULT_OS_TAG=$(get_tag "$TARGET" "$MINOR_VERSION_TAG" "")
        docker tag "$FULL_TAG" "$MINOR_DEFAULT_OS_TAG"
        TAGS+=("$MINOR_DEFAULT_OS_TAG")
      fi
    fi

    # Add the latest version tags.
    if [[ "$TAG_LATEST" == "1" ]]; then
      LATEST_TAG=$(get_tag "$TARGET" "latest" "$OS_TAG")
      docker tag "$FULL_TAG" "$LATEST_TAG"
      TAGS+=("$LATEST_TAG")

      # Add the default OS tag.
      if [[ "$BASE_OS" == "$DEFAULT_OS" ]]; then
        LATEST_DEFAULT_OS_TAG=$(get_tag "$TARGET" "latest" "")
        docker tag "$FULL_TAG" "$LATEST_DEFAULT_OS_TAG"
        TAGS+=("$LATEST_DEFAULT_OS_TAG")
      fi
    fi

    # Remove the hash tags if the aren't wanted.
    if [[ "$TAG_HASH" != "1" && "$IS_RELEASE_VERSION" != "1" ]]; then
      HASH_TAG_PATTERN="$REPOSITORY:*$VCS_REF*"
      docker rmi $(docker images -q "$HASH_TAG_PATTERN" --format "{{.Repository}}:{{.Tag}}")
    fi
  done
done

if [[ "$PUBLISH" == 1 ]]; then
  if [[ "$IS_RELEASE_VERSION" != "1" ]]; then
    echo "ERROR: Only release versions can be published. Found version $VERSION ($VCS_REF)"
    exit 1
  fi
  for TAG in "${TAGS[@]}"; do
    docker push "${TAG}"
  done
fi
