#!/bin/bash
########################################################################
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
# This script generates site documentation and Javadocs by running
# make_site.sh in a Linux Docker container. This makes it possible to
# generate the documentation for publishing purposes on a non-Linux host,
# e.g. on macOS.
#
# All the arguments are passed to make_site.sh.
#
# Environment variables:
#   KUDU_REF          The git revision to build the docs from (default: HEAD).
#                     Only committed changes are built.
#   KUDU_SITE_OUT     The directory to write the generated site into
#                     (default: <source root>/build/docker-site).
#   KUDU_SITE_REMOTE  The repository to clone the gh-pages branch from
#                     (default: https://github.com/apache/kudu.git).
#   DOCKER_PLATFORM   The platform of the container, e.g. linux/amd64
#                     (default: the platform of the Docker host).
########################################################################
set -e
set -o pipefail

SOURCE_ROOT=$(cd $(dirname $0)/../../..; pwd)
IMAGE=kudu-site-builder

if ! which docker >/dev/null 2>&1; then
  echo "Error: docker not found."
  exit 1
fi

KUDU_SHA=$(git -C "$SOURCE_ROOT" rev-parse --verify "${KUDU_REF:-HEAD}^{commit}")
KUDU_SITE_OUT=${KUDU_SITE_OUT:-$SOURCE_ROOT/build/docker-site}
mkdir -p "$KUDU_SITE_OUT"
KUDU_SITE_OUT=$(cd "$KUDU_SITE_OUT"; pwd)

if [ -n "$DOCKER_PLATFORM" ]; then
  PLATFORM_FLAG="--platform $DOCKER_PLATFORM"
  ARCH=${DOCKER_PLATFORM##*/}
else
  PLATFORM_FLAG=""
  ARCH=$(docker version --format '{{.Server.Arch}}')
fi
# The workspace with thirdparty and build artifacts is kept in a volume
# between runs. Use separate volumes for different architectures.
VOLUME="kudu-site-build-$ARCH"

if ! git -C "$SOURCE_ROOT" diff --quiet HEAD 2>/dev/null; then
  echo "NOTE: uncommitted changes in $SOURCE_ROOT are not included in the build."
fi
echo "Building the site from $KUDU_SHA for linux/$ARCH"

docker build $PLATFORM_FLAG -t "$IMAGE" \
  -f "$SOURCE_ROOT/docs/support/docker/Dockerfile" \
  "$SOURCE_ROOT/docker"

TTY_FLAG=""
if [ -t 0 ] && [ -t 1 ]; then
  TTY_FLAG="-it"
fi
docker run --rm $TTY_FLAG $PLATFORM_FLAG \
  -v "$SOURCE_ROOT":/src:ro \
  -v "$VOLUME":/work \
  -v "$KUDU_SITE_OUT":"$KUDU_SITE_OUT" \
  -e KUDU_SHA="$KUDU_SHA" \
  -e KUDU_SITE_OUT="$KUDU_SITE_OUT" \
  -e KUDU_SITE_REMOTE \
  "$IMAGE" \
  /src/docs/support/docker/build-site.sh "$@"

VERSION=$(git -C "$SOURCE_ROOT" show "$KUDU_SHA:version.txt")
cat <<EOF

Generated web site at $KUDU_SITE_OUT
Release documentation generated at $KUDU_SITE_OUT/releases/$VERSION

To view the site locally at http://localhost:4000/, run:
  $SOURCE_ROOT/docs/support/scripts/site_tool_docker.sh $KUDU_SITE_OUT jekyll serve

To copy the release documentation into a gh-pages worktree, run:
  rsync -a --delete $KUDU_SITE_OUT/releases/$VERSION/ <gh-pages>/releases/$VERSION/
  rsync -a $KUDU_SITE_OUT/{docs,apidocs,cpp-client-api} <gh-pages>/
Note the trailing slashes in the first command.

To check for broken links in, or to publish, the gh-pages worktree, run:
  $SOURCE_ROOT/docs/support/scripts/site_tool_docker.sh <gh-pages> proof
  $SOURCE_ROOT/docs/support/scripts/site_tool_docker.sh <gh-pages> publish

The build workspace is kept in the '$VOLUME' Docker volume to speed up
subsequent runs. To reclaim the space, run: docker volume rm $VOLUME
EOF
