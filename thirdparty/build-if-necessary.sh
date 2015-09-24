#!/bin/bash
# Copyright 2013 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Script which downloads and builds the thirdparty dependencies
# only if necessary (i.e if they have changed or the local repository
# is dirty).

set -e
set -o pipefail

TP_DIR=$(dirname $BASH_SOURCE)
cd $TP_DIR

NEEDS_BUILD=

# Determine whether this subtree in the git repo has changed since thirdparty
# was last built
CUR_THIRDPARTY_HASH=$(cd .. && git ls-tree -d HEAD thirdparty | awk '{print $3}')
LAST_BUILD_HASH=$(cat .build-hash || :)
if [ "$CUR_THIRDPARTY_HASH" != "$LAST_BUILD_HASH" ]; then
  echo "Rebuilding thirdparty: the repository has changed since thirdparty was last built."
  echo "Old git hash: $LAST_BUILD_HASH"
  echo "New build hash: $CUR_THIRDPARTY_HASH"
  NEEDS_BUILD=1
else
  # Determine whether the developer has any local changes
  if ! ( git diff --quiet .  && git diff --cached --quiet . ) ; then
    echo "Rebuilding thirdparty: There are local changes in the repository."
    NEEDS_BUILD=1
  fi
fi

if [ -n "$NEEDS_BUILD" ]; then
  rm -f .build-hash
  ./download-thirdparty.sh
  ./build-thirdparty.sh
  echo $CUR_THIRDPARTY_HASH > .build-hash
else
  echo Not rebuilding thirdparty. No changes since last build.
fi

