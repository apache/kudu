#!/bin/bash
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

# Script which downloads and builds the thirdparty dependencies
# only if necessary.
#
# In a git repo, this uses git checksum information on the thirdparty
# tree. Otherwise, it uses a 'stamp file' approach.

set -e
set -o pipefail

DEPENDENCY_GROUPS=
case $1 in
  "")
    DEPENDENCY_GROUPS="common uninstrumented"
    ;;
  "tsan")
    DEPENDENCY_GROUPS="common tsan"
    ;;
  "all")
    DEPENDENCY_GROUPS="common uninstrumented tsan"
    ;;
  *)
    echo "Unknown build configuration: $1"
    exit 1
    ;;
esac


TP_DIR=$(dirname $BASH_SOURCE)
cd $TP_DIR

NEEDS_BUILD=
NEEDS_REHASH=

IS_IN_GIT=$(test -d ../.git && echo true || :)

if [ -n "$IS_IN_GIT" ]; then
  # Determine whether this subtree in the git repo has changed since thirdparty
  # was last built.
  CUR_THIRDPARTY_HASH=$(cd .. && git ls-tree -d HEAD thirdparty | awk '{print $3}')

  for GROUP in $DEPENDENCY_GROUPS; do
    LAST_BUILD_HASH=$(cat .build-hash.$GROUP || :)
    if [ "$CUR_THIRDPARTY_HASH" != "$LAST_BUILD_HASH" ]; then
      echo "Rebuilding thirdparty dependency group '$GROUP': the repository has changed since it was last built."
      echo "Old git hash: $LAST_BUILD_HASH"
      echo "New build hash: $CUR_THIRDPARTY_HASH"
      NEEDS_BUILD="$NEEDS_BUILD $GROUP"
      NEEDS_REHASH="$NEEDS_REHASH $GROUP"
    fi
  done

  if [ -z "$NEEDS_BUILD" ]; then
    # All the hashes matched. Determine whether the developer has any local changes.
    if ! ( git diff --quiet .  && git diff --cached --quiet . ) ; then
      echo "Rebuilding thirdparty dependency groups '$DEPENDENCY_GROUPS': there are local changes in the repository."
      NEEDS_BUILD="$DEPENDENCY_GROUPS"
    fi
  fi

  if [ -z "$NEEDS_BUILD" ]; then
    echo "Not rebuilding thirdparty. No changes since last build."
  fi
else
  # If we aren't inside running inside a git repository (e.g. we are
  # part of a source distribution tarball) then we can't use git to find
  # out whether the build is clean. Instead, look at the ctimes of special
  # stamp files, and see if any files inside this directory have been
  # modified since then.
  for GROUP in $DEPENDENCY_GROUPS; do
    STAMP_FILE=.build-stamp.$GROUP
    if [ -f $STAMP_FILE ]; then
      CHANGED_FILE_COUNT=$(find . -cnewer $STAMP_FILE | wc -l)
      echo "$CHANGED_FILE_COUNT file(s) been modified since thirdparty dependency group '$GROUP' was last built."
      if [ $CHANGED_FILE_COUNT -gt 0 ]; then
        echo "Rebuilding."
        NEEDS_BUILD="$NEEDS_BUILD $GROUP"
      fi
    else
      echo "It appears that thirdparty dependency group '$GROUP' was never built. Building."
      NEEDS_BUILD="$NEEDS_BUILD $GROUP"
    fi
  done
fi

if [ -z "$NEEDS_BUILD" ]; then
  exit 0
fi

# Remove the old hashes/stamps before building so that if the build is aborted
# and the repository is taken backwards in time to the point where the old
# hashes/stamps matched, the repository would still be rebuilt.
for GROUP in $NEEDS_BUILD; do
  rm -f .build-hash.$GROUP .build-stamp.$GROUP
done

# Download and build the necessary dependency groups.
./download-thirdparty.sh
./build-thirdparty.sh $NEEDS_BUILD

# The build succeeded. Update the appropriate hashes/stamps.
if [ -n "$IS_IN_GIT" ]; then
  for GROUP in $NEEDS_REHASH; do
    echo $CUR_THIRDPARTY_HASH > .build-hash.$GROUP
  done
else
  for GROUP in $NEEDS_BUILD; do
    touch .build-stamp.$GROUP
  done
fi
