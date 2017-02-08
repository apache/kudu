#!/bin/bash -e
################################################################################
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
################################################################################
# Script to make a source archive from the Breakpad project, which does not run
# releases.
#
# We export only checked-in files, without the git metadata, and construct
# the expected directory structure to compile it. Then we tar it up.

BREAKPAD_REPO=https://chromium.googlesource.com/breakpad/breakpad
LSS_REPO=https://chromium.googlesource.com/linux-syscall-support
LSS_REVISION=3f6478ac95edf86cd3da300c2c0d34a438f5dbeb

TOPDIR=breakpad-repos

rm -rf "$TOPDIR"
mkdir $TOPDIR
pushd $TOPDIR

BREAKPAD_DIR=breakpad.git
git clone $BREAKPAD_REPO $BREAKPAD_DIR
pushd $BREAKPAD_DIR

REVISION=${1:-$(git rev-parse HEAD)}
NAME=breakpad-$REVISION

git checkout $REVISION
# Export the checked-in files, without .git metadata, into "../$NAME/".
git archive --format=tar --prefix=$NAME/ $REVISION | (cd .. && tar xf -)
popd

LSS_DIR=lss.git
git clone $LSS_REPO $LSS_DIR
pushd $LSS_DIR
# Export into the thirdparty directory under "../$NAME/".
git archive --format=tar --prefix=$NAME/src/third_party/lss/ $LSS_REVISION | (cd .. && tar xf -)
popd

FILENAME="$NAME.tar.gz"
tar czvf "$FILENAME" "$NAME/"
mv "$FILENAME" ..
popd
echo "Archive created at $(pwd)/$FILENAME"

exit 0
