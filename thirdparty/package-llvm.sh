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

# Packages LLVM for inclusing in the Kudu thirdparty build.
#
# Our llvm tarball includes clang, extra clang tools, lld, and compiler-rt.
#
# See http://clang.llvm.org/get_started.html and http://lld.llvm.org/ for
# details on how they're laid out in the llvm tarball.
#
# Summary:
# 1.  Unpack the llvm tarball
# 2.  Unpack the clang tarball as tools/clang (rename from cfe-<version> to clang)
# 3.  Unpack the extra clang tools tarball as tools/clang/tools/extra
# 4.  Unpack the lld tarball as tools/lld
# 5.  Unpack the compiler-rt tarball as projects/compiler-rt
# 6.  Unpack the libc++ tarball as projects/libcxx
# 7.  Unpack the libc++abi tarball as projects/libcxxabi
# 9.  Unpack the IWYU tarball in tools/clang/tools/include-what-you-use
# 10. Create new tarball from the resulting source tree
#
# Usage:
#  $ env VERSION=6.0.0 IWYU_VERSION=0.9 thirdparty/package-llvm.sh

set -eux

for ARTIFACT in llvm cfe compiler-rt libcxx libcxxabi lld clang-tools-extra; do
  wget https://releases.llvm.org/$VERSION/$ARTIFACT-$VERSION.src.tar.xz
  tar xf $ARTIFACT-$VERSION.src.tar.xz
  rm $ARTIFACT-$VERSION.src.tar.xz
done

IWYU_TAR=include-what-you-use-${IWYU_VERSION}.src.tar.gz
wget https://include-what-you-use.org/downloads/$IWYU_TAR
tar xf $IWYU_TAR
rm $IWYU_TAR

mv cfe-$VERSION.src llvm-$VERSION.src/tools/clang
mv clang-tools-extra-$VERSION.src llvm-$VERSION.src/tools/clang/tools/extra
mv lld-$VERSION.src llvm-$VERSION.src/tools/lld
mv compiler-rt-$VERSION.src llvm-$VERSION.src/projects/compiler-rt
mv libcxx-$VERSION.src llvm-$VERSION.src/projects/libcxx
mv libcxxabi-$VERSION.src llvm-$VERSION.src/projects/libcxxabi
mv include-what-you-use llvm-$VERSION.src/tools/clang/tools/

tar czf llvm-$VERSION-iwyu-$IWYU_VERSION.src.tar.gz llvm-$VERSION.src
