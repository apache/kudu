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

# This script serves to workaround a problematic OpenSSL ABI change
# made between RHEL 6.4 and 6.5. Namely:
#
#  RHEL 6.4's OpenSSL library is built with no symbol versioning. For example:
#  $ objdump -T libssl.so | grep SSL_CTX_new
#    0000000000037110 g    DF .text  0000000000000577  Base        SSL_CTX_new
#
#  RHEL 6.5's OpenSSL library has symbol versions. For example:
#  $ objdump -T /usr/lib64/libssl.so | grep SSL_CTX_new
#    0000003ae8243610 g    DF .text  0000000000000597  libssl.so.10 SSL_CTX_new
#
# Thus, if we build Kudu on RHEL 6.5 or later, the resulting binaries expect
# the versioned symbols in libssl and will not run on RHEL 6.4 or earlier:
#
#  $ objdump -T kudu-tserver | grep SSL_CTX_new
#    0000000000000000      DF *UND*     0000000000000000  libssl.so.10 SSL_CTX_new
#
# In contrast, if a binary is built not expecting versioned symbols, the runtime
# linker can still resolve those symbols by choosing the versioned ones. Thus,
# binaries built against RHEL 6.4 are forward-compatible to later versions, but
# not vice versa.
#
# Note that Kudu cannot simply be built on RHEL 6.4 because the devtoolset toolchain is
# not available. So, given that we want to produce binaries that run on RHEL 6.4,
# we need to perform a workaround such that our binaries built on 6.6 don't depend
# on the versioned symbols in OpenSSL. This script provides such a workaround.
#
# The workaround itself is quite simple: we download the OpenSSL RPMs from CentOS 6.4
# and unpack them into a directory in thirdparty/. If we then build against those
# the resulting binaries can run on either el6.4 or el6.6.

set -e

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
source $TP_DIR/vars.sh

mkdir -p $OPENSSL_WORKAROUND_DIR
cd $OPENSSL_WORKAROUND_DIR

# Clean up any previous leftovers.
rm -Rf usr etc

# Download and unpack OpenSSL RPMs from CentOS 6.4.
#
# We have mirrored these in our S3 bucket, but the original sources are in
# http://vault.centos.org/6.4/os/x86_64/Packages/ .
for FILENAME in openssl-1.0.0-27.el6.x86_64.rpm openssl-devel-1.0.0-27.el6.x86_64.rpm ; do
  FULL_URL="${DEPENDENCY_URL}/${FILENAME}"
  if [ -r "$FILENAME" ]; then
    echo $FILENAME already exists. Not re-downloading.
  else
    echo "Fetching $FILENAME from $FULL_URL"
    curl -L -O "${FULL_URL}"
  fi

  echo "Unpacking $FILENAME"
  rpm2cpio $FILENAME | cpio -idm
done

