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
#
# If on a RHEL-based system where the default C++ compiler does not support
# all features necessary to compile Kudu from source (C++17), this script
# enables the Red Hat devtoolset and executes the arguments.
#
# If on a SUSE-based system where the default C++ compiler does not support
# all features necessary to compile Kudu from source (C++17), this script
# enables the GCC-8 compiler as the default CC and GCC.
#
# On other any other systems, the arguments are executed without changes to
# the environment.
#
# USAGE: ./enable_devtoolset.sh <command> <args>...

set -e

ROOT=$(cd $(dirname "$BASH_SOURCE") ; pwd)

if [[ "$OSTYPE" =~ ^linux ]]; then
  if ! lsb_release -irs > /dev/null 2>&1; then
    echo "ERROR: could not retrieve Linux distribution info; "
    echo "       make sure the 'lsb_release' utility is present in PATH"
    exit 1
  fi

  LSB_INFO="$(lsb_release -irs)"
  if [[ "$LSB_INFO" =~ (CentOS|RedHatEnterpriseServer|OracleServer)[[:space:]]+(6|7)\.[[:digit:]]+ ]]; then
    # Enable devtoolset-8 and invoke the inner script, which may do some
    # additional customization within the devtoolset.
    scl enable devtoolset-8 "$ROOT/enable_devtoolset_inner.sh $*"
  elif [[ "$LSB_INFO" =~ (SUSE)[[:space:]]+1[25]\.[[:digit:]]+ ]]; then
    # On SLES12 and SLES15, Kudu can be built both by gcc-8 and gcc-7 since
    # either one supports the C++17 standard. Prefer gcc-8 over gcc-7 if both
    # compilers are present.
    GCC_MAJOR_VERSION=
    if /usr/bin/gcc-8 -v > /dev/null 2>&1 && /usr/bin/g++-8 -v > /dev/null 2>&1; then
      GCC_MAJOR_VERSION=8
    elif /usr/bin/gcc-7 -v > /dev/null 2>&1 && /usr/bin/g++-7 -v > /dev/null 2>&1; then
      GCC_MAJOR_VERSION=7
    fi

    if [ -z "$GCC_MAJOR_VERSION" ]; then
      echo "ERROR: found neither gcc/g++-8 nor gcc/g++-7 in /usr/bin"
      exit 2
    fi

    # If ccache was on the PATH and CC/CXX have not already been set,
    # set them to gcc-[7,8] specific ccache helper scripts, enabling ccache.
    if which ccache > /dev/null 2>&1 && [ ! "$CC" -a ! "$CXX" ]; then
      export CC="$ROOT/ccache-gcc-$GCC_MAJOR_VERSION/cc"
      export CXX="$ROOT/ccache-gcc-$GCC_MAJOR_VERSION/c++"
    fi

    # If CC/CXX have not already been set, set them accordingly.
    if [ ! "$CC" -a ! "$CXX" ]; then
      export CC="/usr/bin/gcc-$GCC_MAJOR_VERSION"
      export CXX="/usr/bin/g++-$GCC_MAJOR_VERSION"
    fi
  fi

  # TODO(aserbin): patch the protobuf in thirdparty to accept standard CC
  #                and CXX environment variables to allow for building with
  #                versioned gcc/g++ binaries on SLES and remove x_FOR_BUILD
  export CC_FOR_BUILD="$CC"
  export CPP_FOR_BUILD="$CC -E"
  export CXXCPP_FOR_BUILD="$CXX -E"
fi

# Execute the arguments
$@
