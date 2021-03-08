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

if [[ "$OSTYPE" =~ ^linux ]] && \
   [[ "$(lsb_release -irs)" =~ (CentOS|RedHatEnterpriseServer|OracleServer)[[:space:]]+(6|7)\.[[:digit:]]+ ]]; then
  # Enable devtoolset-8 and invoke the inner script, which may do some
  # additional customization within the devtoolset.
  scl enable devtoolset-8 "$ROOT/enable_devtoolset_inner.sh $*"
elif [[ "$OSTYPE" =~ ^linux ]] && \
   [[ "$(lsb_release -irs)" =~ (SUSE)[[:space:]]+1[25]\.[[:digit:]]+ ]]; then
  # If ccache was on the PATH and CC/CXX have not already been set,
  # set them to gcc-8 specific ccache helper scripts (thus enabling ccache).
  if which ccache > /dev/null 2>&1 && [ ! "$CC" -a ! "$CXX" ]; then
    export CC="$ROOT/ccache-gcc-8/cc"
    export CXX="$ROOT/ccache-gcc-8/c++"
  fi

  # If CC/CXX have not already been set, set them to gcc-8.
  if [ ! "$CC" -a ! "$CXX" ]; then
    export CC="/usr/bin/gcc-8"
    export CXX="/usr/bin/g++-8"
  fi

  # TODO(aserbin): patch the protobuf in thirdparty to allow building with
  #                gcc-8/g++-8 and remove these x_FOR_BUILD below
  export CC_FOR_BUILD="$CC"
  export CPP_FOR_BUILD="$CC -E"
  export CXXCPP_FOR_BUILD="$CXX -E"

  # Execute the arguments
  $@
else
  $@
fi
