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
#
# This script handles bootstrapping a base OS for
# the Apache Kudu base docker images.
#
##########################################################

set -xe
set -o pipefail

# Install the prerequisite libraries, if they are not installed.
# CentOS/RHEL
if [[ -f "/usr/bin/yum" ]]; then
  # Update the repo.
  yum update -y

  # Install core build libraries.
  yum install -y \
    autoconf \
    automake \
    chrony \
    chrpath \
    cyrus-sasl-devel \
    cyrus-sasl-gssapi \
    cyrus-sasl-plain \
    flex \
    gcc \
    gcc-c++ \
    gdb \
    git \
    java-1.8.0-openjdk-devel \
    krb5-server \
    krb5-workstation \
    libtool \
    lsof \
    make \
    openssl-devel \
    patch \
    pkgconfig \
    redhat-lsb-core \
    rsync \
    unzip \
    vim-common \
    which \
    wget

  # Get the major version for version specific package logic below.
  OS_MAJOR_VERSION=$(lsb_release -rs | cut -f1 -d.)

  # Install exta impala packages for the impala images. They are nominal in size.
  # --no-install-recommends keeps the install smaller
  yum install -y \
    libffi-devel \
    lzo-devel \
    tzdata

  # We need to enable the PowerTools repository on versions 8.0 and newer
  # to install the ninja-build package.
  if [[ "$OS_MAJOR_VERSION" -gt "7" ]]; then
    yum install -y 'dnf-command(config-manager)'
    yum config-manager --set-enabled PowerTools
  fi

  # Install libraries often used for Kudu development and build performance.
  yum install -y epel-release
  yum install -y \
    ccache \
    cmake \
    ninja-build \
    vim

  # Install docs build libraries.
  # Note: Uncomment to include in your dev images. These are excluded to reduce image size and build time.
  # yum install -y \
  #  doxygen \
  #  gem \
  #  graphviz \
  #  ruby-devel \
  #  zlib-devel

  # To build on a version older than 7.0, the Red Hat Developer Toolset
  # must be installed (in order to have access to a C++11 capable compiler).
  if [[ "$OS_MAJOR_VERSION" -lt "7" ]]; then
    DTLS_RPM=rhscl-devtoolset-3-epel-6-x86_64-1-2.noarch.rpm
    DTLS_RPM_URL=https://www.softwarecollections.org/repos/rhscl/devtoolset-3/epel-6-x86_64/noarch/${DTLS_RPM}
    wget ${DTLS_RPM_URL} -O ${DTLS_RPM}
    yum install -y scl-utils ${DTLS_RPM}
    yum install -y devtoolset-3-toolchain
    rm -f $DTLS_RPM
  fi

  # Reduce the image size by cleaning up after the install.
  yum clean all
  rm -rf /var/cache/yum /tmp/* /var/tmp/*
# Ubuntu/Debian
elif [[ -f "/usr/bin/apt-get" ]]; then
  # Ensure the Debian frontend is noninteractive.
  export DEBIAN_FRONTEND=noninteractive

  # Update the repo.
  apt-get update -y

  # Install core build libraries.
  # --no-install-recommends keeps the install smaller
  apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    chrony \
    chrpath \
    curl \
    flex \
    g++ \
    gcc \
    gdb \
    git \
    krb5-admin-server \
    krb5-kdc \
    krb5-user \
    libkrb5-dev \
    libsasl2-dev \
    libsasl2-modules \
    libsasl2-modules-gssapi-mit \
    libssl-dev \
    libtool \
    lsb-release \
    lsof \
    make \
    openssl \
    patch \
    pkg-config \
    python \
    rsync \
    unzip \
    vim-common \
    wget

  # Install exta impala packages for the impala images. They are nominal in size.
  # --no-install-recommends keeps the install smaller
  apt-get install -y --no-install-recommends \
    libffi-dev \
    liblzo2-2 \
    tzdata

  # Install libraries often used for Kudu development and build performance.
  apt-get install -y --no-install-recommends \
    ccache \
    cmake \
    ninja-build \
    vim

  # Install docs build libraries.
  # Note: Uncomment to include in your dev images. These are excluded to reduce image size and build time.
  # apt-get install -y --no-install-recommends \
  #  doxygen \
  #  gem \
  #  graphviz \
  #  ruby-dev \
  #  xsltproc \
  #  zlib1g-dev

  # Reduce the image size by cleaning up after the install.
  apt-get clean
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

  unset DEBIAN_FRONTEND
else
  echo "Unsupported OS"
  exit 1
fi