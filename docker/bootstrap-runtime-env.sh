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

# Install the required runtime packages, if they are not installed.
# CentOS/RHEL
if [[ -f "/usr/bin/yum" ]]; then
  # Update the repo.
  yum update -y

  # Install runtime packages.
  yum install -y \
    cyrus-sasl-gssapi \
    cyrus-sasl-plain \
    krb5-server \
    krb5-workstation \
    openssl

  # Install exta impala runtime packages used by the impala-runtime image. They are nominal in size.
  # --no-install-recommends keeps the install smaller
  yum install -y \
    cyrus-sasl-devel \
    lzo-devel \
    tzdata \
    which

  # Reduce the image size by cleaning up after the install.
  yum clean all
  rm -rf /var/cache/yum /tmp/* /var/tmp/*
# Ubuntu/Debian
elif [[ -f "/usr/bin/apt-get" ]]; then
  # Ensure the Debian frontend is noninteractive.
  export DEBIAN_FRONTEND=noninteractive

  # Update the repo.
  apt-get update -y

  # Install runtime packages.
  # --no-install-recommends keeps the install smaller
  apt-get install -y --no-install-recommends \
    krb5-admin-server \
    krb5-kdc \
    krb5-user \
    libsasl2-2 \
    libsasl2-modules \
    libsasl2-modules-gssapi-mit \
    openssl

  # Install exta impala runtime packages used by the impala-runtime image. They are nominal in size.
  # --no-install-recommends keeps the install smaller
  apt-get install -y --no-install-recommends \
    liblzo2-2 \
    libsasl2-dev \
    tzdata

  # Reduce the image size by cleaning up after the install.
  apt-get clean
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

  unset DEBIAN_FRONTEND
# OpenSUSE/SLES
elif [[ -f "/usr/bin/zypper" ]]; then
  # Update the repo.
  zypper update -y

  zypper install -y \
    cyrus-sasl-gssapi \
    cyrus-sasl-plain \
    krb5-devel \
    krb5-server \
    openssl

  # Install extra impala runtime packages used by the impala-runtime image. They are nominal in size.
  # TODO(ghenke): tzdata equivalent package. This is not an issue given we currently
  # only build the Impala images with in CentOS 7.
  zypper install -y \
    cyrus-sasl-devel \
    liblzo2-2 \
    which

  # Reduce the image size by cleaning up after the install.
  zypper clean --all
  rm -rf /var/lib/zypp/* /tmp/* /var/tmp/*
else
  echo "Unsupported OS"
  exit 1
fi
