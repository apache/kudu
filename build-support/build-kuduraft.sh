#!/bin/bash


# Author(s) - anirbanr-fb
# Borrowed from 
# https://kudu.apache.org/docs/installation.html#build_from_source

# INSTALL ALL DEPENDENCIES
sudo yum install -y autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
  cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
  krb5-server krb5-workstation libtool make openssl-devel patch \
  pkgconfig redhat-lsb-core rsync unzip vim-common which

# DOWNLOAD AND BUILD THE THIRD-PARTY libraries
# IF THINGS BREAK IN MIDDLE
# wipe out: you can use the big hammer!
#  rm -f thirdparty/{src,installed,build}
#  rerun
build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh

mkdir -p build/release
cd build/release
../../build-support/enable_devtoolset.sh \
  cmake -DCMAKE_BUILD_TYPE=release ../..
make -j20
