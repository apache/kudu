#!/bin/bash
set -eux
: ${REPO:=https://github.com/cloudera/kudu-examples.git}

git clone ${REPO}
pushd kudu-examples
pushd demo-vm-setup

./setup-kudu-demo-vm.sh

popd
