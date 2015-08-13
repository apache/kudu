#!/bin/bash
set -eux
: ${REPO:=http://github.mtv.cloudera.com/CDH/kudu-examples.git}

git clone ${REPO}
pushd kudu-examples
pushd demo-vm-setup

./setup-kudu-demo-vm.sh

popd
