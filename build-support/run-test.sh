#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.
#
# Script which wraps running a test and redirects its output to a
# test log directory.

ME=$(dirname $BASH_SOURCE)
ROOT=$(readlink -f $ME/..)

TEST_OUT=$ROOT/build/test-logs/
mkdir -p $TEST_OUT
TEST_NAME=$(basename $1)

set -e
set -o pipefail

"$@" 2>&1 | tee $TEST_OUT/$TEST_NAME.txt
