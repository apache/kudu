#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.
#
# Script which wraps running a test and redirects its output to a
# test log directory.
#
# If $KUDU_COMPRESS_TEST_OUTPUT is non-empty, then the logs will be
# gzip-compressed while they are written.

ME=$(dirname $BASH_SOURCE)
ROOT=$(readlink -f $ME/..)

TEST_OUT=$ROOT/build/test-logs/
mkdir -p $TEST_OUT
TEST_NAME=$(basename $1)

set -e
set -o pipefail

OUT=$TEST_OUT/$TEST_NAME.txt

# Remove both the compressed and uncompressed output, so the developer
# doesn't accidentally get confused and read output from a prior test
# run.
rm -f $OUT $OUT.gz

if [ -n "$KUDU_COMPRESS_TEST_OUTPUT" ] && [ "$KUDU_COMPRESS_TEST_OUTPUT" -ne 0 ] ; then
  pipe_cmd=gzip
  OUT=${OUT}.gz
else
  pipe_cmd=cat
fi

echo Running $TEST_NAME, redirecting output into $OUT
"$@"  2>&1 | $pipe_cmd > $OUT