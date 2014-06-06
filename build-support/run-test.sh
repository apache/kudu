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

TEST_OUT=$ROOT/build/test-logs
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

export TSAN_OPTIONS="$TSAN_OPTIONS suppressions=$ME/tsan-suppressions.txt history_size=7"

# Set a 15-minute timeout for tests run via 'make test'.
# This keeps our jenkins builds from hanging in the case that there's
# a deadlock or anything.
KUDU_TEST_TIMEOUT=${KUDU_TEST_TIMEOUT:-900}

echo Running $TEST_NAME, redirecting output into $OUT
"$@" --test_timeout_after $KUDU_TEST_TIMEOUT 2>&1 | $ROOT/thirdparty/asan_symbolize.py | c++filt | $pipe_cmd > $OUT

# TSAN doesn't always exit with a non-zero exit code due to a bug:
# mutex errors don't get reported through the normal error reporting infrastructure.
if zgrep --silent "ThreadSanitizer" $OUT ; then
  echo ThreadSanitizer failures in $OUT
  exit 1
fi
