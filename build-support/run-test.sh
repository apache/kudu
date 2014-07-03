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

TEST_LOGDIR=$ROOT/build/test-logs
mkdir -p $TEST_LOGDIR

TEST_DEBUGDIR=$ROOT/build/test-debug
mkdir -p $TEST_DEBUGDIR

TEST_EXECUTABLE=$(readlink -f $1)
shift
TEST_NAME=$(basename $TEST_EXECUTABLE)

# We run each test in its own subdir to avoid core file related races.
TEST_WORKDIR=$ROOT/build/test-work/$TEST_NAME
mkdir -p $TEST_WORKDIR
pushd $TEST_WORKDIR || exit 1
rm -f *

set -o pipefail

LOGFILE=$TEST_LOGDIR/$TEST_NAME.txt

# Remove both the compressed and uncompressed output, so the developer
# doesn't accidentally get confused and read output from a prior test
# run.
rm -f $LOGFILE $OUT.gz

if [ -n "$KUDU_COMPRESS_TEST_OUTPUT" ] && [ "$KUDU_COMPRESS_TEST_OUTPUT" -ne 0 ] ; then
  pipe_cmd=gzip
  LOGFILE=${LOGFILE}.gz
else
  pipe_cmd=cat
fi

export TSAN_OPTIONS="$TSAN_OPTIONS suppressions=$ME/tsan-suppressions.txt history_size=7"

# Set a 15-minute timeout for tests run via 'make test'.
# This keeps our jenkins builds from hanging in the case that there's
# a deadlock or anything.
KUDU_TEST_TIMEOUT=${KUDU_TEST_TIMEOUT:-900}

# Allow for collecting core dumps.
KUDU_TEST_ULIMIT_CORE=${KUDU_TEST_ULIMIT_CORE:-0}
ulimit -c $KUDU_TEST_ULIMIT_CORE

# Run the actual test.
echo Running $TEST_NAME, redirecting output into $LOGFILE
$TEST_EXECUTABLE "$@" --test_timeout_after $KUDU_TEST_TIMEOUT 2>&1 \
    | $ROOT/thirdparty/asan_symbolize.py \
    | c++filt \
    | $ROOT/build-support/stacktrace_addr2line.pl $TEST_EXECUTABLE \
    | $pipe_cmd > $LOGFILE
STATUS=$?

# Capture and compress core file and binary.
COREFILES=$(ls | grep ^core)
if [ -n "$COREFILES" ]; then
  echo Found core dump. Saving executable and core files.
  gzip < $TEST_EXECUTABLE > "$TEST_DEBUGDIR/$TEST_NAME.gz" || exit $?
  for COREFILE in $COREFILES; do
    gzip < $COREFILE > "$TEST_DEBUGDIR/$TEST_NAME.$COREFILE.gz" || exit $?
  done
  # Pull in any .so files as well.
  for LIB in $(ldd $TEST_EXECUTABLE | grep $ROOT | awk '{print $3}'); do
    LIB_NAME=$(basename $LIB)
    gzip < $LIB > "$TEST_DEBUGDIR/$LIB_NAME.gz" || exit $?
  done
fi

# TSAN doesn't always exit with a non-zero exit code due to a bug:
# mutex errors don't get reported through the normal error reporting infrastructure.
if zgrep --silent "ThreadSanitizer" $LOGFILE ; then
  echo ThreadSanitizer failures in $LOGFILE
  STATUS=1
fi

popd
rm -Rf $TEST_WORKDIR

exit $STATUS
