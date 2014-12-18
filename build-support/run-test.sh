#!/bin/bash
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Script which wraps running a test and redirects its output to a
# test log directory.
#
# If KUDU_COMPRESS_TEST_OUTPUT is non-empty, then the logs will be
# gzip-compressed while they are written.
#
# If KUDU_FLAKY_TEST_ATTEMPTS is non-zero, and the test being run matches
# one of the lines in the file KUDU_FLAKY_TEST_LIST, then the test will
# be retried on failure up to the specified number of times. This can be
# used in the gerrit workflow to prevent annoying false -1s caused by
# tests that are known to be flaky in master.
#
# If KUDU_REPORT_TEST_RESULTS is non-zero, then tests are reported to the
# central test server.

ME=$(dirname $BASH_SOURCE)
ROOT=$(readlink -f $ME/..)

TEST_LOGDIR=$ROOT/build/test-logs
mkdir -p $TEST_LOGDIR

TEST_DEBUGDIR=$ROOT/build/test-debug
mkdir -p $TEST_DEBUGDIR

TEST_EXECUTABLE=$(readlink -f $1)
shift
TEST_NAME=$(basename $TEST_EXECUTABLE | perl -pe 's/\..+?$//') # Remove path and extension (if any).

# Determine whether the test is a known flaky by comparing against the user-specified
# list.
TEST_EXECUTION_ATTEMPTS=1
if [ -n "$KUDU_FLAKY_TEST_LIST" ]; then
  if [ -f "$KUDU_FLAKY_TEST_LIST" ]; then
    IS_KNOWN_FLAKY=$(grep --count --line-regexp "$TEST_NAME" "$KUDU_FLAKY_TEST_LIST")
  else
    echo "Flaky test list file $KUDU_FLAKY_TEST_LIST missing"
    IS_KNOWN_FLAKY=0
  fi
  if [ "$IS_KNOWN_FLAKY" -gt 0 ]; then
    TEST_EXECUTION_ATTEMPTS=${KUDU_FLAKY_TEST_ATTEMPTS:-1}
    echo $TEST_NAME is a known-flaky test. Will attempt running it
    echo up to $TEST_EXECUTION_ATTEMPTS times.
  fi
fi


# We run each test in its own subdir to avoid core file related races.
TEST_WORKDIR=$ROOT/build/test-work/$TEST_NAME
mkdir -p $TEST_WORKDIR
pushd $TEST_WORKDIR >/dev/null || exit 1
rm -f *

set -o pipefail

LOGFILE=$TEST_LOGDIR/$TEST_NAME.txt
XMLFILE=$TEST_LOGDIR/$TEST_NAME.xml

# Remove both the compressed and uncompressed output, so the developer
# doesn't accidentally get confused and read output from a prior test
# run.
rm -f $LOGFILE $LOGFILE.gz

if [ -n "$KUDU_COMPRESS_TEST_OUTPUT" ] && [ "$KUDU_COMPRESS_TEST_OUTPUT" -ne 0 ] ; then
  pipe_cmd=gzip
  LOGFILE=${LOGFILE}.gz
else
  pipe_cmd=cat
fi

export TSAN_OPTIONS="$TSAN_OPTIONS suppressions=$ROOT/build-support/tsan-suppressions.txt history_size=7"

# Set a 15-minute timeout for tests run via 'make test'.
# This keeps our jenkins builds from hanging in the case that there's
# a deadlock or anything.
KUDU_TEST_TIMEOUT=${KUDU_TEST_TIMEOUT:-900}

# Allow for collecting core dumps.
KUDU_TEST_ULIMIT_CORE=${KUDU_TEST_ULIMIT_CORE:-0}
ulimit -c $KUDU_TEST_ULIMIT_CORE

# Run the actual test.
for ATTEMPT_NUMBER in $(seq 1 $TEST_EXECUTION_ATTEMPTS) ; do
  if [ $ATTEMPT_NUMBER -lt $TEST_EXECUTION_ATTEMPTS ]; then
    # If the test fails, the test output may or may not be left behind,
    # depending on whether the test cleaned up or exited immediately. Either
    # way we need to clean it up. We do this by comparing the data directory
    # contents before and after the test runs, and deleting anything new.
    #
    # The comm program requires that its two inputs be sorted.
    TEST_TMPDIR_BEFORE=$(find $TEST_TMPDIR -maxdepth 1 -type d | sort)
  fi

  # gtest won't overwrite old junit test files, resulting in a build failure
  # even when retries are successful.
  rm -f $XMLFILE

  echo "Running $TEST_NAME, redirecting output into $LOGFILE" \
    "(attempt ${ATTEMPT_NUMBER}/$TEST_EXECUTION_ATTEMPTS)"
  $TEST_EXECUTABLE "$@" --test_timeout_after $KUDU_TEST_TIMEOUT 2>&1 \
    | $ROOT/thirdparty/asan_symbolize.py \
    | c++filt \
    | $ROOT/build-support/stacktrace_addr2line.pl $TEST_EXECUTABLE \
    | $pipe_cmd > $LOGFILE
  STATUS=$?

  # TSAN doesn't always exit with a non-zero exit code due to a bug:
  # mutex errors don't get reported through the normal error reporting infrastructure.
  # So we make sure to detect this and exit 1.
  #
  # Additionally, certain types of failures won't show up in the standard JUnit
  # XML output from gtest. We assume that gtest knows better than us and our
  # regexes in most cases, but for certain errors we delete the resulting xml
  # file and let our own post-processing step regenerate it.
  export GREP=$(which egrep)
  if zgrep --silent "ThreadSanitizer|Leak check.*detected leaks" $LOGFILE ; then
    echo ThreadSanitizer or leak check failures in $LOGFILE
    STATUS=1
    rm -f $XMLFILE
  fi

  if [ $ATTEMPT_NUMBER -lt $TEST_EXECUTION_ATTEMPTS ]; then
    # Now delete any new test output.
    TEST_TMPDIR_AFTER=$(find $TEST_TMPDIR -maxdepth 1 -type d | sort)
    DIFF=$(comm -13 <(echo "$TEST_TMPDIR_BEFORE") \
                    <(echo "$TEST_TMPDIR_AFTER"))
    for DIR in $DIFF; do
      # Multiple tests may be running concurrently. To avoid deleting the
      # wrong directories, constrain to only directories beginning with the
      # test name.
      #
      # This may delete old test directories belonging to this test, but
      # that's not typically a concern when rerunning flaky tests.
      if [[ $DIR =~ ^$TEST_TMPDIR/$TEST_NAME ]]; then
        echo Deleting leftover flaky test directory "$DIR"
        rm -Rf "$DIR"
      fi
    done
  fi

  if [ -n "$KUDU_REPORT_TEST_RESULTS" ]; then
    echo Reporting results
    $ROOT/build-support/report-test.sh "$TEST_EXECUTABLE" "$LOGFILE" "$STATUS" &

    # On success, we'll do "best effort" reporting, and disown the subprocess.
    # On failure, we want to upload the failed test log. So, in that case,
    # wait for the report-test.sh job to finish, lest we accidentally run
    # a test retry and upload the wrong log.
    if [ "$STATUS" -eq "0" ]; then
      disown
    else
      wait
    fi
  fi

  if [ "$STATUS" -eq "0" ]; then
    break
  elif [ "$ATTEMPT_NUMBER" -lt "$TEST_EXECUTION_ATTEMPTS" ]; then
    echo Test failed attempt number $ATTEMPT_NUMBER
    echo Will retry...
  fi
done

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

popd
rm -Rf $TEST_WORKDIR

exit $STATUS
