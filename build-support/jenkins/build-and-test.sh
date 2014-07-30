#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.
#
# This script is invoked from the Jenkins builds to build Kudu
# and run all the unit tests.
#
# Environment variables may be used to customize operation:
#   BUILD_TYPE: Default: DEBUG
#     Maybe be one of ASAN|TSAN|LEAKCHECK|DEBUG|RELEASE|COVERAGE|LINT
#
#   KUDU_ALLOW_SLOW_TESTS   Default: 1
#     Runs the "slow" version of the unit tests. Set to 0 to
#     run the tests more quickly.
#
#   RUN_FLAKY_ONLY    Default: 0
#     Only runs tests which have failed recently, if this is 1.
#     Used by the kudu-flaky-tests jenkins build.
#
#   KUDU_FLAKY_TEST_ATTEMPTS  Default: 1
#     If more than 1, will fetch the list of known flaky tests
#     from the kudu-test jenkins job, and allow those tests to
#     be flaky in this build.
#
#   BUILD_JAVA        Default: 1
#     Build and test java code if this is set to 1.

# If a commit messages contains a line that says 'DONT_BUILD', exit
# immediately.
DONT_BUILD=$(git show|egrep '^\s{4}DONT_BUILD$')
if [ "x$DONT_BUILD" != "x" ]; then
    echo "*** Build not requested. Exiting."
    exit 1
fi

set -e
# We pipe our build output to a log file with tee.
# This bash setting ensures that the script exits if the build fails.
set -o pipefail
# gather core dumps
ulimit -c unlimited

BUILD_TYPE=${BUILD_TYPE:-DEBUG}
BUILD_TYPE=$(echo "$BUILD_TYPE" | tr a-z A-Z) # capitalize

# Set up defaults for environment variables.
DEFAULT_ALLOW_SLOW_TESTS=1

# TSAN builds are pretty slow, so don't do SLOW tests unless explicitly
# requested
if [ "$BUILD_TYPE" = "TSAN" ]; then
  DEFAULT_ALLOW_SLOW_TESTS=0
fi

export KUDU_FLAKY_TEST_ATTEMPTS=${KUDU_FLAKY_TEST_ATTEMPTS:-1}
export KUDU_ALLOW_SLOW_TESTS=${KUDU_ALLOW_SLOW_TESTS:-$DEFAULT_ALLOW_SLOW_TESTS}
export KUDU_COMPRESS_TEST_OUTPUT=${KUDU_COMPRESS_TEST_OUTPUT:-1}
BUILD_JAVA=${BUILD_JAVA:-1}

# If they specified an explicit test directory, ensure it's going to be usable.
if [ -n "$TEST_TMPDIR" ]; then
  if [ ! -d "$TEST_TMPDIR" ]; then
    mkdir -p "$TEST_TMPDIR"
  fi
  if [ ! -w "$TEST_TMPDIR" ]; then
    echo "Error: Test output directory ($TEST_TMPDIR) is not writable on $(hostname) by user $(whoami)"
    exit 1
  fi
fi

ROOT=$(readlink -f $(dirname "$BASH_SOURCE")/../..)
cd $ROOT

TEST_LOGDIR="$ROOT/build/test-logs"
TEST_DEBUGDIR="$ROOT/build/test-debug"

# Remove testing artifacts from the previous run before we do anything
# else. Otherwise, if we fail during the "build" step, Jenkins will
# archive the test logs from the previous run, thinking they came from
# this run, and confuse us when we look at the failed build.
rm -Rf Testing/Temporary
rm -f build.log
rm -Rf $TEST_LOGDIR
rm -Rf $TEST_DEBUGDIR
rm -rf CMakeCache.txt CMakeFiles src/kudu/*/CMakeFiles

# PATH=<toolchain_stuff>:$PATH
export TOOLCHAIN=/mnt/toolchain/toolchain.sh
if [ -f "$TOOLCHAIN" ]; then
  source $TOOLCHAIN
fi

thirdparty/build-if-necessary.sh

# PATH=<thirdparty_stuff>:<toolchain_stuff>:$PATH
THIRDPARTY_BIN=$(pwd)/thirdparty/installed/bin
export PATH=$THIRDPARTY_BIN:$PATH
export PPROF_PATH=$(pwd)/thirdparty/installed/bin/pprof

# Configure the build
if [ "$BUILD_TYPE" = "ASAN" ]; then
  # NB: passing just "clang++" below causes an infinite loop, see
  # http://www.cmake.org/pipermail/cmake/2012-December/053071.html
  CC=$THIRDPARTY_BIN/clang CXX=$THIRDPARTY_BIN/clang++ \
   cmake -DKUDU_USE_ASAN=1 -DKUDU_USE_UBSAN=1 .
  BUILD_TYPE=fastdebug
elif [ "$BUILD_TYPE" = "TSAN" ]; then
  CC=$THIRDPARTY_BIN/clang CXX=$THIRDPARTY_BIN/clang++ \
   cmake -DKUDU_USE_TSAN=1 .
  BUILD_TYPE=fastdebug
  EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS -LE no_tsan"
elif [ "$BUILD_TYPE" = "LEAKCHECK" ]; then
  BUILD_TYPE=release
  export HEAPCHECK=normal
  # Workaround for gperftools issue #497
  export LD_BIND_NOW=1
elif [ "$BUILD_TYPE" = "COVERAGE" ]; then
  DO_COVERAGE=1
  BUILD_TYPE=debug
  cmake -DKUDU_GENERATE_COVERAGE=1 .
  # Reset coverage info from previous runs
  find src -name \*.gcda -o -name \*.gcno -exec rm {} \;
elif [ "$BUILD_TYPE" = "LINT" ]; then
  # Create empty test logs or else Jenkins fails to archive artifacts, which
  # results in the build failing.
  mkdir -p Testing/Temporary
  mkdir -p $TEST_LOGDIR

  cmake .
  make lint | tee $TEST_LOGDIR/lint.log
  exit $?
fi

# Only enable test core dumps for certain build types.
if [ "$BUILD_TYPE" != "ASAN" ]; then
  export KUDU_TEST_ULIMIT_CORE=unlimited
fi

# If we are supposed to be resistant to flaky tests, we need to fetch the
# list of tests to ignore
if [ "$KUDU_FLAKY_TEST_ATTEMPTS" -gt 1 ]; then
  echo Fetching flaky test list...
  export KUDU_FLAKY_TEST_LIST=$ROOT/build/flaky-tests.txt
  mkdir -p $(dirname $KUDU_FLAKY_TEST_LIST)
  echo -n > $KUDU_FLAKY_TEST_LIST
  if $ROOT/build-support/jenkins/determine-flaky-tests.py --list-tests-only \
    > $KUDU_FLAKY_TEST_LIST ; then
    echo Will retry flaky tests up to $KUDU_FLAKY_TEST_ATTEMPTS times:
    cat $KUDU_FLAKY_TEST_LIST
    echo ----------
  else
    echo Unable to fetch flaky test list. Disabling flaky test resistance.
    export KUDU_FLAKY_TEST_ATTEMPTS=1
  fi
fi

cmake . -DCMAKE_BUILD_TYPE=${BUILD_TYPE}

# our tests leave lots of data lying around, clean up before we run
make clean
rm -Rf /tmp/kudutest-$UID

# actually do the build
NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)
make -j$NUM_PROCS 2>&1 | tee build.log

# Run tests
export GTEST_OUTPUT="xml:$TEST_LOGDIR/" # Enable JUnit-compatible XML output.
if [ "$RUN_FLAKY_ONLY" == "1" ] ; then
  echo Running flaky tests only:
  $ROOT/build-support/jenkins/determine-flaky-tests.py -l | tee build/flaky-tests.txt
  test_regex=$(perl -e '
    chomp(my @lines = <>);
    print join("|", map { "^" . quotemeta($_) . "\$" } @lines);
   ' build/flaky-tests.txt)
  EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS -R $test_regex"

  # We don't support detecting java flaky tests at the moment.
  echo Disabling Java build since RUN_FLAKY_ONLY=1
  BUILD_JAVA=0
fi

ctest -j$NUM_PROCS $EXTRA_TEST_FLAGS

if [ "$DO_COVERAGE" == "1" ]; then
  echo Generating coverage report...
  ./thirdparty/gcovr-3.0/scripts/gcovr -r src/  -e '.*\.pb\..*' --xml > build/coverage.xml
fi

if [ "$BUILD_JAVA" == "1" ]; then
  # PATH=<build_output>:<thirdparty_stuff>:<toolchain_stuff>:$PATH
  export PATH=$(pwd)/build/latest/:$PATH
  pushd java
  export TSAN_OPTIONS="$TSAN_OPTIONS suppressions=$ROOT/build-support/tsan-suppressions.txt history_size=7"
  set -x
  mvn clean test
  set +x
  popd
fi

# Check that the heap checker actually worked. Need to temporarily remove
# -e to allow for failed commands.
set +e
if [ "$HEAPCHECK" = normal ]; then
  FAILED_TESTS=$(zgrep -L -- "WARNING: Perftools heap leak checker is active -- Performance may suffer" build/test-logs/*-test.txt*)
  if [ -n "$FAILED_TESTS" ]; then
    echo "Some tests didn't heap check properly:"
    for FTEST in $FAILED_TESTS; do
      echo $FTEST
    done
    exit 1
  else
    echo "All tests heap checked properly"
  fi
fi
set -e
