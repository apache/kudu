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
#   LLVM_DIR
#     Path in which to find bin/clang and bin/clang++

# If a commit messages contains a line that says 'DONT_BUILD', exit
# immediately.
DONT_BUILD=$(git show|grep '^DONT_BUILD$')
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

export KUDU_ALLOW_SLOW_TESTS=${KUDU_ALLOW_SLOW_TESTS:-$DEFAULT_ALLOW_SLOW_TESTS}
LLVM_DIR=${LLVM_DIR:-/opt/toolchain/llvm-3.3/}
export KUDU_COMPRESS_TEST_OUTPUT=${KUDU_COMPRESS_TEST_OUTPUT:-1}
export TOOLCHAIN=/mnt/toolchain/toolchain.sh

if [ ! -d $LLVM_DIR ]; then
  echo "No LLVM found ($LLVM_DIR does not exist)"
  echo "Set LLVM_DIR to the prefix for clang"
  exit 1
fi

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

# Remove testing artifacts from the previous run before we do anything
# else. Otherwise, if we fail during the "build" step, Jenkins will
# archive the test logs from the previous run, thinking they came from
# this run, and confuse us when we look at the failed build.
rm -Rf Testing/Temporary
rm -f build.log

thirdparty/build-if-necessary.sh

export PATH=$(pwd)/thirdparty/installed/bin:$PATH
export PPROF_PATH=$(pwd)/thirdparty/installed/bin/pprof

rm -rf CMakeCache.txt CMakeFiles src/*/CMakeFiles


if [ "$BUILD_TYPE" = "ASAN" ]; then
  # NB: passing just "clang++" below causes an infinite loop, see http://www.cmake.org/pipermail/cmake/2012-December/053071.html
  CC=$LLVM_DIR/bin/clang CXX=$LLVM_DIR/bin/clang++ cmake -DKUDU_USE_ASAN=1 -DKUDU_USE_UBSAN=1 .
  BUILD_TYPE=fastdebug
elif [ "$BUILD_TYPE" = "TSAN" ]; then
  CC=$LLVM_DIR/bin/clang CXX=$LLVM_DIR/bin/clang++ cmake -DKUDU_USE_TSAN=1
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
  mkdir -p build/test-logs

  cmake .
  make lint | tee build/test-logs/lint.log
  exit $?
fi

cmake . -DCMAKE_BUILD_TYPE=${BUILD_TYPE}
make clean

# our tests leave lots of data lying around, clean up before we run
rm -Rf /tmp/kudutest-$UID
NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)

make -j$NUM_PROCS 2>&1 | tee build.log

ctest -j$NUM_PROCS $EXTRA_TEST_FLAGS

if [ "$DO_COVERAGE" == "1" ]; then
  echo Generating coverage report...
  ./thirdparty/gcovr-3.0/scripts/gcovr -r src/  -e '.*\.pb\..*' --xml > build/coverage.xml
fi

export PATH=$(pwd)/build/latest/:$PATH
if [ -f "$TOOLCHAIN" ]; then
  source $TOOLCHAIN
fi
cd java
./kudu-client/dev-support/build-proto.sh
mvn clean test
