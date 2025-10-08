#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# This script is invoked from the Jenkins builds to build Kudu
# and run all the unit tests.
#
# Environment variables may be used to customize operation:
#   BUILD_TYPE: Default: DEBUG
#     Maybe be one of ASAN|TSAN|DEBUG|RELEASE|COVERAGE|LINT|IWYU|TIDY
#
#   KUDU_ALLOW_SLOW_TESTS   Default: 1
#     Runs the "slow" version of the unit tests. Set to 0 to
#     run the tests more quickly.
#
#   TEST_TMPDIR   Default: /tmp/$BUILD_TAG if BUILD_TAG is defined (Jenkins),
#                          /tmp/kudutest-$UID otherwise
#     Specifies the temporary directory where tests should write their data
#     and logs. It is expected that following successful completion
#     (i.e. no failures) of all the tests, this directory is empty: every
#     successfully completed test cleans up after itself. Failed tests leave
#     relevant data and logs in the temporary directory upon exit: that's for
#     post-mortem troubleshooting and debugging. In case of Jenkins builds
#     (i.e. when the BUILD_TAG environment variable is defined),
#     the whole TEST_TMPDIR directory is removed when this script exists.
#
#   RUN_FLAKY_ONLY    Default: 0
#     Only runs tests which have failed recently, if this is 1.
#     Used by the kudu-flaky-tests jenkins build.
#
#   KUDU_FLAKY_TEST_ATTEMPTS  Default: 1
#     If more than 1, will fetch the list of known flaky tests
#     from the jenkins jobs matching the "%jenkins-%" pattern, and allow those
#     tests to be flaky in this build.
#
#   TEST_RESULT_SERVER  Default: none
#     The host:port pair of a server running test_result_server.py.
#     This must be configured for flaky test resistance or test result
#     reporting to work.
#
#   ENABLE_DIST_TEST  Default: 0
#     If set to 1, will submit C++ and Java tests to be run by the
#     distributed test runner instead of running them locally.
#     This requires that $DIST_TEST_HOME be set to a working dist_test
#     checkout (and that dist_test itself be appropriately configured to
#     point to a cluster)
#
#   BUILD_JAVA        Default: 1
#     Build and test java code if this is set to 1.
#     Note: The Java code required for the C++ code and tests to run,
#     such as the HMS plugin and the Java subprocess, will still be built
#     even if set to 0.
#
#   BUILD_PYTHON       Default: 1
#     Build and test the Python wrapper of the client API.
#
#   BUILD_PYTHON3      Default: 1
#     Build and test the Python wrapper of the client API in Python. This
#     option is not mutually exclusive from BUILD_PYTHON. If both options
#     are set (default), then both will be run.
#
#   PIP_FLAGS  Default: ""
#     Extra flags which are passed to 'pip' when setting up the build
#     environment for the Python wrapper.
#
#   PIP_INSTALL_FLAGS  Default: ""
#     Extra flags which are passed to 'pip install' when setting up the build
#     environment for the Python wrapper. Python arguments are
#     context-dependent so sadly we can't reuse PIP_FLAGS here.
#
#   GRADLE_FLAGS       Default: ""
#     Extra flags which are passed to 'gradle' when running Gradle commands.
#
#   ERROR_ON_TEST_FAILURE    Default: 1
#     Whether test failures will cause this script to return an error.
#
#   KUDU_REPORT_TEST_RESULTS Default: 0
#     If non-zero, tests are reported to the central test server.
#
#   KUDU_ALLOW_SKIPPED_TESTS Default: 0
#     If set to 1, commits with changes that do not impact the build or tests exit early.
#     Additionally, commits with "DONT_BUILD" in the commit message will exit early.
#
#   PARALLEL    Default: number of available cores
#     Parallelism to use when compiling; by default is set to the number of
#     available cores as reported by `getconf _NPROCESSORS_ONLN`. The latter
#     might report total number of cores available to the _host_ OS in case of
#     containerized VM instances: for optimal results, it's useful to override
#     this variable to reflect actual restrictions.
#
#  PARALLEL_TESTS Default: the value of PARALLEL above
#     Parallelism to use when testing locally (not using dist-test).
#     By default this is set the PARALLEL described above.
#     This value can be important to set on resource constrained machines
#     running some of the more intense long running integration tests.

if [ "$KUDU_ALLOW_SKIPPED_TESTS" == "1" ]; then
  # If the commit only contains changes that do not impact the build or tests, exit immediately.
  # This check is conservative and attempts to have no false positives, but may have false negatives.
  if ! git diff-tree --no-commit-id --name-only -r HEAD |
     grep -qvE "^\.dockerignore|^\.gitignore|^\.ycm_extra_conf.py|^docker/|^docs/|^kubernetes/|LICENSE\.txt|NOTICE\.txt|.*\.adoc|.*\.md"; then
    echo
    echo ------------------------------------------------------------
    echo "*** Changes are only in files or directories that do not impact the build or tests. Exiting."
    echo ------------------------------------------------------------
    echo
    exit 0
  fi

  # If a commit messages contains a line that says 'DONT_BUILD', exit immediately.
  DONT_BUILD=$(git show|egrep '^\s{4}DONT_BUILD$')
  if [ "x$DONT_BUILD" != "x" ]; then
    echo
    echo ------------------------------------------------------------
    echo "*** Build not requested. Exiting."
    echo ------------------------------------------------------------
    echo
    exit 1
  fi
fi

set -e
# We pipe our build output to a log file with tee.
# This bash setting ensures that the script exits if the build fails.
set -o pipefail
# gather core dumps
ulimit -c unlimited

BUILD_TYPE=${BUILD_TYPE:-DEBUG}
BUILD_TYPE=$(echo "$BUILD_TYPE" | tr a-z A-Z) # capitalize
BUILD_TYPE_LOWER=$(echo "$BUILD_TYPE" | tr A-Z a-z)

# Set up defaults for environment variables.
DEFAULT_ALLOW_SLOW_TESTS=1

# TSAN builds are pretty slow, so don't do SLOW tests unless explicitly
# requested. Setting KUDU_USE_TSAN influences the thirdparty build.
if [ "$BUILD_TYPE" = "TSAN" ]; then
  DEFAULT_ALLOW_SLOW_TESTS=0
  export KUDU_USE_TSAN=1
fi

SOURCE_ROOT=$(cd $(dirname "$BASH_SOURCE")/../..; pwd)
BUILD_ROOT=$SOURCE_ROOT/build/$BUILD_TYPE_LOWER

export KUDU_FLAKY_TEST_ATTEMPTS=${KUDU_FLAKY_TEST_ATTEMPTS:-1}
export KUDU_ALLOW_SLOW_TESTS=${KUDU_ALLOW_SLOW_TESTS:-$DEFAULT_ALLOW_SLOW_TESTS}
export KUDU_COMPRESS_TEST_OUTPUT=${KUDU_COMPRESS_TEST_OUTPUT:-1}
if [ -n "$BUILD_TAG" ]; then
  export TEST_TMPDIR=${TEST_TMPDIR:-/tmp/$BUILD_TAG}
else
  export TEST_TMPDIR=${TEST_TMPDIR:-/tmp/kudutest-$UID}
fi
export PARALLEL=${PARALLEL:-$(getconf _NPROCESSORS_ONLN)}
export PARALLEL_TESTS=${PARALLEL_TESTS:-$PARALLEL}
export THIRDPARTY_DIR=${THIRDPARTY_DIR:-$SOURCE_ROOT/thirdparty}

BUILD_JAVA=${BUILD_JAVA:-1}
BUILD_GRADLE=${BUILD_GRADLE:-1}
BUILD_PYTHON=${BUILD_PYTHON:-1}
BUILD_PYTHON3=${BUILD_PYTHON3:-1}
ERROR_ON_TEST_FAILURE=${ERROR_ON_TEST_FAILURE:-1}

# Ensure that the test data directory is usable.
mkdir -p "$TEST_TMPDIR"
if [ ! -w "$TEST_TMPDIR" ]; then
  echo "Error: Test output directory ($TEST_TMPDIR) is not writable on $(hostname) by user $(whoami)"
  exit 1
fi

# Remove testing artifacts from the previous run before we do anything
# else. Otherwise, if we fail during the "build" step, Jenkins will
# archive the test logs from the previous run, thinking they came from
# this run, and confuse us when we look at the failed build.
rm -rf $BUILD_ROOT
mkdir -p $BUILD_ROOT

# Same for the Java tests, which aren't inside BUILD_ROOT
rm -rf $SOURCE_ROOT/java/*/build

# The build_pattern is %jenkins-% because we are interested in two types of runs:
# 1. As of now build_and_test pipeline job which is triggered by the pre-commit pipeline job.
# 2. Any other job which is used to run the flaky tests only.
list_flaky_tests() {
  local url="http://$TEST_RESULT_SERVER/list_failed_tests?num_days=3&build_pattern=%25jenkins-%25"
  >&2 echo Fetching flaky test list from "$url" ...
  curl -s --show-error "$url"
  return $?
}

TEST_LOGDIR="$BUILD_ROOT/test-logs"
TEST_DEBUGDIR="$BUILD_ROOT/test-debug"

cleanup() {
  echo Cleaning up all build artifacts and temporary data...
  $SOURCE_ROOT/build-support/jenkins/post-build-clean.sh
}
# If we're running inside Jenkins (the BUILD_TAG is set), then install
# an exit handler which will clean up all of our build results and temporary
# data.
if [ -n "$BUILD_TAG" ]; then
  trap cleanup EXIT
fi

ARTIFACT_ARCH=$(uname -m)
# Configure the build
#
# ASAN/TSAN can't build the Python bindings because the exported Kudu client
# library (which the bindings depend on) is missing ASAN/TSAN symbols.
if [ "$BUILD_TYPE" = "ASAN" ]; then
  USE_CLANG=1
  CMAKE_BUILD=fastdebug
  EXTRA_BUILD_FLAGS="-DKUDU_USE_ASAN=1 -DKUDU_USE_UBSAN=1"
  # workaround for github.com/google/sanitizers/issues/1208
  # ASAN with dynamic linking cause all tests fail on aarch64,
  # we don't apply ENABLE_DIST_TEST on aarch64, so use static linking.
  if [ "$ARTIFACT_ARCH" = "aarch64" ]; then
    EXTRA_BUILD_FLAGS="$EXTRA_BUILD_FLAGS -DKUDU_LINK=static"
  fi
  BUILD_PYTHON=0
  BUILD_PYTHON3=0
elif [ "$BUILD_TYPE" = "TSAN" ]; then
  USE_CLANG=1
  CMAKE_BUILD=fastdebug
  EXTRA_BUILD_FLAGS="-DKUDU_USE_TSAN=1"
  EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS -LE no_tsan"
  BUILD_PYTHON=0
  BUILD_PYTHON3=0
elif [ "$BUILD_TYPE" = "COVERAGE" ]; then
  USE_CLANG=1
  CMAKE_BUILD=debug
  EXTRA_BUILD_FLAGS="-DKUDU_GENERATE_COVERAGE=1"
  DO_COVERAGE=1
  BUILD_JAVA=1

  # We currently dont capture coverage for Python.
  BUILD_PYTHON=0
  BUILD_PYTHON3=0
elif [ "$BUILD_TYPE" = "LINT" ]; then
  CMAKE_BUILD=debug
elif [ "$BUILD_TYPE" = "IWYU" ]; then
  USE_CLANG=1
  CMAKE_BUILD=debug
elif [ "$BUILD_TYPE" = "TIDY" ]; then
  USE_CLANG=1
  CMAKE_BUILD=debug
  BUILD_JAVA=0
  BUILD_PYTHON=0
  BUILD_PYTHON3=0
  BUILD_GRADLE=0
else
  # Must be DEBUG or RELEASE
  CMAKE_BUILD=$BUILD_TYPE
fi

# If we are supposed to be resistant to flaky tests or run just flaky tests,
# we need to fetch the list.
if [ "$RUN_FLAKY_ONLY" == "1" -o "$KUDU_FLAKY_TEST_ATTEMPTS" -gt 1 ]; then
  export KUDU_FLAKY_TEST_LIST=$BUILD_ROOT/flaky-tests.txt
  mkdir -p $(dirname $KUDU_FLAKY_TEST_LIST)
  if list_flaky_tests > $KUDU_FLAKY_TEST_LIST; then
    echo The list of flaky tests:
    echo ------------------------------------------------------------
    cat $KUDU_FLAKY_TEST_LIST
    echo
    echo ------------------------------------------------------------
  else
    echo "Could not fetch flaky tests list."
    if [ "$RUN_FLAKY_ONLY" == "1" ]; then
      exit 1
    fi

    echo No list of flaky tests, disabling the flaky test resistance.
    export KUDU_FLAKY_TEST_ATTEMPTS=1
  fi

  if [ "$RUN_FLAKY_ONLY" == "1" ]; then
    test_regex=$(perl -e '
      chomp(my @lines = <>);
      print join("|", map { "^" . quotemeta($_) } @lines);
     ' $KUDU_FLAKY_TEST_LIST)
    if [ -z "$test_regex" ]; then
      echo No tests are flaky.
      exit 0
    fi

    # Set up ctest/gradle to run only those tests found in the flaky test list.
    #
    # Note: the flaky test list contains both C++ and Java tests and we pass it
    # in its entirety to both ctest and gradle. This is safe because:
    # 1. There are no test name collisions between C++ and Java tests.
    # 2. Both ctest and gradle will happily ignore tests they can't find.
    #
    # If either of these assumptions changes, we'll need to explicitly split the
    # test list into two lists, either here or in the test result server.
    EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS -R $test_regex"
    while IFS="" read t || [ -n "$t" ]
    do
      EXTRA_GRADLE_TEST_FLAGS="--tests $t $EXTRA_GRADLE_TEST_FLAGS"
    done < $KUDU_FLAKY_TEST_LIST

    # We don't support detecting python flaky tests at the moment.
    echo "RUN_FLAKY_ONLY=1: running flaky tests only, disabling python build."
    BUILD_PYTHON=0
    BUILD_PYTHON3=0
  elif [ "$KUDU_FLAKY_TEST_ATTEMPTS" -gt 1 ]; then
    echo Will retry the flaky tests up to $KUDU_FLAKY_TEST_ATTEMPTS times.
  fi
fi

THIRDPARTY_TYPE=
if [ "$BUILD_TYPE" = "TSAN" ]; then
  THIRDPARTY_TYPE=tsan
fi

if [ ! -n "${NO_REBUILD_THIRDPARTY}" ]; then
  # The settings for PARALLEL (see above) also affects the thirdparty build.
  $SOURCE_ROOT/build-support/enable_devtoolset.sh $THIRDPARTY_DIR/build-if-necessary.sh $THIRDPARTY_TYPE
fi

THIRDPARTY_BIN=$THIRDPARTY_DIR/installed/common/bin
export PPROF_PATH=$THIRDPARTY_BIN/pprof

if which ccache >/dev/null ; then
  CLANG=$(pwd)/build-support/ccache-clang/clang
else
  CLANG=$THIRDPARTY_DIR/clang-toolchain/bin/clang
fi

# Make sure we use JDK8
if [ -n "$JAVA8_HOME" ]; then
  export JAVA_HOME="$JAVA8_HOME"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

# Some portions of the C++ build may depend on Java code, so we may run Gradle
# while building. Pass in some flags suitable for automated builds; these will
# also be used in the Java build.
# These should be set before CMAKE so that the Gradle command in the
# generated make file has the correct flags.
export EXTRA_GRADLE_FLAGS="--console=plain"
EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS --no-daemon"
EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS --continue"
EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS --build-cache"
# KUDU-2524: temporarily disable scalafmt until we can work out its JDK
# incompatibility issue.
EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS -DskipFormat"
EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS $GRADLE_FLAGS"

# Assemble the cmake command line, starting with environment variables.

# There's absolutely no reason to rebuild the thirdparty tree; we just ran
# build-if-necessary.sh above.
CMAKE="env NO_REBUILD_THIRDPARTY=1"

# If using clang, we have to change the compiler via environment variables.
if [ -n "$USE_CLANG" ]; then
  CMAKE="$CMAKE CC=$CLANG CXX=$CLANG++"
fi

# This will be a passthrough for systems without devtoolset.
CMAKE="$CMAKE $SOURCE_ROOT/build-support/enable_devtoolset.sh"

CMAKE="$CMAKE $THIRDPARTY_BIN/cmake"
CMAKE="$CMAKE -DCMAKE_BUILD_TYPE=$CMAKE_BUILD"

# On distributed tests, force dynamic linking even for release builds. Otherwise,
# the test binaries are too large and we spend way too much time uploading them
# to the test slaves.
if [ "$ENABLE_DIST_TEST" == "1" ]; then
  CMAKE="$CMAKE -DKUDU_LINK=dynamic"
fi
if [ -n "$EXTRA_BUILD_FLAGS" ]; then
  CMAKE="$CMAKE $EXTRA_BUILD_FLAGS"
fi
CMAKE="$CMAKE $SOURCE_ROOT"
cd $BUILD_ROOT
$CMAKE

# Create empty test logs or else Jenkins fails to archive artifacts, which
# results in the build failing.
mkdir -p Testing/Temporary
mkdir -p $TEST_LOGDIR

if [ -n "$KUDU_REPORT_TEST_RESULTS" ] && [ "$KUDU_REPORT_TEST_RESULTS" -ne 0 ]; then
  # Export environment variables needed for flaky test reporting.
  #
  # The actual reporting happens in the test runners themselves.

  # On Jenkins, we'll have this variable set. Otherwise,
  # report the build tag as non-jenkins.
  export BUILD_TAG=${BUILD_TAG:-non-jenkins}

  # Figure out the current git revision, and append a "-dirty" tag if it's
  # not a pristine checkout.
  GIT_REVISION=$(cd $SOURCE_ROOT && git rev-parse HEAD)
  if ! ( cd $SOURCE_ROOT && git diff --quiet .  && git diff --cached --quiet . ) ; then
    GIT_REVISION="${GIT_REVISION}-dirty"
  fi
  export GIT_REVISION

  # Parse out our "build config" - a space-separated list of tags
  # which include the cmake build type as well as the list of configured
  # sanitizers.

  # Define BUILD_CONFIG for flaky test reporting.
  BUILD_CONFIG="$CMAKE_BUILD"
  if [ "$BUILD_TYPE" = "ASAN" ]; then
    BUILD_CONFIG="$BUILD_CONFIG asan ubsan"
  elif [ "$BUILD_TYPE" = "TSAN" ]; then
    BUILD_CONFIG="$BUILD_CONFIG tsan"
  fi
  export BUILD_CONFIG
fi

# Short circuit for LINT builds.
if [ "$BUILD_TYPE" = "LINT" ]; then
  LINT_FAILURES=""
  LINT_RESULT=0

  if [ "$BUILD_JAVA" == "1" ]; then
    # Run Java static code analysis via Gradle.
    pushd $SOURCE_ROOT/java
    if ! ./gradlew $EXTRA_GRADLE_FLAGS clean check -x test $EXTRA_GRADLE_TEST_FLAGS; then
      LINT_RESULT=1
      LINT_FAILURES="$LINT_FAILURES"$'Java Gradle check failed\n'
    fi

    # Make sure every module generates JARs, that we need to verify.
    if ! ./gradlew $EXTRA_GRADLE_FLAGS assemble; then
      LINT_RESULT=1
      LINT_FAILURES="$LINT_FAILURES"$'Java Gradle assemble failed\n'
    fi

    # Verify the contents of the JARs to ensure the shading and
    # packaging is correct.
    if ! $SOURCE_ROOT/build-support/verify_jars.pl .; then
      LINT_RESULT=1
      LINT_FAILURES="$LINT_FAILURES"$'Java verify Jars check failed\n'
    fi
    popd
  fi

  if ! make lint | tee $TEST_LOGDIR/lint.log; then
    LINT_RESULT=1
    LINT_FAILURES="$LINT_FAILURES"$'make lint failed\n'
  fi

  if [ -n "$LINT_FAILURES" ]; then
    echo
    echo
    echo ======================================================================
    echo Lint Failure summary
    echo ======================================================================
    echo $LINT_FAILURES
    echo
    echo
  fi

  exit $LINT_RESULT
fi

# Short circuit for IWYU builds: run the include-what-you-use tool on the files
# modified since the last committed changelist committed upstream.
if [ "$BUILD_TYPE" = "IWYU" ]; then
  make iwyu | tee $TEST_LOGDIR/iwyu.log
  exit $?
fi

# Short circuit for TIDY builds: run the clang-tidy tool on the C++ source
# files in the HEAD revision for the gerrit branch.
if [ "$BUILD_TYPE" = "TIDY" ]; then
  make -j$PARALLEL generated-headers 2>&1 | tee $TEST_LOGDIR/tidy.log
  $SOURCE_ROOT/build-support/clang_tidy_gerrit.py HEAD 2>&1 | \
      tee -a $TEST_LOGDIR/tidy.log
  exit $?
fi

# Only enable test core dumps for certain build types.
if [ "$BUILD_TYPE" != "ASAN" ]; then
  export KUDU_TEST_ULIMIT_CORE=unlimited
fi

# actually do the build
echo
echo Building C++ code.
echo ------------------------------------------------------------
make -j$PARALLEL 2>&1 | tee build.log

# If compilation succeeds, try to run all remaining steps despite any failures.
set +e

TESTS_FAILED=0
EXIT_STATUS=0
FAILURES=""

# If we're running distributed C++ tests, submit them asynchronously while
# we run any local tests.
if [ "$ENABLE_DIST_TEST" == "1" ]; then
  echo
  echo Submitting C++ distributed-test job.
  echo ------------------------------------------------------------
  # dist-test uses DIST_TEST_JOB_PATH to define where to output it's id file.
  export DIST_TEST_JOB_PATH=$BUILD_ROOT/c-dist-test-job-id
  rm -f $DIST_TEST_JOB_PATH
  if ! $SOURCE_ROOT/build-support/dist_test.py --no-wait run ; then
    EXIT_STATUS=1
    FAILURES="$FAILURES"$'Could not submit C++ distributed test job\n'
  fi
  # Still need to run a few non-dist-test-capable tests locally.
  EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS -L no_dist_test"
fi

# If we are running coverage, we are running all the tests with ctest on the build host.
# We can decrease coverage variance caused by flaky test failures by retrying failed tests.
if [ "$DO_COVERAGE" == "1" ]; then
  EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS --repeat until-pass:3"
fi

if ! $THIRDPARTY_BIN/ctest -j$PARALLEL_TESTS $EXTRA_TEST_FLAGS ; then
  TESTS_FAILED=1
  FAILURES="$FAILURES"$'C++ tests failed\n'
fi

if [ "$DO_COVERAGE" == "1" ]; then

  # Create a directory for the coverage report files.
  COVERAGE_REPORT_DIR=$BUILD_ROOT/coverage_report
  mkdir -p $COVERAGE_REPORT_DIR
  # gcovr requires us to be in the source root.
  pushd $SOURCE_ROOT
  echo
  echo Generating coverage report...
  echo ------------------------------------------------------------
  if $THIRDPARTY_DIR/installed/common/bin/gcovr \
      -r $SOURCE_ROOT \
      --gcov-filter='.*#src#kudu#.*' \
      --exclude='.*\.pb\.(h|cc)$' \
      --exclude='.*test\.cc$' \
      --exclude='.*test-base\.(h|cc)$' \
      --exclude='.*/src/kudu/test-util/.*' \
      --exclude='.*/src/kudu/benchmarks/.*' \
      --exclude='.*/src/kudu/gutil/.*' \
      --exclude='.*/src/kudu/integration-tests/.*' \
      --gcov-executable=$SOURCE_ROOT/build-support/llvm-gcov-wrapper \
      -o $COVERAGE_REPORT_DIR/report.html \
      --html --html-details --verbose; then
    virtualenv -p python3 $BUILD_ROOT/py_env
    source $BUILD_ROOT/py_env/bin/activate
    pip install bs4
    if ! python $SOURCE_ROOT/build-support/process_gcovr_report.py $COVERAGE_REPORT_DIR; then
      EXIT_STATUS=1
      FAILURES="$FAILURES"$'gcovr report processing failed\n'
    fi
    deactivate
    rm -Rf $BUILD_ROOT/py_env
  else
    EXIT_STATUS=1
    FAILURES="$FAILURES"$'Coverage report failed\n'
  fi

  popd
fi

if [ "$BUILD_JAVA" == "1" ]; then
  echo
  echo Building and testing java...
  echo ------------------------------------------------------------

  pushd $SOURCE_ROOT/java
  set -x

  # Run the full Gradle build.
  # If we're running distributed Java tests, submit them asynchronously.
  if [ "$ENABLE_DIST_TEST" == "1" ]; then
    if ! ./gradlew $EXTRA_GRADLE_FLAGS clean assemble; then
      TESTS_FAILED=1
      FAILURES="$FAILURES"$'Java Gradle build failed\n'
    fi
    echo
    echo Submitting Java distributed-test job.
    echo ------------------------------------------------------------
    # dist-test uses DIST_TEST_JOB_PATH to define where to output it's id file.
    export DIST_TEST_JOB_PATH=$BUILD_ROOT/java-dist-test-job-id
    rm -f $DIST_TEST_JOB_PATH
    if ! $SOURCE_ROOT/build-support/dist_test.py --no-wait java run-all ; then
      EXIT_STATUS=1
      FAILURES="$FAILURES"$'Could not submit Java distributed test job\n'
    fi
  else
    if [ "$DO_COVERAGE" == "1" ]; then
      # Clean previous report results
      rm -rf ./build

      # jacocoAggregatedReport will trigger test execution if necessary.
      if ! ./gradlew $EXTRA_GRADLE_FLAGS clean jacocoAggregatedReport; then
        TESTS_FAILED=1
        EXIT_STATUS=1
        FAILURES="$FAILURES"$'Java Gradle test/coverage aggregation failed\n'
      fi

      if ! $SOURCE_ROOT/build-support/process_jacoco_report.sh; then
        EXIT_STATUS=1
        FAILURES="$FAILURES"$'Java Jacoco report processing failed\n'
      fi
    else
      if ! ./gradlew $EXTRA_GRADLE_FLAGS clean test $EXTRA_GRADLE_TEST_FLAGS; then
        TESTS_FAILED=1
        FAILURES="$FAILURES"$'Java Gradle build/test failed\n'
      fi
    fi
  fi
  set +x
  popd
fi


if [ "$BUILD_PYTHON" == "1" ]; then
  echo
  echo Building and testing python.
  echo ------------------------------------------------------------

  # Failing to compile the Python client should result in a build failure.
  set -e
  export KUDU_HOME=$SOURCE_ROOT
  export KUDU_BUILD=$BUILD_ROOT
  pushd $SOURCE_ROOT/python

  # Create a sane test environment.
  rm -Rf $KUDU_BUILD/py_env
  virtualenv $KUDU_BUILD/py_env
  source $KUDU_BUILD/py_env/bin/activate

  pip $PIP_FLAGS install $PIP_INSTALL_FLAGS -r requirements_dev.txt

  # Delete old Cython extensions to force them to be rebuilt.
  rm -Rf build kudu_python.egg-info kudu/*.so

  # Build the Python bindings. This assumes we run this script from base dir.
  CC=$CLANG CXX=$CLANG++ python setup.py build_ext

  # A testing environment might have HTTP/HTTPS proxy configured to proxy
  # requests even for 127.0.0.0/8 network or other quirks. Since some Python
  # Kudu tests rely on Kudu components listening on 127.0.0.0/8 addresses,
  # let's unset environment variables that might affect behavior of the
  # Python libraries like urllib/urllib2.
  unset ALL_PROXY
  unset HTTP_PROXY
  unset HTTPS_PROXY
  unset http_proxy
  unset https_proxy

  # Run the Python tests. This may also involve some compiler work.
  set +e
  if ! CC=$CLANG CXX=$CLANG++ python setup.py test \
      --addopts="kudu --junit-xml=$TEST_LOGDIR/python_client.xml" \
      2> $TEST_LOGDIR/python_client.log ; then
    TESTS_FAILED=1
    FAILURES="$FAILURES"$'Python tests failed\n'
  fi

  # Run the Python examples tests
  if ! ./test-python-examples.sh python2 ; then
    TESTS_FAILED=1
    FAILURES="$FAILURES"$'Python examples tests failed\n'
  fi

  deactivate
  popd
fi

if [ "$BUILD_PYTHON3" == "1" ]; then
  echo
  echo Building and testing python 3.
  echo ------------------------------------------------------------

  # Failing to compile the Python client should result in a build failure.
  set -e
  export KUDU_HOME=$SOURCE_ROOT
  export KUDU_BUILD=$BUILD_ROOT
  pushd $SOURCE_ROOT/python

  # Create a sane test environment.
  rm -Rf $KUDU_BUILD/py_env
  virtualenv -p python3 $KUDU_BUILD/py_env
  source $KUDU_BUILD/py_env/bin/activate


  pip $PIP_FLAGS install $PIP_INSTALL_FLAGS -r requirements_dev.txt

  # Delete old Cython extensions to force them to be rebuilt.
  rm -Rf build kudu_python.egg-info kudu/*.so

  # Build the Python bindings. This assumes we run this script from base dir.
  CC=$CLANG CXX=$CLANG++ python setup.py build_ext
  set +e

  # A testing environment might have HTTP/HTTPS proxy configured to proxy
  # requests even for 127.0.0.0/8 network or other quirks. Since some Python
  # Kudu tests rely on Kudu components listening on 127.0.0.0/8 addresses,
  # let's unset environment variables that might affect behavior of the
  # Python libraries like urllib/urllib2.
  unset ALL_PROXY
  unset HTTP_PROXY
  unset HTTPS_PROXY
  unset http_proxy
  unset https_proxy

  # Run the Python tests. This may also involve some compiler work.
  if ! CC=$CLANG CXX=$CLANG++ python setup.py test \
      --addopts="kudu --junit-xml=$TEST_LOGDIR/python3_client.xml" \
      2> $TEST_LOGDIR/python3_client.log ; then
    TESTS_FAILED=1
    FAILURES="$FAILURES"$'Python 3 tests failed\n'
  fi

  # Run the Python examples tests
  if ! ./test-python-examples.sh python3 ; then
    TESTS_FAILED=1
    FAILURES="$FAILURES"$'Python 3 examples tests failed\n'
  fi

  deactivate
  popd
fi

# If we submitted the tasks earlier, go fetch the results now
if [ "$ENABLE_DIST_TEST" == "1" ]; then
  echo
  echo Fetching previously submitted C++ dist-test results...
  echo ------------------------------------------------------------
  C_DIST_TEST_ID=`cat $BUILD_ROOT/c-dist-test-job-id`
  if ! $DIST_TEST_HOME/bin/client watch $C_DIST_TEST_ID ; then
    TESTS_FAILED=1
    FAILURES="$FAILURES"$'Distributed C++ tests failed\n'
  fi
  DT_DIR=$TEST_LOGDIR/dist-test-out
  rm -Rf $DT_DIR
  $DIST_TEST_HOME/bin/client fetch --artifacts -d $DT_DIR $C_DIST_TEST_ID
  # Fetching the artifacts expands each log into its own directory.
  # Move them back into the main log directory
  rm -f $DT_DIR/*zip
  for arch_dir in $DT_DIR/* ; do
    mv $arch_dir/build/$BUILD_TYPE_LOWER/test-logs/* $TEST_LOGDIR
    rm -Rf $arch_dir
  done

  echo
  echo Fetching previously submitted Java dist-test results...
  echo ------------------------------------------------------------
  JAVA_DIST_TEST_ID=`cat $BUILD_ROOT/java-dist-test-job-id`
  if ! $DIST_TEST_HOME/bin/client watch $JAVA_DIST_TEST_ID ; then
    TESTS_FAILED=1
    FAILURES="$FAILURES"$'Distributed Java tests failed\n'
  fi
  DT_DIR=$TEST_LOGDIR/java-dist-test-out
  rm -Rf $DT_DIR
  $DIST_TEST_HOME/bin/client fetch --artifacts -d $DT_DIR $JAVA_DIST_TEST_ID
  # Fetching the artifacts expands each log into its own directory.
  # Move them back into the main log directory
  rm -f $DT_DIR/*zip
  for arch_dir in $DT_DIR/* ; do
    mv $arch_dir/build/java/test-logs/* $TEST_LOGDIR
    rm -Rf $arch_dir
  done
fi

if [ "$TESTS_FAILED" != "0" -o "$EXIT_STATUS" != "0" ]; then
  echo
  echo Tests failed, making sure we have XML files for all tests.
  echo ------------------------------------------------------------

  # Tests that crash do not generate JUnit report XML files.
  # We go through and generate a kind of poor-man's version of them in those cases.
  for GTEST_OUTFILE in $TEST_LOGDIR/*.txt.gz; do
    TEST_EXE=$(basename $GTEST_OUTFILE .txt.gz)
    GTEST_XMLFILE="$TEST_LOGDIR/$TEST_EXE.xml"
    if [ ! -f "$GTEST_XMLFILE" ]; then
      echo "JUnit report missing:" \
           "generating fake JUnit report file from $GTEST_OUTFILE and saving it to $GTEST_XMLFILE"
      zcat $GTEST_OUTFILE | $SOURCE_ROOT/build-support/parse_test_failure.py -x > $GTEST_XMLFILE
    fi
  done
else
  # If all tests passed, ensure that they cleaned up their test output.
  TEST_TMPDIR_CONTENTS=$(ls $TEST_TMPDIR)
  if [ -n "$TEST_TMPDIR_CONTENTS" ]; then
    echo "All tests passed, yet some left behind their test output."
    echo "TEST_TMPDIR: $TEST_TMPDIR"
    find $TEST_TMPDIR -ls
    EXIT_STATUS=1
  fi
fi

set -e

if [ -n "$FAILURES" ]; then
  echo
  echo
  echo ======================================================================
  echo Failure summary
  echo ======================================================================
  echo $FAILURES
  echo
  echo
fi

# If any of the tests failed and we are honoring test failures, set the exit
# status to 1. Note that it may have already been set to 1 above.
if [ "$ERROR_ON_TEST_FAILURE" == "1" -a "$TESTS_FAILED" == "1" ]; then
  EXIT_STATUS=1
fi

exit $EXIT_STATUS
