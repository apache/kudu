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
#     Maybe be one of ASAN|TSAN|DEBUG|RELEASE|COVERAGE|LINT|IWYU
#
#   KUDU_ALLOW_SLOW_TESTS   Default: 1
#     Runs the "slow" version of the unit tests. Set to 0 to
#     run the tests more quickly.
#
#   TEST_TMPDIR   Default: /tmp/kudutest-$UID
#     Specifies the temporary directory where tests should write their
#     data. It is expected that following the completion of all tests, this
#     directory is empty (i.e. every test cleaned up after itself).
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
#   TEST_RESULT_SERVER  Default: none
#     The host:port pair of a server running test_result_server.py.
#     This must be configured for flaky test resistance or test result
#     reporting to work.
#
#   ENABLE_DIST_TEST  Default: 0
#     If set to 1, will submit C++ tests to be run by the distributed
#     test runner instead of running them locally. This requires that
#     $DIST_TEST_HOME be set to a working dist_test checkout (and that
#     dist_test itself be appropriately configured to point to a cluster)
#
#   BUILD_JAVA        Default: 1
#     Build and test java code if this is set to 1.
#
#   BUILD_MAVEN       Default: 0
#     When building java code, build with Maven if this is set to 1.
#
#   BUILD_GRADLE      Default: 1
#     When building java code, build with Gradle if this is set to 1.
#
#   BUILD_PYTHON       Default: 1
#     Build and test the Python wrapper of the client API.
#
#   BUILD_PYTHON3      Default: 1
#     Build and test the Python wrapper of the client API in Python. This
#     option is not mutually exclusive from BUILD_PYTHON. If both options
#     are set (default), then both will be run.
#
#   PIP_INSTALL_FLAGS  Default: ""
#     Extra flags which are passed to 'pip install' when setting up the build
#     environment for the Python wrapper.
#
#   MVN_FLAGS          Default: ""
#     Extra flags which are passed to 'mvn' when building and running Java
#     tests. This can be useful, for example, to choose a different maven
#     repository location.
#
#   GRADLE_FLAGS       Default: ""
#     Extra flags which are passed to 'gradle' when running Gradle commands.

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
BUILD_TYPE_LOWER=$(echo "$BUILD_TYPE" | tr A-Z a-z)

# Set up defaults for environment variables.
DEFAULT_ALLOW_SLOW_TESTS=1

# TSAN builds are pretty slow, so don't do SLOW tests unless explicitly
# requested. Setting KUDU_USE_TSAN influences the thirdparty build.
if [ "$BUILD_TYPE" = "TSAN" ]; then
  DEFAULT_ALLOW_SLOW_TESTS=0
  export KUDU_USE_TSAN=1
fi

export KUDU_FLAKY_TEST_ATTEMPTS=${KUDU_FLAKY_TEST_ATTEMPTS:-1}
export KUDU_ALLOW_SLOW_TESTS=${KUDU_ALLOW_SLOW_TESTS:-$DEFAULT_ALLOW_SLOW_TESTS}
export KUDU_COMPRESS_TEST_OUTPUT=${KUDU_COMPRESS_TEST_OUTPUT:-1}
export TEST_TMPDIR=${TEST_TMPDIR:-/tmp/kudutest-$UID}
BUILD_JAVA=${BUILD_JAVA:-1}
BUILD_MAVEN=${BUILD_MAVEN:-0}
BUILD_GRADLE=${BUILD_GRADLE:-1}
BUILD_PYTHON=${BUILD_PYTHON:-1}
BUILD_PYTHON3=${BUILD_PYTHON3:-1}

# Ensure that the test data directory is usable.
mkdir -p "$TEST_TMPDIR"
if [ ! -w "$TEST_TMPDIR" ]; then
  echo "Error: Test output directory ($TEST_TMPDIR) is not writable on $(hostname) by user $(whoami)"
  exit 1
fi

SOURCE_ROOT=$(cd $(dirname "$BASH_SOURCE")/../..; pwd)
BUILD_ROOT=$SOURCE_ROOT/build/$BUILD_TYPE_LOWER

# Remove testing artifacts from the previous run before we do anything
# else. Otherwise, if we fail during the "build" step, Jenkins will
# archive the test logs from the previous run, thinking they came from
# this run, and confuse us when we look at the failed build.
rm -rf $BUILD_ROOT
mkdir -p $BUILD_ROOT

# Same for the Java tests, which aren't inside BUILD_ROOT
rm -rf $SOURCE_ROOT/java/*/target
rm -rf $SOURCE_ROOT/java/*/build

list_flaky_tests() {
  local url="http://$TEST_RESULT_SERVER/list_failed_tests?num_days=3&build_pattern=%25kudu-test%25"
  >&2 echo Fetching flaky test list from "$url" ...
  curl -s --show-error "$url"
  return $?
}

TEST_LOGDIR="$BUILD_ROOT/test-logs"
TEST_DEBUGDIR="$BUILD_ROOT/test-debug"

cleanup() {
  echo Cleaning up all build artifacts...
  $SOURCE_ROOT/build-support/jenkins/post-build-clean.sh
}
# If we're running inside Jenkins (the BUILD_ID is set), then install
# an exit handler which will clean up all of our build results.
if [ -n "$BUILD_ID" ]; then
  trap cleanup EXIT
fi

export TOOLCHAIN_DIR=/opt/toolchain
if [ -d "$TOOLCHAIN_DIR" ]; then
  PATH=$TOOLCHAIN_DIR/apache-maven-3.0/bin:$PATH
fi

THIRDPARTY_TYPE=
if [ "$BUILD_TYPE" = "TSAN" ]; then
  THIRDPARTY_TYPE=tsan
fi
$SOURCE_ROOT/build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh $THIRDPARTY_TYPE

THIRDPARTY_BIN=$(pwd)/thirdparty/installed/common/bin
export PPROF_PATH=$THIRDPARTY_BIN/pprof

if which ccache >/dev/null ; then
  CLANG=$(pwd)/build-support/ccache-clang/clang
else
  CLANG=$(pwd)/thirdparty/clang-toolchain/bin/clang
fi

# Configure the build
#
# ASAN/TSAN can't build the Python bindings because the exported Kudu client
# library (which the bindings depend on) is missing ASAN/TSAN symbols.
cd $BUILD_ROOT
if [ "$BUILD_TYPE" = "ASAN" ]; then
  USE_CLANG=1
  CMAKE_BUILD=fastdebug
  EXTRA_BUILD_FLAGS="-DKUDU_USE_ASAN=1 -DKUDU_USE_UBSAN=1"
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

  # We currently dont capture coverage for Java or Python.
  BUILD_JAVA=0
  BUILD_PYTHON=0
  BUILD_PYTHON3=0
elif [ "$BUILD_TYPE" = "LINT" ]; then
  CMAKE_BUILD=debug
elif [ "$BUILD_TYPE" = "IWYU" ]; then
  USE_CLANG=1
  CMAKE_BUILD=debug
else
  # Must be DEBUG or RELEASE
  CMAKE_BUILD=$BUILD_TYPE
fi

# Assemble the cmake command line.
CMAKE=
if [ -n "$USE_CLANG" ]; then
  CMAKE="env CC=$CLANG CXX=$CLANG++"
fi
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
$CMAKE

# Create empty test logs or else Jenkins fails to archive artifacts, which
# results in the build failing.
mkdir -p Testing/Temporary
mkdir -p $TEST_LOGDIR

# Short circuit for LINT builds.
if [ "$BUILD_TYPE" = "LINT" ]; then
  make lint | tee $TEST_LOGDIR/lint.log
  exit $?
fi

# Short circuit for IWYU builds: run the include-what-you-use tool on the files
# modified since the last committed changelist committed upstream.
if [ "$BUILD_TYPE" = "IWYU" ]; then
  make iwyu | tee $TEST_LOGDIR/iwyu.log
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
  export KUDU_FLAKY_TEST_LIST=$BUILD_ROOT/flaky-tests.txt
  mkdir -p $(dirname $KUDU_FLAKY_TEST_LIST)
  echo -n > $KUDU_FLAKY_TEST_LIST
    if [ -n "$TEST_RESULT_SERVER" ] && \
        list_flaky_tests > $KUDU_FLAKY_TEST_LIST ; then
    echo Will retry flaky tests up to $KUDU_FLAKY_TEST_ATTEMPTS times:
    cat $KUDU_FLAKY_TEST_LIST
    echo ----------
  else
    echo Unable to fetch flaky test list. Disabling flaky test resistance.
    export KUDU_FLAKY_TEST_ATTEMPTS=1
  fi
fi

# our tests leave lots of data lying around, clean up before we run
if [ -d "$TEST_TMPDIR" ]; then
  rm -Rf $TEST_TMPDIR/*
fi

# actually do the build
echo
echo Building C++ code.
echo ------------------------------------------------------------
NUM_PROCS=$(getconf _NPROCESSORS_ONLN)
make -j$NUM_PROCS 2>&1 | tee build.log

# If compilation succeeds, try to run all remaining steps despite any failures.
set +e

# Run tests
if [ "$RUN_FLAKY_ONLY" == "1" ] ; then
  if [ -z "$TEST_RESULT_SERVER" ]; then
    echo Must set TEST_RESULT_SERVER to use RUN_FLAKY_ONLY
    exit 1
  fi
  echo
  echo Running flaky tests only:
  echo ------------------------------------------------------------
  if ! ( set -o pipefail ;
         list_flaky_tests | tee $BUILD_ROOT/flaky-tests.txt) ; then
    echo Could not fetch flaky tests list.
    exit 1
  fi
  test_regex=$(perl -e '
    chomp(my @lines = <>);
    print join("|", map { "^" . quotemeta($_) } @lines);
   ' $BUILD_ROOT/flaky-tests.txt)
  if [ -z "$test_regex" ]; then
    echo No tests are flaky.
    exit 0
  fi
  EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS -R $test_regex"

  # We don't support detecting java and python flaky tests at the moment.
  echo Disabling Java and python build since RUN_FLAKY_ONLY=1
  BUILD_PYTHON=0
  BUILD_PYTHON3=0
  BUILD_JAVA=0
fi

EXIT_STATUS=0
FAILURES=""

# If we're running distributed tests, submit them asynchronously while
# we run the Java and Python tests.
if [ "$ENABLE_DIST_TEST" == "1" ]; then
  echo
  echo Submitting distributed-test job.
  echo ------------------------------------------------------------
  export DIST_TEST_JOB_PATH=$BUILD_ROOT/dist-test-job-id
  rm -f $DIST_TEST_JOB_PATH
  if ! $SOURCE_ROOT/build-support/dist_test.py --no-wait run ; then
    EXIT_STATUS=1
    FAILURES="$FAILURES"$'Could not submit distributed test job\n'
  fi
  # Still need to run a few non-dist-test-capable tests locally.
  EXTRA_TEST_FLAGS="$EXTRA_TEST_FLAGS -L no_dist_test"
fi

if ! $THIRDPARTY_BIN/ctest -j$NUM_PROCS $EXTRA_TEST_FLAGS ; then
  EXIT_STATUS=1
  FAILURES="$FAILURES"$'C++ tests failed\n'
fi

if [ "$DO_COVERAGE" == "1" ]; then
  echo
  echo Generating coverage report...
  echo ------------------------------------------------------------
  if ! $SOURCE_ROOT/thirdparty/installed/common/bin/gcovr \
      -r $SOURCE_ROOT \
      --gcov-filter='.*src#kudu.*' \
      --gcov-executable=$SOURCE_ROOT/build-support/llvm-gcov-wrapper \
      --xml \
      > $BUILD_ROOT/coverage.xml ; then
    EXIT_STATUS=1
    FAILURES="$FAILURES"$'Coverage report failed\n'
  fi
fi

if [ "$BUILD_JAVA" == "1" ]; then
  echo
  echo Building and testing java...
  echo ------------------------------------------------------------

  # Make sure we use JDK8
  export JAVA_HOME=$JAVA8_HOME
  export PATH=$JAVA_HOME/bin:$PATH
  pushd $SOURCE_ROOT/java
  export TSAN_OPTIONS="$TSAN_OPTIONS suppressions=$SOURCE_ROOT/build-support/tsan-suppressions.txt history_size=7"
  set -x

  # Run the full Maven build.
  if [ "$BUILD_MAVEN" == "1" ]; then
    EXTRA_MVN_FLAGS="-B"
    EXTRA_MVN_FLAGS="$EXTRA_MVN_FLAGS -Dsurefire.rerunFailingTestsCount=3"
    EXTRA_MVN_FLAGS="$EXTRA_MVN_FLAGS -Dfailsafe.rerunFailingTestsCount=3"
    EXTRA_MVN_FLAGS="$EXTRA_MVN_FLAGS -Dmaven.javadoc.skip"
    EXTRA_MVN_FLAGS="$EXTRA_MVN_FLAGS $MVN_FLAGS"
    if ! mvn $EXTRA_MVN_FLAGS clean verify ; then
      EXIT_STATUS=1
      FAILURES="$FAILURES"$'Java Maven build/test failed\n'
    fi
  fi

  # Run the full Gradle build.
  if [ "$BUILD_GRADLE" == "1" ]; then
    EXTRA_GRADLE_FLAGS="--console=plain"
    EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS --no-daemon"
    EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS --continue"
    EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS -DrerunFailingTestsCount=3"
    # KUDU-2524: temporarily disable scalafmt until we can work out its JDK
    # incompatibility issue.
    EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS -DskipFormat"
    EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS $GRADLE_FLAGS"
    # TODO: Run `gradle check` in BUILD_TYPE DEBUG when static code analysis is fixed
    if ! ./gradlew $EXTRA_GRADLE_FLAGS clean test ; then
      EXIT_STATUS=1
      FAILURES="$FAILURES"$'Java Gradle build/test failed\n'
    fi
  fi

  # Run a script to verify the contents of the JARs to ensure the shading and
  # packaging is correct.
  $SOURCE_ROOT/build-support/verify_jars.pl .

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

  # Old versions of pip (such as the one found in el6) default to pypi's http://
  # endpoint which no longer exists. The -i option lets us switch to the
  # https:// endpoint in such cases.
  #
  # Unfortunately, in these old versions of pip, -i doesn't appear to apply
  # recursively to transitive dependencies installed via a direct dependency's
  # "python setup.py" command. Therefore we have no choice but to upgrade to a
  # new version of pip to proceed.
  #
  # Beginning with pip 10, Python 2.6 is no longer supported. Attempting to
  # upgrade to pip 10 on Python 2.6 yields syntax errors. We don't need any new
  # pip features, so let's pin to the last pip version to support Python 2.6.
  pip install -i https://pypi.python.org/simple $PIP_INSTALL_FLAGS --upgrade 'pip < 10.0.0b1'

  # New versions of pip raise an exception when upgrading old versions of
  # setuptools (such as the one found in el6). The workaround is to upgrade
  # setuptools on its own, outside of requirements.txt, and with the pip version
  # check disabled.
  pip install --disable-pip-version-check $PIP_INSTALL_FLAGS --upgrade 'setuptools >= 0.8'

  # We've got a new pip and new setuptools. We can now install the rest of the
  # Python client's requirements.
  #
  # Installing the Cython dependency may involve some compiler work, so we must
  # pass in the current values of CC and CXX.
  CC=$CLANG CXX=$CLANG++ pip install $PIP_INSTALL_FLAGS -r requirements.txt

  # Delete old Cython extensions to force them to be rebuilt.
  rm -Rf build kudu_python.egg-info kudu/*.so

  # Build the Python bindings. This assumes we run this script from base dir.
  CC=$CLANG CXX=$CLANG++ python setup.py build_ext
  set +e

  # Run the Python tests.
  if ! python setup.py test \
      --addopts="kudu --junit-xml=$TEST_LOGDIR/python_client.xml" \
      2> $TEST_LOGDIR/python_client.log ; then
    EXIT_STATUS=1
    FAILURES="$FAILURES"$'Python tests failed\n'
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

  # Old versions of pip (such as the one found in el6) default to pypi's http://
  # endpoint which no longer exists. The -i option lets us switch to the
  # https:// endpoint in such cases.
  #
  # Unfortunately, in these old versions of pip, -i doesn't appear to apply
  # recursively to transitive dependencies installed via a direct dependency's
  # "python setup.py" command. Therefore we have no choice but to upgrade to a
  # new version of pip to proceed.
  pip install -i https://pypi.python.org/simple $PIP_INSTALL_FLAGS --upgrade pip

  # New versions of pip raise an exception when upgrading old versions of
  # setuptools (such as the one found in el6). The workaround is to upgrade
  # setuptools on its own, outside of requirements.txt, and with the pip version
  # check disabled.
  pip install --disable-pip-version-check $PIP_INSTALL_FLAGS --upgrade 'setuptools >= 0.8'

  # We've got a new pip and new setuptools. We can now install the rest of the
  # Python client's requirements.
  #
  # Installing the Cython dependency may involve some compiler work, so we must
  # pass in the current values of CC and CXX.
  CC=$CLANG CXX=$CLANG++ pip install $PIP_INSTALL_FLAGS -r requirements.txt

  # Delete old Cython extensions to force them to be rebuilt.
  rm -Rf build kudu_python.egg-info kudu/*.so

  # Build the Python bindings. This assumes we run this script from base dir.
  CC=$CLANG CXX=$CLANG++ python setup.py build_ext
  set +e

  # Run the Python tests.
  if ! python setup.py test \
      --addopts="kudu --junit-xml=$TEST_LOGDIR/python3_client.xml" \
      2> $TEST_LOGDIR/python3_client.log ; then
    EXIT_STATUS=1
    FAILURES="$FAILURES"$'Python 3 tests failed\n'
  fi

  deactivate
  popd
fi

# If we submitted the tasks earlier, go fetch the results now
if [ "$ENABLE_DIST_TEST" == "1" ]; then
  echo
  echo Fetching previously submitted dist-test results...
  echo ------------------------------------------------------------
  if ! $DIST_TEST_HOME/bin/client watch ; then
    EXIT_STATUS=1
    FAILURES="$FAILURES"$'Distributed tests failed\n'
  fi
  DT_DIR=$TEST_LOGDIR/dist-test-out
  rm -Rf $DT_DIR
  $DIST_TEST_HOME/bin/client fetch --artifacts -d $DT_DIR
  # Fetching the artifacts expands each log into its own directory.
  # Move them back into the main log directory
  rm -f $DT_DIR/*zip
  for arch_dir in $DT_DIR/* ; do
    mv $arch_dir/build/$BUILD_TYPE_LOWER/test-logs/* $TEST_LOGDIR
    rm -Rf $arch_dir
  done
fi

if [ $EXIT_STATUS != 0 ]; then
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
fi

# If all tests passed, ensure that they cleaned up their test output.
if [ $EXIT_STATUS == 0 ]; then
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

exit $EXIT_STATUS
