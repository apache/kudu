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

SOURCE_ROOT=$(cd $(dirname "$BASH_SOURCE")/../..; pwd)
BUILD_ROOT=$SOURCE_ROOT/build/$BUILD_TYPE_LOWER

# Remove testing artifacts from the previous run before we do anything
# else. Otherwise, if we fail during the "build" step, Jenkins will
# archive the test logs from the previous run, thinking they came from
# this run, and confuse us when we look at the failed build.
rm -rf $BUILD_ROOT
mkdir -p $BUILD_ROOT

# Same for the Java tests, which aren't inside BUILD_ROOT
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

# Configure the build
#
# ASAN/TSAN can't build the Python bindings because the exported Kudu client
# library (which the bindings depend on) is missing ASAN/TSAN symbols.
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
$SOURCE_ROOT/build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh $THIRDPARTY_TYPE

THIRDPARTY_BIN=$(pwd)/thirdparty/installed/common/bin
export PPROF_PATH=$THIRDPARTY_BIN/pprof

if which ccache >/dev/null ; then
  CLANG=$(pwd)/build-support/ccache-clang/clang
else
  CLANG=$(pwd)/thirdparty/clang-toolchain/bin/clang
fi

# Make sure we use JDK8
if [ -n "$JAVA8_HOME" ]; then
  export JAVA_HOME=$JAVA8_HOME
  export PATH=$JAVA_HOME/bin:$PATH
fi

# Some portions of the C++ build may depend on Java code, so we may run Gradle
# while building. Pass in some flags suitable for automated builds; these will
# also be used in the Java build.
# These should be set before CMAKE so that the Gradle command in the
# generated make file has the correct flags.
export EXTRA_GRADLE_FLAGS="--console=plain"
EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS --no-daemon"
EXTRA_GRADLE_FLAGS="$EXTRA_GRADLE_FLAGS --continue"
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
    pushd $SOURCE_ROOT/java
    if ! ./gradlew $EXTRA_GRADLE_FLAGS clean check -x test $EXTRA_GRADLE_TEST_FLAGS; then
      LINT_RESULT=1
      LINT_FAILURES="$LINT_FAILURES"$'Java Gradle check failed\n'
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
  make -j$NUM_PROCS generated-headers 2>&1 | tee $TEST_LOGDIR/tidy.log
  $SOURCE_ROOT/build-support/clang_tidy_gerrit.py HEAD 2>&1 | \
      tee -a $TEST_LOGDIR/tidy.log
  exit $?
fi

# Only enable test core dumps for certain build types.
if [ "$BUILD_TYPE" != "ASAN" ]; then
  export KUDU_TEST_ULIMIT_CORE=unlimited
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

if ! $THIRDPARTY_BIN/ctest -j$NUM_PROCS $EXTRA_TEST_FLAGS ; then
  TESTS_FAILED=1
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

  pushd $SOURCE_ROOT/java
  set -x

  # Run the full Gradle build.
  # If we're running distributed Java tests, submit them asynchronously.
  if [ "$ENABLE_DIST_TEST" == "1" ]; then
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
    # TODO: Run `gradle check` in BUILD_TYPE DEBUG when static code analysis is fixed
    if ! ./gradlew $EXTRA_GRADLE_FLAGS clean test $EXTRA_GRADLE_TEST_FLAGS; then
      TESTS_FAILED=1
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
  #
  # The absence of $PIP_FLAGS is intentional: older versions of pip may not
  # support the flags that we want to use.
  pip install -i https://pypi.python.org/simple $PIP_INSTALL_FLAGS --upgrade 'pip <10.0.0b1'

  # New versions of pip raise an exception when upgrading old versions of
  # setuptools (such as the one found in el6). The workaround is to upgrade
  # setuptools on its own, outside of requirements.txt, and with the pip version
  # check disabled.
  #
  # Setuptools 42.0.0 changes something that causes build_ext to fail with a
  # missing wheel package. Let's pin to an older version known to work.
  pip $PIP_FLAGS install --disable-pip-version-check $PIP_INSTALL_FLAGS --upgrade 'setuptools >=0.8,<42.0.0'

  # One of our dependencies is pandas, installed below. It depends on numpy, and
  # if we don't install numpy directly, the pandas installation will install the
  # latest numpy which is incompatible with Python 2.6.
  #
  # To work around this, we need to install a 2.6-compatible version of numpy
  # before installing pandas. Installing numpy may involve some compiler work,
  # so we must pass in the current values of CC and CXX.
  #
  # See https://github.com/numpy/numpy/releases/tag/v1.12.0 for more details.
  CC=$CLANG CXX=$CLANG++ pip $PIP_FLAGS install $PIP_INSTALL_FLAGS 'numpy <1.12.0'

  # We've got a new pip and new setuptools. We can now install the rest of the
  # Python client's requirements.
  #
  # Installing the Cython dependency may involve some compiler work, so we must
  # pass in the current values of CC and CXX.
  CC=$CLANG CXX=$CLANG++ pip $PIP_FLAGS install $PIP_INSTALL_FLAGS -r requirements.txt

  # We need to install Pandas manually because although it's not a required
  # package, it is needed to run all of the tests.
  #
  # Installing pandas may involve some compiler work, so we must pass in the
  # current values of CC and CXX.
  #
  # pandas 0.18 dropped support for python 2.6. See https://pandas.pydata.org/pandas-docs/version/0.23.0/whatsnew.html#v0-18-0-march-13-2016
  # for more details.
  CC=$CLANG CXX=$CLANG++ pip $PIP_FLAGS install $PIP_INSTALL_FLAGS 'pandas <0.18'

  # Delete old Cython extensions to force them to be rebuilt.
  rm -Rf build kudu_python.egg-info kudu/*.so

  # Build the Python bindings. This assumes we run this script from base dir.
  CC=$CLANG CXX=$CLANG++ python setup.py build_ext
  set +e

  # Run the Python tests. This may also involve some compiler work.
  if ! CC=$CLANG CXX=$CLANG++ python setup.py test \
      --addopts="kudu --junit-xml=$TEST_LOGDIR/python_client.xml" \
      2> $TEST_LOGDIR/python_client.log ; then
    TESTS_FAILED=1
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
  #
  # pip 19.1 doesn't support Python 3.4, which is the version of Python 3
  # shipped with Ubuntu 14.04. However, there appears to be a bug[1] in pip 19.0
  # preventing it from working properly with Python 3.4 as well. Therefore we
  # must pin to a pip version from before 19.0.
  #
  # The absence of $PIP_FLAGS is intentional: older versions of pip may not
  # support the flags that we want to use.
  #
  # 1. https://github.com/pypa/pip/issues/6175
  pip install -i https://pypi.python.org/simple $PIP_INSTALL_FLAGS --upgrade 'pip <19.0'

  # New versions of pip raise an exception when upgrading old versions of
  # setuptools (such as the one found in el6). The workaround is to upgrade
  # setuptools on its own, outside of requirements.txt, and with the pip version
  # check disabled.
  pip $PIP_FLAGS install --disable-pip-version-check $PIP_INSTALL_FLAGS --upgrade 'setuptools >=0.8'

  # One of our dependencies is pandas, installed below. It depends on numpy, and
  # if we don't install numpy directly, the pandas installation will install the
  # latest numpy which is incompatible with Python 3.4 (the version of Python 3
  # shipped with Ubuntu 14.04).
  #
  # To work around this, we need to install a 3.4-compatible version of numpy
  # before installing pandas. Installing numpy may involve some compiler work,
  # so we must pass in the current values of CC and CXX.
  #
  # See https://github.com/numpy/numpy/releases/tag/v1.16.0rc1 for more details.
  CC=$CLANG CXX=$CLANG++ pip $PIP_FLAGS install $PIP_INSTALL_FLAGS 'numpy <1.16.0'

  # We've got a new pip and new setuptools. We can now install the rest of the
  # Python client's requirements.
  #
  # Installing the Cython dependency may involve some compiler work, so we must
  # pass in the current values of CC and CXX.
  CC=$CLANG CXX=$CLANG++ pip $PIP_FLAGS install $PIP_INSTALL_FLAGS -r requirements.txt

  # We need to install Pandas manually because although it's not a required
  # package, it is needed to run all of the tests.
  #
  # Installing pandas may involve some compiler work, so we must pass in the
  # current values of CC and CXX.
  CC=$CLANG CXX=$CLANG++ pip $PIP_FLAGS install $PIP_INSTALL_FLAGS pandas

  # Delete old Cython extensions to force them to be rebuilt.
  rm -Rf build kudu_python.egg-info kudu/*.so

  # Build the Python bindings. This assumes we run this script from base dir.
  CC=$CLANG CXX=$CLANG++ python setup.py build_ext
  set +e

  # Run the Python tests. This may also involve some compiler work.
  if ! CC=$CLANG CXX=$CLANG++ python setup.py test \
      --addopts="kudu --junit-xml=$TEST_LOGDIR/python3_client.xml" \
      2> $TEST_LOGDIR/python3_client.log ; then
    TESTS_FAILED=1
    FAILURES="$FAILURES"$'Python 3 tests failed\n'
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
