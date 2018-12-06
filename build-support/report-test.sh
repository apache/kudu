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
# Reports a test run to the central test server, which records
# the results in a database. This is what drives our "flaky test dashboard".
# This script does blocking network IO, so if you are running it from the
# context of a build, you may want to run it in the background.
#
# Note that this may exit with a non-zero code if the network is flaky or the
# test result server is down.
#
# Expects BUILD_TAG, GIT_REVISION, and BUILD_CONFIG environment variables to be set.

set -e

# Verify required environment variables.
if [ -z "$BUILD_TAG" ]; then
  echo "BUILD_TAG environment variable must be set"
  exit 1
fi
if [ -z "$GIT_REVISION" ]; then
  echo "GIT_REVISION environment variable must be set"
  exit 1
fi
if [ -z "$BUILD_CONFIG" ]; then
  echo "BUILD_CONFIG environment variable must be set"
  exit 1
fi

# Verify and parse command line and options
if [ $# -ne 3 ]; then
  echo "usage: $0 <path/to/test> <path/to/log> <exit-code>"
  echo
  echo The \$TEST_RESULT_SERVER environment variable may be used
  echo to specify where to report the tests.
  exit 1
fi
TEST_EXECUTABLE=$1
LOGFILE=$2
STATUS=$3
TEST_RESULT_SERVER=${TEST_RESULT_SERVER:-localhost:8080}
REPORT_TIMEOUT=${REPORT_TIMEOUT:-10}

# We sometimes have flaky infrastructure where NTP is broken. In that case
# do not report it as a failed test.
if zgrep -q 'Clock considered unsynchronized' $LOGFILE ; then
  echo Not reporting test that failed due to NTP issues.
  exit 1
fi

# Only upload a log if the test failed.
# This saves some space on S3, network bandwidth, etc, and we don't
# have a lot of use for the logs of successful tests anyway.
if [ "$STATUS" -ne 0 ]; then
  LOG_PARAM="-F log=@$LOGFILE"
else
  LOG_PARAM=""
fi

# In the backend, the BUILD_TAG field is called 'build_id', but we can't use
# that as an env variable because it'd collide with Jenkins' BUILD_ID.
curl -s \
    --max-time $REPORT_TIMEOUT \
    $LOG_PARAM \
    -F "build_id=$BUILD_TAG" \
    -F "hostname=$(hostname)" \
    -F "test_name=$(basename $TEST_EXECUTABLE)" \
    -F "status=$STATUS" \
    -F "revision=$GIT_REVISION" \
    -F "build_config=$BUILD_CONFIG" \
    http://$TEST_RESULT_SERVER/add_result
