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

##############################
# sentry_thrift
##############################

THRIFT_GENERATE_CPP(
  SENTRY_THRIFT_SRCS SENTRY_THRIFT_HDRS SENTRY_THRIFT_TGTS
  THRIFT_FILES sentry_common_service.thrift sentry_policy_service.thrift)

add_library(sentry_thrift ${SENTRY_THRIFT_SRCS} thrift_operators.cc)
target_link_libraries(sentry_thrift thrift)
add_dependencies(sentry_thrift ${SENTRY_THRIFT_TGTS})

##############################
# kudu_sentry
##############################

set(SENTRY_SRCS
  sentry_client.cc)
set(SENTRY_DEPS
  kudu_common
  kudu_thrift
  kudu_util
  sentry_thrift)

add_library(kudu_sentry ${SENTRY_SRCS})
target_link_libraries(kudu_sentry ${SENTRY_DEPS})

##############################
# mini_sentry
##############################

execute_process(COMMAND ln -nsf
                "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/opt/sentry"
                "${EXECUTABLE_OUTPUT_PATH}/sentry-home")
execute_process(COMMAND ln -nsf
                "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/opt/hadoop"
                "${EXECUTABLE_OUTPUT_PATH}/hadoop-home")
execute_process(COMMAND ln -nsf
                "${JAVA_HOME}"
                "${EXECUTABLE_OUTPUT_PATH}/java-home")

set(MINI_SENTRY_SRCS
  mini_sentry.cc)

add_library(mini_sentry ${MINI_SENTRY_SRCS})
target_link_libraries(mini_sentry
  gutil
  kudu_test_util
  kudu_util)

##############################
# sentry tests
##############################

if (NOT NO_TESTS)
  SET_KUDU_TEST_LINK_LIBS(
    kudu_sentry
    mini_kdc
    mini_sentry)

  ADD_KUDU_TEST(sentry_client-test)
endif()
