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

set(CLOCK_SRCS
  hybrid_clock.cc
  logical_clock.cc
  mock_ntp.cc
  system_unsync_time.cc)

if (NOT APPLE)
  set(CLOCK_SRCS ${CLOCK_SRCS} system_ntp.cc)
endif()

add_library(clock ${CLOCK_SRCS})

target_link_libraries(clock
  kudu_common
  kudu_util)

##############################
# mini_chronyd
##############################

# These are copied/installed instead of linking because:
#  * symlinks would not work with dist-test
#  * hardlinks would not work if the target directory is at different
#    filesystem than thirdparty
file(COPY "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/bin/chronyc"
  DESTINATION "${EXECUTABLE_OUTPUT_PATH}")
file(COPY "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/sbin/chronyd"
  DESTINATION "${EXECUTABLE_OUTPUT_PATH}")

set(MINI_CHRONYD_SRCS test/mini_chronyd.cc)

add_library(mini_chronyd ${MINI_CHRONYD_SRCS})
target_link_libraries(mini_chronyd
  gutil
  kudu_test_util
  kudu_util)

set(MINI_CHRONYD_TEST_UTIL_SRCS test/mini_chronyd_test_util.cc)
add_library(mini_chronyd_test_util ${MINI_CHRONYD_TEST_UTIL_SRCS})
target_link_libraries(mini_chronyd_test_util
  gutil
  kudu_test_util
  kudu_util
  mini_cluster)

SET_KUDU_TEST_LINK_LIBS(clock)
ADD_KUDU_TEST(hybrid_clock-test PROCESSORS 3)
ADD_KUDU_TEST(logical_clock-test)

SET_KUDU_TEST_LINK_LIBS(clock mini_chronyd mini_chronyd_test_util)
ADD_KUDU_TEST(test/mini_chronyd-test)
