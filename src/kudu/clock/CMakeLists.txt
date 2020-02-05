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
  builtin_ntp.cc
  builtin_ntp-internal.cc
  hybrid_clock.cc
  logical_clock.cc
  mock_ntp.cc
  system_unsync_time.cc)

# Darwin 17 (macOS High Sierra, ver. 10.13.6) and higher supports get_ntptime()
if (NOT APPLE OR ${CMAKE_SYSTEM_VERSION} VERSION_GREATER_EQUAL "17.0.0")
  set(CLOCK_SRCS ${CLOCK_SRCS} system_ntp.cc)
  set_source_files_properties(hybrid_clock.cc hybrid_clock-test.cc
    PROPERTIES COMPILE_DEFINITIONS "KUDU_HAS_SYSTEM_TIME_SOURCE=1")
endif()

add_library(clock ${CLOCK_SRCS})

target_link_libraries(clock
  kudu_cloud_util
  kudu_common
  kudu_util)

##############################
# mini_chronyd
##############################

if (NOT NO_CHRONY)
  # Link the chrony binaries so that they can be found via
  # MiniChronyd::GetPath in MiniChronyd::Start.
  execute_process(COMMAND ln -nsf
                  "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/bin/chronyc"
                  "${EXECUTABLE_OUTPUT_PATH}/chronyc")
  execute_process(COMMAND ln -nsf
                  "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/sbin/chronyd"
                  "${EXECUTABLE_OUTPUT_PATH}/chronyd")

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
endif() # (NOT NO_CHRONY)

SET_KUDU_TEST_LINK_LIBS(clock)
ADD_KUDU_TEST(hybrid_clock-test PROCESSORS 3)
ADD_KUDU_TEST(logical_clock-test)

if (NO_CHRONY)
  SET_KUDU_TEST_LINK_LIBS(clock)
else()
  SET_KUDU_TEST_LINK_LIBS(clock mini_chronyd mini_chronyd_test_util)
  ADD_KUDU_TEST(test/mini_chronyd-test)
endif()
ADD_KUDU_TEST(ntp-test)
