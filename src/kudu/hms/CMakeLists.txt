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

THRIFT_GENERATE_CPP(
  HMS_THRIFT_SRCS HMS_THRIFT_HDRS HMS_THRIFT_TGTS
  THRIFT_FILES hive_metastore.thrift)

add_library(hms_thrift ${HMS_THRIFT_SRCS})
target_link_libraries(hms_thrift thrift)
add_dependencies(hms_thrift ${HMS_THRIFT_TGTS})

set(HMS_SRCS
  hms_catalog.cc
  hms_client.cc
  sasl_client_transport.cc)
set(HMS_DEPS
  gflags
  glog
  hms_thrift
  krpc
  kudu_common
  kudu_util)

add_library(kudu_hms ${HMS_SRCS})
target_link_libraries(kudu_hms ${HMS_DEPS})

##############################
# mini_hms
##############################

execute_process(COMMAND ln -nsf
                "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/opt/hive"
                "${EXECUTABLE_OUTPUT_PATH}/hive-home")
execute_process(COMMAND ln -nsf
                "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/opt/hadoop"
                "${EXECUTABLE_OUTPUT_PATH}/hadoop-home")
execute_process(COMMAND ln -nsf
                "${JAVA_HOME}"
                "${EXECUTABLE_OUTPUT_PATH}/java-home")

file(GLOB DEPENDENCY_JARS
  "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/opt/hive/lib/*"
  "${CMAKE_SOURCE_DIR}/thirdparty/installed/common/opt/hadoop/share/hadoop/common/*")

add_jar(hms-plugin
  "${CMAKE_SOURCE_DIR}/java/kudu-hive/src/main/java/org/apache/kudu/hive/metastore/KuduMetastorePlugin.java"
  INCLUDE_JARS ${DEPENDENCY_JARS}
  OUTPUT_DIR "${EXECUTABLE_OUTPUT_PATH}")

set(MINI_HMS_SRCS
  mini_hms.cc)

add_library(mini_hms ${MINI_HMS_SRCS})
target_link_libraries(mini_hms
  gutil
  krpc
  kudu_test_util
  kudu_util)
add_dependencies(mini_hms hms-plugin)

# Tests
if (NOT NO_TESTS)
  set(KUDU_TEST_LINK_LIBS
    kudu_hms
    mini_hms
    mini_kdc
    ${KUDU_MIN_TEST_LIBS})

  # These tests must run serially, otherwise starting the HMS can take a very long time.
  ADD_KUDU_TEST(hms_catalog-test RUN_SERIAL true NUM_SHARDS 4)
  ADD_KUDU_TEST(hms_client-test RUN_SERIAL true NUM_SHARDS 4)
endif()
