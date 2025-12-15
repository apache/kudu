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

# Everything in this module depends on test infrastructure.
if (NO_TESTS)
  return()
endif()

set(TPCH_SRCS
  tpch/rpc_line_item_dao.cc
)

add_library(tpch ${TPCH_SRCS})
target_link_libraries(tpch
  itest_util
  kudu_client)

# tpch1
add_executable(tpch1 tpch/tpch1.cc)
target_link_libraries(tpch1
  ${KUDU_MIN_TEST_LIBS}
  tpch)
if(NOT NO_ROCKSDB)
  target_link_libraries(tpch1
    rocksdb)
endif()

# tpch_real_world
add_executable(tpch_real_world tpch/tpch_real_world.cc)
target_link_libraries(tpch_real_world
  ${KUDU_MIN_TEST_LIBS}
  tpch)
if(NOT NO_ROCKSDB)
  target_link_libraries(tpch_real_world
    rocksdb)
endif()

# rle
add_executable(rle rle.cc)
target_link_libraries(rle
  ${KUDU_MIN_TEST_LIBS}
  kudu_util)

# Disabled on macOS since it relies on fdatasync() and sync_file_range().
if(NOT APPLE)
  add_executable(wal_hiccup wal_hiccup.cc)
  target_link_libraries(wal_hiccup
    ${KUDU_MIN_TEST_LIBS}
    kudu_util)
endif()

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(tpch)
if(NOT NO_ROCKSDB)
  ADD_KUDU_TEST_LINK_LIBS(
    rocksdb)
endif()
ADD_KUDU_TEST(tpch/rpc_line_item_dao-test)

#######################################
# serdes-test
#######################################
PROTOBUF_GENERATE_CPP(
  ARRAYS_PROTO_SRCS ARRAYS_PROTO_HDRS ARRAYS_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES serdes/arrays.proto)
ADD_EXPORTABLE_LIBRARY(serdes_arrays
  SRCS ${ARRAYS_PROTO_SRCS}
  DEPS protobuf
  NONLINK_DEPS ${ARRAYS_PROTO_TGTS})

FLATBUFFERS_GENERATE_CPP(ARRAYS_FBS_HDRS ARRAYS_FBS_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  FBS_FILES serdes/arrays.fbs)

ADD_KUDU_TEST(serdes/serdes-test)
ADD_KUDU_TEST_DEPENDENCIES(serdes/serdes-test ${ARRAYS_FBS_TGTS})
if(NOT NO_TESTS)
  target_link_libraries(serdes-test
    serdes_arrays)
endif()
