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

PROTOBUF_GENERATE_CPP(
  TRANSACTIONS_PROTO_SRCS TRANSACTIONS_PROTO_HDRS TRANSACTIONS_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES transactions.proto)

add_library(transactions_proto
  ${TRANSACTIONS_PROTO_SRCS}
  ${TRANSACTIONS_PROTO_HDRS})
target_link_libraries(transactions_proto
  protobuf
  wire_protocol_proto
)

set(TRANSACTIONS_SRCS
  coordinator_rpc.cc
  txn_status_entry.cc
  txn_status_manager.cc
  txn_status_tablet.cc
  txn_system_client.cc)

add_library(transactions ${TRANSACTIONS_SRCS})
target_link_libraries(transactions
  kudu_client
  kudu_common
  tablet
  transactions_proto
  ${KUDU_BASE_LIBS}
)

SET_KUDU_TEST_LINK_LIBS(transactions tablet_test_util)
ADD_KUDU_TEST(txn_status_manager-test)
ADD_KUDU_TEST(txn_status_tablet-test)
