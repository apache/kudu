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

#######################################
# tool_proto
#######################################

PROTOBUF_GENERATE_CPP(
  TOOL_PROTO_SRCS TOOL_PROTO_HDRS TOOL_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tool.proto)

add_library(tool_proto
  ${TOOL_PROTO_SRCS}
  ${TOOL_PROTO_HDRS})
target_link_libraries(tool_proto
  kudu_common_proto
  protobuf
  tablet_proto
  wire_protocol_proto)

#######################################
# kudu_tools_util
#######################################

add_library(kudu_tools_util
  color.cc
  diagnostics_log_parser.cc
  table_scanner.cc
  tool_action.cc
  tool_action_common.cc
)
target_link_libraries(kudu_tools_util
  cfile
  consensus
  gutil
  kudu_client
  kudu_common
  kudu_fs
  kudu_util
  log
  server_process
  tablet
  tool_proto
  ${KUDU_BASE_LIBS}
)

#######################################
# ksck
#######################################

add_library(ksck
    ksck.cc
    ksck_checksum.cc
    ksck_remote.cc
    ksck_results.cc
)
target_link_libraries(ksck
  consensus
  kudu_tools_util
  master
  master_proto
  rebalance
  server_base_proto
  tserver_proto
  tserver_service_proto
  ${KUDU_BASE_LIBS}
)

#######################################
# kudu_tools_location
#######################################

add_library(kudu_tools_location
  location_maintenance.cc
)
target_link_libraries(kudu_tools_location
  gutil
  kudu_util
  rebalance
  ${KUDU_BASE_LIBS}
)

#######################################
# kudu_tools_rebalance
#######################################

add_library(kudu_tools_rebalance
  rebalancer_tool.cc
  tool_replica_util.cc
)
target_link_libraries(kudu_tools_rebalance
  ksck
  kudu_common
  rebalance
  ${KUDU_BASE_LIBS}
)

#######################################
# kudu
#######################################

# The CLI action sources (the Build*Mode() factories and their helpers). These
# build into a static library, separate from main(), so unit tests can
# reconstruct the full action tree in-process by linking the library instead of
# recompiling every source into each test.
set(KUDU_CLI_ACTION_SRCS
  master_rebuilder.cc
  tool_action_cluster.cc
  tool_action_diagnose.cc
  tool_action_fs.cc
  tool_action_hms.cc
  tool_action_local_replica.cc
  tool_action_master.cc
  tool_action_pbc.cc
  tool_action_perf.cc
  tool_action_remote_replica.cc
  tool_action_root.cc
  tool_action_table.cc
  tool_action_tablet.cc
  tool_action_tserver.cc
  tool_action_txn.cc
  tool_action_wal.cc
)

if (KUDU_CLI_TEST_TOOL_ENABLED)
  set(KUDU_CLI_ACTION_SRCS ${KUDU_CLI_ACTION_SRCS} tool_action_test.cc)
endif()

set(KUDU_CLI_ACTION_LINK_LIBS
  clock
  consensus
  gutil
  krpc
  ksck
  kudu_client
  kudu_common
  kudu_fs
  kudu_subprocess
  kudu_tools_location
  kudu_tools_rebalance
  kudu_util
  log
  master
  tablet
  tool_proto
  transactions
  tserver
  ${KUDU_BASE_LIBS}
)
if(NOT NO_ROCKSDB)
  list(APPEND KUDU_CLI_ACTION_LINK_LIBS
    rocksdb)
endif()

# The mini_cluster is used only by the test-only 'test' mode.
if (KUDU_CLI_TEST_TOOL_ENABLED)
  set(KUDU_CLI_ACTION_LINK_LIBS ${KUDU_CLI_ACTION_LINK_LIBS} mini_cluster)
endif()

add_library(kudu_cli_actions ${KUDU_CLI_ACTION_SRCS})
target_link_libraries(kudu_cli_actions ${KUDU_CLI_ACTION_LINK_LIBS})

# The 'kudu' executable is the main() entrypoint linked against the action
# library. The override link flags must come first in the final binary (see the
# top-level CMakeLists and security/krb5_realm_override.cc).
add_executable(kudu tool_main.cc)
target_link_libraries(kudu
  ${SANITIZER_OPTIONS_OVERRIDE}
  ${KRB5_REALM_OVERRIDE}
  kudu_cli_actions)

option(KUDU_CLIENT_INSTALL "Whether to install the Kudu command line tool" ON)
if(KUDU_CLIENT_INSTALL)
  install(TARGETS kudu RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
else()
  message(STATUS "Skipping install rule for the Kudu command line tool")
endif()

#######################################
# kudu_tools_test_util
#######################################

if(NOT NO_TESTS)
  # The kudu_tools_test_util library is used only by tests.
  add_library(kudu_tools_test_util
    tool_test_util.cc
  )
  target_link_libraries(kudu_tools_test_util
    kudu_tools_util
    kudu_util
  )
endif()

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(
  itest_util
  ksck
  kudu_hms
  kudu_subprocess
  kudu_tools_location
  kudu_tools_rebalance
  kudu_tools_test_util
  kudu_tools_util
  mini_cluster)
ADD_KUDU_TEST(create-table-tool-test
  NUM_SHARDS 4 PROCESSORS 3)
ADD_KUDU_TEST_DEPENDENCIES(create-table-tool-test
  kudu)
ADD_KUDU_TEST(diagnostics_log_parser-test)
ADD_KUDU_TEST(ksck-test)
ADD_KUDU_TEST(ksck_remote-test
  PROCESSORS 3
  DATA_FILES ../scripts/first_argument.sh)
ADD_KUDU_TEST(kudu-admin-test
  NUM_SHARDS 8 PROCESSORS 3)
ADD_KUDU_TEST_DEPENDENCIES(kudu-admin-test
  kudu)
ADD_KUDU_TEST(kudu-tool-test
  NUM_SHARDS 4 PROCESSORS 3
  DATA_FILES testdata/sample-diagnostics-log.txt testdata/bad-diagnostics-log.txt
  DATA_FILES ../scripts/first_argument.sh ../scripts/assign-location.py)
ADD_KUDU_TEST_DEPENDENCIES(kudu-tool-test
  kudu)
ADD_KUDU_TEST(kudu-ts-cli-test)
ADD_KUDU_TEST_DEPENDENCIES(kudu-ts-cli-test
  kudu)
ADD_KUDU_TEST(kudu-txn-cli-test)
ADD_KUDU_TEST_DEPENDENCIES(kudu-txn-cli-test
  kudu)
ADD_KUDU_TEST(location_maintenance-test)
ADD_KUDU_TEST(rebalancer_tool-test
  NUM_SHARDS 8 PROCESSORS 3
  DATA_FILES ../scripts/assign-location.py)
ADD_KUDU_TEST_DEPENDENCIES(rebalancer_tool-test
  kudu)
ADD_KUDU_TEST(tool_action-test)
