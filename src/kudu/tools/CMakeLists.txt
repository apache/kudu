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

set(LINK_LIBS
  kudu_client
  log
  consensus
  tserver
  kudu_common
  kudu_fs
  kudu_util
  gutil
  cfile
  tablet
  ${KUDU_BASE_LIBS}
)

add_library(kudu_tools_util
  color.cc
  data_gen_util.cc)
target_link_libraries(kudu_tools_util
  ${LINK_LIBS})

add_executable(create-demo-table create-demo-table.cc)
target_link_libraries(create-demo-table
  ${LINK_LIBS})

add_executable(insert-generated-rows insert-generated-rows.cc)
target_link_libraries(insert-generated-rows
  kudu_tools_util
  ${LINK_LIBS})

add_executable(kudu-admin kudu-admin.cc)
target_link_libraries(kudu-admin
  ${LINK_LIBS})

add_executable(kudu-ts-cli ts-cli.cc)
target_link_libraries(kudu-ts-cli
  ${LINK_LIBS})

add_library(fs_tool fs_tool.cc)
target_link_libraries(fs_tool
  gutil
  kudu_common
  server_common
  consensus
  tablet)

add_executable(kudu-fs_list fs_list-tool.cc)
target_link_libraries(kudu-fs_list
  ${LINK_LIBS}
  fs_tool)

add_executable(kudu-fs_dump fs_dump-tool.cc)
target_link_libraries(kudu-fs_dump
  ${LINK_LIBS}
  fs_tool)

add_library(ksck
    ksck.cc
    ksck_remote.cc
)
target_link_libraries(ksck
  consensus
  kudu_tools_util
  master_proto
  server_base_proto
  tserver_proto
  tserver_service_proto
  ${KUDU_BASE_LIBS}
)

add_executable(kudu
  tool_action.cc
  tool_action_cluster.cc
  tool_action_fs.cc
  tool_action_pbc.cc
  tool_action_tablet.cc
  tool_main.cc
)
target_link_libraries(kudu
  consensus
  gutil
  kudu_common
  kudu_fs
  kudu_util
  ksck
  master
  tserver
  ${KUDU_BASE_LIBS}
)

set(KUDU_TEST_LINK_LIBS
  ksck
  kudu_tools_util
  integration-tests
  ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(ksck-test)
ADD_KUDU_TEST(ksck_remote-test RESOURCE_LOCK "master-rpc-ports")
ADD_KUDU_TEST(kudu-admin-test)
ADD_KUDU_TEST_DEPENDENCIES(kudu-admin-test
  kudu-admin)
ADD_KUDU_TEST(kudu-tool-test)
ADD_KUDU_TEST_DEPENDENCIES(kudu-tool-test
  kudu)
ADD_KUDU_TEST(kudu-ts-cli-test)
ADD_KUDU_TEST_DEPENDENCIES(kudu-ts-cli-test
  kudu-ts-cli)

