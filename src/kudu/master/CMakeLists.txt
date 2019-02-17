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

KRPC_GENERATE(
  MASTER_KRPC_SRCS MASTER_KRPC_HDRS MASTER_KRPC_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES master.proto)
set(MASTER_KRPC_LIBS
  consensus_metadata_proto
  krpc
  protobuf
  rpc_header_proto
  tablet_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(master_proto
  SRCS ${MASTER_KRPC_SRCS}
  DEPS ${MASTER_KRPC_LIBS}
  NONLINK_DEPS ${MASTER_KRPC_TGTS})

set(MASTER_SRCS
  authz_provider.cc
  catalog_manager.cc
  hms_notification_log_listener.cc
  location_cache.cc
  master.cc
  master_cert_authority.cc
  master_options.cc
  master_path_handlers.cc
  master_runner.cc
  master_service.cc
  mini_master.cc
  placement_policy.cc
  sentry_authz_provider.cc
  sentry_client_metrics.cc
  sentry_privileges_cache_metrics.cc
  sentry_privileges_fetcher.cc
  sys_catalog.cc
  table_metrics.cc
  ts_descriptor.cc
  ts_manager.cc)

add_library(master ${MASTER_SRCS})
target_link_libraries(master
  clock
  gutil
  krpc
  kserver
  kudu_common
  kudu_hms
  kudu_sentry
  kudu_thrift
  kudu_util
  master_proto
  rpc_header_proto
  security
  server_process
  tablet
  token_proto
  tserver)

# Tests
SET_KUDU_TEST_LINK_LIBS(
  kudu_client
  kudu_curl_util
  master
  master_proto
  mini_hms
  mini_kdc
  mini_sentry)

ADD_KUDU_TEST(catalog_manager-test)
ADD_KUDU_TEST(hms_notification_log_listener-test)
ADD_KUDU_TEST(location_cache-test DATA_FILES ../scripts/first_argument.sh)
ADD_KUDU_TEST(master-test RESOURCE_LOCK "master-web-port"
                          DATA_FILES ../scripts/first_argument.sh)
ADD_KUDU_TEST(mini_master-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(placement_policy-test)
ADD_KUDU_TEST(sentry_authz_provider-test NUM_SHARDS 8)
ADD_KUDU_TEST(sys_catalog-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(ts_descriptor-test DATA_FILES ../scripts/first_argument.sh)
ADD_KUDU_TEST(ts_state-test)

#########################################
# kudu-master
#########################################

add_executable(kudu-master master_main.cc)
target_link_libraries(kudu-master
  ${SANITIZER_OPTIONS_OVERRIDE}
  ${KRB5_REALM_OVERRIDE}
  master
  ${KUDU_BASE_LIBS})

option(KUDU_MASTER_INSTALL "Whether to install the Kudu Master executable" ON)
if(KUDU_MASTER_INSTALL)
  install(TARGETS kudu-master RUNTIME DESTINATION ${CMAKE_INSTALL_SBINDIR})
else()
  message(STATUS "Skipping install rule for the Kudu Master executable")
endif()
