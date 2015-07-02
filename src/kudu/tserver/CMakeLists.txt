# Copyright (c) 2012, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

#########################################
# remote_bootstrap_proto
#########################################

KRPC_GENERATE(REMOTE_BOOTSTRAP_KRPC_SRCS REMOTE_BOOTSTRAP_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES remote_bootstrap.proto)
set(REMOTE_BOOTSTRAP_KRPC_LIBS
  protobuf
  krpc
  consensus_proto
  tablet_proto
  rpc_header_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(remote_bootstrap_proto
  SRCS ${REMOTE_BOOTSTRAP_KRPC_SRCS}
  DEPS ${REMOTE_BOOTSTRAP_KRPC_LIBS})

#########################################
# tserver_proto
#########################################

PROTOBUF_GENERATE_CPP(TSERVER_PROTO_SRCS TSERVER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver.proto)
set(TSERVER_PROTO_LIBS
  kudu_common_proto
  consensus_metadata_proto
  tablet_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(tserver_proto
  SRCS ${TSERVER_PROTO_SRCS}
  DEPS ${TSERVER_PROTO_LIBS})

#########################################
# tserver_admin_proto
#########################################

KRPC_GENERATE(TSERVER_ADMIN_KRPC_SRCS TSERVER_ADMIN_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver_admin.proto)
set(TSERVER_ADMIN_KRPC_LIBS
  protobuf
  krpc
  rpc_header_proto
  tserver_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(tserver_admin_proto
  SRCS ${TSERVER_ADMIN_KRPC_SRCS}
  DEPS ${TSERVER_ADMIN_KRPC_LIBS})

#########################################
# tserver_service_proto
#########################################

KRPC_GENERATE(TSERVER_KRPC_SRCS TSERVER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver_service.proto)
set(TSERVER_KRPC_LIBS
  protobuf
  kudu_common_proto
  krpc
  tserver_proto
  wire_protocol_proto
  remote_bootstrap_proto)
ADD_EXPORTABLE_LIBRARY(tserver_service_proto
  SRCS ${TSERVER_KRPC_SRCS}
  DEPS ${TSERVER_KRPC_LIBS})

#########################################
# tserver
#########################################

set(TSERVER_SRCS
  heartbeater.cc
  mini_tablet_server.cc
  remote_bootstrap_client.cc
  remote_bootstrap_service.cc
  remote_bootstrap_session.cc
  scanner_metrics.cc
  scanners.cc
  tablet_server.cc
  tablet_server_options.cc
  tablet_service.cc
  ts_tablet_manager.cc
  tserver-path-handlers.cc
)

add_library(tserver ${TSERVER_SRCS})
target_link_libraries(tserver
  protobuf
  tserver_proto
  tserver_admin_proto
  tserver_service_proto
  remote_bootstrap_proto
  master_rpc
  master_proto
  consensus_proto
  log_proto
  log
  consensus
  krpc
  server_common
  server_process
  tablet)

#########################################
# kudu-tablet_server
#########################################

add_executable(kudu-tablet_server tablet_server_main.cc)
target_link_libraries(kudu-tablet_server
  tserver
  ${KUDU_BASE_LIBS})

#########################################
# tserver_test_util
#########################################

set(TSERVER_TEST_UTIL_SRCS
  tablet_server_test_util.cc
)

add_library(tserver_test_util ${TSERVER_TEST_UTIL_SRCS})
target_link_libraries(tserver_test_util
  tserver)

#########################################
# tserver tests
#########################################

set(KUDU_TEST_LINK_LIBS
  tserver
  tserver_test_util
  ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(remote_bootstrap_client-test)
ADD_KUDU_TEST(remote_bootstrap_session-test)
ADD_KUDU_TEST(remote_bootstrap_service-test)
ADD_KUDU_TEST(tablet_server-test)
ADD_KUDU_TEST(tablet_server-stress-test RUN_SERIAL true)
ADD_KUDU_TEST(scanners-test)
ADD_KUDU_TEST(ts_tablet_manager-test)
