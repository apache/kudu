# Copyright (c) 2012, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

#########################################
# remote_bootstrap_proto
#########################################

KRPC_GENERATE(REMOTE_BOOTSTRAP_KRPC_SRCS REMOTE_BOOTSTRAP_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES remote_bootstrap.proto)

add_library(remote_bootstrap_proto ${REMOTE_BOOTSTRAP_KRPC_SRCS} ${REMOTE_BOOTSTRAP_KRPC_HDRS})
target_link_libraries(remote_bootstrap_proto
  protobuf
  krpc
  consensus_proto
  tablet_proto
  rpc_header_proto
  wire_protocol_proto)

#########################################
# tserver_proto
#########################################

PROTOBUF_GENERATE_CPP(TSERVER_PROTO_SRCS TSERVER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver.proto)

add_library(tserver_proto ${TSERVER_PROTO_SRCS} ${TSERVER_PROTO_HDRS})
target_link_libraries(tserver_proto
  kudu_common_proto
  metadata_proto
  tablet_proto
  wire_protocol_proto)

#########################################
# tserver_admin__proto
#########################################

KRPC_GENERATE(TSERVER_ADMIN_KRPC_SRCS TSERVER_ADMIN_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver_admin.proto)

add_library(tserver_admin_proto ${TSERVER_ADMIN_KRPC_SRCS}
${TSERVER_ADMIN_KRPC_HDRS})
target_link_libraries(tserver_admin_proto
  protobuf
  krpc
  rpc_header_proto
  tserver_proto
  wire_protocol_proto)

#########################################
# tserver_service_proto
#########################################

KRPC_GENERATE(TSERVER_KRPC_SRCS TSERVER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver_service.proto)

add_library(tserver_service_proto ${TSERVER_KRPC_SRCS} ${TSERVER_KRPC_HDRS})
target_link_libraries(tserver_service_proto
  protobuf
  kudu_common_proto
  krpc
  tserver_proto
  wire_protocol_proto
  remote_bootstrap_proto)

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
# tserver tests
#########################################

set(KUDU_TEST_LINK_LIBS tserver ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(remote_bootstrap_client-test)
ADD_KUDU_TEST(remote_bootstrap_session-test)
ADD_KUDU_TEST(remote_bootstrap_service-test)
ADD_KUDU_TEST(tablet_server-test)
ADD_KUDU_TEST(tablet_server-stress-test RUN_SERIAL true)
ADD_KUDU_TEST(scanners-test)
ADD_KUDU_TEST(ts_tablet_manager-test)
