# Copyright (c) 2012, Cloudera, inc.

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

KRPC_GENERATE(TSERVER_KRPC_SRCS TSERVER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver_service.proto)

add_library(tserver_service_proto ${TSERVER_KRPC_SRCS} ${TSERVER_KRPC_HDRS})
target_link_libraries(tserver_service_proto
  protobuf
  consensus_proto
  kudu_common_proto
  krpc
  tserver_proto
  wire_protocol_proto)

set(TSERVER_SRCS
  heartbeater.cc
  mini_tablet_server.cc
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
  tserver_service_proto
  master_proto
  consensus_proto
  log_proto
  log
  consensus
  krpc
  server_common
  server_process
  tablet)

# Tests
set(KUDU_TEST_LINK_LIBS tserver ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(tablet_server-test)
ADD_KUDU_TEST(tablet_server-stress-test RUN_SERIAL true)
ADD_KUDU_TEST(scanners-test)
ADD_KUDU_TEST(ts_tablet_manager-test)

add_executable(kudu-tablet_server tablet_server_main.cc)
target_link_libraries(kudu-tablet_server
  tserver
  ${KUDU_BASE_LIBS})
