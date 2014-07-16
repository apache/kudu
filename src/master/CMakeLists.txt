# Copyright (c) 2013, Cloudera, inc.

KRPC_GENERATE(MASTER_KRPC_SRCS MASTER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES master.proto)

add_library(master_proto ${MASTER_KRPC_SRCS} ${MASTER_KRPC_HDRS})
target_link_libraries(master_proto
  metadata_proto
  wire_protocol_proto
  protobuf
  rpc_header_proto)

set(MASTER_SRCS
  catalog_manager.cc
  master.cc
  master_options.cc
  master_service.cc
  master-path-handlers.cc
  mini_master.cc
  sys_tables.cc
  ts_descriptor.cc
  ts_manager.cc
)

add_library(master ${MASTER_SRCS} ${MASTER_PROTO_SRCS} ${MASTER_KRPC_SRCS})
target_link_libraries(master
  kudu_common
  tablet
  server_common
  server_process
  krpc
  gutil
  kudu_util
  tserver_service_proto
  master_proto
  rpc_header_proto)

# Tests
set(KUDU_TEST_LINK_LIBS master master_proto client ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(catalog_manager-test)
ADD_KUDU_TEST(master-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(sys_tables-test RESOURCE_LOCK "master-web-port")

# Actual master executable
add_executable(kudu-master master_main.cc)
target_link_libraries(kudu-master
  master
  ${KUDU_BASE_LIBS})
