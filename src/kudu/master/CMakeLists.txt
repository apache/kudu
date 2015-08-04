# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

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
  catalog_manager.cc
  master.cc
  master_options.cc
  master_service.cc
  master-path-handlers.cc
  mini_master.cc
  sys_catalog.cc
  ts_descriptor.cc
  ts_manager.cc
)

add_library(master ${MASTER_SRCS})
target_link_libraries(master
  kudu_common
  tablet
  server_common
  server_process
  krpc
  gutil
  kudu_util
  tserver
  tserver_service_proto
  master_proto
  rpc_header_proto)

set(MASTER_RPC_SRCS
  master_rpc.cc)
set(MASTER_RPC_LIBS
  kudu_common
  krpc
  gutil
  kudu_util
  master_proto
  rpc_header_proto)
ADD_EXPORTABLE_LIBRARY(master_rpc
  SRCS ${MASTER_RPC_SRCS}
  DEPS ${MASTER_RPC_LIBS})

# Tests
set(KUDU_TEST_LINK_LIBS master master_proto kudu_client ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(catalog_manager-test)
ADD_KUDU_TEST(master-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(sys_catalog-test RESOURCE_LOCK "master-web-port")

# Actual master executable
add_executable(kudu-master master_main.cc)
target_link_libraries(kudu-master
  master
  ${KUDU_BASE_LIBS})
