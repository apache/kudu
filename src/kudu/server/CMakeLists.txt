# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

#########################################
# metadata_proto
#########################################

PROTOBUF_GENERATE_CPP(METADATA_PROTO_SRCS METADATA_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES metadata.proto)

add_library(metadata_proto ${METADATA_PROTO_SRCS} ${METADATA_PROTO_HDRS})
target_link_libraries(metadata_proto
  kudu_common_proto
  fs_proto
  protobuf)

#########################################
# server_common
#########################################

set(SERVER_COMMON_SRCS
  hybrid_clock.cc
  logical_clock.cc
)

add_library(server_common ${SERVER_COMMON_SRCS})
target_link_libraries(server_common
  kudu_common
  codegen
  gutil
  kudu_fs
  kudu_util
  metadata_proto)

#########################################
# server_common tests
#########################################

set(KUDU_TEST_LINK_LIBS server_common ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(hybrid_clock-test)
ADD_KUDU_TEST(logical_clock-test)

#########################################
# server_base_proto
#########################################

PROTOBUF_GENERATE_CPP(SERVER_BASE_PROTO_SRCS SERVER_BASE_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES server_base.proto)

add_library(server_base_proto ${SERVER_BASE_PROTO_SRCS} ${SERVER_BASE_PROTO_HDRS})
target_link_libraries(server_base_proto
  protobuf
  wire_protocol_proto
  kudu_common_proto)

#########################################
# server_process
#########################################

set(SERVER_PROCESS_SRCS
  global-flags.cc
  default-path-handlers.cc
  pprof-path-handlers.cc
  rpcz-path-handler.cc
  rpc_server.cc
  server_base.cc
  server_base_options.cc
  tcmalloc_metrics.cc
  webserver.cc
  webserver_options.cc
  webui_util.cc
)

add_library(server_process ${SERVER_PROCESS_SRCS})
target_link_libraries(server_process
  server_base_proto
  server_common
  kudu_common
  kudu_fs
  gutil
  krpc
  kudu_util
  squeasel)

# This module depends on tcmalloc and profiler directly, so need to make
# sure that they get linked in the right order.
if(${KUDU_TCMALLOC_AVAILABLE})
  target_link_libraries(server_process tcmalloc profiler)
endif()

#########################################
# server_process tests
#########################################

set(KUDU_TEST_LINK_LIBS server_process ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(webserver-test)
