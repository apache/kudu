# Copyright (c) 2013, Cloudera, inc.

PROTOBUF_GENERATE_CPP(METADATA_PROTO_SRCS METADATA_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES metadata.proto)

add_library(metadata_proto ${METADATA_PROTO_SRCS} ${METADATA_PROTO_HDRS})
target_link_libraries(metadata_proto
  kudu_common_proto
  protobuf)

set(SERVER_COMMON_SRCS
  fsmanager.cc
  hybrid_clock.cc
  logical_clock.cc
  metadata.cc
  metadata_util.cc
  oid_generator.cc
)

add_library(server_common ${SERVER_COMMON_SRCS})
target_link_libraries(server_common
  kudu_common
  gutil
  kudu_util
  metadata_proto)

set(KUDU_TEST_LINK_LIBS server_common ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(fsmanager-test)
ADD_KUDU_TEST(logical_clock-test)
ADD_KUDU_TEST(hybrid_clock-test)

#------------------------------

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

PROTOBUF_GENERATE_CPP(SERVER_BASE_PROTO_SRCS SERVER_BASE_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES server_base.proto)

add_library(server_process ${SERVER_PROCESS_SRCS} ${SERVER_BASE_PROTO_SRCS} ${SERVER_BASE_PROTO_HDRS})
target_link_libraries(server_process
  server_common
  kudu_common
  gutil
  krpc
  kudu_util
  squeasel)

# This module depends on tcmalloc and profiler directly, so need to make
# sure that they get linked in the right order.
if(${KUDU_TCMALLOC_AVAILABLE})
  target_link_libraries(server_process tcmalloc profiler)
endif()

# Tests
set(KUDU_TEST_LINK_LIBS server_process ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(webserver-test)
