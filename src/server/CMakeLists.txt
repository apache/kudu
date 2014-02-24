# Copyright (c) 2013, Cloudera, inc.

PROTOBUF_GENERATE_CPP(METADATA_PROTO_SRCS METADATA_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES metadata.proto)

add_library(metadata_proto ${METADATA_PROTO_SRCS} ${METADATA_PROTO_HDRS})
add_dependencies(metadata_proto kudu_common_proto)

set(SERVER_SRCS
  default-path-handlers.cc
  fsmanager.cc
  global-flags.cc
  metadata.cc
  metadata_util.cc
  oid_generator.cc
  pprof-path-handlers.cc
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

add_library(server ${SERVER_SRCS} ${SERVER_BASE_PROTO_SRCS} ${SERVER_BASE_PROTO_HDRS})
add_dependencies(server kudu_common)
add_dependencies(server cfile_proto)
target_link_libraries(server metadata_proto)
add_dependencies(server krpc)

# Tests
ADD_KUDU_TEST(fsmanager-test)
ADD_KUDU_TEST(webserver-test)
