# Copyright (c) 2013, Cloudera, inc.

PROTOBUF_GENERATE_CPP(METADATA_PROTO_SRCS METADATA_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES metadata.proto)

add_library(metadata_proto ${METADATA_PROTO_SRCS} ${METADATA_PROTO_HDRS})
add_dependencies(metadata_proto kudu_common)

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
  tcmalloc_metrics.cc
  webserver.cc
  webserver_options.cc
)

add_library(server ${SERVER_SRCS} ${METADATA_PROTO_SRCS})
add_dependencies(server kudu_common)
add_dependencies(server cfile_proto)
add_dependencies(server metadata_proto)
add_dependencies(server krpc)

# Tests
ADD_KUDU_TEST(fsmanager-test)
ADD_KUDU_TEST(webserver-test)
