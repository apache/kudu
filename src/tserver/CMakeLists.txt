# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(TSERVER_PROTO_SRCS TSERVER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES tserver.proto)
KRPC_GENERATE(TSERVER_KRPC_SRCS TSERVER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES tserver.proto)

add_library(tserver_proto ${TSERVER_PROTO_SRCS} ${TSERVER_PROTO_HDRS} ${TSERVER_KRPC_SRCS})
ADD_DEPENDENCIES(tserver_proto kudu_common)
ADD_DEPENDENCIES(tserver_proto rpc_header_proto)

SET(TSERVER_SRCS
  mini_tablet_server.cc
  scanners.cc
  tablet_server.cc
  tablet_service.cc
)

add_library(tserver ${TSERVER_SRCS})
target_link_libraries(tserver tserver_proto)

ADD_DEPENDENCIES(tserver kudu_common)
ADD_DEPENDENCIES(tserver server)
ADD_DEPENDENCIES(tserver tablet)
ADD_DEPENDENCIES(tserver tserver_proto)
ADD_DEPENDENCIES(tserver krpc)

# Tests
ADD_KUDU_TEST(tablet_server-test RESOURCE_LOCK "tserver-web-port")
ADD_KUDU_TEST(scanners-test)

ADD_EXECUTABLE(tablet_server tablet_server_main.cc)
TARGET_LINK_LIBRARIES(tablet_server ${KUDU_LINK_LIBS})
