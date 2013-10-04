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
  tablet_server_options.cc
  tablet_service.cc
  ts_tablet_manager.cc
)

add_library(tserver ${TSERVER_SRCS})
target_link_libraries(tserver tserver_proto)
target_link_libraries(tserver consensus_proto)
target_link_libraries(tserver log_proto)
target_link_libraries(tserver log)
target_link_libraries(tserver consensus)

ADD_DEPENDENCIES(tserver kudu_common)
ADD_DEPENDENCIES(tserver server)
ADD_DEPENDENCIES(tserver tablet)
ADD_DEPENDENCIES(tserver tserver_proto)
ADD_DEPENDENCIES(tserver krpc)
ADD_DEPENDENCIES(tserver log)
ADD_DEPENDENCIES(tserver log_proto)
ADD_DEPENDENCIES(tserver consensus)
ADD_DEPENDENCIES(tserver consensus_proto)

# Tests
#SET(KUDU_TEST_LINK_LIBS tserver ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(tablet_server-test)
ADD_KUDU_TEST(scanners-test)

ADD_EXECUTABLE(kudu-tablet_server tablet_server_main.cc)
TARGET_LINK_LIBRARIES(kudu-tablet_server ${KUDU_LINK_LIBS})
