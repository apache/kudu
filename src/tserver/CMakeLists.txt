# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(TSERVER_PROTO_SRCS TSERVER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES tserver.proto)

add_library(tserver_proto ${TSERVER_PROTO_SRCS} ${TSERVER_PROTO_HDRS})
ADD_DEPENDENCIES(tserver_proto kudu_common)
ADD_DEPENDENCIES(tserver_proto metadata_proto)

KRPC_GENERATE(TSERVER_KRPC_SRCS TSERVER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES tserver_service.proto)

add_library(tserver_service_proto ${TSERVER_KRPC_SRCS} ${TSERVER_KRPC_HDRS})
target_link_libraries(tserver_service_proto krpc)
add_dependencies(tserver_service_proto kudu_common tserver_proto)


set(TSERVER_SRCS
  heartbeater.cc
  mini_tablet_server.cc
  scanners.cc
  tablet_server.cc
  tablet_server_options.cc
  tablet_service.cc
  ts_tablet_manager.cc
  tserver-path-handlers.cc
)

add_library(tserver ${TSERVER_SRCS})
target_link_libraries(tserver tserver_proto)
target_link_libraries(tserver tserver_service_proto)
target_link_libraries(tserver master_proto)
target_link_libraries(tserver consensus_proto)
target_link_libraries(tserver log_proto)
target_link_libraries(tserver log)
target_link_libraries(tserver consensus)

add_dependencies(tserver kudu_common)
add_dependencies(tserver server)
add_dependencies(tserver tablet)
add_dependencies(tserver tserver_proto)
add_dependencies(tserver tserver_service_proto)
add_dependencies(tserver krpc)
add_dependencies(tserver log)
add_dependencies(tserver log_proto)
add_dependencies(tserver consensus)
add_dependencies(tserver consensus_proto)

# Tests
ADD_KUDU_TEST(tablet_server-test)
ADD_KUDU_TEST(scanners-test)
ADD_KUDU_TEST(ts_tablet_manager-test)

add_executable(kudu-tablet_server tablet_server_main.cc)
target_link_libraries(kudu-tablet_server ${KUDU_LINK_LIBS})
