# Copyright (c) 2013, Cloudera, inc.

#### Global header protobufs
PROTOBUF_GENERATE_CPP(RPC_HEADER_PROTO_SRCS RPC_HEADER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES rpc_header.proto)
add_library(rpc_header_proto ${RPC_HEADER_PROTO_SRCS} ${RPC_HEADER_PROTO_HDFS})
target_link_libraries(rpc_header_proto protobuf)

### RPC library
ADD_LIBRARY(krpc
    acceptor_pool.cc
    client_call.cc
    connection.cc
    messenger.cc
    proxy.cc
    reactor.cc
    rpc_controller.cc
    server_call.cc
    service_if.cc
    service_pool.cc
    sockaddr.cc
    socket.cc
    transfer.cc
)

TARGET_LINK_LIBRARIES(krpc
  rpc_header_proto
  kudu_util
  gutil
  ${LIBEV_STATIC_LIB}
  ${KUDU_DEP_LIBS}
)
SET(RPC_LIBS krpc)

#### RPC test
PROTOBUF_GENERATE_CPP(RTEST_KRPC_SRCS RTEST_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES rtest.proto)

add_library(rtest_krpc ${RTEST_KRPC_SRCS} ${RTEST_KRPC_HDRS})

ADD_KUDU_TEST(rpc-test)
TARGET_LINK_LIBRARIES(rpc-test rtest_krpc krpc)
