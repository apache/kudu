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
    sasl_common.cc
    serialization.cc
    server_call.cc
    service_if.cc
    service_pool.cc
    transfer.cc
)

TARGET_LINK_LIBRARIES(krpc
  rpc_header_proto
  kudu_util
  gutil
  ${LIBEV_STATIC_LIB}
  ${CYRUS_SASL_STATIC_LIB}
  ${CYRUS_SASL_ANON_DYN_LIB}
  ${CYRUS_SASL_PLAIN_DYN_LIB}
  dl # needed by cyrus-sasl on g++ but not clang++ for some reason
  crypt
  ${KUDU_DEP_LIBS}
)
SET(RPC_LIBS krpc)

### RPC generator tool
add_executable(protoc-gen-krpc protoc-gen-krpc.cc)
target_link_libraries(protoc-gen-krpc
    ${KUDU_DEP_LIBS} 
    ${PROTOBUF_PROTOC_LIBRARY} 
    ${PROTOBUF_LIBRARY} 
    gutil
    kudu_util)

#### RPC test
KRPC_GENERATE(RTEST_KRPC_SRCS RTEST_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES rtest.proto)

add_library(rtest_krpc ${RTEST_KRPC_SRCS} ${RTEST_KRPC_HDRS})

ADD_KUDU_TEST(rpc-test)
TARGET_LINK_LIBRARIES(rpc-test rtest_krpc krpc)

ADD_KUDU_TEST(rpc-bench)
TARGET_LINK_LIBRARIES(rpc-bench rtest_krpc krpc)

ADD_KUDU_TEST(rpc_stub-test)
TARGET_LINK_LIBRARIES(rpc_stub-test rtest_krpc krpc)

ADD_KUDU_TEST(sasl_rpc-test)
TARGET_LINK_LIBRARIES(sasl_rpc-test rtest_krpc krpc)
