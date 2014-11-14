# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

#### Global header protobufs
PROTOBUF_GENERATE_CPP(RPC_HEADER_PROTO_SRCS RPC_HEADER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES rpc_header.proto)
add_library(rpc_header_proto ${RPC_HEADER_PROTO_SRCS} ${RPC_HEADER_PROTO_HDRS})
target_link_libraries(rpc_header_proto
  protobuf)

PROTOBUF_GENERATE_CPP(RPC_INTROSPECTION_PROTO_SRCS RPC_INTROSPECTION_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES rpc_introspection.proto)
add_library(rpc_introspection_proto ${RPC_INTROSPECTION_PROTO_SRCS} ${RPC_INTROSPECTION_PROTO_HDRS})
target_link_libraries(rpc_introspection_proto
  rpc_header_proto
  protobuf)

### RPC library
add_library(krpc
    acceptor_pool.cc
    auth_store.cc
    blocking_ops.cc
    outbound_call.cc
    connection.cc
    constants.cc
    inbound_call.cc
    messenger.cc
    negotiation.cc
    proxy.cc
    reactor.cc
    rpc.cc
    rpc_context.cc
    rpc_controller.cc
    sasl_common.cc
    sasl_client.cc
    sasl_helper.cc
    sasl_server.cc
    serialization.cc
    service_if.cc
    service_pool.cc
    transfer.cc
)

target_link_libraries(krpc
  rpc_header_proto
  rpc_introspection_proto
  kudu_util
  gutil
  libev
  cyrus_sasl
)
set(RPC_LIBS krpc)

### RPC generator tool
add_executable(protoc-gen-krpc protoc-gen-krpc.cc)
target_link_libraries(protoc-gen-krpc
    ${KUDU_BASE_LIBS}
    protoc
    protobuf
    gutil
    kudu_util)

#### RPC test
PROTOBUF_GENERATE_CPP(RPC_TEST_DIFF_PACKAGE_SRCS RPC_TEST_DIFF_PACKAGE_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES rtest_diff_package.proto)
add_library(rtest_diff_package_proto ${RPC_TEST_DIFF_PACKAGE_SRCS} ${RPC_TEST_DIFF_PACKAGE_HDRS})
target_link_libraries(rtest_diff_package_proto rpc_header_proto)

KRPC_GENERATE(RTEST_KRPC_SRCS RTEST_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES rtest.proto)
add_library(rtest_krpc ${RTEST_KRPC_SRCS} ${RTEST_KRPC_HDRS})
target_link_libraries(rtest_krpc
  rtest_diff_package_proto)

# Tests
set(KUDU_TEST_LINK_LIBS rtest_krpc krpc ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(mt-rpc-test RUN_SERIAL true)
ADD_KUDU_TEST(reactor-test)
ADD_KUDU_TEST(rpc-bench RUN_SERIAL true)
ADD_KUDU_TEST(rpc-test)
ADD_KUDU_TEST(rpc_stub-test)
ADD_KUDU_TEST(sasl_rpc-test)
