# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(COMMON_PROTO_SRCS COMMON_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES common.proto)

add_library(kudu_common_proto ${COMMON_PROTO_SRCS} ${COMMON_PROTO_HDRS})
target_link_libraries(kudu_common_proto
  protobuf)

PROTOBUF_GENERATE_CPP(WIRE_PROTOCOL_PROTO_SRCS WIRE_PROTOCOL_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES wire_protocol.proto)

add_library(wire_protocol_proto ${WIRE_PROTOCOL_PROTO_SRCS} ${WIRE_PROTOCOL_PROTO_HDRS})
target_link_libraries(wire_protocol_proto
  kudu_common_proto
  protobuf)

add_library(kudu_common
  generic_iterators.cc
  iterator_stats.cc
  key_encoder.cc
  partial_row.cc
  rowblock.cc
  row_changelist.cc
  row_operations.cc
  wire_protocol.cc
  scan_predicate.cc
  predicate_encoder.cc
  encoded_key.cc
  scan_spec.cc
  schema.cc
  timestamp.cc
  types.cc
  maintenance_manager.cc
)

target_link_libraries(kudu_common
  kudu_common_proto
  wire_protocol_proto
  kudu_util
  gutil)

set(KUDU_TEST_LINK_LIBS kudu_common ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(encoded_key-test)
ADD_KUDU_TEST(generic_iterators-test)
ADD_KUDU_TEST(maintenance_manager-test)
ADD_KUDU_TEST(partial_row-test)
ADD_KUDU_TEST(predicate-test)
ADD_KUDU_TEST(predicate_encoder-test)
ADD_KUDU_TEST(row_changelist-test)
ADD_KUDU_TEST(row_operations-test)
ADD_KUDU_TEST(schema-test)
ADD_KUDU_TEST(wire_protocol-test)
