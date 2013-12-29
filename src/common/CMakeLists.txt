# Copyright (c) 2012, Cloudera, inc.

set(COMMON_SRCS
  generic_iterators.cc
  key_encoder.cc
  rowblock.cc
  row_changelist.cc
  wire_protocol.cc
  scan_predicate.cc
  predicate_encoder.cc
  encoded_key.cc
  scan_spec.cc
  schema.cc
  types.cc
)

PROTOBUF_GENERATE_CPP(COMMON_PROTO_SRCS COMMON_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES common.proto)
set(COMMON_SRCS ${COMMON_SRCS} ${COMMON_PROTO_SRCS})

PROTOBUF_GENERATE_CPP(WIRE_PROTOCOL_PROTO_SRCS WIRE_PROTOCOL_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES wire_protocol.proto)
set(COMMON_SRCS ${COMMON_SRCS} ${WIRE_PROTOCOL_PROTO_SRCS})

add_library(kudu_common ${COMMON_SRCS})
set(KUDU_COMMON_LIBS kudu_common)

set(KUDU_TEST_LINK_LIBS kudu_common kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(schema-test)
ADD_KUDU_TEST(generic_iterators-test)
ADD_KUDU_TEST(row_changelist-test)
ADD_KUDU_TEST(predicate-test)
ADD_KUDU_TEST(wire_protocol-test)
ADD_KUDU_TEST(predicate_encoder-test)
