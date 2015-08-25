# Copyright (c) 2012, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

PROTOBUF_GENERATE_CPP(
  COMMON_PROTO_SRCS COMMON_PROTO_HDRS COMMON_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES common.proto)
ADD_EXPORTABLE_LIBRARY(kudu_common_proto
  SRCS ${COMMON_PROTO_SRCS}
  DEPS protobuf
  NONLINK_DEPS ${COMMON_PROTO_TGTS})

PROTOBUF_GENERATE_CPP(
  WIRE_PROTOCOL_PROTO_SRCS WIRE_PROTOCOL_PROTO_HDRS WIRE_PROTOCOL_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES wire_protocol.proto)
set(WIRE_PROTOCOL_PROTO_LIBS
  kudu_common_proto
  consensus_metadata_proto
  protobuf)
ADD_EXPORTABLE_LIBRARY(wire_protocol_proto
  SRCS ${WIRE_PROTOCOL_PROTO_SRCS}
  DEPS ${WIRE_PROTOCOL_PROTO_LIBS}
  NONLINK_DEPS ${WIRE_PROTOCOL_PROTO_TGTS})

set(COMMON_SRCS
  encoded_key.cc
  generic_iterators.cc
  id_mapping.cc
  iterator_stats.cc
  key_encoder.cc
  partial_row.cc
  partition.cc
  predicate_encoder.cc
  rowblock.cc
  row_changelist.cc
  row_key-util.cc
  row_operations.cc
  scan_predicate.cc
  scan_spec.cc
  schema.cc
  timestamp.cc
  types.cc
  wire_protocol.cc)

# Workaround for clang bug https://llvm.org/bugs/show_bug.cgi?id=23757
# in which it incorrectly optimizes row_key-util.cc and causes incorrect results.
if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_source_files_properties(row_key-util.cc PROPERTIES COMPILE_FLAGS -fwrapv)
endif()

set(COMMON_LIBS
  kudu_common_proto
  consensus_metadata_proto
  wire_protocol_proto
  kudu_util
  gutil)

ADD_EXPORTABLE_LIBRARY(kudu_common
  SRCS ${COMMON_SRCS}
  DEPS ${COMMON_LIBS})

set(KUDU_TEST_LINK_LIBS kudu_common ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(encoded_key-test)
ADD_KUDU_TEST(generic_iterators-test)
ADD_KUDU_TEST(id_mapping-test)
ADD_KUDU_TEST(partial_row-test)
ADD_KUDU_TEST(partition-test)
ADD_KUDU_TEST(predicate-test)
ADD_KUDU_TEST(predicate_encoder-test)
ADD_KUDU_TEST(row_changelist-test)
ADD_KUDU_TEST(row_key-util-test)
ADD_KUDU_TEST(row_operations-test)
ADD_KUDU_TEST(schema-test)
ADD_KUDU_TEST(wire_protocol-test)
