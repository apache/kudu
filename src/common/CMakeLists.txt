# Copyright (c) 2012, Cloudera, inc.

SET(COMMON_SRCS
  generic_iterators.cc
  key_encoder.cc
  rowblock.cc
  row_changelist.cc
  scan_predicate.cc
  scan_spec.cc
  types.cc
)
PROTOBUF_GENERATE_CPP(COMMON_PROTO_SRCS COMMON_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES common.proto)
SET(COMMON_SRCS ${COMMON_SRCS} ${COMMON_PROTO_SRCS})
ADD_LIBRARY(kudu_common ${COMMON_SRCS})
SET(KUDU_COMMON_LIBS kudu_common)

SET(KUDU_TEST_LINK_LIBS kudu_common kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(schema-test)
ADD_KUDU_TEST(generic_iterators-test)
ADD_KUDU_TEST(row_changelist-test)
ADD_KUDU_TEST(predicate-test)
