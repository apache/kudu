# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(CFILE_PROTO_SRCS CFILE_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES cfile.proto)

add_library(cfile_proto ${CFILE_PROTO_SRCS} ${CFILE_PROTO_HDRS})
target_link_libraries(cfile_proto
  kudu_common_proto
  protobuf)

add_library(cfile
  block_cache.cc
  block_compression.cc
  bloomfile.cc
  cfile.cc
  cfile_reader.cc
  cfile_util.cc
  compression_codec.cc
  gvint_block.cc
  index_block.cc
  index_btree.cc
  string_plain_block.cc
  string_prefix_block.cc
  type_encodings.cc)

target_link_libraries(cfile
  kudu_common
  kudu_util
  gutil
  cfile_proto
  lz4
  snappy
  zlib)

# Tests
set(KUDU_TEST_LINK_LIBS cfile ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(index-test)
ADD_KUDU_TEST(cfile-test)
ADD_KUDU_TEST(encoding-test LABELS no_tsan)
ADD_KUDU_TEST(bloomfile-test)
ADD_KUDU_TEST(block_cache-test)
ADD_KUDU_TEST(compression-test)

# Tools
add_executable(cfile-dump cfile-dump.cc)
target_link_libraries(cfile-dump cfile ${KUDU_BASE_LIBS})
