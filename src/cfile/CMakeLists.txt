# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(CFILE_PROTO_SRCS CFILE_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES cfile.proto)

add_library(cfile_proto ${CFILE_PROTO_SRCS} ${CFILE_PROTO_HDRS})
add_dependencies(cfile_proto kudu_common_proto)

set(CFILE_SRCS cfile.cc index_block.cc index_btree.cc cfile_reader.cc

  block_cache.cc
  bloomfile.cc
  type_encodings.cc

  compression_codec.cc
  block_compression.cc

  gvint_block.cc
  string_prefix_block.cc
  string_plain_block.cc
)

add_library(cfile ${CFILE_SRCS})
target_link_libraries(cfile kudu_common)
target_link_libraries(cfile cfile_proto)
target_link_libraries(cfile ${KUDU_DEP_LIBS})

# Tests
ADD_KUDU_TEST(index-test)
ADD_KUDU_TEST(cfile-test)
ADD_KUDU_TEST(encoding-test)
ADD_KUDU_TEST(bloomfile-test)
ADD_KUDU_TEST(block_cache-test)
ADD_KUDU_TEST(compression-test)

# Tools
add_executable(cfile-dump cfile-dump.cc)
target_link_libraries(cfile-dump ${KUDU_LINK_LIBS})
