# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(CFILE_PROTO_SRCS CFILE_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES cfile.proto)

add_library(cfile_proto ${CFILE_PROTO_SRCS} ${CFILE_PROTO_HDRS})
ADD_DEPENDENCIES(cfile_proto kudu_common)

SET(CFILE_SRCS cfile.cc index_block.cc index_btree.cc cfile_reader.cc
  gvint_block.cc
  string_prefix_block.cc
)

add_library(cfile ${CFILE_SRCS} ${CFILE_PROTO_SRCS})
ADD_DEPENDENCIES(cfile kudu_common)
ADD_DEPENDENCIES(cfile cfile_proto)

# Tests
ADD_KUDU_TEST(index-test)
ADD_KUDU_TEST(cfile-test)
ADD_KUDU_TEST(encoding-test)


# Tools
ADD_EXECUTABLE(cfile-dump cfile-dump.cc)
TARGET_LINK_LIBRARIES(cfile-dump ${KUDU_LINK_LIBS})