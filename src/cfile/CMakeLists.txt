# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(CFILE_PROTO_SRCS CFILE_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES cfile.proto)

add_library(cfile_proto ${CFILE_PROTO_SRCS} ${CFILE_PROTO_HDRS})

SET(CFILE_SRCS cfile.cc index_block.cc index_btree.cc cfile_reader.cc block_encodings.cc )

add_library(cfile ${CFILE_SRCS} ${CFILE_PROTO_SRCS})
ADD_DEPENDENCIES(cfile kudu_common)
ADD_DEPENDENCIES(cfile cfile_proto)

# index-test
add_executable(index-test index-test.cc)
TARGET_LINK_LIBRARIES(index-test cfile ${KUDU_TEST_LINK_LIBS})

# cfile-test
add_executable(cfile-test cfile-test.cc)
TARGET_LINK_LIBRARIES(cfile-test cfile ${KUDU_TEST_LINK_LIBS})

# encoding-test
add_executable(encoding-test encoding-test.cc)
TARGET_LINK_LIBRARIES(encoding-test cfile ${KUDU_TEST_LINK_LIBS})