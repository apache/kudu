# Copyright (c) 2014, Cloudera, inc.

PROTOBUF_GENERATE_CPP(FS_PROTO_SRCS FS_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES fs.proto)

add_library(fs_proto ${FS_PROTO_SRCS} ${FS_PROTO_HDRS})

add_library(kudu_fs
  block_id.cc
  fs_manager.cc)

target_link_libraries(kudu_fs
  fs_proto
  kudu_util
  gutil)

# Tests
set(KUDU_TEST_LINK_LIBS kudu_fs ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(fs_manager-test)