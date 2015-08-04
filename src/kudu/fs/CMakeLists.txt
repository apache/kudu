# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

PROTOBUF_GENERATE_CPP(
  FS_PROTO_SRCS FS_PROTO_HDRS FS_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES fs.proto)
ADD_EXPORTABLE_LIBRARY(fs_proto
  SRCS ${FS_PROTO_SRCS}
  DEPS protobuf
  NONLINK_DEPS ${FS_PROTO_TGTS})

add_library(kudu_fs
  block_id.cc
  block_manager.cc
  block_manager_metrics.cc
  block_manager_util.cc
  file_block_manager.cc
  fs_manager.cc
  log_block_manager.cc)

target_link_libraries(kudu_fs
  fs_proto
  kudu_util
  gutil)

# Tests
set(KUDU_TEST_LINK_LIBS kudu_fs ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(block_manager-test)
ADD_KUDU_TEST(block_manager_util-test)
ADD_KUDU_TEST(block_manager-stress-test RUN_SERIAL true)
ADD_KUDU_TEST(fs_manager-test)
