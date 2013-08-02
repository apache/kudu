# Copyright (c) 2013, Cloudera, inc.

PROTOBUF_GENERATE_CPP(METADATA_PROTO_SRCS METADATA_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES metadata.proto)

add_library(metadata_proto ${METADATA_PROTO_SRCS} ${METADATA_PROTO_HDRS})
ADD_DEPENDENCIES(metadata_proto kudu_common)
ADD_DEPENDENCIES(metadata_proto cfile_proto)

SET(SERVER_SRCS
  fsmanager.cc
  metadata.cc
  metadata_util.cc
  oid_generator.cc
)

add_library(server ${SERVER_SRCS} ${METADATA_PROTO_SRCS})
ADD_DEPENDENCIES(server kudu_common)
ADD_DEPENDENCIES(server cfile_proto)
ADD_DEPENDENCIES(server metadata_proto)

# Tests
SET(KUDU_TEST_LINK_LIBS server kudu_common kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(fsmanager-test)
