# Copyright (c) 2013, Cloudera, inc.

SET(SERVER_SRCS
  fsmanager.cc
  oid_generator.cc
)

add_library(server ${SERVER_SRCS} ${SERVER_PROTO_SRCS})
ADD_DEPENDENCIES(server kudu_common)
ADD_DEPENDENCIES(server cfile_proto)

# Tests
SET(KUDU_TEST_LINK_LIBS server kudu_common kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(fsmanager-test)
