# Copyright (c) 2012, Cloudera, inc.

SET(TABLET_SRCS
  tasks/tasks.cc
  tablet.cc
  tablet_bootstrap.cc
  tablet_peer.cc
  transaction_context.cc
  cfile_set.cc
  compaction.cc
  compaction_policy.cc
  diskrowset.cc
  lock_manager.cc
  memrowset.cc
  mutation.cc
  mvcc.cc
  rowset.cc
  rowset_tree.cc
  deltamemstore.cc deltafile.cc delta_tracker.cc
  delta_compaction.cc
)

PROTOBUF_GENERATE_CPP(TABLET_PROTO_SRCS TABLET_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES tablet.proto)

add_library(tablet_proto ${TABLET_PROTO_SRCS} ${TABLET_PROTO_HDRS})
ADD_DEPENDENCIES(tablet_proto kudu_common)

SET(TABLET_SRCS ${TABLET_SRCS} ${TABLET_PROTO_SRCS})

ADD_LIBRARY(tablet ${TABLET_SRCS})
ADD_DEPENDENCIES(tablet kudu_common)
ADD_DEPENDENCIES(tablet gutil)
ADD_DEPENDENCIES(tablet cfile_proto)
ADD_DEPENDENCIES(tablet server)
ADD_DEPENDENCIES(tablet consensus_proto)
ADD_DEPENDENCIES(tablet log_proto)

# tablet-test
ADD_KUDU_TEST(tablet-test)
ADD_KUDU_TEST(mt-tablet-test RUN_SERIAL true)
ADD_KUDU_TEST(diskrowset-test)
ADD_KUDU_TEST(mt-diskrowset-test RUN_SERIAL true)
ADD_KUDU_TEST(memrowset-test)
ADD_KUDU_TEST(deltamemstore-test)
ADD_KUDU_TEST(deltafile-test)
ADD_KUDU_TEST(cfile_set-test)
ADD_KUDU_TEST(tablet-pushdown-test)
ADD_KUDU_TEST(tablet-schema-test)
ADD_KUDU_TEST(mvcc-test)
ADD_KUDU_TEST(compaction-test)
ADD_KUDU_TEST(compaction_policy-test)
ADD_KUDU_TEST(lock_manager-test)
ADD_KUDU_TEST(rowset_tree-test)
ADD_KUDU_TEST(composite-pushdown-test)
ADD_KUDU_TEST(delta_compaction-test)
ADD_KUDU_TEST(mt-rowset_delta_compaction-test)
ADD_KUDU_TEST(major_delta_compaction-test)

# Some tests don't have dependencies on other tablet stuff
SET(KUDU_TEST_LINK_LIBS kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(cbtree-test RUN_SERIAL true)
