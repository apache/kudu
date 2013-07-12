# Copyright (c) 2012, Cloudera, inc.

SET(TABLET_SRCS
  tablet.cc
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
)
ADD_LIBRARY(tablet ${TABLET_SRCS})
ADD_DEPENDENCIES(tablet kudu_common)
ADD_DEPENDENCIES(tablet cfile_proto)

# tablet-test
ADD_KUDU_TEST(tablet-test)
ADD_KUDU_TEST(mt-tablet-test)
ADD_KUDU_TEST(diskrowset-test)
ADD_KUDU_TEST(mt-diskrowset-test)
ADD_KUDU_TEST(memrowset-test)
ADD_KUDU_TEST(deltamemstore-test)
ADD_KUDU_TEST(deltafile-test)
ADD_KUDU_TEST(cfile_set-test)
ADD_KUDU_TEST(tablet-pushdown-test)
ADD_KUDU_TEST(mvcc-test)
ADD_KUDU_TEST(compaction-test)
ADD_KUDU_TEST(rowset_tree-test)

# Some tests don't have dependencies on other tablet stuff
SET(KUDU_TEST_LINK_LIBS kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(cbtree-test)
