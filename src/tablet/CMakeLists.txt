# Copyright (c) 2012, Cloudera, inc.

SET(TABLET_SRCS
  tablet.cc tablet-util.cc
  cfile_set.cc layer.cc
  memstore.cc
  deltamemstore.cc deltafile.cc delta_tracker.cc
)
ADD_LIBRARY(tablet ${TABLET_SRCS})
ADD_DEPENDENCIES(tablet kudu_common)
ADD_DEPENDENCIES(tablet cfile_proto)

# tablet-test
ADD_KUDU_TEST(tablet-test)
ADD_KUDU_TEST(mt-tablet-test)
ADD_KUDU_TEST(layer-test)
ADD_KUDU_TEST(mt-layer-test)
ADD_KUDU_TEST(memstore-test)
ADD_KUDU_TEST(deltamemstore-test)
ADD_KUDU_TEST(deltafile-test)
ADD_KUDU_TEST(cfile_set-test)
ADD_KUDU_TEST(tablet-pushdown-test)

# cbtree-test doesn't have dependencies on other tablet stuff
SET(KUDU_TEST_LINK_LIBS kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(cbtree-test)
