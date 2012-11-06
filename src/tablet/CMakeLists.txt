# Copyright (c) 2012, Cloudera, inc.

SET(TABLET_SRCS memstore.cc)
ADD_LIBRARY(tablet ${TABLET_SRCS})

# tablet-test
ADD_EXECUTABLE(tablet-test tablet-test.cc)
TARGET_LINK_LIBRARIES(tablet-test tablet ${KUDU_TEST_LINK_LIBS})

# schema-test
ADD_EXECUTABLE(schema-test schema-test.cc)
TARGET_LINK_LIBRARIES(schema-test ${KUDU_TEST_LINK_LIBS})

