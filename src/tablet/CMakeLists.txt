# Copyright (c) 2012, Cloudera, inc.

SET(TABLET_SRCS memstore.cc tablet.cc layer.cc deltamemstore.cc)
ADD_LIBRARY(tablet ${TABLET_SRCS})

# tablet-test
ADD_EXECUTABLE(tablet-test tablet-test.cc)
TARGET_LINK_LIBRARIES(tablet-test tablet ${KUDU_TEST_LINK_LIBS})

# layer-test
ADD_EXECUTABLE(layer-test layer-test.cc)
TARGET_LINK_LIBRARIES(layer-test tablet ${KUDU_TEST_LINK_LIBS})

# schema-test
ADD_EXECUTABLE(schema-test schema-test.cc)
TARGET_LINK_LIBRARIES(schema-test ${KUDU_TEST_LINK_LIBS})

# deltamemstore-test
ADD_EXECUTABLE(deltamemstore-test deltamemstore-test.cc)
TARGET_LINK_LIBRARIES(deltamemstore-test tablet ${KUDU_TEST_LINK_LIBS})

