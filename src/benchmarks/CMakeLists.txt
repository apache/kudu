# Copyright (c) 2012, Cloudera, inc.

SET(TPCH_SRCS
  tpch/local_line_item_dao.cc
  tpch/rpc_line_item_dao.cc
)

ADD_LIBRARY(tpch ${TPCH_SRCS})
target_link_libraries(tpch client)
ADD_DEPENDENCIES(tpch client)

# tpch1
ADD_EXECUTABLE(tpch1 tpch/tpch1.cc)
ADD_DEPENDENCIES(tpch1 tpch)
TARGET_LINK_LIBRARIES(tpch1 client tpch ${KUDU_TEST_LINK_LIBS})

# ms3_demo
ADD_EXECUTABLE(ms3_demo tpch/ms3_demo.cc tpch/rpc_line_item_dao.cc)
ADD_DEPENDENCIES(ms3_demo client)
TARGET_LINK_LIBRARIES(ms3_demo client tpch ${KUDU_TEST_LINK_LIBS})

# rle
ADD_EXECUTABLE(rle rle.cc)
TARGET_LINK_LIBRARIES(rle ${KUDU_TEST_LINK_LIBS})

# Tests
SET(KUDU_TEST_LINK_LIBS tpch ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(tpch/rpc_line_item_dao-test)
