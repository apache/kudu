# Copyright (c) 2012, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

set(TPCH_SRCS
  tpch/rpc_line_item_dao.cc
)

add_library(tpch ${TPCH_SRCS})
target_link_libraries(tpch
  kudu_client
  integration-tests)

# tpch1
add_executable(tpch1 tpch/tpch1.cc)
target_link_libraries(tpch1
  tpch
  ${KUDU_TEST_LINK_LIBS})

# tpch_real_world
add_executable(tpch_real_world tpch/tpch_real_world.cc)
target_link_libraries(tpch_real_world
  tpch
  ${KUDU_TEST_LINK_LIBS})

# rle
add_executable(rle rle.cc)
target_link_libraries(rle
  kudu_util
  ${KUDU_TEST_LINK_LIBS})

# wal_hiccup
add_executable(wal_hiccup wal_hiccup.cc)
target_link_libraries(wal_hiccup
  kudu_util
  ${KUDU_TEST_LINK_LIBS})

# Tests
set(KUDU_TEST_LINK_LIBS tpch ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(tpch/rpc_line_item_dao-test)
