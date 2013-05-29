ADD_LIBRARY(kudu_util
  env.cc env_posix.cc env_util.cc
  bloom_filter.cc
  cache.cc
  coding.cc
  group_varint.cc
  memcmpable_varint.cc
  logging.cc
  hexdump.cc
  memory/arena.cc memory/memory.cc
  memenv/memenv.cc
  pb_util.cc
  status.cc
  test_graph.cc
  errno.cc
)
SET(KUDU_UTIL_LIBS kudu_util)

ADD_LIBRARY(kudu_test_main test_main.cc)

SET(KUDU_TEST_LINK_LIBS kudu_util gutil ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS} )
ADD_KUDU_TEST(bitmap-test)
ADD_KUDU_TEST(blocking_queue-test)
ADD_KUDU_TEST(cache-test)
ADD_KUDU_TEST(memory/arena-test)
ADD_KUDU_TEST(bloom_filter-test)
ADD_KUDU_TEST(group_varint-test)
ADD_KUDU_TEST(inline_slice-test)
ADD_KUDU_TEST(memenv/memenv-test)
ADD_KUDU_TEST(object_pool-test)
ADD_KUDU_TEST(status-test)
ADD_KUDU_TEST(memcmpable_varint-test)
