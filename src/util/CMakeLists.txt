ADD_LIBRARY(kudu_util
  env.cc env_posix.cc env_util.cc
  bitmap.cc
  bloom_filter.cc
  bitmap.cc
  cache.cc
  coding.cc
  group_varint.cc
  memcmpable_varint.cc
  debug-util.cc
  hexdump.cc
  logging.cc
  memory/arena.cc memory/memory.cc
  memenv/memenv.cc
  net/net_util.cc
  net/sockaddr.cc
  net/socket.cc
  pb_util.cc
  status.cc
  string_case.cc
  threadpool.cc
  task_executor.cc
  test_graph.cc
  user.cc
  errno.cc
  monotime.cc
  url-coding.cc
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
ADD_KUDU_TEST(knapsack_solver-test)
ADD_KUDU_TEST(inline_slice-test)
ADD_KUDU_TEST(interval_tree-test)
ADD_KUDU_TEST(memenv/memenv-test)
ADD_KUDU_TEST(net/net_util-test)
ADD_KUDU_TEST(object_pool-test)
ADD_KUDU_TEST(rle-test)
ADD_KUDU_TEST(status-test)
ADD_KUDU_TEST(string_case-test)
ADD_KUDU_TEST(memcmpable_varint-test)
ADD_KUDU_TEST(monotime-test)
ADD_KUDU_TEST(pb_util-test)
ADD_KUDU_TEST(threadpool-test)
ADD_KUDU_TEST(task_executor-test)
ADD_KUDU_TEST(user-test)
ADD_KUDU_TEST(safe_math-test)
ADD_KUDU_TEST(bit-util-test)
ADD_KUDU_TEST(url-coding-test)
