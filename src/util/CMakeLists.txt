add_library(kudu_util
  bitmap.cc
  bloom_filter.cc
  bitmap.cc
  cache.cc
  coding.cc
  crc.cc
  curl_util.cc
  debug-util.cc
  env.cc env_posix.cc env_util.cc
  errno.cc
  faststring.cc
  group_varint.cc
  pstack_watcher.cc
  hdr_histogram.cc
  hexdump.cc
  jsonwriter.cc
  kernel_stack_watchdog.cc
  logging.cc
  memcmpable_varint.cc
  memory/arena.cc memory/memory.cc
  memenv/memenv.cc
  metrics.cc
  monotime.cc
  net/dns_resolver.cc
  net/net_util.cc
  net/sockaddr.cc
  net/socket.cc
  os-util.cc
  path_util.cc
  pb_util.cc
  random_util.cc
  resettable_heartbeater.cc
  rwc_lock.cc
  semaphore.cc
  slice.cc
  spinlock_profiling.cc
  status.cc
  string_case.cc
  subprocess.cc
  sync_point.cc
  task_executor.cc
  test_graph.cc
  thread.cc
  threadlocal.cc
  threadpool.cc
  trace.cc
  user.cc
  url-coding.cc
)

PROTOBUF_GENERATE_CPP(HISTOGRAM_PROTO_SRCS HISTOGRAM_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES histogram.proto)
add_library(histogram_proto ${HISTOGRAM_PROTO_SRCS} ${HISTOGRAM_PROTO_HDRS})
target_link_libraries(histogram_proto
  protobuf)

# TODO: we don't really need to link agaist libcurl, since it's only used by # tests.
target_link_libraries(kudu_util
  ${CURL_LIBRARIES}
  ${Boost_LIBRARIES}
  crcutil
  gutil
  histogram_proto
  protobuf
  glog
  rt)

add_library(kudu_test_main test_main.cc)
target_link_libraries(kudu_test_main
  gflags
  glog
  gtest
  rt)

set(KUDU_TEST_LINK_LIBS kudu_util gutil ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(bit-util-test)
ADD_KUDU_TEST(bitmap-test)
ADD_KUDU_TEST(blocking_queue-test)
ADD_KUDU_TEST(bloom_filter-test)
ADD_KUDU_TEST(cache-test)
ADD_KUDU_TEST(crc-test RUN_SERIAL true) # has a benchmark
ADD_KUDU_TEST(env-test LABELS no_tsan)
ADD_KUDU_TEST(group_varint-test)
ADD_KUDU_TEST(pstack_watcher-test)
ADD_KUDU_TEST(hdr_histogram-test)
ADD_KUDU_TEST(inline_slice-test)
ADD_KUDU_TEST(interval_tree-test)
ADD_KUDU_TEST(knapsack_solver-test)
ADD_KUDU_TEST(map-util-test)
ADD_KUDU_TEST(memcmpable_varint-test LABELS no_tsan)
ADD_KUDU_TEST(memenv/memenv-test)
ADD_KUDU_TEST(memory/arena-test)
ADD_KUDU_TEST(metrics-test)
ADD_KUDU_TEST(monotime-test)
ADD_KUDU_TEST(mt-hdr_histogram-test RUN_SERIAL true)
ADD_KUDU_TEST(mt-metrics-test RUN_SERIAL true)
ADD_KUDU_TEST(mt-threadlocal-test RUN_SERIAL true)
ADD_KUDU_TEST(net/dns_resolver-test)
ADD_KUDU_TEST(net/net_util-test)
ADD_KUDU_TEST(object_pool-test)
ADD_KUDU_TEST(os-util-test)
ADD_KUDU_TEST(pb_util-test)
ADD_KUDU_TEST(random_util-test)
ADD_KUDU_TEST(resettable_heartbeater-test)
ADD_KUDU_TEST(rle-test)
ADD_KUDU_TEST(rw_semaphore-test)
ADD_KUDU_TEST(rwc_lock-test)
ADD_KUDU_TEST(safe_math-test)
ADD_KUDU_TEST(slice-test)
ADD_KUDU_TEST(spinlock_profiling-test)
ADD_KUDU_TEST(status-test)
ADD_KUDU_TEST(string_case-test)
ADD_KUDU_TEST(subprocess-test)
ADD_KUDU_TEST(sync_point-test)
ADD_KUDU_TEST(task_executor-test)
ADD_KUDU_TEST(thread-test)
ADD_KUDU_TEST(threadpool-test)
ADD_KUDU_TEST(url-coding-test)
ADD_KUDU_TEST(user-test)
ADD_KUDU_TEST(trace-test)

PROTOBUF_GENERATE_CPP(JSONWRITER_TEST_PROTO_SRCS JSONWRITER_TEST_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES jsonwriter_test.proto)
add_library(jsonwriter_test_proto ${JSONWRITER_TEST_PROTO_SRCS} ${JSONWRITER_TEST_PROTO_HDRS})
target_link_libraries(jsonwriter_test_proto
  protobuf)

ADD_KUDU_TEST(jsonwriter-test)
target_link_libraries(jsonwriter-test
  jsonwriter_test_proto)

add_executable(protoc-gen-insertions protoc-gen-insertions.cc)
target_link_libraries(protoc-gen-insertions gutil protobuf protoc ${KUDU_BASE_LIBS})
