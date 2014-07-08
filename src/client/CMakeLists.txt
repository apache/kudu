# Copyright (c) 2013, Cloudera, inc.

set(CLIENT_SRCS
  batcher.cc
  client.cc
  encoded_key.cc
  error_collector.cc
  meta_cache.cc
  row_result.cc
  scan_predicate.cc
  schema.cc
  write_op.cc
)

add_library(client ${CLIENT_SRCS})
target_link_libraries(client
  kudu_common
  master_proto
  tserver_proto
  tserver_service_proto
  kudu_util
  gutil
  krpc)

# Tests
set(KUDU_TEST_LINK_LIBS client integration-tests ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(client-test)
