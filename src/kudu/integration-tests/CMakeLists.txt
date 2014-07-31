# Copyright(c) 2013, Cloudera, inc.

set(INTEGRATION_TESTS_SRCS
  mini_cluster.cc
  external_mini_cluster.cc
)

add_library(integration-tests ${INTEGRATION_TESTS_SRCS})
target_link_libraries(integration-tests
  tserver
  master
  client
  kudu_fs
  kudu_test_util)
add_dependencies(integration-tests
  kudu-tablet_server
  kudu-master)

# Tests
set(KUDU_TEST_LINK_LIBS integration-tests ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(alter_table-test)
ADD_KUDU_TEST(registration-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(raft_consensus-itest)
ADD_KUDU_TEST(create-table-stress-test)
ADD_KUDU_TEST(external_mini_cluster-test)
ADD_KUDU_TEST(linked_list-test)

# Some tests have additional dependencies
set(KUDU_TEST_LINK_LIBS client ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(full_stack-insert-scan-test)
