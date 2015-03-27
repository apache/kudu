# Copyright(c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

set(INTEGRATION_TESTS_SRCS
  mini_cluster.cc
  cluster_verifier.cc
  external_mini_cluster.cc
  test_workload.cc
)

add_library(integration-tests ${INTEGRATION_TESTS_SRCS})
target_link_libraries(integration-tests
  tserver
  master
  ksck
  kudu_client
  kudu_client_test_util
  kudu_fs
  kudu_test_util)
add_dependencies(integration-tests
  kudu-tablet_server
  kudu-master)

# Tests
set(KUDU_TEST_LINK_LIBS integration-tests ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(alter_table-test)
ADD_KUDU_TEST(registration-test RESOURCE_LOCK "master-web-port")

# Can't be used with the exported client; it references hidden symbols.
if (NOT KUDU_EXPORTED_CLIENT)
  ADD_KUDU_TEST(raft_consensus-itest)
  ADD_KUDU_TEST(master_replication-itest RESOURCE_LOCK "master-rpc-ports")
  ADD_KUDU_TEST(master_failover-itest RESOURCE_LOCK "master-rpc-ports")
endif()

ADD_KUDU_TEST(create-table-stress-test)
ADD_KUDU_TEST(external_mini_cluster-test RESOURCE_LOCK "master-rpc-ports")
ADD_KUDU_TEST(linked_list-test RESOURCE_LOCK "master-rpc-ports")

# Some tests have additional dependencies
set(KUDU_TEST_LINK_LIBS kudu_client ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(full_stack-insert-scan-test)
