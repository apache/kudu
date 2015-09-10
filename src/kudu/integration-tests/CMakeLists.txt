# Copyright(c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

set(INTEGRATION_TESTS_SRCS
  cluster_itest_util.cc
  cluster_verifier.cc
  external_mini_cluster.cc
  external_mini_cluster_fs_inspector.cc
  mini_cluster.cc
  test_workload.cc
)

add_library(integration-tests ${INTEGRATION_TESTS_SRCS})
target_link_libraries(integration-tests
  tserver
  tserver_test_util
  master
  ksck
  kudu_client
  kudu_client_test_util
  kudu_fs
  kudu_test_util)
add_dependencies(integration-tests
  kudu-tserver
  kudu-master)

# Tests
set(KUDU_TEST_LINK_LIBS integration-tests ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(alter_table-test)
ADD_KUDU_TEST(alter_table-randomized-test)
ADD_KUDU_TEST(registration-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(client-stress-test
  RESOURCE_LOCK "master-rpc-ports"
  RUN_SERIAL true)
ADD_KUDU_TEST(master_replication-itest RESOURCE_LOCK "master-rpc-ports")
ADD_KUDU_TEST(master_failover-itest RESOURCE_LOCK "master-rpc-ports")
ADD_KUDU_TEST(raft_consensus-itest RUN_SERIAL true)
ADD_KUDU_TEST(ts_tablet_manager-itest)
ADD_KUDU_TEST(ts_recovery-itest)
ADD_KUDU_TEST(create-table-stress-test)
ADD_KUDU_TEST(delete_table-test)
ADD_KUDU_TEST(external_mini_cluster-test RESOURCE_LOCK "master-rpc-ports")
ADD_KUDU_TEST(linked_list-test RESOURCE_LOCK "master-rpc-ports")
ADD_KUDU_TEST(all_types-itest RESOURCE_LOCK "master-rpc-ports")
ADD_KUDU_TEST(remote_bootstrap-itest)
ADD_KUDU_TEST(tablet_replacement-itest)

# Some tests have additional dependencies
set(KUDU_TEST_LINK_LIBS kudu_client kudu_tools_util ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(full_stack-insert-scan-test RUN_SERIAL true)
ADD_KUDU_TEST(update_scan_delta_compact-test RUN_SERIAL true)
ADD_KUDU_TEST(flex_partitioning-itest)
