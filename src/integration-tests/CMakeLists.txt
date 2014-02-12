# Copyright(c) 2013, Cloudera, inc.

set(INTEGRATION_TESTS_SRCS
  mini_cluster.cc
  external_mini_cluster.cc
)

add_library(integration-tests ${INTEGRATION_TESTS_SRCS})
set(INTEGRATION_TESTS_DEPS kudu_common tserver master kudu-tablet_server kudu-master)
add_dependencies(integration-tests ${INTEGRATION_TESTS_DEPS})

# Tests
set(KUDU_TEST_LINK_LIBS integration-tests ${KUDU_TEST_LINK_LIBS})

ADD_KUDU_TEST(alter_table-test)
ADD_KUDU_TEST(registration-test RESOURCE_LOCK "master-web-port")
ADD_KUDU_TEST(create-table-stress-test)
ADD_KUDU_TEST(external_mini_cluster-test)
