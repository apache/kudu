# Copyright(c) 2013, Cloudera, inc.

set(INTEGRATION_TESTS_SRCS
  mini_cluster.cc
)

add_library(integration-tests ${INTEGRATION_TESTS_SRCS})
set(INTEGRATION_TESTS_DEPS kudu_common tserver master)
add_dependencies(integration-tests ${INTEGRATION_TESTS_DEPS})

# Tests
set(KUDU_TEST_LINK_LIBS integration-tests ${KUDU_TEST_LINK_LIBS})

ADD_KUDU_TEST(registration-test)