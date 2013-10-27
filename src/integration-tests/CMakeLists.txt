# Copyright(c) 2013, Cloudera, inc.

SET(INTEGRATION_TESTS_SRCS
  mini_cluster.cc
)

add_library(integration-tests ${INTEGRATION_TESTS_SRCS})
SET(INTEGRATION_TESTS_DEPS kudu_common tserver master)
ADD_DEPENDENCIES(integration-tests ${INTEGRATION_TESTS_DEPS})

# Tests
SET(KUDU_TEST_LINK_LIBS integration-tests ${KUDU_TEST_LINK_LIBS})

ADD_KUDU_TEST(registration-test)