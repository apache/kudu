# Copyright (c) 2013, Cloudera, inc.

SET(CLIENT_SRCS
  client.cc
  meta_cache.cc
)

add_library(client ${CLIENT_SRCS})
SET(CLIENT_DEPS kudu_common tserver_proto tserver_service_proto krpc)
ADD_DEPENDENCIES(client ${CLIENT_DEPS})

# Tests
SET(KUDU_TEST_LINK_LIBS client ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(client-test)