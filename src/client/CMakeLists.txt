# Copyright (c) 2013, Cloudera, inc.

set(CLIENT_SRCS
  client.cc
  meta_cache.cc
  partial_row.cc
)

add_library(client ${CLIENT_SRCS})
set(CLIENT_DEPS kudu_common tserver_proto tserver_service_proto krpc)
add_dependencies(client ${CLIENT_DEPS})

# Tests
set(KUDU_TEST_LINK_LIBS client ${KUDU_TEST_LINK_LIBS})
ADD_KUDU_TEST(client-test)
ADD_KUDU_TEST(partial_row-test)