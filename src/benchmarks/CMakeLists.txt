# Copyright (c) 2012, Cloudera, inc.

# tpch1
ADD_EXECUTABLE(tpch1 tpch/tpch1.cc)
TARGET_LINK_LIBRARIES(tpch1 ${KUDU_TEST_LINK_LIBS})
