# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(CONSENSUS_PROTO_SRCS CONSENSUS_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES consensus.proto)

add_library(consensus_proto ${CONSENSUS_PROTO_SRCS} ${CONSENSUS_PROTO_HDRS})
ADD_DEPENDENCIES(consensus_proto kudu_common)
ADD_DEPENDENCIES(consensus_proto cfile_proto)
ADD_DEPENDENCIES(consensus_proto metadata_proto)
ADD_DEPENDENCIES(consensus_proto tserver_proto)
ADD_DEPENDENCIES(consensus_proto tablet_proto)

PROTOBUF_GENERATE_CPP(LOG_PROTO_SRCS LOG_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES log.proto)

add_library(log_proto ${LOG_PROTO_SRCS} ${LOG_PROTO_HDRS})
ADD_DEPENDENCIES(log_proto consensus_proto)

SET(LOG_SRCS
  log_util.cc
  log.cc
  log_reader.cc
)

add_library(log ${LOG_SRCS} ${LOG_PROTO_SRCS})
target_link_libraries(log gutil)
target_link_libraries(log kudu_common)
ADD_DEPENDENCIES(log kudu_common)
ADD_DEPENDENCIES(log log_proto)
ADD_DEPENDENCIES(log server)

SET(CONSENSUS_SRCS
  consensus.cc
  local_consensus.cc
  tasks/tasks.cc
)

add_library(consensus ${CONSENSUS_SRCS} ${CONSENSUS_PROTO_SRCS})
target_link_libraries(consensus krpc)
target_link_libraries(consensus tserver_proto)
ADD_DEPENDENCIES(consensus kudu_common)
ADD_DEPENDENCIES(consensus consensus_proto)
ADD_DEPENDENCIES(consensus log)

SET(KUDU_TEST_LINK_LIBS
  log
  server
  consensus
  tserver
  kudu_common
  kudu_util
  gutil
  cfile
  tablet
  ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS}
)

ADD_KUDU_TEST(log_util-test)
ADD_KUDU_TEST(log-test)

#Tools
SET(KUDU_LINK_LIBS
  log
  server
  consensus
  tserver
  kudu_common
  kudu_util
  gutil
  cfile
  tablet
  ${KUDU_LINK_LIBS}
)

ADD_EXECUTABLE(log-dump log-dump.cc)
TARGET_LINK_LIBRARIES(log-dump ${KUDU_LINK_LIBS})