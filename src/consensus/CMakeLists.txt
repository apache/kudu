# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(CONSENSUS_PROTO_SRCS CONSENSUS_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES consensus.proto)

add_library(consensus_proto ${CONSENSUS_PROTO_SRCS} ${CONSENSUS_PROTO_HDRS})
target_link_libraries(consensus_proto tserver_proto protobuf)
target_link_libraries(consensus_proto kudu_common_proto)
target_link_libraries(consensus_proto cfile_proto)
target_link_libraries(consensus_proto metadata_proto)
target_link_libraries(consensus_proto tablet_proto)

PROTOBUF_GENERATE_CPP(LOG_PROTO_SRCS LOG_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES log.proto)

add_library(log_proto ${LOG_PROTO_SRCS} ${LOG_PROTO_HDRS})
target_link_libraries(log_proto consensus_proto)

set(LOG_SRCS
  log_util.cc
  log.cc
  log_reader.cc
  opid_anchor_registry.cc
  log_metrics.cc
)

add_library(log ${LOG_SRCS} ${LOG_PROTO_SRCS})
target_link_libraries(log server_common gutil)
target_link_libraries(log kudu_common)
target_link_libraries(log consensus_proto)
target_link_libraries(log log_proto metadata_proto)


set(CONSENSUS_SRCS
  consensus.cc
  consensus_base.cc
  consensus_queue.cc
  local_consensus.cc
)

add_library(consensus ${CONSENSUS_SRCS})
target_link_libraries(consensus consensus_proto)
target_link_libraries(consensus kudu_common)
target_link_libraries(consensus log)

set(KUDU_TEST_LINK_LIBS
  log
  consensus
  tserver
  cfile
  tablet
  kudu_util
  ${KUDU_DEP_LIBS} ${KUDU_MIN_TEST_LIBS}
)

ADD_KUDU_TEST(consensus_queue-test)
ADD_KUDU_TEST(log-test)
ADD_KUDU_TEST(mt-log-test)
ADD_KUDU_TEST(opid_anchor_registry-test)

#Tools
set(KUDU_LINK_LIBS
  log
  consensus
  tserver
  kudu_common
  kudu_util
  gutil
  cfile
  tablet
  ${KUDU_LINK_LIBS}
)

add_executable(log-dump log-dump.cc)
target_link_libraries(log-dump ${KUDU_LINK_LIBS})
