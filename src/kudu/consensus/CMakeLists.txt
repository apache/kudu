# Copyright (c) 2012, Cloudera, inc.

PROTOBUF_GENERATE_CPP(CONSENSUS_PROTO_SRCS CONSENSUS_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES consensus.proto)

add_library(consensus_proto ${CONSENSUS_PROTO_SRCS} ${CONSENSUS_PROTO_HDRS})
target_link_libraries(consensus_proto
  tserver_proto protobuf
  kudu_common_proto
  cfile_proto
  metadata_proto
  tablet_proto)

PROTOBUF_GENERATE_CPP(LOG_PROTO_SRCS LOG_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES log.proto)

add_library(log_proto ${LOG_PROTO_SRCS} ${LOG_PROTO_HDRS})
target_link_libraries(log_proto
  consensus_proto)

set(LOG_SRCS
  log_util.cc
  log.cc
  log_reader.cc
  opid_anchor_registry.cc
  log_metrics.cc
)

add_library(log ${LOG_SRCS} ${LOG_PROTO_SRCS})
target_link_libraries(log
  server_common
  gutil
  kudu_common
  kudu_fs
  consensus_proto
  log_proto
  metadata_proto)

set(CONSENSUS_SRCS
  consensus.cc
  consensus_queue.cc
  consensus_peers.cc
  local_consensus.cc
  raft_consensus.cc
  raft_consensus_state.cc
  opid_waiter_set.cc
)

add_library(consensus ${CONSENSUS_SRCS})
target_link_libraries(consensus
  tserver_service_proto
  consensus_proto
  kudu_common
  log)

set(KUDU_TEST_LINK_LIBS
  log
  consensus
  tserver
  cfile
  tablet
  kudu_util
  ${KUDU_MIN_TEST_LIBS}
)

ADD_KUDU_TEST(consensus_queue-test)
ADD_KUDU_TEST(consensus_peers-test)
ADD_KUDU_TEST(log-test)
ADD_KUDU_TEST(mt-log-test)
ADD_KUDU_TEST(opid_anchor_registry-test)
ADD_KUDU_TEST(raft_consensus-test)

#Tools
add_executable(log-dump log-dump.cc)
target_link_libraries(log-dump
  log
  ${KUDU_BASE_LIBS})
