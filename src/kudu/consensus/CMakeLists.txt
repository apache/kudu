# Copyright (c) 2012, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

KRPC_GENERATE(CONSENSUS_KRPC_SRCS CONSENSUS_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES consensus.proto)

set(CONSENSUS_KRPC_SRCS
  ${CONSENSUS_KRPC_SRCS}
  opid_util.cc
)

add_library(consensus_proto ${CONSENSUS_KRPC_SRCS} ${CONSENSUS_KRPC_HDRS})
target_link_libraries(consensus_proto
  tserver_admin_proto
  protobuf
  kudu_common_proto
  cfile_proto
  metadata_proto
  krpc
  wire_protocol_proto
  tablet_proto)

PROTOBUF_GENERATE_CPP(LOG_PROTO_SRCS LOG_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES log.proto)

add_library(log_proto ${LOG_PROTO_SRCS} ${LOG_PROTO_HDRS})
target_link_libraries(log_proto
  consensus_proto)

set(LOG_SRCS
  async_log_reader.cc
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
  consensus_meta.cc
  consensus_peers.cc
  consensus_queue.cc
  leader_election.cc
  local_consensus.cc
  opid_waiter_set.cc
  quorum_util.cc
  raft_consensus.cc
  raft_consensus_state.cc
)

add_library(consensus ${CONSENSUS_SRCS})
target_link_libraries(consensus
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

ADD_KUDU_TEST(async_log_reader-test)
ADD_KUDU_TEST(consensus_meta-test)
ADD_KUDU_TEST(consensus_peers-test)
ADD_KUDU_TEST(consensus_queue-test)
ADD_KUDU_TEST(leader_election-test)
ADD_KUDU_TEST(log-test)
ADD_KUDU_TEST(mt-log-test)
ADD_KUDU_TEST(opid_anchor_registry-test)
ADD_KUDU_TEST(raft_consensus_quorum-test)
ADD_KUDU_TEST(raft_consensus_state-test)

#Tools
add_executable(log-dump log-dump.cc)
target_link_libraries(log-dump
  log
  ${KUDU_BASE_LIBS})
