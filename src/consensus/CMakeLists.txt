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

PROTOBUF_GENERATE_CPP(LOG_PROTO_SRCS LOG_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES log.proto)

add_library(log_proto ${LOG_PROTO_SRCS} ${LOG_PROTO_HDRS})
ADD_DEPENDENCIES(log_proto consensus_proto)

SET(CONSENSUS_SRCS
  consensus.cc
)

add_library(consensus ${CONSENSUS_SRCS} ${CONSENSUS_PROTO_SRCS})
ADD_DEPENDENCIES(consensus kudu_common)
ADD_DEPENDENCIES(consensus consensus_proto)
ADD_DEPENDENCIES(consensus log_proto)