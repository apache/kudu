# Copyright (c) 2013, Cloudera, inc.

PROTOBUF_GENERATE_CPP(MASTER_PROTO_SRCS MASTER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES master.proto)
KRPC_GENERATE(MASTER_KRPC_SRCS MASTER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES master.proto)

add_library(master_proto ${MASTER_PROTO_SRCS} ${MASTER_PROTO_HDRS})
ADD_DEPENDENCIES(master_proto kudu_common)

SET(MASTER_SRCS
  master.cc
  master_options.cc
  master_service.cc
  ts_descriptor.cc
  ts_manager.cc
)

add_library(master ${MASTER_SRCS} ${MASTER_PROTO_SRCS} ${MASTER_KRPC_SRCS})
ADD_DEPENDENCIES(master kudu_common)
ADD_DEPENDENCIES(master tablet)
ADD_DEPENDENCIES(master server)
ADD_DEPENDENCIES(master master_proto)
ADD_DEPENDENCIES(master krpc)

# Tests
ADD_KUDU_TEST(master-test RESOURCE_LOCK "master-web-port")
TARGET_LINK_LIBRARIES(master-test master master_proto ${KUDU_LINK_LIBS})

ADD_EXECUTABLE(kudu-master master_main.cc)
TARGET_LINK_LIBRARIES(kudu-master master master_proto ${KUDU_LINK_LIBS})
