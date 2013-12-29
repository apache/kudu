# Copyright (c) 2013, Cloudera, inc.

PROTOBUF_GENERATE_CPP(MASTER_PROTO_SRCS MASTER_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES master.proto)
KRPC_GENERATE(MASTER_KRPC_SRCS MASTER_KRPC_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES master.proto)

add_library(master_proto ${MASTER_PROTO_SRCS} ${MASTER_PROTO_HDRS} ${MASTER_KRPC_SRCS})
add_dependencies(master_proto kudu_common)
target_link_libraries(master_proto krpc)

set(MASTER_SRCS
  master.cc
  master_options.cc
  master_service.cc
  master-path-handlers.cc
  mini_master.cc
  m_tablet_manager.cc
  sys_tables.cc
  ts_descriptor.cc
  ts_manager.cc
)

add_library(master ${MASTER_SRCS} ${MASTER_PROTO_SRCS} ${MASTER_KRPC_SRCS})
add_dependencies(master kudu_common)
add_dependencies(master tablet)
add_dependencies(master server)
add_dependencies(master master_proto)
add_dependencies(master krpc)

# Tests
ADD_KUDU_TEST(master-test RESOURCE_LOCK "master-web-port")
target_link_libraries(master-test master master_proto ${KUDU_LINK_LIBS})

add_executable(kudu-master master_main.cc)
target_link_libraries(kudu-master master master_proto ${KUDU_LINK_LIBS})
