# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#########################################
# tserver_admin_proto
#########################################

KRPC_GENERATE(
  TSERVER_ADMIN_KRPC_SRCS TSERVER_ADMIN_KRPC_HDRS TSERVER_ADMIN_KRPC_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES tserver_admin.proto)
set(TSERVER_ADMIN_KRPC_LIBS
  krpc
  protobuf)
ADD_EXPORTABLE_LIBRARY(tserver_admin_proto
  SRCS ${TSERVER_ADMIN_KRPC_SRCS}
  DEPS ${TSERVER_ADMIN_KRPC_LIBS}
  NONLINK_DEPS ${TSERVER_ADMIN_KRPC_TGTS})

#########################################
# tserver
#########################################

set(TSERVER_SRCS
  tablet_server.cc
  tablet_server_options.cc
  consensus_service.cc
  simple_tablet_manager.cc
)

add_library(tserver ${TSERVER_SRCS})
target_link_libraries(tserver
  clock
  consensus
  consensus_proto
  krpc
  log
  log_proto
  protobuf
  security
  kserver
  server_process
  tserver_admin_proto
  )

#########################################
# libkudu_combined.a
#########################################
# To reduce complexity for end applications, we want to
# combine all .a's of kuduraft into a single kudu_combined.a
set(C_LIB ${CMAKE_BINARY_DIR}/lib/libkudu_combined.a)
add_custom_command(TARGET tserver
    POST_BUILD
    COMMAND touch touch_link
    COMMAND ar -t $<TARGET_FILE:tserver> >> touch_link
    COMMAND ar -t $<TARGET_FILE:kserver> >> touch_link
    COMMAND ar -t $<TARGET_FILE:server_process> >> touch_link
    COMMAND ar -t $<TARGET_FILE:consensus> >> touch_link
    COMMAND ar -t $<TARGET_FILE:log> >> touch_link
    COMMAND ar -t $<TARGET_FILE:clock> >> touch_link
    COMMAND ar -t $<TARGET_FILE:kudu_util> >> touch_link
    COMMAND ar -t $<TARGET_FILE:kudu_common> >> touch_link
    COMMAND ar -t $<TARGET_FILE:kudu_fs> >> touch_link
    COMMAND ar -t $<TARGET_FILE:kudu_util_compression> >> touch_link
    COMMAND ar -t $<TARGET_FILE:security> >> touch_link
    COMMAND ar -t $<TARGET_FILE:krpc> >> touch_link
    COMMAND ar -t $<TARGET_FILE:krb5_realm_override> >> touch_link
    COMMAND ar -t $<TARGET_FILE:kudu_common_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:maintenance_manager_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:wire_protocol_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:util_compression_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:fs_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:consensus_metadata_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:consensus_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:rpc_header_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:histogram_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:version_info_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:log_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:rpc_introspection_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:token_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:pb_util_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:tserver_admin_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:server_base_proto> >> touch_link
    COMMAND ar -t $<TARGET_FILE:gutil> >> touch_link
    COMMAND cat touch_link | xargs ar -qcs ${C_LIB}
    COMMAND ranlib ${C_LIB}
    DEPENDS tcmalloc profiler tserver kserver server_process consensus log kudu_fs kudu_common kudu_util clock kudu_util_compression kudu_common_proto maintenance_manager_proto util_compression_proto fs_proto log_proto consensus_metadata_proto consensus_proto histogram_proto rpc_header_proto rpc_introspection_proto token_proto pb_util_proto tserver_admin_proto server_base_proto
)

#########################################
# kudu-tserver
#########################################
add_executable(kudu-tserver tablet_server_main.cc)
target_link_libraries(kudu-tserver
  ${SANITIZER_OPTIONS_OVERRIDE}
  ${KRB5_REALM_OVERRIDE}
  tserver
  ${KUDU_BASE_LIBS})
