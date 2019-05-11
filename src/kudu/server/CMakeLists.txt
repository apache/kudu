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
# server_base_proto
#########################################

KRPC_GENERATE(
  SERVER_BASE_PROTO_SRCS SERVER_BASE_PROTO_HDRS SERVER_BASE_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES server_base.proto)

add_library(server_base_proto ${SERVER_BASE_PROTO_SRCS} ${SERVER_BASE_PROTO_HDRS})
target_link_libraries(server_base_proto
  kudu_common_proto
  protobuf
  version_info_proto
  wire_protocol_proto)

#########################################
# server_process
#########################################

set(SERVER_PROCESS_SRCS
  rpc_server.cc
  server_base.cc
  server_base_options.cc
)

add_library(server_process ${SERVER_PROCESS_SRCS})
target_link_libraries(server_process
  clock
  gutil
  krpc
  kudu_common
  kudu_fs
  kudu_util
  server_base_proto
  )
