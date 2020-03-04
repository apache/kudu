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

#######################################
# ranger_proto
#######################################

PROTOBUF_GENERATE_CPP(
  RANGER_PROTO_SRCS RANGER_PROTO_HDRS RANGER_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES ranger.proto)

add_library(ranger_proto
  ${RANGER_PROTO_SRCS}
  ${RANGER_PROTO_HDRS})
target_link_libraries(ranger_proto
  protobuf
)

##############################
# kudu_ranger
##############################

set(RANGER_SRCS
  ranger_client.cc)
set(RANGER_DEPS
  gflags
  kudu_subprocess
  ranger_proto)

add_library(kudu_ranger ${RANGER_SRCS})
target_link_libraries(kudu_ranger ${RANGER_DEPS})

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(
  kudu_ranger)

ADD_KUDU_TEST(ranger_client-test)
