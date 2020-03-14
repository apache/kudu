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
# subprocess_proto
#######################################

PROTOBUF_GENERATE_CPP(
  SUBPROCESS_PROTO_SRCS SUBPROCESS_PROTO_HDRS SUBPROCESS_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES subprocess.proto)

add_library(subprocess_proto
  ${SUBPROCESS_PROTO_SRCS}
  ${SUBPROCESS_PROTO_HDRS})
target_link_libraries(subprocess_proto
  protobuf
  wire_protocol_proto)

#######################################
# kudu_subprocess
#######################################
set(SUBPROCESS_JAR ${EXECUTABLE_OUTPUT_PATH}/kudu-subprocess.jar)

add_custom_command(OUTPUT ${SUBPROCESS_JAR}
  COMMAND ./gradlew :kudu-subprocess:jar ${GRADLE_FLAGS}
  COMMAND cp -f
    "${JAVA_DIR}/kudu-subprocess/build/libs/kudu-subprocess-${KUDU_VERSION_NUMBER}.jar"
    "${SUBPROCESS_JAR}"
  WORKING_DIRECTORY "${JAVA_DIR}"
  DEPENDS init_gradle)
add_custom_target(subprocess_jar DEPENDS ${SUBPROCESS_JAR})

add_library(kudu_subprocess
  server.cc
  subprocess_protocol.cc
)
target_link_libraries(kudu_subprocess
  gutil
  kudu_util
  subprocess_proto
  tool_proto
  ${KUDU_BASE_LIBS}
)
add_dependencies(kudu_subprocess subprocess_jar)

add_library(echo_subprocess
  echo_subprocess.cc
)
target_link_libraries(echo_subprocess
  kudu_subprocess
)

#######################################
# Unit tests
#######################################

if (NOT NO_TESTS)
  SET_KUDU_TEST_LINK_LIBS(
    kudu_subprocess
    echo_subprocess
  )
endif()

ADD_KUDU_TEST(subprocess_proxy-test)
ADD_KUDU_TEST(subprocess_server-test)
