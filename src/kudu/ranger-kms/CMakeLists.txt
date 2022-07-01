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
# mini_ranger_kms
#######################################
execute_process(COMMAND ln -nsf
        "${THIRDPARTY_DIR}/installed/common/opt/ranger-kms"
        "${EXECUTABLE_OUTPUT_PATH}/ranger_kms-home")

set(MINI_RANGER_KMS_SRCS
        mini_ranger_kms.cc)
set(MINI_RANGER_KMS_DEPS
        kudu_test_util
        kudu_util
        mini_postgres
        mini_ranger)

add_library(mini_ranger_kms ${MINI_RANGER_KMS_SRCS})
target_link_libraries(mini_ranger_kms ${MINI_RANGER_KMS_DEPS})

#######################################
# ranger_kms_client
#######################################
set(RANGER_KMS_CLIENT_SRCS
        ranger_kms_client.cc)
set(RANGER_KMS_CLIENT_DEPS
        kudu_curl_util
        kudu_util)

add_library(ranger_kms_client ${RANGER_KMS_CLIENT_SRCS})
target_link_libraries(ranger_kms_client ${RANGER_KMS_CLIENT_DEPS})

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(
        itest_util
        mini_postgres
        mini_ranger_kms)
