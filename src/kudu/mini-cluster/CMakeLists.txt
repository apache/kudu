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

set(MINI_CLUSTER_SRCS
  external_mini_cluster.cc
  internal_mini_cluster.cc
  mini_cluster.cc
)

add_library(mini-cluster ${MINI_CLUSTER_SRCS})
target_link_libraries(mini-cluster
  gflags
  glog
  gmock
  gutil
  kudu_client
  kudu_common
  kudu_curl_util
  kudu_test_util
  kudu_util
  krpc
  master
  master_proto
  mini_kdc
  server_base_proto
  tablet_proto
  tserver
  tserver_proto
  tserver_service_proto
  wire_protocol_proto
)
add_dependencies(mini-cluster
  kudu-tserver
  kudu-master)

# Tests
set(KUDU_TEST_LINK_LIBS mini-cluster ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(external_mini_cluster-test RESOURCE_LOCK "master-rpc-ports")
