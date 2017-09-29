// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/client/shared_ptr.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/test_util.h"

namespace kudu {

namespace client {
class KuduClient;
} // namespace client

namespace itest {
struct TServerDetails;
} // namespace itest

// Simple base utility class to provide an external mini cluster with common
// setup routines useful for integration tests.
class ExternalMiniClusterITestBase : public KuduTest {
 public:
  void TearDown() override;

 protected:
  void StartCluster(const std::vector<std::string>& extra_ts_flags = {},
                    const std::vector<std::string>& extra_master_flags = {},
                    int num_tablet_servers = 3,
                    int num_data_dirs = 1);

  void StartClusterWithOpts(cluster::ExternalMiniClusterOptions opts);

  void StopCluster();

  std::unique_ptr<cluster::ExternalMiniCluster> cluster_;
  std::unique_ptr<itest::ExternalMiniClusterFsInspector> inspect_;
  client::sp::shared_ptr<client::KuduClient> client_;
  std::unordered_map<std::string, itest::TServerDetails*> ts_map_;
};

} // namespace kudu
