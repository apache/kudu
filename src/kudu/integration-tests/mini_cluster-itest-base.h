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

#ifndef KUDU_INTEGRATION_TESTS_MINI_CLUSTER_ITEST_BASE_H_
#define KUDU_INTEGRATION_TESTS_MINI_CLUSTER_ITEST_BASE_H_

#include <gtest/gtest.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/util/test_util.h"

namespace kudu {

// Simple base utility class to provide a mini cluster with common setup
// routines useful for integration tests.
class MiniClusterITestBase : public KuduTest {
 public:
  virtual void TearDown() OVERRIDE {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  void StartCluster(int num_tablet_servers = 3);

  gscoped_ptr<MiniCluster> cluster_;
  client::sp::shared_ptr<client::KuduClient> client_;
};

void MiniClusterITestBase::StartCluster(int num_tablet_servers) {
  MiniClusterOptions opts;
  opts.num_tablet_servers = num_tablet_servers;
  cluster_.reset(new MiniCluster(env_, opts));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
}

} // namespace kudu

#endif // KUDU_INTEGRATION_TESTS_MINI_CLUSTER_ITEST_BASE_H_
