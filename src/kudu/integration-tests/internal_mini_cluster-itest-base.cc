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

#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"

#include <gtest/gtest.h>

#include "kudu/gutil/stl_util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {

using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;

void MiniClusterITestBase::TearDown() {
  StopCluster();
  KuduTest::TearDown();
}

void MiniClusterITestBase::StartCluster(int num_tablet_servers) {
  InternalMiniClusterOptions opts;
  opts.num_tablet_servers = num_tablet_servers;
  cluster_.reset(new InternalMiniCluster(env_, opts));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy(),
                                         cluster_->messenger(),
                                         &ts_map_));
}

void MiniClusterITestBase::StopCluster() {
  if (!cluster_) return;

  cluster_->Shutdown();
  cluster_.reset();
  client_.reset();
  STLDeleteValues(&ts_map_);
}

} // namespace kudu
