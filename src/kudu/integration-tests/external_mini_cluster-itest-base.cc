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

#include "kudu/integration-tests/external_mini_cluster-itest-base.h"

#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/util/pstack_watcher.h"

namespace kudu {

void ExternalMiniClusterITestBase::TearDown() {
  StopCluster();
  KuduTest::TearDown();
}

void ExternalMiniClusterITestBase::StartCluster(
    const std::vector<std::string>& extra_ts_flags,
    const std::vector<std::string>& extra_master_flags,
    int num_tablet_servers) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = num_tablet_servers;
  opts.extra_master_flags = extra_master_flags;
  opts.extra_tserver_flags = extra_ts_flags;
  StartClusterWithOpts(std::move(opts));
}

void ExternalMiniClusterITestBase::StartClusterWithOpts(
    ExternalMiniClusterOptions opts) {
  cluster_.reset(new ExternalMiniCluster(std::move(opts)));
  ASSERT_OK(cluster_->Start());
  inspect_.reset(new itest::ExternalMiniClusterFsInspector(cluster_.get()));
  ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy(),
                                         cluster_->messenger(),
                                         &ts_map_));
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
}

void ExternalMiniClusterITestBase::StopCluster() {
  if (!cluster_) {
    return;
  }

  if (HasFatalFailure()) {
    LOG(INFO) << "Found fatal failure";
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      if (!cluster_->tablet_server(i)->IsProcessAlive()) {
        LOG(INFO) << "Tablet server " << i << " is not running. Cannot dump its stacks.";
        continue;
      }
      LOG(INFO) << "Attempting to dump stacks of TS " << i
                << " with UUID " << cluster_->tablet_server(i)->uuid()
                << " and pid " << cluster_->tablet_server(i)->pid();
      WARN_NOT_OK(PstackWatcher::DumpPidStacks(cluster_->tablet_server(i)->pid()),
                  "Couldn't dump stacks");
    }
  }
  cluster_->Shutdown();
  client_.reset();
  inspect_.reset();
  cluster_.reset();
  STLDeleteValues(&ts_map_);
}

} // namespace kudu
