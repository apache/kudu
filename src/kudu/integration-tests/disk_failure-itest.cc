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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/fs/block_manager.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

namespace kudu {

using cluster::ExternalMiniClusterOptions;
using cluster::ExternalTabletServer;
using fs::BlockManager;
using std::string;
using std::vector;
using strings::Substitute;

const MonoDelta kAgreementTimeout = MonoDelta::FromSeconds(30);

class DiskFailureITest : public ExternalMiniClusterITestBase,
                         public ::testing::WithParamInterface<string> {
 public:

  // Waits for 'ext_tserver' to experience 'target_failed_disks' disk failures.
  void WaitForDiskFailures(const ExternalTabletServer* ext_tserver,
                           int64_t target_failed_disks = 1) const {
    ASSERT_EVENTUALLY([&] {
      int64_t failed_on_ts;
      ASSERT_OK(ext_tserver->GetInt64Metric(
          &METRIC_ENTITY_server, nullptr, &METRIC_data_dirs_failed, "value", &failed_on_ts));
      ASSERT_EQ(target_failed_disks, failed_on_ts);
    });
  }
};

// Test ensuring that tablet server can be started with failed directories. A
// cluster is started and loaded with some tablets. The tablet server is then
// shut down and restarted. Errors are injected to one of the directories while
// it is shut down.
TEST_P(DiskFailureITest, TestFailDuringServerStartup) {
  // Set up a cluster with three servers with five disks each.
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 3;
  opts.num_data_dirs = 5;
  opts.block_manager_type = GetParam();
  NO_FATALS(StartClusterWithOpts(opts));
  const int kNumTablets = 5;
  const int kNumRows = 100;

  // Write some data to a tablet. This will spread blocks across all
  // directories.
  TestWorkload write_workload(cluster_.get());
  write_workload.set_num_tablets(kNumTablets);
  write_workload.Setup();
  write_workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(kNumRows, write_workload.rows_inserted());
  });
  write_workload.StopAndJoin();

  // Ensure the tablets get to a running state.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, kNumTablets, kAgreementTimeout));

  // Introduce flags to fail one of the directories, avoiding the metadata
  // directory, the next time the tablet server starts.
  string failed_dir = ts->data_dirs()[1];
  vector<string> extra_flags = {
      Substitute("--env_inject_eio_globs=$0", JoinPathSegments(failed_dir, "**")),
      "--env_inject_eio=1.0",
      "--crash_on_eio=false",
  };
  ts->mutable_flags()->insert(ts->mutable_flags()->begin(), extra_flags.begin(), extra_flags.end());
  ts->Shutdown();

  // Restart the tablet server with disk failures and ensure it can startup.
  ASSERT_OK(ts->Restart());
  NO_FATALS(WaitForDiskFailures(cluster_->tablet_server(0)));

  // Ensure that the tablets are successfully evicted and copied.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(write_workload.table_name(), ClusterVerifier::AT_LEAST,
                            write_workload.batches_completed()));
}

INSTANTIATE_TEST_CASE_P(DiskFailure, DiskFailureITest,
    ::testing::ValuesIn(BlockManager::block_manager_types()));

}  // namespace kudu
