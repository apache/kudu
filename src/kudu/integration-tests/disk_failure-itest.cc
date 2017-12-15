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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/fs/block_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::fs::BlockManager;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

const MonoDelta kAgreementTimeout = MonoDelta::FromSeconds(30);

class DiskFailureITest : public ExternalMiniClusterITestBase,
                         public ::testing::WithParamInterface<std::tuple<string, bool>> {
 public:

  // Waits for 'ext_tserver' to experience 'target_failed_disks' disk failures.
  void WaitForDiskFailures(const ExternalTabletServer* ext_tserver,
                           int64_t target_failed_disks = 1) const {
    ASSERT_EVENTUALLY([&] {
      int64_t failed_on_ts;
      ASSERT_OK(itest::GetInt64Metric(ext_tserver->bound_http_hostport(),
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
  const string block_manager_type = std::get<0>(GetParam());
  const bool is_3_4_3_mode = std::get<1>(GetParam());
  // Set up a cluster with five disks at each tablet server. In case of 3-4-3
  // replication scheme one more tablet server is needed to put the replacement
  // non-voter replica there.
  const auto kNumTabletServers = is_3_4_3_mode ? 4 : 3;
  const auto kNumTablets = 5;
  const auto kNumRows = 100;

  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTabletServers;
  opts.num_data_dirs = 5;
  opts.block_manager_type = block_manager_type;
  opts.extra_master_flags.push_back(
      Substitute("--raft_prepare_replacement_before_eviction=$0", is_3_4_3_mode));
  opts.extra_tserver_flags.push_back(
      Substitute("--raft_prepare_replacement_before_eviction=$0", is_3_4_3_mode));
  NO_FATALS(StartClusterWithOpts(opts));

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

  // Arbitrarily select one tablet server which hosts a replica of the tablet.
  ExternalTabletServer* ts = nullptr;
  for (const auto& e : ts_map_) {
    vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
    ASSERT_OK(itest::ListTablets(e.second, kAgreementTimeout, &tablets));
    if (!tablets.empty()) {
      ts = cluster_->tablet_server_by_uuid(e.first);
      break;
    }
  }
  ASSERT_NE(nullptr, ts);

  // Ensure at least one tablet get to a running state at one of the tablet servers.
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, 1, kAgreementTimeout));

  // Introduce flags to fail one of the directories, avoiding the metadata
  // directory, the next time the tablet server starts.
  const string& failed_dir = ts->data_dirs()[1];
  const vector<string> extra_flags = {
      Substitute("--env_inject_eio_globs=$0", JoinPathSegments(failed_dir, "**")),
      "--env_inject_eio=1.0",
      "--crash_on_eio=false",
  };
  ts->mutable_flags()->insert(ts->mutable_flags()->begin(), extra_flags.begin(), extra_flags.end());
  ts->Shutdown();

  // Restart the tablet server with disk failures and ensure it can startup.
  ASSERT_OK(ts->Restart());
  NO_FATALS(WaitForDiskFailures(ts));

  // Ensure that the tablets are successfully evicted and copied.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(write_workload.table_name(), ClusterVerifier::AT_LEAST,
                            write_workload.batches_completed()));
}

INSTANTIATE_TEST_CASE_P(DiskFailure, DiskFailureITest,
    ::testing::Combine(
        ::testing::ValuesIn(BlockManager::block_manager_types()),
        ::testing::Bool()));

}  // namespace kudu
