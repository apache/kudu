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
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/wire_protocol-test-util.h"
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
METRIC_DECLARE_gauge_uint32(tablets_num_failed);

using kudu::cluster::ExternalDaemon;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::fs::BlockManager;
using std::pair;
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

enum class ErrorType {
  CFILE_CORRUPTION,
  DISK_FAILURE,
};

class DiskErrorITestBase : public ExternalMiniClusterITestBase,
                           public ::testing::WithParamInterface<ErrorType> {
 public:
  typedef vector<pair<string, string>> FlagList;
  static constexpr int kNumDataDirs = 3;

  // Set the flags on the given server based on the contents of `flags`.
  Status SetFlags(ExternalDaemon* server, const FlagList& flags) const {
    for (const auto& flag_pair : flags) {
      RETURN_NOT_OK(cluster_->SetFlag(server, flag_pair.first, flag_pair.second));
    }
    return Status::OK();
  }

  // Returns the appropriate injection flags for the given error and server.
  FlagList InjectionFlags(ErrorType error, ExternalDaemon* error_server) const {
    FlagList injection_flags;
    switch (error) {
      case ErrorType::DISK_FAILURE: {
        // Avoid injecting errors to the first data directory.
        string data_dirs = Substitute("$0,$1",
                                      JoinPathSegments(error_server->data_dirs()[1], "**"),
                                      JoinPathSegments(error_server->data_dirs()[2], "**"));
        injection_flags.emplace_back("env_inject_eio_globs", data_dirs);
        injection_flags.emplace_back("env_inject_eio", "1.0");
        break;
      }
      case ErrorType::CFILE_CORRUPTION:
        injection_flags.emplace_back("cfile_inject_corruption", "1.0");
        break;
    }
    return injection_flags;
  }
};

// A generalized test for different kinds of disk errors in tablet servers.
class TabletServerDiskErrorITest : public DiskErrorITestBase {
 public:
  const int kNumTablets = 10;

  // Set up a cluster with 4 tservers, with `kNumTablets` spread across the
  // first three tservers. This ensures that injecting failures into any of the
  // first three tservers will hit all tablets.
  //
  // Also configure the cluster to not delete or copy tablets, even on error.
  // This allows us to check all tablets are failed appropriately.
  void SetUp() override {
    const int kNumRows = 5000;
    ExternalMiniClusterOptions opts;
    // Use 3 tservers at first; we'll add an empty one later.
    opts.num_tablet_servers = 3;
    opts.num_data_dirs = kNumDataDirs;
    opts.extra_tserver_flags = {
      // Flush frequently so we actually get some data blocks.
      "--flush_threshold_secs=1",
      "--flush_threshold_mb=1",
    };
    opts.extra_master_flags = {
      // Prevent the master from tombstoning replicas that may not be part of
      // the config (e.g. if a leader fails, it can be "evicted", despite
      // setting `--evict_failed_follower=false`)
      "--master_tombstone_evicted_tablet_replicas=false"
    };
    NO_FATALS(StartClusterWithOpts(std::move(opts)));

    // Write some rows to the three servers.
    TestWorkload writes(cluster_.get());
    writes.set_num_tablets(kNumTablets);
    writes.Setup();
    writes.Start();
    while (writes.rows_inserted() < kNumRows) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    NO_FATALS(writes.StopAndJoin());

    // Now add the last server.
    cluster_->AddTabletServer();
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      // Prevent attempts to copy over replicas, e.g. ones that don't get to a
      // running state due to an error.
      ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i), "enable_tablet_copy", "false"));
    }
  }

  // Set the flags that would allow for the recovery of failed tablets.
  Status AllowRecovery() const {
    LOG(INFO) << "Resetting error injection flags";
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      const FlagList recovery_flags = {
        // First, stop injecting errors.
        { "env_inject_eio", "0.0" },
        { "cfile_inject_corruption", "0.0" },

        // Then allow for recovery.
        { "enable_tablet_copy", "true" },
      };
      return SetFlags(cluster_->tablet_server(i), recovery_flags);
    }
    return Status::OK();
  }

  // Waits for the number of failed tablets on the tablet server to reach
  // `num_failed`.
  void WaitForFailedTablets(ExternalTabletServer* ts, int num_failed) const {
    ASSERT_EVENTUALLY([&] {
      int64_t failed_on_ts;
      ASSERT_OK(itest::GetInt64Metric(ts->bound_http_hostport(),
          &METRIC_ENTITY_server, nullptr, &METRIC_tablets_num_failed, "value", &failed_on_ts));
      LOG(INFO) << "Currently has " << failed_on_ts << " failed tablets";
      ASSERT_EQ(num_failed, failed_on_ts);
    });
  }
};

INSTANTIATE_TEST_CASE_P(TabletServerDiskError, TabletServerDiskErrorITest,
                        ::testing::Values(ErrorType::CFILE_CORRUPTION, ErrorType::DISK_FAILURE));

TEST_P(TabletServerDiskErrorITest, TestFailOnBootstrap) {
  // Inject the errors into one of the non-empty servers.
  ExternalTabletServer* error_ts = cluster_->tablet_server(0);
  for (auto flag_pair : InjectionFlags(GetParam(), error_ts)) {
    error_ts->mutable_flags()->emplace_back(
        Substitute("--$0=$1", flag_pair.first, flag_pair.second));
  }
  error_ts->Shutdown();
  LOG(INFO) << "Restarting server with injected errors...";
  ASSERT_OK(error_ts->Restart());

  // Wait for all the tablets to reach a failed state.
  NO_FATALS(WaitForFailedTablets(error_ts, kNumTablets));
  ASSERT_OK(AllowRecovery());

  // Wait for the cluster to return to a healthy state.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
};

TEST_P(TabletServerDiskErrorITest, TestFailDuringScanWorkload) {
  // Set up a workload that only reads from the tablets.
  TestWorkload read(cluster_.get());
  read.set_num_write_threads(0);
  read.set_num_read_threads(1);
  read.Setup();
  read.Start();

  // Inject the errors into one of the non-empty servers.
  ExternalTabletServer* error_ts = cluster_->tablet_server(0);
  ASSERT_OK(SetFlags(error_ts, InjectionFlags(GetParam(), error_ts)));

  // Wait for all the tablets to reach a failed state.
  NO_FATALS(WaitForFailedTablets(error_ts, kNumTablets));
  ASSERT_OK(AllowRecovery());
  NO_FATALS(read.StopAndJoin());

  // Verify the cluster can get to a healthy state.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
}

class MasterDiskErrorITest : public DiskErrorITestBase {
};

INSTANTIATE_TEST_CASE_P(MasterDiskError, MasterDiskErrorITest,
                        ::testing::Values(ErrorType::CFILE_CORRUPTION, ErrorType::DISK_FAILURE));

// Test that triggers disk error in master during maintenance manager operations like compaction.
TEST_P(MasterDiskErrorITest, TestMasterDiskFailure) {
  constexpr int kNumReplicas = 1;
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumReplicas;
  opts.num_data_dirs = kNumDataDirs;
  opts.extra_master_flags = {
      // Flush frequently so we actually get some disk row sets
      "--flush_threshold_secs=1",
      "--flush_threshold_mb=1",
      "--enable_rowset_compaction=false"
  };
  NO_FATALS(StartClusterWithOpts(std::move(opts)));

  // Create bunch of tables to populate the system catalog with overlapping entries
  // that'll require compaction of the disk row sets.
  std::unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  auto client_schema = client::KuduSchema::FromSchema(GetSimpleTestSchema());
  for (int table_suffix = 0; table_suffix < 20; table_suffix++) {
    string table_name = Substitute("test-$0", table_suffix);
    LOG(INFO) << "Creating table " << table_name;
    ASSERT_OK(table_creator->table_name(table_name)
                  .schema(&client_schema)
                  .set_range_partition_columns({ "key" })
                  .num_replicas(kNumReplicas)
                  .wait(true)
                  .Create());
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  // Trigger disk failure and enable compaction.
  auto leader_master = cluster_->leader_master();
  auto flag_list = InjectionFlags(GetParam(), leader_master);
  flag_list.emplace_back("enable_rowset_compaction", "true");
  ASSERT_OK(SetFlags(leader_master, flag_list));

  // Wait for the master to crash
  ASSERT_OK(leader_master->WaitForFatal(MonoDelta::FromSeconds(20)));
}
}  // namespace kudu
