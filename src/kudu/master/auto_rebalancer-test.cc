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
#include "kudu/master/auto_rebalancer.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/auto_leader_rebalancer.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/rebalance/rebalance_algo.h"
#include "kudu/rebalance/rebalancer.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::EXCLUDE_HEALTH_REPORT;
using kudu::consensus::GetConsensusStateRequestPB;
using kudu::consensus::GetConsensusStateResponsePB;
using kudu::itest::GetTableLocations;
using kudu::itest::ListTabletServers;
using kudu::master::GetTableLocationsResponsePB;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::rpc::RpcController;
using google::FlagSaver;
using std::map;
using std::max;
using std::make_unique;
using std::min;
using std::numeric_limits;
using std::nullopt;
using std::optional;
using std::set;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_bool(auto_leader_rebalancing_enabled);
DECLARE_bool(auto_rebalancing_prefer_follower_replica_moves);
DECLARE_bool(auto_rebalancing_enabled);
DECLARE_bool(auto_rebalancing_enable_range_rebalancing);
DECLARE_bool(auto_rebalancing_fail_moves_for_test);
DECLARE_bool(enable_range_replica_placement);
DECLARE_bool(enable_minidumps);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(follower_unavailable_considered_failed_sec);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int32(tablet_copy_download_file_inject_latency_ms);
DECLARE_int32(tserver_unresponsive_timeout_ms);
DECLARE_uint32(auto_leader_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_max_moves_per_server);
DECLARE_uint32(auto_rebalancing_wait_for_replica_moves_seconds);

METRIC_DECLARE_gauge_int32(tablet_copy_open_client_sessions);
METRIC_DECLARE_counter(tablet_copy_bytes_fetched);
METRIC_DECLARE_counter(tablet_copy_bytes_sent);

namespace {

// Some of these tests will set a low Raft timeout to move election traffic
// along more quickly. When built with TSAN, this can lead to timeouts, so ease
// up a bit.
#ifdef THREAD_SANITIZER
  constexpr int kLowRaftTimeout = 300;
#else
  constexpr int kLowRaftTimeout = 100;
#endif

} // anonymous namespace


namespace kudu {
namespace master {

enum class BalanceThreadType {
  REPLICA_REBALANCE,
  LEADER_REBALANCE
};

class AutoRebalancerTest : public KuduTest {
  public:

  Status CreateAndStartCluster(bool enable_leader_rebalance = true) {
    FLAGS_auto_rebalancing_interval_seconds = 1; // Shorten for testing.
    FLAGS_auto_rebalancing_wait_for_replica_moves_seconds = 0; // Shorten for testing.
    FLAGS_auto_rebalancing_enabled = true; // Enable for testing.
    FLAGS_auto_leader_rebalancing_enabled = enable_leader_rebalance; // If enable for testing.
    FLAGS_auto_leader_rebalancing_interval_seconds = 2; // shorten for testing.
    cluster_.reset(new InternalMiniCluster(env_, cluster_opts_));
    return cluster_->Start();
  }

  // This function will assign tservers to available locations as
  // evenly as possible.
  void AssignLocationsEvenly(int num_locations) {
    const int num_tservers = cluster_->num_tablet_servers();
    ASSERT_GE(num_tservers, num_locations);

    int master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&master_idx));

    TSDescriptorVector descs;

    ASSERT_EVENTUALLY([&] {
      cluster_->mini_master(master_idx)->master()->ts_manager()->
        GetAllDescriptors(&descs);
      ASSERT_EQ(num_tservers, descs.size());
    });

    // Assign num_locations unique locations to the first num_locations tservers.
    for (int i = 0; i < num_tservers; ++i) {
      descs[i]->AssignLocationForTesting(Substitute("L$0", i % num_locations));
    }
  }

  // This function expects there to be more tservers than available
  // locations. As many tservers as possible will be assigned unique
  // locations, then any additional tservers will all be assigned
  // to the first location.
  void AssignLocationsWithSkew(int num_locations) {
    const int num_tservers = cluster_->num_tablet_servers();
    ASSERT_GT(num_tservers, num_locations);

    int master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&master_idx));

    TSDescriptorVector descs;

    ASSERT_EVENTUALLY([&] {
      cluster_->mini_master(master_idx)->master()->ts_manager()->
        GetAllDescriptors(&descs);
      ASSERT_EQ(num_tservers, descs.size());
    });

    // Assign num_locations unique locations to the first num_locations tservers.
    for (int i = 0; i < num_locations; ++i) {
      descs[i]->AssignLocationForTesting(Substitute("L$0", i));
    }

    for (int i = num_locations; i < num_tservers; ++i) {
      descs[i]->AssignLocationForTesting(Substitute("L$0", 0));
    }
  }

  // Make sure the leader master has begun the auto-rebalancing thread.
  void CheckAutoRebalancerStarted(BalanceThreadType type =
                                  BalanceThreadType::REPLICA_REBALANCE) {
    ASSERT_EVENTUALLY([&] {
      int leader_idx;
      ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
      ASSERT_LT(0, NumLoopIterations(leader_idx, type));
    });
  }

  void CheckNoLeaderMovesScheduled() {
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    const auto leader_initial_loop_iterations =
      NumLoopIterations(leader_idx, BalanceThreadType::LEADER_REBALANCE);
    ASSERT_EVENTUALLY([&] {
      if (FLAGS_auto_leader_rebalancing_enabled) {
        ASSERT_LT(leader_initial_loop_iterations + 3,
                  NumLoopIterations(leader_idx, BalanceThreadType::LEADER_REBALANCE));
      } else {
        SleepFor(MonoDelta::FromSeconds(3 * FLAGS_auto_rebalancing_interval_seconds));
      }
      ASSERT_EQ(0, NumMovesScheduled(leader_idx, BalanceThreadType::LEADER_REBALANCE));
    });
  }
  // Make sure the auto-rebalancing loop has iterated a few times,
  // and no moves were scheduled.
  void CheckNoMovesScheduled() {
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    const auto initial_loop_iterations = NumLoopIterations(leader_idx);
    ASSERT_EVENTUALLY([&] {
      ASSERT_LT(initial_loop_iterations + 3, NumLoopIterations(leader_idx));
      ASSERT_EQ(0, NumMovesScheduled(leader_idx));
    });
    CheckNoLeaderMovesScheduled();
  }

  void CheckSomeMovesScheduled() {
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    const auto initial_loop_iterations = NumLoopIterations(leader_idx);
    ASSERT_EVENTUALLY([&] {
      ASSERT_LT(initial_loop_iterations, NumLoopIterations(leader_idx));
      ASSERT_LT(0, NumMovesScheduled(leader_idx));
    });
  }

  static Status BuildClusterRawInfoForTest(
      AutoRebalancerTask* auto_rebalancer,
      const optional<string>& location,
      rebalance::ClusterRawInfo* raw_info) {
    return auto_rebalancer->BuildClusterRawInfo(location, raw_info);
  }

  static Status BuildClusterInfoForTest(
      AutoRebalancerTask* auto_rebalancer,
      const rebalance::ClusterRawInfo& raw_info,
      rebalance::ClusterInfo* cluster_info,
      const rebalance::Rebalancer::MovesInProgress& moves_in_progress = {}) {
    return auto_rebalancer->rebalancer_.BuildClusterInfo(
        raw_info, moves_in_progress, cluster_info);
  }

  static map<string, int> ComputeRangeReplicaSkew(
      const rebalance::ClusterRawInfo& raw_info,
      const string& table_id) {
    vector<string> tserver_uuids;
    tserver_uuids.reserve(raw_info.tserver_summaries.size());
    for (const auto& ts : raw_info.tserver_summaries) {
      tserver_uuids.emplace_back(ts.uuid);
    }

    unordered_map<string, unordered_map<string, int>> counts_by_tag;
    for (const auto& tablet : raw_info.tablet_summaries) {
      if (tablet.table_id != table_id) {
        continue;
      }
      auto& counts_by_ts = counts_by_tag[tablet.range_key_begin];
      for (const auto& replica : tablet.replicas) {
        counts_by_ts[replica.ts_uuid]++;
      }
    }

    map<string, int> skew_by_tag;
    for (const auto& [tag, counts_by_ts] : counts_by_tag) {
      int min_count = numeric_limits<int>::max();
      int max_count = numeric_limits<int>::min();
      for (const auto& ts_uuid : tserver_uuids) {
        const int count = FindWithDefault(counts_by_ts, ts_uuid, 0);
        min_count = min(min_count, count);
        max_count = max(max_count, count);
      }
      skew_by_tag.emplace(tag, max_count - min_count);
    }
    return skew_by_tag;
  }

  // Maps from tserver UUID to the bytes sent and fetched by each tserver as a
  // part of tablet copying.
  typedef unordered_map<string, int> MetricByUuid;
  MetricByUuid GetCountersByUuid(CounterPrototype* prototype) const {
    MetricByUuid metric_by_uuid;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      const auto& ts = cluster_->mini_tablet_server(i);
      scoped_refptr<Counter> metric =
          prototype->Instantiate(ts->server()->metric_entity());
      EmplaceOrDie(&metric_by_uuid, ts->uuid(), metric->value());
    }
    return metric_by_uuid;
  }
  MetricByUuid GetBytesSentByTServer() const {
    return GetCountersByUuid(&METRIC_tablet_copy_bytes_sent);
  }
  MetricByUuid GetBytesFetchedByTServer() const {
    return GetCountersByUuid(&METRIC_tablet_copy_bytes_fetched);
  }

  // Returns an aggregate of the counts in 'bytes_by_uuid' for tablet servers
  // with indices in the range ['start_ts_idx', 'end_ts_idx').
  int AggregateMetricCounts(const MetricByUuid& bytes_by_uuid,
                            int start_ts_idx, int end_ts_idx) {
    int ret = 0;
    for (int i = start_ts_idx; i < end_ts_idx; i++) {
      const auto& uuid = cluster_->mini_tablet_server(i)->uuid();
      ret += FindOrDie(bytes_by_uuid, uuid);
    }
    return ret;
  }

  int NumLoopIterations(int master_idx,
                        BalanceThreadType type = BalanceThreadType::REPLICA_REBALANCE) {
    DCHECK(cluster_ != nullptr);
    if (type == BalanceThreadType::REPLICA_REBALANCE) {
      return cluster_->mini_master(master_idx)->master()->catalog_manager()->
        auto_rebalancer()->number_of_loop_iterations_for_test_;
    }
    return cluster_->mini_master(master_idx)->master()->catalog_manager()->
        auto_leader_rebalancer()->number_of_loop_iterations_for_test_;
  }

  int NumMovesScheduled(int master_idx,
                        BalanceThreadType type = BalanceThreadType::REPLICA_REBALANCE) {
    DCHECK(cluster_ != nullptr);
    if (type == BalanceThreadType::REPLICA_REBALANCE) {
    return cluster_->mini_master(master_idx)->master()->catalog_manager()->
        auto_rebalancer()->moves_scheduled_this_round_for_test_;
    }
    return cluster_->mini_master(master_idx)->master()->catalog_manager()->
        auto_leader_rebalancer()->moves_scheduled_this_round_for_test_;
  }

  void CreateWorkloadTable(int num_tablets, int num_replicas) {
    workload_.reset(new TestWorkload(cluster_.get()));
    workload_->set_num_tablets(num_tablets);
    workload_->set_num_replicas(num_replicas);
    workload_->Setup();
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    // Restore any flags after the cluster is fully shut down.
    flag_saver_.reset();
    KuduTest::TearDown();
  }

  protected:
    unique_ptr<InternalMiniCluster> cluster_;
    InternalMiniClusterOptions cluster_opts_;
    unique_ptr<TestWorkload> workload_;
    unique_ptr<FlagSaver> flag_saver_;
  };

// Make sure that only the leader master is doing auto-rebalancing
// and auto leader-rebalancing.
TEST_F(AutoRebalancerTest, OnlyLeaderDoesAutoRebalancing) {
  const int kNumMasters = 3;
  const int kNumTservers = 3;
  const int kNumTablets = 4;
  cluster_opts_.num_masters = kNumMasters;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());
  NO_FATALS(CheckAutoRebalancerStarted(BalanceThreadType::LEADER_REBALANCE));

  CreateWorkloadTable(kNumTablets, /*num_replicas*/1);

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  for (int i = 0; i < kNumMasters; i++) {
    if (i == leader_idx) {
      ASSERT_EVENTUALLY([&] {
        ASSERT_LT(0, NumLoopIterations(i, BalanceThreadType::REPLICA_REBALANCE));
        ASSERT_LT(0, NumLoopIterations(i, BalanceThreadType::LEADER_REBALANCE));
      });
    } else {
      ASSERT_EVENTUALLY([&] {
        ASSERT_EQ(0, NumLoopIterations(i, BalanceThreadType::REPLICA_REBALANCE));
        ASSERT_EQ(0, NumLoopIterations(i, BalanceThreadType::LEADER_REBALANCE));
      });
    }
  }
}

// Make sure the auto-rebalancing can be toggled on/off in runtime.
TEST_F(AutoRebalancerTest, AutoRebalancingTurnOffAndOn) {
  cluster_opts_.num_masters = 1;
  cluster_opts_.num_tablet_servers = 3;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(8, /*num_replicas*/ 3);
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  ASSERT_EQ(0, leader_idx);

  FLAGS_auto_rebalancing_enabled = false;
  ASSERT_OK(cluster_->AddTabletServer());
  for (int i = 0; i < 3; i++) {
    // Wait a schedule period, 1s.
    SleepFor(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_interval_seconds));
    ASSERT_EQ(0, NumMovesScheduled(leader_idx));
  }
  int num_iterations = NumLoopIterations(leader_idx);
  SleepFor(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_interval_seconds * 2));
  ASSERT_EQ(num_iterations, NumLoopIterations(leader_idx));

  FLAGS_auto_rebalancing_enabled = true;
  CheckSomeMovesScheduled();
}

// If the leader master goes down, the next elected master should perform
// auto-rebalancing.
TEST_F(AutoRebalancerTest, NextLeaderResumesAutoRebalancing) {
  const int kNumMasters = 3;
  const int kNumTservers = 3;
  const int kNumTablets = 3;
  cluster_opts_.num_masters = kNumMasters;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());
  NO_FATALS(CheckAutoRebalancerStarted(BalanceThreadType::LEADER_REBALANCE));

  CreateWorkloadTable(kNumTablets, /*num_replicas*/1);

  // Verify that non-leaders are not performing rebalancing,
  // then take down the leader master.
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  for (int i = 0; i < kNumMasters; i++) {
    if (i != leader_idx) {
      ASSERT_EQ(0, NumLoopIterations(i));
      ASSERT_EQ(0, NumLoopIterations(i, BalanceThreadType::LEADER_REBALANCE));
    }
  }
  cluster_->mini_master(leader_idx)->Shutdown();

  // Let another master become leader and resume auto-rebalancing.
  // Number of auto-rebalancing iterations should increase again.
  int new_leader_idx;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&new_leader_idx));
    ASSERT_NE(leader_idx, new_leader_idx);
    auto iterations = NumLoopIterations(new_leader_idx);
    ASSERT_LT(0, iterations);

    auto iterations_leader = NumLoopIterations(new_leader_idx, BalanceThreadType::LEADER_REBALANCE);
    ASSERT_LT(0, iterations_leader);
  });
}

// Create a cluster that is initially balanced and leader balanced.
// Bring up another tserver, and verify that moves are scheduled,
// since the cluster is no longer balanced.
TEST_F(AutoRebalancerTest, MovesScheduledIfAddTserver) {
  const int kNumTServers = 3;
  const int kNumTablets = 2;
  cluster_opts_.num_tablet_servers = kNumTServers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  NO_FATALS(CheckNoMovesScheduled());
  // Take a snapshot of the number of copy sources started on the original
  // tablet servers.
  const int initial_bytes_sent_in_orig_tservers =
      AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumTServers);

  // Add a tablet server and verify that the master schedules some moves, and
  // the tablet servers copy bytes as appropriate.
  ASSERT_OK(cluster_->AddTabletServer());
  NO_FATALS(CheckSomeMovesScheduled());
  ASSERT_EVENTUALLY([&] {
    int bytes_sent_in_orig_tservers =
        AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumTServers);
    ASSERT_GT(bytes_sent_in_orig_tservers, initial_bytes_sent_in_orig_tservers);
  });
  // Our new tablet servers should start fetching data as well.
  ASSERT_EVENTUALLY([&] {
    int bytes_fetched_in_new_tservers =
        AggregateMetricCounts(GetBytesFetchedByTServer(), kNumTServers,
                              kNumTServers + 1);
    ASSERT_GT(bytes_fetched_in_new_tservers, 0);
  });

  NO_FATALS(CheckNoMovesScheduled());
}

// A cluster with no tservers is balanced.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfNoTservers) {
  const int kNumTservers = 0;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());
  NO_FATALS(CheckNoMovesScheduled());
}

// A cluster with no tablets is balanced.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfNoTablets) {
  const int kNumTservers = 3;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());
  NO_FATALS(CheckNoMovesScheduled());
}

// Verify that range-aware mode groups balance info by range start key, while
// non-range-aware mode collapses all ranges into a single tag.
TEST_F(AutoRebalancerTest, RangeAwareBuildClusterInfoGroupsByRange) {
  flag_saver_ = make_unique<FlagSaver>();
  FLAGS_auto_rebalancing_enable_range_rebalancing = true;
  // Avoid minidump handler thread teardown races in mini-cluster shutdown.
  FLAGS_enable_minidumps = false;

  cluster_opts_.num_tablet_servers = 3;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));

  const string kTableName = "range_aware_auto_rebalancer_test";
  KuduSchema schema;
  KuduSchemaBuilder builder;
  builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull();
  builder.SetPrimaryKey({ "key" });
  ASSERT_OK(builder.Build(&schema));

  unique_ptr<KuduPartialRow> lower0(schema.NewRow());
  unique_ptr<KuduPartialRow> upper0(schema.NewRow());
  ASSERT_OK(lower0->SetInt32("key", 0));
  ASSERT_OK(upper0->SetInt32("key", 10));
  unique_ptr<KuduPartialRow> lower1(schema.NewRow());
  unique_ptr<KuduPartialRow> upper1(schema.NewRow());
  ASSERT_OK(lower1->SetInt32("key", 10));
  ASSERT_OK(upper1->SetInt32("key", 20));

  // Create a table with two explicit ranges and hash-partition each range
  // into two buckets, so we get tablets in two distinct range groups.
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
                .schema(&schema)
                .add_hash_partitions({ "key" }, 2)
                .set_range_partition_columns({ "key" })
                .add_range_partition(lower0.release(), upper0.release())
                .add_range_partition(lower1.release(), upper1.release())
                .num_replicas(3)
                .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  const auto& table_id = table->id();

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  auto* auto_rebalancer =
      cluster_->mini_master(leader_idx)->master()->catalog_manager()->auto_rebalancer();

  rebalance::ClusterRawInfo raw_info;
  ASSERT_OK(BuildClusterRawInfoForTest(auto_rebalancer, nullopt, &raw_info));

  // Raw tablet summaries should carry non-empty range tags in range-aware mode.
  unordered_set<string> range_tags;
  for (const auto& tablet : raw_info.tablet_summaries) {
    if (tablet.table_id != table_id) {
      continue;
    }
    ASSERT_FALSE(tablet.range_key_begin.empty());
    range_tags.insert(tablet.range_key_begin);
  }
  ASSERT_EQ(2, range_tags.size());

  // Balance info should now be grouped by table_id + range tag.
  rebalance::ClusterInfo cluster_info;
  ASSERT_OK(BuildClusterInfoForTest(auto_rebalancer, raw_info, &cluster_info));
  unordered_set<string> tags_in_balance;
  for (const auto& elem : cluster_info.balance.table_info_by_skew) {
    const auto& tbi = elem.second;
    if (tbi.table_id == table_id) {
      tags_in_balance.insert(tbi.tag);
    }
  }
  ASSERT_EQ(range_tags, tags_in_balance);

  // With range-aware mode disabled, range tags should be empty.
  FLAGS_auto_rebalancing_enable_range_rebalancing = false;
  rebalance::ClusterRawInfo raw_info_no_range;
  ASSERT_OK(BuildClusterRawInfoForTest(auto_rebalancer, nullopt, &raw_info_no_range));
  int tablet_count = 0;
  for (const auto& tablet : raw_info_no_range.tablet_summaries) {
    if (tablet.table_id != table_id) {
      continue;
    }
    ++tablet_count;
    ASSERT_TRUE(tablet.range_key_begin.empty());
  }
  ASSERT_GT(tablet_count, 0);

  // Balance info should collapse to a single empty tag.
  rebalance::ClusterInfo cluster_info_no_range;
  rebalance::Rebalancer local_rebalancer(rebalance::Rebalancer::Config{});
  ASSERT_OK(local_rebalancer.BuildClusterInfo(
      raw_info_no_range, rebalance::Rebalancer::MovesInProgress(), &cluster_info_no_range));
  unordered_set<string> tags_no_range;
  for (const auto& elem : cluster_info_no_range.balance.table_info_by_skew) {
    const auto& tbi = elem.second;
    if (tbi.table_id == table_id) {
      tags_no_range.insert(tbi.tag);
    }
  }
  ASSERT_EQ(1, tags_no_range.size());
  ASSERT_TRUE(ContainsKey(tags_no_range, ""));
}

TEST_F(AutoRebalancerTest, RangeAwareRebalancesNewRangeReplicas) {
  flag_saver_ = make_unique<FlagSaver>();
  FLAGS_auto_rebalancing_enable_range_rebalancing = true;
  FLAGS_auto_rebalancing_enabled = false;
  FLAGS_enable_range_replica_placement = false;
  // Avoid minidump handler thread teardown races in mini-cluster shutdown.
  FLAGS_enable_minidumps = false;

  cluster_opts_.num_tablet_servers = 3;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));

  const string kTableName = "range_aware_rebalance_new_range";
  KuduSchema schema;
  KuduSchemaBuilder builder;
  builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull();
  builder.SetPrimaryKey({ "key" });
  ASSERT_OK(builder.Build(&schema));

  unique_ptr<KuduPartialRow> lower0(schema.NewRow());
  unique_ptr<KuduPartialRow> upper0(schema.NewRow());
  ASSERT_OK(lower0->SetInt32("key", 0));
  ASSERT_OK(upper0->SetInt32("key", 10));

  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
                .schema(&schema)
                .add_hash_partitions({ "key" }, 8)
                .set_range_partition_columns({ "key" })
                .add_range_partition(lower0.release(), upper0.release())
                .num_replicas(3)
                .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  const auto& table_id = table->id();

  // Add a new tserver with no replicas yet.
  ASSERT_OK(cluster_->AddTabletServer());

  // Add a second range after the empty tserver joins.
  unique_ptr<KuduPartialRow> lower1(schema.NewRow());
  unique_ptr<KuduPartialRow> upper1(schema.NewRow());
  ASSERT_OK(lower1->SetInt32("key", 10));
  ASSERT_OK(upper1->SetInt32("key", 20));
  unique_ptr<KuduTableAlterer> alterer(
      client->NewTableAlterer(kTableName));
  ASSERT_OK(alterer->AddRangePartition(lower1.release(), upper1.release())
                ->Alter());

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  auto* auto_rebalancer =
      cluster_->mini_master(leader_idx)->master()->catalog_manager()->auto_rebalancer();

  // Wait until both ranges are present and replicas are placed.
  rebalance::ClusterRawInfo raw_info;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(BuildClusterRawInfoForTest(auto_rebalancer, nullopt, &raw_info));
    int tablet_count = 0;
    unordered_set<string> tags;
    for (const auto& tablet : raw_info.tablet_summaries) {
      if (tablet.table_id != table_id) {
        continue;
      }
      ++tablet_count;
      ASSERT_FALSE(tablet.range_key_begin.empty());
      tags.insert(tablet.range_key_begin);
    }
    ASSERT_EQ(16, tablet_count);
    ASSERT_EQ(2, tags.size());
  });

  // Capture skew before rebalancing; it may or may not be imbalanced depending
  // on placement randomness, so we don't assert on it.
  const auto skew_by_tag = ComputeRangeReplicaSkew(raw_info, table_id);

  // Enable the auto-rebalancer and wait for skew <= 1 for each range.
  FLAGS_auto_rebalancing_enabled = true;
  ASSERT_EVENTUALLY([&] {
    rebalance::ClusterRawInfo raw_info_after;
    ASSERT_OK(BuildClusterRawInfoForTest(auto_rebalancer, nullopt, &raw_info_after));
    const auto skew_after = ComputeRangeReplicaSkew(raw_info_after, table_id);
    for (const auto& elem : skew_after) {
      ASSERT_LE(elem.second, 1) << "range tag: " << elem.first;
    }
  });
}

// Assign each tserver to its own location.
// A cluster with location load skew = 1 is balanced.
// In this test, 3 tservers should have load = 1, 1 tserver should have load = 0.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfLocationLoadSkewedByOne) {
  const int kNumTservers = 4;
  const int kNumTablets = 1;
  const int kNumLocations = kNumTservers;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  NO_FATALS(AssignLocationsEvenly(kNumLocations));

  const auto timeout = MonoDelta::FromSeconds(30);
  vector<master::ListTabletServersResponsePB_Entry> tservers;
  ASSERT_OK(ListTabletServers(cluster_->master_proxy(),
                              timeout,
                              &tservers));
  set<string> locations;
  for (const auto& tserver : tservers) {
    const auto& ts_location = tserver.location();
    locations.insert(ts_location);
    ASSERT_FALSE(ts_location.empty());
  }
  ASSERT_EQ(kNumTservers, locations.size());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  NO_FATALS(CheckNoMovesScheduled());
}

// Assign tservers to one of two locations.
// If placement policy can never be satisfied, the auto-rebalancer should
// not attempt to schedule any replica movements.
// In this test, the tablet should have the majority (2/3) of its replicas
// in the same location. This violation cannot be fixed.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfCannotFixPlacementPolicy) {
  const int kNumTservers = 3;
  const int kNumTablets = 1;
  const int kNumLocations = 2;
  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  NO_FATALS(AssignLocationsWithSkew(kNumLocations));

  const auto timeout = MonoDelta::FromSeconds(30);
  vector<master::ListTabletServersResponsePB_Entry> tservers;
  ASSERT_OK(itest::ListTabletServers(cluster_->master_proxy(),
                                     timeout,
                                     &tservers));
  set<string> locations;
  for (const auto& tserver : tservers) {
    const auto& ts_location = tserver.location();
    locations.insert(ts_location);
    ASSERT_FALSE(ts_location.empty());
  }
  ASSERT_EQ(kNumLocations, locations.size());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  NO_FATALS(CheckNoMovesScheduled());
}

// Verify that each server's number of tablet copying sessions
// doesn't exceed the value in the flag auto_rebalancing_max_moves_per_server.
TEST_F(AutoRebalancerTest, TestMaxMovesPerServer) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Make tablet replica copying slow, to make it easier to spot violations.
  FLAGS_tablet_copy_download_file_inject_latency_ms = 2000;

  const int kNumOrigTservers = 3;
  const int kNumAdditionalTservers = 3;
  const int kNumTablets = 12;

  cluster_opts_.num_tablet_servers = kNumOrigTservers;
  // Disable leader rebalancing to avoid interfering with replica rebalancing test.
  ASSERT_OK(CreateAndStartCluster(/*enable_leader_rebalance=*/false));
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);
  workload_->Start();
  while (workload_->rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  workload_->StopAndJoin();

  // Take a snapshot of the number of copy sources started on the original
  // tablet servers.
  const int initial_bytes_sent_in_orig_tservers =
      AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumOrigTservers);

  // Bring up additional tservers, to trigger tablet replica copying.
  for (int i = 0; i < kNumAdditionalTservers; ++i) {
    ASSERT_OK(cluster_->AddTabletServer());
  }

  // At some point we should start scheduling moves and see some copy source
  // sessions start on the original tablet servers.
  NO_FATALS(CheckSomeMovesScheduled());
  ASSERT_EVENTUALLY([&] {
    int bytes_sent_in_orig_tservers =
        AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumOrigTservers);
    ASSERT_GT(bytes_sent_in_orig_tservers, initial_bytes_sent_in_orig_tservers);
  });
  // Our new tablet servers should start fetching data as well.
  ASSERT_EVENTUALLY([&] {
    int bytes_fetched_in_new_tservers =
        AggregateMetricCounts(GetBytesFetchedByTServer(), kNumOrigTservers,
                              kNumOrigTservers + kNumAdditionalTservers);
    ASSERT_GT(bytes_fetched_in_new_tservers, 0);
  });

  // Check metric 'tablet_copy_open_client_sessions', which must be
  // less than the auto_rebalancing_max_moves_per_server, for each tserver.
  // Use ASSERT_EVENTUALLY to handle timing issues: the auto-rebalancer
  // schedules moves asynchronously, so we need to retry the check to allow
  // for momentary violations during scheduling.
  ASSERT_EVENTUALLY([&] {
    MetricByUuid open_copy_clients_by_uuid;
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      const auto& ts = cluster_->mini_tablet_server(i);
      int open_client_sessions =
          METRIC_tablet_copy_open_client_sessions.Instantiate(ts->server()->metric_entity(), 0)
              ->value();
      EmplaceOrDie(&open_copy_clients_by_uuid, ts->uuid(), open_client_sessions);

      ASSERT_LE(open_client_sessions, FLAGS_auto_rebalancing_max_moves_per_server)
          << "Tserver " << ts->uuid() << " exceeded max moves per server";
    }

    int total_moves =
        AggregateMetricCounts(open_copy_clients_by_uuid, 0, cluster_->num_tablet_servers());
    int max_total = FLAGS_auto_rebalancing_max_moves_per_server * cluster_->num_tablet_servers();
    ASSERT_LE(total_moves, max_total)
        << "Total moves " << total_moves << " exceeds max " << max_total;
  });

  NO_FATALS(CheckNoLeaderMovesScheduled());
}

// Attempt rebalancing a cluster with unstable Raft configurations.
TEST_F(AutoRebalancerTest, AutoRebalancingUnstableCluster) {
  // Set a low Raft heartbeat.
  FLAGS_raft_heartbeat_interval_ms = kLowRaftTimeout;

  const int kNumTServers = 3;
  const int kNumTablets = 3;

  cluster_opts_.num_tablet_servers = kNumTServers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  // Inject latency to make tablets' Raft configurations unstable due to
  // frequent leader re-elections.
  FLAGS_consensus_inject_latency_ms_in_notifications = kLowRaftTimeout;

  // Take a snapshot of the number of copy sources started on the original
  // tablet servers.
  const int initial_bytes_sent_in_orig_tservers =
      AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumTServers);

  // Bring up an additional tserver to trigger rebalancing.
  // Moves should be scheduled, even if they cannot be completed because of
  // the frequent leadership changes.
  ASSERT_OK(cluster_->AddTabletServer());
  NO_FATALS(CheckSomeMovesScheduled());
  // At some point, the move should succeed though.
  ASSERT_EVENTUALLY([&] {
    int bytes_sent_in_orig_tservers =
        AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumTServers);
    ASSERT_GT(bytes_sent_in_orig_tservers, initial_bytes_sent_in_orig_tservers);
  });
  ASSERT_EVENTUALLY([&] {
    int bytes_fetched_in_new_tservers =
        AggregateMetricCounts(GetBytesFetchedByTServer(), kNumTServers,
                              cluster_->num_tablet_servers());
    ASSERT_GT(bytes_fetched_in_new_tservers, 0);
  });

  NO_FATALS(CheckNoLeaderMovesScheduled());
}

// A cluster that cannot become healthy and meet the replication factor
// will not attempt rebalancing.
TEST_F(AutoRebalancerTest, NoReplicaMovesIfCannotMeetReplicationFactor) {
  const int kNumTservers = 3;
  const int kNumTablets = 1;

  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster());
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  // Take down a tserver, to make it impossible to meet RF=3 with 2 tservers.
  NO_FATALS(cluster_->mini_tablet_server(0)->Shutdown());
  NO_FATALS(CheckNoMovesScheduled());
}

// Verify that movement of replicas to meet the replication factor
// does not count towards rebalancing, i.e. the auto-rebalancer will
// not consider recovering replicas as candidates for replica movement.
TEST_F(AutoRebalancerTest, NoRebalancingIfReplicasRecovering) {
  // Set a low timeout for an unresponsive tserver to be presumed dead by the master.
  FLAGS_tserver_unresponsive_timeout_ms = 1000;

  // Shorten the interval for recovery re-replication to begin.
  FLAGS_follower_unavailable_considered_failed_sec = 1;

  const int kNumTservers = 3;
  const int kNumTablets = 4;

  cluster_opts_.num_tablet_servers = kNumTservers;
  bool auto_leader_rebalancing_enabled = false;
  ASSERT_OK(CreateAndStartCluster(auto_leader_rebalancing_enabled));
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  // Kill a tablet server so when we add a tablet server, auto-rebalancing will
  // do nothing. Also wait a bit until the tserver is deemed unresponsive so
  // the auto-rebalancer will not attempt to rebalance anything.
  NO_FATALS(cluster_->mini_tablet_server(0)->Shutdown());
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_tserver_unresponsive_timeout_ms));
  ASSERT_OK(cluster_->AddTabletServer());

  // No rebalancing should occur while there are under-replicated replicas.
  NO_FATALS(CheckNoMovesScheduled());
}

// Make sure the auto-rebalancer reports the failure of scheduled replica movements,
// in the case that tablet server failure is not yet accounted for by the TSManager.
TEST_F(AutoRebalancerTest, TestHandlingFailedTservers) {
  // Set a high timeout for an unresponsive tserver to be presumed dead,
  // so the TSManager believes it is still available.
  FLAGS_tserver_unresponsive_timeout_ms = 120 * 1000;
  bool auto_leader_rebalancing_enabled = false; // Disable for testing.

  const int kNumTservers = 3;
  const int kNumTablets = 4;

  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster(auto_leader_rebalancing_enabled));
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  // Take down all the original tservers.
  for (int i = 0; i < kNumTservers; ++i) {
    NO_FATALS(cluster_->mini_tablet_server(i)->Shutdown());
  }

  // Capture the glog output to ensure failed replica movements occur
  // and the warnings are logged.
  StringVectorSink pre_capture_logs;
  {
    ScopedRegisterSink reg(&pre_capture_logs);
    // Bring up a new tserver.
    ASSERT_OK(cluster_->AddTabletServer());

    // The TSManager should still believe the original tservers are available,
    // so the auto-rebalancer should attempt to schedule replica moves from those
    // tservers to the new one.
    NO_FATALS(CheckSomeMovesScheduled());
  }
  {
    SCOPED_TRACE(JoinStrings(pre_capture_logs.logged_msgs(), "\n"));
    ASSERT_STRINGS_ANY_MATCH(pre_capture_logs.logged_msgs(),
        "scheduled replica move failed to complete|failed to send replica move request");
  }

  // Wait for the TSManager to realize that the original tservers are unavailable.
  FLAGS_tserver_unresponsive_timeout_ms = 5 * 1000;
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_tserver_unresponsive_timeout_ms));

  // Bring back the tservers.
  for (int i = 0; i < kNumTservers; ++i) {
    ASSERT_OK(cluster_->mini_tablet_server(i)->Restart());
  }

  // Make sure the auto-rebalancer thread schedules replica moves because of the
  // re-available tservers.
  StringVectorSink post_capture_logs;
  {
    ScopedRegisterSink reg(&post_capture_logs);
    NO_FATALS(CheckSomeMovesScheduled());
  }

  // These replica moves should not fail because the tservers are unavailable.
  for (const auto& str : post_capture_logs.logged_msgs()) {
    ASSERT_STR_NOT_CONTAINS(str, "scheduled replica move failed to complete: Network error");
  }
  NO_FATALS(CheckNoLeaderMovesScheduled());
}

// Test that we schedule moves even if some tablets belong to deleted tables.
// The moves scheduled shouldn't move replicas of deleted tables.
TEST_F(AutoRebalancerTest, TestDeletedTables) {
  // Set a low timeout for an unresponsive tserver to be presumed dead by the
  // master and shorten the interval for recovery re-replication to begin.
  FLAGS_tserver_unresponsive_timeout_ms = 1000;
  FLAGS_follower_unavailable_considered_failed_sec = 1;
  const int kNumTServers = 3;
  const int kNumTablets = 4;
  cluster_opts_.num_tablet_servers = kNumTServers;
  bool auto_leader_rebalancing_enabled = false; // Disable for testing.
  ASSERT_OK(CreateAndStartCluster(auto_leader_rebalancing_enabled));
  NO_FATALS(CheckAutoRebalancerStarted());

  // Create some tablets across multiple tables, so we can test rebalancing
  // when a table is deleted.
  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);
  const string kNewTableName = "dugtrio";
  {
    TestWorkload w(cluster_.get());
    w.set_num_tablets(kNumTablets);
    w.set_table_name(kNewTableName);
    w.Setup();
  }
  const int initial_bytes_sent_in_orig_tservers =
      AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumTServers);
  // Kill a tablet server so when we add a tablet server, the auto-rebalancer
  // see an unhealthy cluster and do nothing, waiting a bit until the master
  // deems the server unresponsive.
  NO_FATALS(cluster_->mini_tablet_server(0)->Shutdown());
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_tserver_unresponsive_timeout_ms));

  // Delete one of the tables after figuring out what its tablet IDs are.
  GetTableLocationsResponsePB table_locs;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(), kNewTableName,
                              MonoDelta::FromSeconds(10), ReplicaTypeFilter::ANY_REPLICA,
                              /*table_id*/nullopt, &table_locs));
  unordered_set<string> deleted_tablet_ids;
  for (const auto& t : table_locs.tablet_locations()) {
    EmplaceIfNotPresent(&deleted_tablet_ids, t.tablet_id());
  }
  ASSERT_OK(workload_->client()->DeleteTable(kNewTableName));

  // Add a new tablet server and restart the stopped server so the rebalancer
  // can proceed.
  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());
  NO_FATALS(CheckSomeMovesScheduled());

  // Even though not all tables are running, this should succeed because the
  // table that isn't running has been deleted.
  ASSERT_EVENTUALLY([&] {
    int bytes_sent_in_orig_tservers =
        AggregateMetricCounts(GetBytesSentByTServer(), 0, kNumTServers);
    ASSERT_GT(bytes_sent_in_orig_tservers, initial_bytes_sent_in_orig_tservers);
  });
  ASSERT_EVENTUALLY([&] {
    int bytes_fetched_in_new_tservers =
        AggregateMetricCounts(GetBytesFetchedByTServer(), kNumTServers,
                              cluster_->num_tablet_servers());
    ASSERT_GT(bytes_fetched_in_new_tservers, 0);
  });

  // Wait for the replicas to balance out.
  // NOTE: a skew of 1 is acceptable.
  vector<string> tablets_on_new_ts;
  ASSERT_EVENTUALLY([&] {
    tablets_on_new_ts = cluster_->mini_tablet_server(kNumTServers)->ListTablets();
    ASSERT_NEAR(kNumTablets * 3 / cluster_->num_tablet_servers(), tablets_on_new_ts.size(), 1);
  });
  // The tablets moved to the new server shouldn't belong to the deleted table.
  for (const auto& tid : tablets_on_new_ts) {
    ASSERT_FALSE(ContainsKey(deleted_tablet_ids, tid)) <<
        Substitute("Tablets from deleted table: $0, tablets on new server: $1",
                   JoinStrings(deleted_tablet_ids, ","),
                   JoinStrings(tablets_on_new_ts, ","));
  }
  NO_FATALS(CheckNoLeaderMovesScheduled());
}

// Test that the replace marker will be cleared if a rebalancing move fails.
TEST_F(AutoRebalancerTest, TestRemoveReplaceFlagIfMoveFails) {
  bool auto_leader_rebalancing_enabled = false; // Disable for testing.
  const bool original_fail_moves = FLAGS_auto_rebalancing_fail_moves_for_test;
  SCOPED_CLEANUP({ FLAGS_auto_rebalancing_fail_moves_for_test = original_fail_moves; });

  const int kNumTservers = 3;
  const int kNumTablets = 4;

  cluster_opts_.num_tablet_servers = kNumTservers;
  ASSERT_OK(CreateAndStartCluster(auto_leader_rebalancing_enabled));

  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);

  // Set the test flag to true so that the moves are all regarded as failed.
  FLAGS_auto_rebalancing_fail_moves_for_test = true;

  // Bring up a new tserver.
  ASSERT_OK(cluster_->AddTabletServer());

  // The TSManager should still believe the original tservers are available,
  // so the auto-rebalancer should attempt to schedule replica moves from those
  // tservers to the new one.
  NO_FATALS(CheckSomeMovesScheduled());

  // Stop the auto rebalancing after current loop.
  FLAGS_auto_rebalancing_enabled = false;

  // Check all the replace markers are false.
  ASSERT_EVENTUALLY([&] {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      const auto& tserver = cluster_->mini_tablet_server(i);
      const auto& tablet_ids = tserver->ListTablets();
      for (const auto& tablet_id: tablet_ids) {
        GetConsensusStateRequestPB req;
        GetConsensusStateResponsePB resp;
        RpcController controller;
        controller.set_timeout(MonoDelta::FromSeconds(60));
        req.set_dest_uuid(tserver->uuid());
        req.add_tablet_ids(tablet_id);
        req.set_report_health(EXCLUDE_HEALTH_REPORT);
        ASSERT_OK(cluster_->tserver_consensus_proxy(i)->GetConsensusState(
            req, &resp, &controller));

        const auto& committed_config = resp.tablets(0).cstate().committed_config();
        for (int p = 0; p < committed_config.peers_size(); p++) {
          const auto& peer = committed_config.peers(p);
          ASSERT_FALSE(peer.attrs().replace());
        }
      }
    }
  });
}

// Rebalancer::BuildClusterInfo() fills ClusterInfo::ts_with_followers_by_table_and_tag
// from non-leader replicas in the ksck-derived view. Exercise the same path as
// production via BuildClusterRawInfoForTest + BuildClusterInfoForTest on a live
// cluster with RF>1 data.
TEST_F(AutoRebalancerTest, BuildClusterInfoPopulatesFollowersByTableAndTag) {
  cluster_opts_.num_tablet_servers = 3;
  ASSERT_OK(CreateAndStartCluster(/*enable_leader_rebalance=*/false));
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(/*num_tablets*/6, /*num_replicas*/3);
  workload_->Start();
  while (workload_->rows_inserted() < 500) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  workload_->StopAndJoin();

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  auto* auto_rebalancer =
      cluster_->mini_master(leader_idx)->master()->catalog_manager()->auto_rebalancer();

  rebalance::ClusterRawInfo raw_info;
  rebalance::ClusterInfo cluster_info;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(BuildClusterRawInfoForTest(auto_rebalancer, nullopt, &raw_info));
    ASSERT_OK(BuildClusterInfoForTest(auto_rebalancer, raw_info, &cluster_info));
    ASSERT_FALSE(cluster_info.ts_with_followers_by_table_and_tag.empty())
        << "expected at least one {table,tag} with non-leader replicas";
  });
  for (const auto& elem : cluster_info.ts_with_followers_by_table_and_tag) {
    ASSERT_FALSE(elem.second.empty());
  }
}

// A replica listed as the source of an in-progress move is not counted in
// BuildClusterInfo(), so it must not appear in ts_with_followers_by_table_and_tag
// for that {table_id, tag}. Use a single tablet (RF=3) so the moving follower
// is the table's only replica on that server, making the effect visible.
TEST_F(AutoRebalancerTest, BuildClusterInfoExcludesMovingFollowerSourceFromFollowerMap) {
  cluster_opts_.num_tablet_servers = 3;
  ASSERT_OK(CreateAndStartCluster(/*enable_leader_rebalance=*/false));
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(/*num_tablets*/1, /*num_replicas*/3);
  workload_->Start();
  while (workload_->rows_inserted() < 500) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  workload_->StopAndJoin();

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  auto* auto_rebalancer =
      cluster_->mini_master(leader_idx)->master()->catalog_manager()->auto_rebalancer();

  rebalance::ClusterRawInfo raw_info;
  string tablet_id;
  string follower_ts_uuid;
  rebalance::TableIdAndTag table_and_tag;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(BuildClusterRawInfoForTest(auto_rebalancer, nullopt, &raw_info));
    bool found = false;
    for (const auto& tablet : raw_info.tablet_summaries) {
      if (tablet.result != kudu::cluster_summary::HealthCheckResult::HEALTHY) {
        continue;
      }
      for (const auto& replica : tablet.replicas) {
        if (replica.is_leader || !replica.ts_healthy) {
          continue;
        }
        tablet_id = tablet.id;
        follower_ts_uuid = replica.ts_uuid;
        table_and_tag.table_id = tablet.table_id;
        table_and_tag.tag = FLAGS_auto_rebalancing_enable_range_rebalancing
            ? tablet.range_key_begin : "";
        found = true;
        break;
      }
      if (found) {
        break;
      }
    }
    ASSERT_TRUE(found);
  });

  rebalance::ClusterInfo baseline;
  ASSERT_OK(BuildClusterInfoForTest(auto_rebalancer, raw_info, &baseline));
  const auto* baseline_followers = FindOrNull(
      baseline.ts_with_followers_by_table_and_tag, table_and_tag);
  ASSERT_TRUE(baseline_followers != nullptr);
  ASSERT_TRUE(ContainsKey(*baseline_followers, follower_ts_uuid));

  rebalance::Rebalancer::MovesInProgress moves_in_progress;
  moves_in_progress.emplace(
      tablet_id,
      rebalance::Rebalancer::ReplicaMove{ tablet_id, follower_ts_uuid, "" });

  rebalance::ClusterInfo with_move;
  ASSERT_OK(BuildClusterInfoForTest(
      auto_rebalancer, raw_info, &with_move, moves_in_progress));
  const auto* followers_after = FindOrNull(
      with_move.ts_with_followers_by_table_and_tag, table_and_tag);
  ASSERT_TRUE(followers_after != nullptr);
  ASSERT_FALSE(ContainsKey(*followers_after, follower_ts_uuid));
}

// Parameterized fixture for testing --auto_rebalancing_prefer_follower_replica_moves
// with both true and false. Replica moves must be scheduled in both cases;
// the flag only affects follower-vs-leader preference among equal candidates.
class PreferFollowerRebalancingTest :
    public AutoRebalancerTest,
    public ::testing::WithParamInterface<bool> {
};
INSTANTIATE_TEST_SUITE_P(, PreferFollowerRebalancingTest, ::testing::Bool());

TEST_P(PreferFollowerRebalancingTest, SchedulesReplicaMoves) {
  flag_saver_ = make_unique<FlagSaver>();
  FLAGS_auto_rebalancing_prefer_follower_replica_moves = GetParam();

  constexpr int kNumOrigTservers = 3;
  constexpr int kNumTablets = 6;

  cluster_opts_.num_tablet_servers = kNumOrigTservers;
  ASSERT_OK(CreateAndStartCluster(/*enable_leader_rebalance=*/false));
  NO_FATALS(CheckAutoRebalancerStarted());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/3);
  workload_->Start();
  while (workload_->rows_inserted() < 500) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  workload_->StopAndJoin();

  ASSERT_OK(cluster_->AddTabletServer());
  NO_FATALS(CheckSomeMovesScheduled());
}

// Mixed RF scenario: one RF=1 table (no followers) and one RF=3 table (has
// followers). The rebalancer must handle both simultaneously without crashing
// and still schedule moves.
TEST_F(AutoRebalancerTest, TestReplicaRebalancingMixedRFNoCrash) {
  flag_saver_ = make_unique<FlagSaver>();
  FLAGS_auto_rebalancing_prefer_follower_replica_moves = true;

  constexpr int kNumOrigTservers = 3;
  constexpr int kNumTablets = 6;

  cluster_opts_.num_tablet_servers = kNumOrigTservers;
  ASSERT_OK(CreateAndStartCluster(/*enable_leader_rebalance=*/false));
  NO_FATALS(CheckAutoRebalancerStarted());

  // RF=1 table: all replicas are leaders, follower map will be empty for it.
  CreateWorkloadTable(kNumTablets, /*num_replicas*/1);
  workload_->Start();
  while (workload_->rows_inserted() < 500) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  workload_->StopAndJoin();

  // RF=3 table: use a distinct name so Setup() creates a second table rather
  // than reopening the already-existing RF=1 table (TestWorkload default name
  // is "test-workload" for both, so an explicit name is required here).
  workload_.reset(new TestWorkload(cluster_.get()));
  workload_->set_num_tablets(kNumTablets);
  workload_->set_num_replicas(3);
  workload_->set_table_name("test-workload-rf3");
  workload_->Setup();
  workload_->Start();
  while (workload_->rows_inserted() < 500) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  workload_->StopAndJoin();

  ASSERT_OK(cluster_->AddTabletServer());
  NO_FATALS(CheckSomeMovesScheduled());
}

} // namespace master
} // namespace kudu
