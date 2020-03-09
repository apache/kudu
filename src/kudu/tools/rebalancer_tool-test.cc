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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <memory>
#include <map>
#include <numeric>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
class Schema;
}  // namespace kudu

DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

using boost::none;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::cluster::LocationInfo;
using kudu::itest::TabletServerMap;
using kudu::tserver::ListTabletsResponsePB;
using std::atomic;
using std::back_inserter;
using std::copy;
using std::endl;
using std::ostream;
using std::ostringstream;
using std::string;
using std::thread;
using std::tuple;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace tools {

  // Helper to format info when a tool action fails.
static string ToolRunInfo(const Status& s, const string& out, const string& err) {
  ostringstream str;
  str << s.ToString() << endl;
  str << "stdout: " << out << endl;
  str << "stderr: " << err << endl;
  return str.str();
}

// Helper macro for tool tests. Use as follows:
//
// ASSERT_TOOL_OK("cluster", "ksck", master_addrs);
//
// The failure Status result of RunKuduTool is usually useless, so this macro
// also logs the stdout and stderr in case of failure, for easier diagnosis.
// TODO(wdberkeley): Add a macro to retrieve stdout or stderr, or a macro for
//                   when one of those should match a string.
#define ASSERT_TOOL_OK(...) do { \
  const vector<string>& _args{__VA_ARGS__}; \
  string _out, _err; \
  const Status& _s = RunKuduTool(_args, &_out, &_err); \
  if (_s.ok()) { \
    SUCCEED(); \
  } else { \
    FAIL() << ToolRunInfo(_s, _out, _err); \
  } \
} while (0)

class AdminCliTest : public tserver::TabletServerIntegrationTestBase {
};

TEST_F(AdminCliTest, RebalancerReportOnly) {
  static const char kReferenceOutput[] =
    R"***(Per-server replica distribution summary:
       Statistic       |  Value
-----------------------+----------
 Minimum Replica Count | 0
 Maximum Replica Count | 1
 Average Replica Count | 0.600000

Per-table replica distribution summary:
 Replica Skew |  Value
--------------+----------
 Minimum      | 1
 Maximum      | 1
 Average      | 1.000000)***";

  FLAGS_num_tablet_servers = 5;
  NO_FATALS(BuildAndStart());

  string out;
  string err;
  Status s = RunKuduTool({
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--report_only",
  }, &out, &err);
  ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  // The rebalancer should report on tablet replica distribution. The output
  // should match the reference report: the distribution of the replicas
  // is 100% repeatable given the number of tables created by the test,
  // the replication factor and the number of tablet servers.
  ASSERT_STR_CONTAINS(out, kReferenceOutput);
  // The actual rebalancing should not run.
  ASSERT_STR_NOT_CONTAINS(out, "rebalancing is complete:")
      << ToolRunInfo(s, out, err);
}

// Make sure the rebalancer doesn't start if a tablet server is down.
// The rebalancer starts only when the dead tablet server is in the
// list of ignored_tservers.
class RebalanceStartCriteriaTest :
    public AdminCliTest,
    public ::testing::WithParamInterface<Kudu1097> {
};
INSTANTIATE_TEST_CASE_P(, RebalanceStartCriteriaTest,
                        ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(RebalanceStartCriteriaTest, TabletServerIsDown) {
  const bool is_343_scheme = (GetParam() == Kudu1097::Enable);
  const vector<string> kMasterFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };
  const vector<string> kTserverFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };

  FLAGS_num_tablet_servers = 5;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));

  // Shutdown one of the tablet servers.
  HostPort ts_host_port;
  {
    auto* ts = cluster_->tablet_server(0);
    ASSERT_NE(nullptr, ts);
    ts_host_port = ts->bound_rpc_hostport();
    ts->Shutdown();
  }

  // Rebalancer doesn't start if a tablet server is down.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString()
    }, &out, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << ToolRunInfo(s, out, err);
    const auto err_msg_pattern = Substitute(
        "Illegal state: tablet server .* \\($0\\): "
        "unacceptable health status UNAVAILABLE",
        ts_host_port.ToString());
    ASSERT_STR_MATCHES(err, err_msg_pattern);
  }

  // Rebalancer starts when specifying the dead tablet server in 'ignored_tservers'.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--ignored_tservers=" + cluster_->tablet_server(0)->uuid()
    }, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }
}

// Make sure the rebalancer doesn't start if specified too many ignored tservers.
class RebalanceStartSafetyTest :
    public AdminCliTest,
    public ::testing::WithParamInterface<Kudu1097> {
};
INSTANTIATE_TEST_CASE_P(, RebalanceStartSafetyTest,
                        ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(RebalanceStartSafetyTest, TooManyIgnoredTservers) {
  const bool is_343_scheme = (GetParam() == Kudu1097::Enable);
  const vector<string> kMasterFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };
  const vector<string> kTserverFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };

  FLAGS_num_tablet_servers = 5;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));

  // Assign 3 ignored tservers.
  vector<string> ignored_tservers;
  for (int i = 0; i < 3; i++) {
    auto* ts = cluster_->tablet_server(i);
    ASSERT_NE(nullptr, ts);
    ignored_tservers.emplace_back(ts->uuid());
  }

  // Assign move_replicas_from_ignored_tservers=false.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--ignored_tservers=" + JoinStrings(ignored_tservers, ","),
      "--move_replicas_from_ignored_tservers=false"
    }, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }

  // Assign move_replicas_from_ignored_tservers=true.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--ignored_tservers=" + JoinStrings(ignored_tservers, ","),
      "--move_replicas_from_ignored_tservers=true"
    }, &out, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(err,
        "Too many ignored tservers; 2 healthy non-ignored servers exist but 3 are required")
        << "stderr: " << err;
  }
}

class RebalanceIgnoredTserversTest :
    public AdminCliTest {
};
TEST_F(RebalanceIgnoredTserversTest, Basic) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  FLAGS_num_tablet_servers = 5;
  // Start a cluster with a single tablet.
  NO_FATALS(BuildAndStart());

  // Pre-condition: all replicas on ignored tservers should be healthy or recovering.
  // Here we just ensure the cluster is healthy.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_TOOL_OK(
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString()
    );
  });

  // Assign one ignored tserver and move_replicas_from_ignored_tservers=true
  // without setting it into maintenance mode.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--ignored_tservers=" + cluster_->tablet_server(0)->uuid(),
      "--move_replicas_from_ignored_tservers"
    }, &out, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << ToolRunInfo(s, out, err);
    ASSERT_STR_MATCHES(err, "You should set maintenance mode for tablet server");
  }

  // Set the ignored tserver into maintenance mode.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "tserver",
      "state",
      "enter_maintenance",
      cluster_->master()->bound_rpc_addr().ToString(),
      cluster_->tablet_server(0)->uuid()
    }, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  }

  // Run the rebalance again.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--ignored_tservers=" + cluster_->tablet_server(0)->uuid(),
      "--move_replicas_from_ignored_tservers"
    }, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);

    // There would be no replica on the ignored tserver after rebalancing.
    ASSERT_STR_CONTAINS(out, Substitute("$0 | 0", cluster_->tablet_server(0)->uuid()))
        << "stderr: " << err;
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }

  // Assign two ignored tservers, one of which is unhealthy,
  // '-move_replicas_from_ignored_tservers' and
  // '-output_replica_distribution_details' are both enabled.
  auto* ts = cluster_->tablet_server(1);
  ASSERT_NE(nullptr, ts);
  ts->Shutdown();
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--ignored_tservers=" + cluster_->tablet_server(0)->uuid() + "," + ts->uuid(),
      "--move_replicas_from_ignored_tservers",
      "--output_replica_distribution_details"
    }, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);

    // The output info would not contain the unhealthy server.
    ASSERT_STR_NOT_CONTAINS(out, ts->uuid());
    // There would be no replica on the healthy ignored terver after rebalaning.
    ASSERT_STR_CONTAINS(out, Substitute("$0 | 0", cluster_->tablet_server(0)->uuid()))
        << "stderr: " << err;
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }
}

static Status CreateTables(
    cluster::ExternalMiniCluster* cluster,
    client::KuduClient* client,
    const Schema& table_schema,
    const string& table_name_pattern,
    int num_tables,
    int rep_factor,
    vector<string>* table_names = nullptr) {
  // Create tables with their tablet replicas landing only on the tablet servers
  // which are up and running.
  auto client_schema = KuduSchema::FromSchema(table_schema);
  for (auto i = 0; i < num_tables; ++i) {
    string table_name = Substitute(table_name_pattern, i);
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(table_name)
                  .schema(&client_schema)
                  .add_hash_partitions({ "key" }, 3)
                  .num_replicas(rep_factor)
                  .Create());
    RETURN_NOT_OK(RunKuduTool({
      "perf",
      "loadgen",
      cluster->master()->bound_rpc_addr().ToString(),
      Substitute("--table_name=$0", table_name),
      Substitute("--table_num_replicas=$0", rep_factor),
      "--string_fixed=unbalanced_tables_test",
    }));
    if (table_names) {
      table_names->emplace_back(std::move(table_name));
    }
  }

  return Status::OK();
}

// Create tables with unbalanced replica distribution: useful in
// rebalancer-related tests.
static Status CreateUnbalancedTables(
    cluster::ExternalMiniCluster* cluster,
    client::KuduClient* client,
    const Schema& table_schema,
    const string& table_name_pattern,
    int num_tables,
    int rep_factor,
    int tserver_idx_from,
    int tserver_num,
    int tserver_unresponsive_ms,
    vector<string>* table_names = nullptr) {
  // Keep running only some tablet servers and shut down the rest.
  for (auto i = tserver_idx_from; i < tserver_num; ++i) {
    cluster->tablet_server(i)->Shutdown();
  }

  // Wait for the catalog manager to understand that not all tablet servers
  // are available.
  SleepFor(MonoDelta::FromMilliseconds(5 * tserver_unresponsive_ms / 4));
  RETURN_NOT_OK(CreateTables(cluster, client, table_schema, table_name_pattern,
                             num_tables, rep_factor, table_names));
  for (auto i = tserver_idx_from; i < tserver_num; ++i) {
    RETURN_NOT_OK(cluster->tablet_server(i)->Restart());
  }

  return Status::OK();
}

// A test to verify that rebalancing works for both 3-4-3 and 3-2-3 replica
// management schemes. During replica movement, a light workload is run against
// every table being rebalanced. This test covers different replication factors.
class RebalanceParamTest :
    public AdminCliTest,
    public ::testing::WithParamInterface<tuple<int, Kudu1097>> {
};
INSTANTIATE_TEST_CASE_P(, RebalanceParamTest,
    ::testing::Combine(::testing::Values(1, 2, 3, 5),
                       ::testing::Values(Kudu1097::Disable, Kudu1097::Enable)));
TEST_P(RebalanceParamTest, Rebalance) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const auto& param = GetParam();
  const auto kRepFactor = std::get<0>(param);
  const auto is_343_scheme = (std::get<1>(param) == Kudu1097::Enable);
  constexpr auto kNumTservers = 7;
  constexpr auto kNumTables = 5;
  const string table_name_pattern = "rebalance_test_table_$0";
  constexpr auto kTserverUnresponsiveMs = 3000;
  const auto timeout = MonoDelta::FromSeconds(30);
  const vector<string> kMasterFlags = {
    "--allow_unsafe_replication_factor",
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
    Substitute("--tserver_unresponsive_timeout_ms=$0", kTserverUnresponsiveMs),
  };
  const vector<string> kTserverFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };

  FLAGS_num_tablet_servers = kNumTservers;
  FLAGS_num_replicas = kRepFactor;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));

  ASSERT_OK(CreateUnbalancedTables(
      cluster_.get(), client_.get(), schema_, table_name_pattern, kNumTables,
      kRepFactor, kRepFactor + 1, kNumTservers, kTserverUnresponsiveMs));

  // Workloads aren't run for 3-2-3 replica movement with RF = 1 because
  // the tablet is unavailable during the move until the target voter replica
  // is up and running. That might take some time, and to avoid flakiness or
  // setting longer timeouts, RF=1 replicas are moved with no concurrent
  // workload running.
  //
  // TODO(aserbin): clarify why even with 3-4-3 it's a bit flaky now.
  vector<unique_ptr<TestWorkload>> workloads;
  //if (kRepFactor > 1 || is_343_scheme) {
  if (kRepFactor > 1) {
    for (auto i = 0; i < kNumTables; ++i) {
      const string table_name = Substitute(table_name_pattern, i);
      // The workload is light (1 thread, 1 op batches) so that new replicas
      // bootstrap and converge quickly.
      unique_ptr<TestWorkload> workload(new TestWorkload(cluster_.get()));
      workload->set_table_name(table_name);
      workload->set_num_replicas(kRepFactor);
      workload->set_num_write_threads(1);
      workload->set_write_batch_size(1);
      workload->set_write_timeout_millis(timeout.ToMilliseconds());
      workload->set_already_present_allowed(true);
      workload->Setup();
      workload->Start();
      workloads.emplace_back(std::move(workload));
    }
  }

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--move_single_replicas=enabled",
  };

  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }

  // Next run should report the cluster as balanced and no replica movement
  // should be attempted.
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }

  for (auto& workload : workloads) {
    workload->StopAndJoin();
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());

  // Now add a new tablet server into the cluster and make sure the rebalancer
  // will re-distribute replicas.
  ASSERT_OK(cluster_->AddTabletServer());
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
    // The cluster was unbalanced, so many replicas should have been moved.
    ASSERT_STR_NOT_CONTAINS(out, "(moved 0 replicas)");
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// Working around limitations of older libstdc++.
static const unordered_set<string> kEmptySet = unordered_set<string>();

// Common base for the rebalancer-related test below.
class RebalancingTest : public tserver::TabletServerIntegrationTestBase {
 public:
  explicit RebalancingTest(int num_tables = 10,
                           int rep_factor = 3,
                           int num_tservers = 8,
                           int tserver_unresponsive_ms = 3000)
      : num_tables_(num_tables),
        rep_factor_(rep_factor),
        num_tservers_(num_tservers),
        tserver_unresponsive_ms_(tserver_unresponsive_ms) {
    master_flags_ = {
      Substitute("--tserver_unresponsive_timeout_ms=$0",
                 tserver_unresponsive_ms_),
    };
  }

  virtual bool is_343_scheme() const = 0;

 protected:
  static const char* const kExitOnSignalStr;
  static const char* const kTableNamePattern;

  void Prepare(const vector<string>& extra_tserver_flags = {},
               const vector<string>& extra_master_flags = {},
               const LocationInfo& location_info = {},
               const unordered_set<string>& empty_locations = kEmptySet,
               vector<string>* created_tables_names = nullptr) {
    const auto& scheme_flag = Substitute(
        "--raft_prepare_replacement_before_eviction=$0", is_343_scheme());
    master_flags_.push_back(scheme_flag);
    tserver_flags_.push_back(scheme_flag);

    copy(extra_tserver_flags.begin(), extra_tserver_flags.end(),
         back_inserter(tserver_flags_));
    copy(extra_master_flags.begin(), extra_master_flags.end(),
         back_inserter(master_flags_));

    FLAGS_num_tablet_servers = num_tservers_;
    FLAGS_num_replicas = rep_factor_;
    NO_FATALS(BuildAndStart(tserver_flags_, master_flags_, location_info,
                            /*create_table=*/ false));

    if (location_info.empty()) {
      ASSERT_OK(CreateUnbalancedTables(
          cluster_.get(), client_.get(), schema_, kTableNamePattern,
          num_tables_, rep_factor_, rep_factor_ + 1, num_tservers_,
          tserver_unresponsive_ms_, created_tables_names));
    } else {
      ASSERT_OK(CreateTablesExcludingLocations(empty_locations,
                                               kTableNamePattern,
                                               created_tables_names));
    }
  }

  // Create tables placing their tablet replicas everywhere but not at the
  // tablet servers in the specified locations. This is similar to
  // CreateUnbalancedTables() but the set of tablet servers to avoid is defined
  // by the set of the specified locations.
  //
  // Note: 'table_name_pattern' is used to describe the table names created by this
  // function. Each call to this function can create different batch of tables with
  // this parameter set.
  Status CreateTablesExcludingLocations(
      const unordered_set<string>& excluded_locations,
      const string& table_name_pattern = kTableNamePattern,
      vector<string>* table_names = nullptr) {
    // Shutdown all tablet servers in the specified locations so no tablet
    // replicas would be hosted by those servers.
    unordered_set<string> seen_locations;
    if (!excluded_locations.empty()) {
      for (const auto& elem : tablet_servers_) {
        auto* ts = elem.second;
        if (ContainsKey(excluded_locations, ts->location)) {
          cluster_->tablet_server_by_uuid(ts->uuid())->Shutdown();
          EmplaceIfNotPresent(&seen_locations, ts->location);
        }
      }
    }
    // Sanity check: every specified location should have been seen, otherwise
    // something is wrong with the tablet servers' registration.
    CHECK_EQ(excluded_locations.size(), seen_locations.size());

    // Wait for the catalog manager to understand that not all tablet servers
    // are available.
    SleepFor(MonoDelta::FromMilliseconds(5 * tserver_unresponsive_ms_ / 4));
    RETURN_NOT_OK(CreateTables(cluster_.get(), client_.get(), schema_,
                               table_name_pattern, num_tables_, rep_factor_,
                               table_names));
    // Start tablet servers at the excluded locations.
    if (!excluded_locations.empty()) {
      for (const auto& elem : tablet_servers_) {
        auto* ts = elem.second;
        if (ContainsKey(excluded_locations, ts->location)) {
          RETURN_NOT_OK(cluster_->tablet_server_by_uuid(ts->uuid())->Restart());
        }
      }
    }

    return Status::OK();
  }

  // Create tables placing their tablet replicas elsewhere but not at the specified
  // number of tservers in the specified locations.
  Status CreateTablesExcludingTserversInLocations(
      const unordered_map<string, int>& excluded_tserver_by_location,
      const string& table_name_pattern = kTableNamePattern,
      vector<string>* table_names = nullptr) {
    unordered_map<string, int> excluded_tservers = excluded_tserver_by_location;
    vector<string> excluded_tserver_uuids;
    // Shutdown some tablet servers in the specified locations so no tablet
    // replicas would be hosted by those servers.
    if (!excluded_tservers.empty()) {
      for (const auto& elem : tablet_servers_) {
        auto* ts = elem.second;
        if (ContainsKey(excluded_tservers, ts->location)
            && excluded_tservers[ts->location] > 0) {
          excluded_tserver_uuids.push_back(ts->uuid());
          --excluded_tservers[ts->location];
          cluster_->tablet_server_by_uuid(ts->uuid())->Shutdown();
        }
      }
    }

    for (const auto& elem : excluded_tservers) {
      CHECK_EQ(0, elem.second);
    }
    // Wait for the catalog manager to understand that not all tablet servers
    // are available.
    SleepFor(MonoDelta::FromMilliseconds(5 * tserver_unresponsive_ms_ / 4));
    RETURN_NOT_OK(CreateTables(cluster_.get(), client_.get(), schema_,
                               table_name_pattern, num_tables_, rep_factor_,
                               table_names));
    // Start tablet servers at the excluded locations.
    for (const auto& uuid : excluded_tserver_uuids) {
      RETURN_NOT_OK(cluster_->tablet_server_by_uuid(uuid)->Restart());
    }
    return Status::OK();
  }

  // When the rebalancer starts moving replicas, ksck detects corruption
  // (that's why RuntimeError), seeing affected tables as non-healthy
  // with data state of corresponding tablets as TABLET_DATA_COPYING. If using
  // this method, it's a good idea to inject some latency into tablet copying
  // to be able to spot the TABLET_DATA_COPYING state, see the
  // '--tablet_copy_download_file_inject_latency_ms' flag for tservers.
  bool IsRebalancingInProgress() {
    string out;
    const auto s = RunKuduTool({
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString(),
    }, &out);
    return s.IsRuntimeError() &&
        out.find("Data state:  TABLET_DATA_COPYING") != string::npos;
  }

  const int num_tables_;
  const int rep_factor_;
  const int num_tservers_;
  const int tserver_unresponsive_ms_;
  vector<string> tserver_flags_;
  vector<string> master_flags_;
};
const char* const RebalancingTest::kExitOnSignalStr = "kudu: process exited on signal";
const char* const RebalancingTest::kTableNamePattern = "rebalance_test_table_$0";

typedef testing::WithParamInterface<Kudu1097> Kudu1097ParamTest;

// If an ignored tablet server went down during the process of copying data,
// the rebalancer would ignore the health state of the ignored tablet server,
// and replicas on it, run the rebalancing on the other tablet servers and
// exit normally.
class IgnoredTserverGoesDownDuringRebalancingTest : public RebalancingTest {
 public:
  IgnoredTserverGoesDownDuringRebalancingTest()
      : RebalancingTest(/*num_tables=*/ 5) {
  }
  bool is_343_scheme() const override {
    return true;
  }
};
TEST_F(IgnoredTserverGoesDownDuringRebalancingTest, TserverDown) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const vector<string> kTserverExtraFlags = {
    // Slow down tablet copy to make rebalancing step running longer
    // and become observable via tablet data states output by ksck.
    "--tablet_copy_download_file_inject_latency_ms=1500",

    "--follower_unavailable_considered_failed_sec=30",
  };
  NO_FATALS(Prepare(kTserverExtraFlags));

  // Pre-condition: 'kudu cluster ksck' should be happy with the cluster state
  // shortly after initial setup.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_TOOL_OK(
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString()
    );
  });

  const uint32_t ignored_tserver_idx = 0;

  // Set the ignored tserver into maintenance mode.
  {
    string out;
    string err;
    Status s = RunKuduTool({
      "tserver",
      "state",
      "enter_maintenance",
      cluster_->master()->bound_rpc_addr().ToString(),
      cluster_->tablet_server(ignored_tserver_idx)->uuid()
    }, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  }

  atomic<bool> run(true);
  // The thread that shuts down the ignored tserver.
  thread stopper([&]() {
    while (run && !IsRebalancingInProgress()) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    // All right, it's time to stop the ignored tserver.
    cluster_->tablet_server(ignored_tserver_idx)->Shutdown();
  });
  auto stopper_cleanup = MakeScopedCleanup([&]() {
    run = false;
    stopper.join();
  });

  {
    string out;
    string err;
    const auto s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--ignored_tservers=" + cluster_->tablet_server(ignored_tserver_idx)->uuid(),
      "--move_replicas_from_ignored_tservers",
      // Limiting the number of replicas to move. This is to make the rebalancer
      // run longer, making sure the rebalancing is in progress when the tablet
      // server goes down.
      "--max_moves_per_server=1",
    }, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);

    // The rebalancer tool should not crash.
    ASSERT_STR_NOT_CONTAINS(s.ToString(), kExitOnSignalStr);
    // The rebalancer tool would log some information of the unhealthy server.
    ASSERT_STR_CONTAINS(err, Substitute("ignoring unhealthy tablet server $0",
                                       cluster_->tablet_server(ignored_tserver_idx)->uuid()));

    // Restart the ignored tablet server
    ASSERT_OK(cluster_->tablet_server(ignored_tserver_idx)->Restart());
    NO_FATALS(cluster_->AssertNoCrashes());

    // Report replica distribution.
    {
      string out;
      string err;
      const auto s = RunKuduTool({
        "cluster",
        "rebalance",
        cluster_->master()->bound_rpc_addr().ToString(),
        "--output_replica_distribution_details",
        "--report_only",
      }, &out, &err);
      ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
      // There would be some replicas on the ignored tablet server,
      // they hasn't been moved to other tablet servers by the rebalancer tool.
      ASSERT_STR_NOT_CONTAINS(out,
          Substitute("$0 | $1 | 0",
                     cluster_->tablet_server(ignored_tserver_idx)->uuid(),
                     cluster_->tablet_server(ignored_tserver_idx)->bound_rpc_addr().ToString()));
    }
  }

  run = false;
  stopper.join();
  stopper_cleanup.cancel();

  NO_FATALS(cluster_->AssertNoCrashes());
}

// Make sure the rebalancer is able to do its job if running concurrently
// with DDL activity on the cluster.
class DDLDuringRebalancingTest : public RebalancingTest,
                                 public Kudu1097ParamTest {
 public:
  DDLDuringRebalancingTest()
      : RebalancingTest(/*num_tables=*/ 20) {
  }

  bool is_343_scheme() const override {
    return GetParam() == Kudu1097::Enable;
  }
};
INSTANTIATE_TEST_CASE_P(, DDLDuringRebalancingTest,
                        ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(DDLDuringRebalancingTest, TablesCreatedAndDeletedDuringRebalancing) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(Prepare());

  // The latch that controls the lifecycle of the concurrent DDL activity.
  CountDownLatch run_latch(1);

  thread creator([&]() {
    auto client_schema = KuduSchema::FromSchema(schema_);
    for (auto idx = 0; ; ++idx) {
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(500))) {
        break;
      }
      const string table_name = Substitute("rebalancer_extra_table_$0", idx++);
      unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
      CHECK_OK(table_creator->table_name(table_name)
               .schema(&client_schema)
               .add_hash_partitions({ "key" }, 3)
               .num_replicas(rep_factor_)
               .Create());
    }
  });
  auto creator_cleanup = MakeScopedCleanup([&]() {
    run_latch.CountDown();
    creator.join();
  });

  thread deleter([&]() {
    for (auto idx = 0; idx < num_tables_; ++idx) {
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(500))) {
        break;
      }
      CHECK_OK(client_->DeleteTable(Substitute(kTableNamePattern, idx++)));
    }
  });
  auto deleter_cleanup = MakeScopedCleanup([&]() {
    run_latch.CountDown();
    deleter.join();
  });

  thread alterer([&]() {
    const string kTableName = "rebalancer_dynamic_table";
    const string kNewTableName = "rebalancer_dynamic_table_new_name";
    while (true) {
      // Create table.
      {
        KuduSchema schema;
        KuduSchemaBuilder builder;
        builder.AddColumn("key")->Type(KuduColumnSchema::INT64)->
            NotNull()->
            PrimaryKey();
        builder.AddColumn("a")->Type(KuduColumnSchema::INT64);
        CHECK_OK(builder.Build(&schema));
        unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
        CHECK_OK(table_creator->table_name(kTableName)
                 .schema(&schema)
                 .set_range_partition_columns({})
                 .num_replicas(rep_factor_)
                 .Create());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Drop a column.
      {
        unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
        alt->DropColumn("a");
        CHECK_OK(alt->Alter());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Add back the column with different type.
      {
        unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
        alt->AddColumn("a")->Type(KuduColumnSchema::STRING);
        CHECK_OK(alt->Alter());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Rename the table.
      {
        unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
        alt->RenameTo(kNewTableName);
        CHECK_OK(alt->Alter());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Drop the renamed table.
      CHECK_OK(client_->DeleteTable(kNewTableName));
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }
    }
  });
  auto alterer_cleanup = MakeScopedCleanup([&]() {
    run_latch.CountDown();
    alterer.join();
  });

  thread timer([&]() {
    SleepFor(MonoDelta::FromSeconds(30));
    run_latch.CountDown();
  });
  auto timer_cleanup = MakeScopedCleanup([&]() {
    timer.join();
  });

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  // Run the rebalancer concurrently with the DDL operations. The second run
  // of the rebalancer (the second run starts after joining the timer thread)
  // is necessary to balance the cluster after the DDL activity stops: that's
  // the easiest way to make sure the rebalancer will take into account
  // all DDL changes that happened.
  //
  // The signal to terminate the DDL activity (done via run_latch.CountDown())
  // is sent from a separate timer thread instead of doing SleepFor() after
  // the first run of the rebalancer followed by run_latch.CountDown().
  // That's to avoid dependency on the rebalancer behavior if it spots on-going
  // DDL activity and continues running over and over again.
  for (auto i = 0; i < 2; ++i) {
    if (i == 1) {
      timer.join();
      timer_cleanup.cancel();

      // Wait for all the DDL activity to complete.
      alterer.join();
      alterer_cleanup.cancel();

      deleter.join();
      deleter_cleanup.cancel();

      creator.join();
      creator_cleanup.cancel();
    }

    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }

  // Next (3rd) run should report the cluster as balanced and
  // no replica movement should be attempted.
  {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// Make sure it's safe to run multiple rebalancers concurrently. The rebalancers
// might report errors, but they should not get stuck and the cluster should
// remain in good shape (i.e. no crashes, no data inconsistencies). Re-running a
// single rebalancer session again should bring the cluster to a balanced state.
class ConcurrentRebalancersTest : public RebalancingTest,
                                  public Kudu1097ParamTest {
 public:
  ConcurrentRebalancersTest()
      : RebalancingTest(/*num_tables=*/ 10) {
  }

  bool is_343_scheme() const override {
    return GetParam() == Kudu1097::Enable;
  }
};
INSTANTIATE_TEST_CASE_P(, ConcurrentRebalancersTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(ConcurrentRebalancersTest, TwoConcurrentRebalancers) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(Prepare());

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  const auto runner_func = [&]() {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    if (!s.ok()) {
      // One might expect a bad status returned: e.g., due to some race so
      // the rebalancer didn't able to make progress for more than
      // --max_staleness_interval_sec, etc.
      LOG(INFO) << "rebalancer run info: " << ToolRunInfo(s, out, err);
    }

    // Should not exit on a signal: not expecting SIGSEGV, SIGABRT, etc.
    return s.ToString().find(kExitOnSignalStr) == string::npos;
  };

  const constexpr auto kNumConcurrentRunners = 5;
  CountDownLatch start_synchronizer(1);
  vector<thread> concurrent_runners;
  concurrent_runners.reserve(kNumConcurrentRunners);
  for (auto i = 0; i < kNumConcurrentRunners; ++i) {
    concurrent_runners.emplace_back([&]() {
      start_synchronizer.Wait();
      CHECK(runner_func());
    });
  }

  // Run rebalancers concurrently and wait for their completion.
  start_synchronizer.CountDown();
  for (auto& runner : concurrent_runners) {
    runner.join();
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());

  {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }

  // Next run should report the cluster as balanced and no replica movement
  // should be attempted: at least one run of the rebalancer prior to this
  // should succeed, so next run is about running the tool against already
  // balanced cluster.
  {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// The rebalancer should stop and exit upon detecting a tablet server that
// went down. That's a simple and effective way of preventing concurrent replica
// movement by the rebalancer and the automatic re-replication (the catalog
// manager tries to move replicas from the unreachable tablet server).
class TserverGoesDownDuringRebalancingTest : public RebalancingTest,
                                             public Kudu1097ParamTest {
 public:
  TserverGoesDownDuringRebalancingTest()
      : RebalancingTest(/*num_tables=*/ 5) {
  }

  bool is_343_scheme() const override {
    return GetParam() == Kudu1097::Enable;
  }
};
INSTANTIATE_TEST_CASE_P(, TserverGoesDownDuringRebalancingTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(TserverGoesDownDuringRebalancingTest, TserverDown) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const vector<string> kTserverExtraFlags = {
    // Slow down tablet copy to make rebalancing step running longer
    // and become observable via tablet data states output by ksck.
    "--tablet_copy_download_file_inject_latency_ms=1500",

    "--follower_unavailable_considered_failed_sec=30",
  };
  NO_FATALS(Prepare(kTserverExtraFlags));

  // Pre-condition: 'kudu cluster ksck' should be happy with the cluster state
  // shortly after initial setup.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_TOOL_OK(
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString()
    );
  });

  Random r(SeedRandom());
  const uint32_t shutdown_tserver_idx = r.Next() % num_tservers_;

  atomic<bool> run(true);
  // The thread that shuts down the selected tablet server.
  thread stopper([&]() {
    while (run && !IsRebalancingInProgress()) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    // All right, it's time to stop the selected tablet server.
    cluster_->tablet_server(shutdown_tserver_idx)->Shutdown();
  });
  auto stopper_cleanup = MakeScopedCleanup([&]() {
    run = false;
    stopper.join();
  });

  {
    string out;
    string err;
    const auto s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      // Limiting the number of replicas to move. This is to make the rebalancer
      // run longer, making sure the rebalancing is in progress when the tablet
      // server goes down.
      "--max_moves_per_server=1",
    }, &out, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << ToolRunInfo(s, out, err);

    // The rebalancer tool should not crash.
    ASSERT_STR_NOT_CONTAINS(s.ToString(), kExitOnSignalStr);
    ASSERT_STR_MATCHES(
        err, "Illegal state: tablet server .* \\(.*\\): "
             "unacceptable health status UNAVAILABLE");
  }

  run = false;
  stopper.join();
  stopper_cleanup.cancel();

  ASSERT_OK(cluster_->tablet_server(shutdown_tserver_idx)->Restart());
  NO_FATALS(cluster_->AssertNoCrashes());
}

// The rebalancer should continue working and complete rebalancing successfully
// if a new tablet server is added while the cluster is being rebalanced.
class TserverAddedDuringRebalancingTest : public RebalancingTest,
                                          public Kudu1097ParamTest {
 public:
  TserverAddedDuringRebalancingTest()
      : RebalancingTest(/*num_tables=*/ 10) {
  }

  bool is_343_scheme() const override {
    return GetParam() == Kudu1097::Enable;
  }
};
INSTANTIATE_TEST_CASE_P(, TserverAddedDuringRebalancingTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(TserverAddedDuringRebalancingTest, TserverStarts) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const vector<string> kTserverExtraFlags = {
    // Slow down tablet copy to make rebalancing step running longer
    // and become observable via tablet data states output by ksck.
    "--tablet_copy_download_file_inject_latency_ms=1500",

    "--follower_unavailable_considered_failed_sec=30",
  };
  NO_FATALS(Prepare(kTserverExtraFlags));

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  atomic<bool> run(true);
  thread runner([&]() {
    while (run) {
      string out;
      string err;
      const auto s = RunKuduTool(tool_args, &out, &err);
      CHECK(s.ok()) << ToolRunInfo(s, out, err);
    }
  });
  auto runner_cleanup = MakeScopedCleanup([&]() {
    run = false;
    runner.join();
  });

  while (!IsRebalancingInProgress()) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // It's time to sneak in and add new tablet server.
  ASSERT_OK(cluster_->AddTabletServer());
  run = false;
  runner.join();
  runner_cleanup.cancel();

  // The rebalancer should not fail, and eventually, after a new tablet server
  // is added, the cluster should become balanced.
  ASSERT_EVENTUALLY([&]() {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  });

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// Run rebalancer in 'election storms' environment and make sure the rebalancer
// does not exit prematurely or exhibit any other unexpected behavior.
class RebalancingDuringElectionStormTest : public RebalancingTest,
                                           public Kudu1097ParamTest {
 public:
  bool is_343_scheme() const override {
    return GetParam() == Kudu1097::Enable;
  }
};
INSTANTIATE_TEST_CASE_P(, RebalancingDuringElectionStormTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(RebalancingDuringElectionStormTest, RoundRobin) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(Prepare());

  atomic<bool> elector_run(true);
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  // The timeout is a time-to-run for the stormy elector thread as well.
  // Making longer timeout for workload in case of TSAN/ASAN is not needed:
  // having everything generated written is not required.
  const auto timeout = MonoDelta::FromSeconds(5);
#else
  const auto timeout = MonoDelta::FromSeconds(10);
#endif
  const auto start_time = MonoTime::Now();
  thread elector([&]() {
    // Mininum viable divider for modulo ('%') to allow the result to grow by
    // the rules below.
    auto max_sleep_ms = 2.0;
    while (elector_run && MonoTime::Now() < start_time + timeout) {
      for (const auto& e : tablet_servers_) {
        const auto& ts_uuid = e.first;
        const auto* ts = e.second;
        vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
        auto const s = itest::ListTablets(ts, timeout, &tablets);
        if (!s.ok()) {
          LOG(WARNING) << ts_uuid << ": failed to get tablet list :"
                       << s.ToString();
          continue;
        }
        consensus::ConsensusServiceProxy proxy(
            cluster_->messenger(),
            cluster_->tablet_server_by_uuid(ts_uuid)->bound_rpc_addr(),
            "tserver " + ts_uuid);
        for (const auto& tablet : tablets) {
          const auto& tablet_id = tablet.tablet_status().tablet_id();
          consensus::RunLeaderElectionRequestPB req;
          req.set_tablet_id(tablet_id);
          req.set_dest_uuid(ts_uuid);
          rpc::RpcController rpc;
          rpc.set_timeout(timeout);
          consensus::RunLeaderElectionResponsePB resp;
          WARN_NOT_OK(proxy.RunLeaderElection(req, &resp, &rpc),
                      Substitute("failed to start election for tablet $0",
                                 tablet_id));
        }
        if (!elector_run || start_time + timeout <= MonoTime::Now()) {
          break;
        }
        auto sleep_ms = rand() % static_cast<int>(max_sleep_ms);
        SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
        max_sleep_ms = std::min(max_sleep_ms * 1.1, 2000.0);
      }
    }
  });
  auto elector_cleanup = MakeScopedCleanup([&]() {
    elector_run = false;
    elector.join();
  });

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  vector<unique_ptr<TestWorkload>> workloads;
  for (auto i = 0; i < num_tables_; ++i) {
    const string table_name = Substitute(kTableNamePattern, i);
    // The workload is light (1 thread, 1 op batches) and lenient to failures.
    unique_ptr<TestWorkload> workload(new TestWorkload(cluster_.get()));
    workload->set_table_name(table_name);
    workload->set_num_replicas(rep_factor_);
    workload->set_num_write_threads(1);
    workload->set_write_batch_size(1);
    workload->set_write_timeout_millis(timeout.ToMilliseconds());
    workload->set_already_present_allowed(true);
    workload->set_remote_error_allowed(true);
    workload->set_timeout_allowed(true);
    workload->Setup();
    workload->Start();
    workloads.emplace_back(std::move(workload));
  }
#endif

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  while (MonoTime::Now() < start_time + timeout) {
    // Rebalancer should not report any errors even if it's an election storm
    // unless a tablet server is reported as unavailable by ksck: the latter
    // usually happens because GetConsensusState requests are dropped due to
    // backpressure.
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    if (s.ok()) {
      ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
          << ToolRunInfo(s, out, err);
    } else {
      ASSERT_STR_CONTAINS(err, "unacceptable health status UNAVAILABLE")
          << ToolRunInfo(s, out, err);
    }
  }

  elector_run = false;
  elector.join();
  elector_cleanup.cancel();
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  for (auto& workload : workloads) {
    workload->StopAndJoin();
  }
#endif

  // There might be some re-replication started as a result of election storm,
  // etc. Eventually, the system should heal itself and 'kudu cluster ksck'
  // should report no issues.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_TOOL_OK(
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString()
    );
  });

  // The rebalancer should successfully rebalance the cluster after ksck
  // reported 'all is well'.
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << ToolRunInfo(s, out, err);
  }

  NO_FATALS(cluster_->AssertNoCrashes());
}

// A test to verify how the rebalancer handles replicas of single-replica
// tablets in case of various values of the '--move_single_replicas' flag
// and replica management schemes.
class RebalancerAndSingleReplicaTablets :
    public AdminCliTest,
    public ::testing::WithParamInterface<tuple<string, Kudu1097>> {
};
INSTANTIATE_TEST_CASE_P(, RebalancerAndSingleReplicaTablets,
    ::testing::Combine(::testing::Values("auto", "enabled", "disabled"),
                       ::testing::Values(Kudu1097::Disable, Kudu1097::Enable)));
TEST_P(RebalancerAndSingleReplicaTablets, SingleReplicasStayOrMove) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  constexpr auto kRepFactor = 1;
  constexpr auto kNumTservers = 2 * (kRepFactor + 1);
  constexpr auto kNumTables = kNumTservers;
  constexpr auto kTserverUnresponsiveMs = 3000;
  const auto& param = GetParam();
  const auto& move_single_replica = std::get<0>(param);
  const auto is_343_scheme = (std::get<1>(param) == Kudu1097::Enable);
  const string table_name_pattern = "rebalance_test_table_$0";
  const vector<string> master_flags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
    Substitute("--tserver_unresponsive_timeout_ms=$0", kTserverUnresponsiveMs),
  };
  const vector<string> tserver_flags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };

  FLAGS_num_tablet_servers = kNumTservers;
  FLAGS_num_replicas = kRepFactor;
  NO_FATALS(BuildAndStart(tserver_flags, master_flags));

  // Keep running only (kRepFactor + 1) tablet servers and shut down the rest.
  for (auto i = kRepFactor + 1; i < kNumTservers; ++i) {
    cluster_->tablet_server(i)->Shutdown();
  }

  // Wait for the catalog manager to understand that only (kRepFactor + 1)
  // tablet servers are available.
  SleepFor(MonoDelta::FromMilliseconds(5 * kTserverUnresponsiveMs / 4));

  // Create few tables with their tablet replicas landing only on those
  // (kRepFactor + 1) running tablet servers.
  auto client_schema = KuduSchema::FromSchema(schema_);
  for (auto i = 0; i < kNumTables; ++i) {
    const string table_name = Substitute(table_name_pattern, i);
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(table_name)
              .schema(&client_schema)
              .add_hash_partitions({ "key" }, 3)
              .num_replicas(kRepFactor)
              .Create());
    ASSERT_TOOL_OK(
      "perf",
      "loadgen",
      cluster_->master()->bound_rpc_addr().ToString(),
      Substitute("--table_name=$0", table_name),
      // Don't need much data in there.
      "--num_threads=1",
      "--num_rows_per_thread=1",
    );
  }
  for (auto i = kRepFactor + 1; i < kNumTservers; ++i) {
    ASSERT_OK(cluster_->tablet_server(i)->Restart());
  }

  string out;
  string err;
  const Status s = RunKuduTool({
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    Substitute("--move_single_replicas=$0", move_single_replica),
  }, &out, &err);
  ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
      << "stderr: " << err;
  if (move_single_replica == "enabled" ||
      (move_single_replica == "auto" && is_343_scheme)) {
    // Should move appropriate replicas of single-replica tablets.
    ASSERT_STR_NOT_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
    ASSERT_STR_NOT_CONTAINS(err, "has single replica, skipping");
  } else {
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
    ASSERT_STR_MATCHES(err, "tablet .* of table '.*' (.*) has single replica, skipping");
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
}

// Basic fixture for the rebalancer tests.
class LocationAwareRebalancingBasicTest : public RebalancingTest {
 public:
  LocationAwareRebalancingBasicTest()
      : RebalancingTest(/*num_tables=*/ 12,
                        /*rep_factor=*/ 3,
                        /*num_tservers=*/ 6) {
  }

  bool is_343_scheme() const override {
    // These tests are for the 3-4-3 replica management scheme only.
    return true;
  }
};

// Verifying the very basic functionality of the location-aware rebalancer:
// given the very simple cluster configuration of 6 tablet servers spread
// among 3 locations (2+2+2) and 12 tables with RF=3, the initially
// imbalanced distribution of the replicas should become more balanced
// and the placement policy constraints should be reimposed after running
// the rebalancer tool.
TEST_F(LocationAwareRebalancingBasicTest, Basic) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const LocationInfo location_info = { { "/A", 2 }, { "/B", 2 }, { "/C", 2 }, };
  vector<string> table_names;
  NO_FATALS(Prepare({}, {}, location_info, kEmptySet, &table_names));

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  string out;
  string err;
  const auto s = RunKuduTool(tool_args, &out, &err);
  ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  ASSERT_STR_NOT_CONTAINS(s.ToString(), kExitOnSignalStr);

  NO_FATALS(cluster_->AssertNoCrashes());
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());

  unordered_map<string, itest::TServerDetails*> ts_map;
  ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy(0),
                                         cluster_->messenger(),
                                         &ts_map));
  ValueDeleter deleter(&ts_map);

  // Build tablet server UUID --> location map.
  unordered_map<string, string> location_by_ts_id;
  for (const auto& elem : ts_map) {
    EmplaceOrDie(&location_by_ts_id, elem.first, elem.second->location);
  }


  for (const auto& table_name : table_names) {
    master::GetTableLocationsResponsePB table_locations;
    ASSERT_OK(itest::GetTableLocations(cluster_->master_proxy(),
                                       table_name, MonoDelta::FromSeconds(30),
                                       master::ANY_REPLICA,
                                       /*table_id=*/none,
                                       &table_locations));
    const auto tablet_num = table_locations.tablet_locations_size();
    auto total_table_replica_count = 0;
    unordered_map<string, int> total_count_per_location;
    for (auto i = 0; i < tablet_num; ++i) {
      const auto& location = table_locations.tablet_locations(i);
      const auto& tablet_id = location.tablet_id();
      unordered_map<string, int> count_per_location;
      for (const auto& replica : location.interned_replicas()) {
        const auto& ts_id = table_locations.ts_infos(replica.ts_info_idx()).permanent_uuid();
        const auto& location = FindOrDie(location_by_ts_id, ts_id);
        ++LookupOrEmplace(&count_per_location, location, 0);
        ++LookupOrEmplace(&total_count_per_location, location, 0);
        ++total_table_replica_count;
      }

      // Make sure no location has the majority of replicas for the tablet.
      for (const auto& elem : count_per_location) {
        const auto& location = elem.first;
        const auto replica_count = elem.second;
        ASSERT_GT(consensus::MajoritySize(rep_factor_), replica_count)
            << Substitute("tablet $0 (table $1): $2 replicas out of $3 total "
                          "are in location $4",
                          tablet_id, table_name, replica_count, rep_factor_,
                          location);

      }

      // Verify the overall replica distribution for the table.
      double avg = static_cast<double>(total_table_replica_count) / location_info.size();
      for (const auto& elem : total_count_per_location) {
        const auto& location = elem.first;
        const auto replica_num = elem.second;
        ASSERT_GT(avg + 2, replica_num) << "at location " << location;
        ASSERT_LT(avg - 2, replica_num) << "at location " << location;
      }
    }
  }
}

class LocationAwareBalanceInfoTest : public RebalancingTest {
 public:
  LocationAwareBalanceInfoTest()
      : RebalancingTest(/*num_tables=*/ 1,
                        /*rep_factor=*/ 3,
                        /*num_tservers=*/ 5) {
  }

  bool is_343_scheme() const override {
    // These tests are for the 3-4-3 replica management scheme only.
    return true;
  }
};

// Verify the output of the location-aware rebalancer against a cluster
// that has multiple locations.
TEST_F(LocationAwareBalanceInfoTest, ReportOnly) {
  static const char kReferenceOutput[] =
    R"***(Locations load summary:
 Location |   Load
----------+----------
 /A       | 3.000000
 /B       | 3.000000
 /C       | 0.000000

--------------------------------------------------
Location: /A
--------------------------------------------------
Per-server replica distribution summary:
       Statistic       |  Value
-----------------------+----------
 Minimum Replica Count | 3
 Maximum Replica Count | 3
 Average Replica Count | 3.000000

Per-table replica distribution summary:
 Replica Skew |  Value
--------------+----------
 Minimum      | 0
 Maximum      | 0
 Average      | 0.000000

--------------------------------------------------
Location: /B
--------------------------------------------------
Per-server replica distribution summary:
       Statistic       |  Value
-----------------------+----------
 Minimum Replica Count | 3
 Maximum Replica Count | 3
 Average Replica Count | 3.000000

Per-table replica distribution summary:
 Replica Skew |  Value
--------------+----------
 Minimum      | 0
 Maximum      | 0
 Average      | 0.000000

--------------------------------------------------
Location: /C
--------------------------------------------------
Per-server replica distribution summary:
       Statistic       |  Value
-----------------------+----------
 Minimum Replica Count | 0
 Maximum Replica Count | 0
 Average Replica Count | 0.000000

Per-table replica distribution summary:
 Replica Skew | Value
--------------+-------
 N/A          | N/A

Placement policy violations:
 Location | Number of non-complying tables | Number of non-complying tablets
----------+--------------------------------+---------------------------------
 /B       | 1                              | 3
)***";

  const LocationInfo location_info = { { "/A", 1 }, { "/B", 2 }, { "/C", 2 }, };
  NO_FATALS(Prepare({}, {}, location_info, { "/C" }));

  string out;
  string err;
  Status s = RunKuduTool({
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--report_only",
  }, &out, &err);
  ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  // The output should match the reference report.
  ASSERT_STR_CONTAINS(out, kReferenceOutput);
  // The actual rebalancing should not run.
  ASSERT_STR_NOT_CONTAINS(out, "rebalancing is complete:")
      << ToolRunInfo(s, out, err);
}

// Parameters for the location-aware rebalancing tests.
typedef struct {
  int replication_factor;
  int tables_num;

  // Information on the distribution of tablet servers among locations.
  LocationInfo location_info;

  // Locations where tablet servers are shutdown during tablet creation
  // to achieve non-balanced tablet replica distribution.
  unordered_set<string> excluded_locations;
} LaRebalancingParams;

const LaRebalancingParams kLaRebalancingParams[] = {
  // RF=3, 1 table, 1 location, 3 tablet servers.
  { 3, 1,
    {
      { { "/A" }, 3 }
    },
    kEmptySet },

  // RF=3, 3 tables, 2 locations, 4 (3 + 1) tablet servers.
  { 3, 3,
    {
      { { "/A" }, 3 },
      { { "/B" }, 1 },
    },
    { "/B" }
  },

  // RF=3, 3 tables, 2 locations, 5 (3 + 2) tablet servers.
  { 3, 3,
    {
      { { "/A" }, 3 },
      { { "/B" }, 2 },
    },
    { "/B" }
  },

  // RF=3, 3 tables, 2 locations, 6 (3 + 3) tablet servers.
  { 3, 3,
    {
      { { "/A" }, 3 },
      { { "/B" }, 3 },
    },
    { "/B" }
  },

  // RF=3, 3 tables, 3 locations, 6 (3 + 2 + 1) tablet servers.
  { 3, 3,
    {
      { { "/A" }, 3 },
      { { "/B" }, 2 },
      { { "/C" }, 1 },
    },
    { "/B" }
  },

  // RF=5, 3 locations, 4 tables, 8 (3 + 2 + 3) tablet servers.
  { 5, 4,
    {
      { { "/A" }, 3 },
      { { "/B" }, 2 },
      { { "/C" }, 3 },
    }, { "/C" }
  },

  // RF=7, 3 locations, 7 tables, 10 (3 + 4 + 3) tablet servers.
  { 7, 7,
    {
      { { "/A" }, 3 },
      { { "/B" }, 4 },
      { { "/C" }, 3 },
    }, { "/C" }
  },
  // RF=2, 4 locations, 4 (1 + 2 + 1) tablet servers.
  { 2, 3,
    {
      { { "/A" }, 1 },
      { { "/B" }, 2 },
      { { "/C" }, 1 },
    }, { "/B" }
  },

  // RF=4, 4 locations, 6 (3 + 1 + 1 + 1) tablet servers.
  { 4, 8,
    {
      { { "/A" }, 3 },
      { { "/B" }, 1 },
      { { "/C" }, 1 },
      { { "/D" }, 1 },
    }, { "/B", "/D" }
  },
};

// Custom name generator for LA rebalancing scenarios described by
// LaRebalancingParams.
static string LaRebalancingTestName(
    const testing::TestParamInfo<LaRebalancingParams>& info) {
  ostringstream str;
  const auto& p = info.param;
  str << "idx" << info.index
      << "_rf" << p.replication_factor
      << "_t" << p.tables_num
      << "_l";
  for (const auto& elem : p.location_info) {
    str << "_" << elem.second;
  }
  return str.str();
}

// This is used by 'operator<<(ostream&, const LaRebalancingParams&)' below.
ostream& operator <<(ostream& out, const LocationInfo& info) {
  out << "{ ";
  for (const auto& elem : info) {
    out << elem.first << ":" << elem.second << " ";
  }
  out << "}";
  return out;
}

// This is useful to print the configuration of a failed param test.
ostream& operator <<(ostream& out, const LaRebalancingParams& info) {
  out << "{ rep_factor: " << info.replication_factor;
  out << ", num_tables: " << info.tables_num
      << ", location_info: ";
  out << info.location_info;
  out << " }";
  return out;
}

class LocationAwareRebalancingParamTest :
    public RebalancingTest,
    public ::testing::WithParamInterface<LaRebalancingParams> {
 public:
  LocationAwareRebalancingParamTest()
      : RebalancingTest(GetParam().tables_num,
                        GetParam().replication_factor,
                        std::accumulate(GetParam().location_info.begin(),
                                        GetParam().location_info.end(), 0,
                                        [](int sum, const LocationInfo::value_type& e) {
                                          return sum + e.second;
                                        })) {
  }

  bool is_343_scheme() const override {
    // These tests are for the 3-4-3 replica management scheme only.
    return true;
  }
};
INSTANTIATE_TEST_CASE_P(, LocationAwareRebalancingParamTest,
                        ::testing::ValuesIn(kLaRebalancingParams),
                        LaRebalancingTestName);
TEST_P(LocationAwareRebalancingParamTest, Rebalance) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }
  const auto& param = GetParam();
  const auto& location_info = param.location_info;
  const auto& excluded_locations = param.excluded_locations;
  const vector<string>& extra_master_flags = {
    // In this test, the only users of the location assignment test script
    // are the tablet servers: at this sub-class level it's hard to control
    // when the test client connects to the external minicluster.
    "--master_client_location_assignment_enabled=false",

    // This test can exercise scenarios with even replication factor for tables.
    "--allow_unsafe_replication_factor",
  };
  NO_FATALS(Prepare({}, extra_master_flags, location_info, excluded_locations));

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--output_replica_distribution_details=true",
  };

  // The run of the location-aware rebalancing tool should report the cluster
  // as balanced.
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
    if (!param.excluded_locations.empty()) {
      // In all location-aware cluster configurations where the replica were
      // initially placed everywhere but some 'excluded locations', the cluster
      // was unbalanced, so some replicas should have been moved.
      ASSERT_STR_NOT_CONTAINS(out, "(moved 0 replicas)");
    }
    if (param.replication_factor == 2 || param.location_info.size() == 2 ||
        (param.location_info.size() == 3 && param.replication_factor == 4)) {
      // In case some cases it's impossible to satisfy the placement policy's
      // constraints.
      ASSERT_STR_CONTAINS(out, "Placement policy violations:\n");
      ASSERT_STR_CONTAINS(out,
          "Number of non-complying tables | Number of non-complying tablets\n")
          << "stderr: " << err;
      // The "--output_replica_distribution_details" flag is set: the tool
      // should output details on the violations of the placement policy.
      ASSERT_STR_CONTAINS(out, "Placement policy violation details:\n")
          << "stderr: " << err;
    } else if (param.location_info.size() > 1) {
      // In other cases all the violations of the placement policy should be
      // corrected (in the case of a single location the rebalancer does not
      // check against placement policy violations because that case is treated
      // the same as a location-unaware, whole cluster rebalancing).
      ASSERT_STR_CONTAINS(out, "Placement policy violations:\n  none\n");
    }
  }
}

// Basic intra location rebalance tests.
class IntraLocationRebalancingBasicTest : public RebalancingTest {
public:
  IntraLocationRebalancingBasicTest()
      : RebalancingTest(/*num_tables=*/ 1,
                        /*rep_factor=*/ 3,
                        /*num_tservers=*/ 11) {
  }

  bool is_343_scheme() const override {
    return true;
  }
};

TEST_F(IntraLocationRebalancingBasicTest, LocationsWithEmptyTabletServers) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }
  const string first_table_name_pattern = "rebalance_test_first_table_$0";
  const string second_table_name_pattern = "rebalance_test_second_table_$0";
  const LocationInfo location_info = { { "/A", 3 }, { "/B", 3 }, { "/C", 3 },
                                       { "/D", 2 } };
  const vector<string>& extra_master_flags = {
    "--master_client_location_assignment_enabled=false",
  };

  copy(extra_master_flags.begin(), extra_master_flags.end(),
       back_inserter(master_flags_));

  FLAGS_num_tablet_servers = num_tservers_;
  FLAGS_num_replicas = rep_factor_;
  NO_FATALS(BuildAndStart(tserver_flags_, master_flags_, location_info,
                          /*create_table=*/ false));

  // Create special intra location imbalance in location "/D". Location "/D"
  // should only contain tablet replicas of one table.
  //
  // Note: there are 3 tablets in each table and the replica factor is 3,
  // so it's 9 tablet replicas per table.
  //
  // 1. Create table0 with tablet replicas in locations /A, /B and /C. Because
  // the per table location load skew is not greater than 1, it won't trigger
  // cross location rebalance, so no replica of table0 will be moved.
  ASSERT_OK(CreateTablesExcludingLocations({"/D"}, first_table_name_pattern));
  // 2. Create table1 in { "/A", 3 }, { "/B", 3 }, { "/C", 3 }, {"/D", 1 }.
  // There is an imbalance in location "/D" of table1.
  ASSERT_OK(CreateTablesExcludingTserversInLocations({ {"/D", 1 } },
      second_table_name_pattern));

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  // Before the fix added in this changelist the rebalancer would simply
  // crash while running. See KUDU-2987 for detail informations.
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;

    // Intra location rebalancing in location "/D" should move one replica
    // to another tserver, so there is at least one move.
    ASSERT_STR_NOT_CONTAINS(out, "(moved 0 replicas)");
    ASSERT_STR_CONTAINS(out, "Placement policy violations:\n  none\n");
  }
}

} // namespace tools
} // namespace kudu
