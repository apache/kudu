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

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduSchema;
using client::KuduTableCreator;
using client::sp::shared_ptr;
using consensus::ConsensusStatePB;
using itest::TabletServerMap;
using itest::TServerDetails;
using std::string;
using std::vector;
using strings::Substitute;

class AdminCliTest : public tserver::TabletServerIntegrationTestBase {
};

// Test config change while running a workload.
// 1. Instantiate external mini cluster with 3 TS.
// 2. Create table with 2 replicas.
// 3. Invoke CLI to trigger a config change.
// 4. Wait until the new server bootstraps.
// 5. Profit!
TEST_F(AdminCliTest, TestChangeConfig) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 2;
  BuildAndStart({ "--enable_leader_failure_detection=false" },
                { "--catalog_manager_wait_for_new_tablets_to_elect_leader=false" });

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  TabletServerMap active_tablet_servers;
  TabletServerMap::const_iterator iter = tablet_replicas_.find(tablet_id_);
  TServerDetails* leader = iter->second;
  TServerDetails* follower = (++iter)->second;
  InsertOrDie(&active_tablet_servers, leader->uuid(), leader);
  InsertOrDie(&active_tablet_servers, follower->uuid(), follower);

  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != nullptr);

  // Elect the leader (still only a consensus config size of 2).
  ASSERT_OK(StartElection(leader, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30), active_tablet_servers,
                                  tablet_id_, 1));

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.set_write_timeout_millis(10000);
  workload.set_num_replicas(FLAGS_num_replicas);
  workload.set_num_write_threads(1);
  workload.set_write_batch_size(1);
  workload.Setup();
  workload.Start();

  // Wait until the Master knows about the leader tserver.
  TServerDetails* master_observed_leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &master_observed_leader));
  ASSERT_EQ(leader->uuid(), master_observed_leader->uuid());

  LOG(INFO) << "Adding tserver with uuid " << new_node->uuid() << " as VOTER...";
  ASSERT_OK(Subprocess::Call({
    GetKuduCtlAbsolutePath(),
    "tablet",
    "change_config",
    "add_replica",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_,
    new_node->uuid(),
    "VOTER"
  }));

  InsertOrDie(&active_tablet_servers, new_node->uuid(), new_node);
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                leader, tablet_id_,
                                                MonoDelta::FromSeconds(10)));

  workload.StopAndJoin();
  int num_batches = workload.batches_completed();

  LOG(INFO) << "Waiting for replicas to agree...";
  // Wait for all servers to replicate everything up through the last write op.
  // Since we don't batch, there should be at least # rows inserted log entries,
  // plus the initial leader's no-op, plus 1 for
  // the added replica for a total == #rows + 2.
  int min_log_index = num_batches + 2;
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30),
                                  active_tablet_servers, tablet_id_,
                                  min_log_index));

  int rows_inserted = workload.rows_inserted();
  LOG(INFO) << "Number of rows inserted: " << rows_inserted;

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(kTableId, ClusterVerifier::AT_LEAST, rows_inserted));

  // Now remove the server once again.
  LOG(INFO) << "Removing tserver with uuid " << new_node->uuid() << " from the config...";
  ASSERT_OK(Subprocess::Call({
    GetKuduCtlAbsolutePath(),
    "tablet",
    "change_config",
    "remove_replica",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_,
    new_node->uuid()
  }));

  ASSERT_EQ(1, active_tablet_servers.erase(new_node->uuid()));
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                leader, tablet_id_,
                                                MonoDelta::FromSeconds(10)));
}

Status GetTermFromConsensus(const vector<TServerDetails*>& tservers,
                            const string& tablet_id,
                            int64 *current_term) {
  ConsensusStatePB cstate;
  for (auto& ts : tservers) {
    RETURN_NOT_OK(
        itest::GetConsensusState(ts, tablet_id,
                                 consensus::CONSENSUS_CONFIG_COMMITTED,
                                 MonoDelta::FromSeconds(10), &cstate));
    if (cstate.has_leader_uuid() && cstate.has_current_term()) {
      *current_term = cstate.current_term();
      return Status::OK();
    }
  }
  return Status::NotFound(Substitute(
      "No leader replica found for tablet $0", tablet_id));
}

TEST_F(AdminCliTest, TestLeaderStepDown) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  BuildAndStart({}, {});

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());
  for (auto& ts : tservers) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts,
                                            tablet_id_,
                                            MonoDelta::FromSeconds(10)));
  }

  int64 current_term;
  ASSERT_OK(GetTermFromConsensus(tservers, tablet_id_,
                                 &current_term));

  // The leader for the given tablet may change anytime, resulting in
  // the command returning an error code. Hence checking for term advancement
  // only if the leader_step_down succeeds. It is also unsafe to check
  // the term advancement without honoring status of the command since
  // there may not have been another election in the meanwhile.
  string stderr;
  Status s = Subprocess::Call({GetKuduCtlAbsolutePath(),
                               "tablet", "leader_step_down",
                               cluster_->master()->bound_rpc_addr().ToString(),
                               tablet_id_}, nullptr, &stderr);
  bool not_currently_leader = stderr.find(
      Status::IllegalState("").CodeAsString()) != string::npos;
  ASSERT_TRUE(s.ok() || not_currently_leader);
  if (s.ok()) {
    int64 new_term;
    AssertEventually([&]() {
        ASSERT_OK(GetTermFromConsensus(tservers, tablet_id_,
                                       &new_term));
        ASSERT_GT(new_term, current_term);
      });
  }
}

TEST_F(AdminCliTest, TestLeaderStepDownWhenNotPresent) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  BuildAndStart(
      { "--enable_leader_failure_detection=false" },
      { "--catalog_manager_wait_for_new_tablets_to_elect_leader=false" });
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());
  for (auto& ts : tservers) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts,
                                            tablet_id_,
                                            MonoDelta::FromSeconds(10)));
  }

  int64 current_term;
  ASSERT_TRUE(GetTermFromConsensus(tservers, tablet_id_,
                                   &current_term).IsNotFound());
  string stdout;
  ASSERT_OK(Subprocess::Call({
    GetKuduCtlAbsolutePath(),
    "tablet",
    "leader_step_down",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_
  }, &stdout));
  ASSERT_STR_CONTAINS(stdout,
                      Substitute("No leader replica found for tablet $0",
                                 tablet_id_));
}

TEST_F(AdminCliTest, TestDeleteTable) {
  FLAGS_num_tablet_servers = 1;
  FLAGS_num_replicas = 1;
  BuildAndStart({}, {});

  string master_address = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
            .add_master_server_addr(master_address)
            .Build(&client));

  ASSERT_OK(Subprocess::Call({
    GetKuduCtlAbsolutePath(),
    "table",
    "delete",
    master_address,
    kTableId
  }));

  vector<string> tables;
  ASSERT_OK(client->ListTables(&tables));
  ASSERT_TRUE(tables.empty());
}

TEST_F(AdminCliTest, TestListTables) {
  FLAGS_num_tablet_servers = 1;
  FLAGS_num_replicas = 1;

  BuildAndStart({}, {});

  string stdout;
  ASSERT_OK(Subprocess::Call({
    GetKuduCtlAbsolutePath(),
    "table",
    "list",
    cluster_->master()->bound_rpc_addr().ToString()
  }, &stdout, nullptr));

  vector<string> stdout_lines = strings::Split(stdout, ",",
                                               strings::SkipEmpty());
  ASSERT_EQ(1, stdout_lines.size());
  ASSERT_EQ(Substitute("$0\n", kTableId), stdout_lines[0]);
}

TEST_F(AdminCliTest, TestListTablesDetail) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;

  BuildAndStart({}, {});

  // Add another table to test multiple tables output.
  const string kAnotherTableId = "TestAnotherTable";
  KuduSchema client_schema(client::KuduSchemaFromSchema(schema_));
  gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kAnotherTableId)
           .schema(&client_schema)
           .set_range_partition_columns({ "key" })
           .num_replicas(FLAGS_num_replicas)
           .Create());

  // Grab list of tablet_ids from any tserver.
  vector<TServerDetails*> tservers;
  vector<string> tablet_ids;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ListRunningTabletIds(tservers.front(),
                       MonoDelta::FromSeconds(30), &tablet_ids);

  string stdout;
  ASSERT_OK(Subprocess::Call({
    GetKuduCtlAbsolutePath(),
    "table",
    "list",
    "--list_tablets",
    cluster_->master()->bound_rpc_addr().ToString()
  }, &stdout, nullptr));

  vector<string> stdout_lines = strings::Split(stdout, "\n",
                                               strings::SkipEmpty());

  // Verify multiple tables along with their tablets and replica-uuids.
  ASSERT_EQ(4, stdout_lines.size());
  ASSERT_STR_CONTAINS(stdout, kTableId);
  ASSERT_STR_CONTAINS(stdout, kAnotherTableId);
  ASSERT_STR_CONTAINS(stdout, tablet_ids.front());
  ASSERT_STR_CONTAINS(stdout, tablet_ids.back());

  for (auto& ts : tservers) {
    ASSERT_STR_CONTAINS(stdout, ts->uuid());
    ASSERT_STR_CONTAINS(stdout, ts->uuid());
  }
}

} // namespace tools
} // namespace kudu
