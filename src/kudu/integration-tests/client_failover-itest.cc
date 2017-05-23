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

#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/optional.hpp>
#include <glog/logging.h>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/scoped_cleanup.h"

using kudu::client::CountTableRows;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduUpdate;
using kudu::client::sp::shared_ptr;
using kudu::itest::TServerDetails;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using std::set;
using std::string;
using std::thread;
using std::vector;
using std::unique_ptr;
using std::unordered_map;

DECLARE_bool(rpc_reopen_outbound_connections);
DECLARE_int64(rpc_negotiation_timeout_ms);

namespace kudu {

enum ClientTestBehavior {
  kWrite,
  kRead,
  kReadWrite
};

// Integration test for client failover behavior.
class ClientFailoverParamITest : public ExternalMiniClusterITestBase,
                                 public ::testing::WithParamInterface<ClientTestBehavior> {
};

// Test that we can delete the leader replica while scanning it and still get
// results back.
TEST_P(ClientFailoverParamITest, TestDeleteLeaderWhileScanning) {
  ClientTestBehavior test_type = GetParam();
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  vector<string> ts_flags = { "--enable_leader_failure_detection=false",
                              "--enable_tablet_copy=false" };
  vector<string> master_flags = { "--master_add_server_when_underreplicated=false",
                                  "--catalog_manager_wait_for_new_tablets_to_elect_leader=false" };

  // Start up with 4 tablet servers.
  NO_FATALS(StartCluster(ts_flags, master_flags, 4));

  // Create the test table.
  TestWorkload workload(cluster_.get());
  workload.set_write_timeout_millis(kTimeout.ToMilliseconds());
  // We count on each flush from the client corresponding to exactly one
  // consensus operation in this test. If we use batches with more than one row,
  // the client is allowed to (and will on rare occasion) break the batches
  // up into more than one write request, resulting in more than one op
  // in the log.
  workload.set_write_batch_size(1);
  workload.Setup();

  // Figure out the tablet id.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));
  vector<string> tablets = inspect_->ListTablets();
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  // Record the locations of the tablet replicas and the one TS that doesn't have a replica.
  int missing_replica_index = -1;
  set<int> replica_indexes;
  unordered_map<string, itest::TServerDetails*> active_ts_map;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    if (inspect_->ListTabletsOnTS(i).empty()) {
      missing_replica_index = i;
    } else {
      replica_indexes.insert(i);
      TServerDetails* ts = ts_map_[cluster_->tablet_server(i)->uuid()];
      active_ts_map[ts->uuid()] = ts;
      ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()], tablet_id,
                                       kTimeout));
    }
  }
  int leader_index = *replica_indexes.begin();
  TServerDetails* leader = ts_map_[cluster_->tablet_server(leader_index)->uuid()];
  ASSERT_OK(itest::StartElection(leader, tablet_id, kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, active_ts_map, tablet_id,
                                  workload.batches_completed() + 1));

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(TestWorkload::kDefaultTableName, &table));
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  session->SetTimeoutMillis(kTimeout.ToMilliseconds());

  // The row we will update later when testing writes.
  // Note that this counts as an OpId.
  KuduInsert* insert = table->NewInsert();
  ASSERT_OK(insert->mutable_row()->SetInt32(0, 0));
  ASSERT_OK(insert->mutable_row()->SetInt32(1, 1));
  ASSERT_OK(insert->mutable_row()->SetStringNoCopy(2, "a"));
  ASSERT_OK(session->Apply(insert));
  ASSERT_EQ(1, CountTableRows(table.get()));

  // Write data to a tablet.
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();
  LOG(INFO) << "workload completed " << workload.batches_completed() << " batches";

  // We don't want the leader that takes over after we kill the first leader to
  // be unsure whether the writes have been committed, so wait until all
  // replicas have all of the writes.
  //
  // We should have # opids equal to number of batches written by the workload,
  // plus the initial "manual" write we did, plus the no-op written when the
  // first leader was elected.
  ASSERT_OK(WaitForServersToAgree(kTimeout, active_ts_map, tablet_id,
                                  workload.batches_completed() + 2));

  // Open the scanner and count the rows.
  ASSERT_EQ(workload.rows_inserted() + 1, CountTableRows(table.get()));

  // Delete the leader replica. This will cause the next scan to the same
  // leader to get a TABLET_NOT_FOUND error.
  ASSERT_OK(itest::DeleteTablet(leader, tablet_id, TABLET_DATA_TOMBSTONED,
                                boost::none, kTimeout));

  int old_leader_index = leader_index;
  TServerDetails* old_leader = leader;
  leader_index = *(++replica_indexes.begin()); // Select the "next" replica as leader.
  leader = ts_map_[cluster_->tablet_server(leader_index)->uuid()];

  ASSERT_EQ(1, replica_indexes.erase(old_leader_index));
  ASSERT_EQ(1, active_ts_map.erase(old_leader->uuid()));

  // We need to elect a new leader to remove the old node.
  ASSERT_OK(itest::StartElection(leader, tablet_id, kTimeout));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(workload.batches_completed() + 3, leader, tablet_id,
                                          kTimeout));

  // Do a config change to remove the old replica and add a new one.
  // Cause the new replica to become leader, then do the scan again.
  ASSERT_OK(RemoveServer(leader, tablet_id, old_leader, boost::none, kTimeout));
  // Wait until the config is committed, otherwise AddServer() will fail.
  ASSERT_OK(WaitUntilCommittedConfigOpIdIndexIs(workload.batches_completed() + 4, leader, tablet_id,
                                                kTimeout));

  TServerDetails* to_add = ts_map_[cluster_->tablet_server(missing_replica_index)->uuid()];
  ASSERT_OK(AddServer(leader, tablet_id, to_add, consensus::RaftPeerPB::VOTER,
                      boost::none, kTimeout));
  HostPort hp;
  ASSERT_OK(HostPortFromPB(leader->registration.rpc_addresses(0), &hp));
  ASSERT_OK(StartTabletCopy(to_add, tablet_id, leader->uuid(), hp, 1, kTimeout));

  const string& new_ts_uuid = cluster_->tablet_server(missing_replica_index)->uuid();
  InsertOrDie(&replica_indexes, missing_replica_index);
  InsertOrDie(&active_ts_map, new_ts_uuid, ts_map_[new_ts_uuid]);

  // Wait for tablet copy to complete. Then elect the new node.
  ASSERT_OK(WaitForServersToAgree(kTimeout, active_ts_map, tablet_id,
                                  workload.batches_completed() + 5));
  leader_index = missing_replica_index;
  leader = ts_map_[cluster_->tablet_server(leader_index)->uuid()];
  ASSERT_OK(itest::StartElection(leader, tablet_id, kTimeout));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(workload.batches_completed() + 6, leader, tablet_id,
                                          kTimeout));

  if (test_type == kWrite || test_type == kReadWrite) {
    KuduUpdate* update = table->NewUpdate();
    ASSERT_OK(update->mutable_row()->SetInt32(0, 0));
    ASSERT_OK(update->mutable_row()->SetInt32(1, 2));
    ASSERT_OK(update->mutable_row()->SetStringNoCopy(2, "b"));
    ASSERT_OK(session->Apply(update));
    ASSERT_OK(session->Flush());
  }

  if (test_type == kRead || test_type == kReadWrite) {
    ASSERT_EQ(workload.rows_inserted() + 1, CountTableRows(table.get()));
  }
}

ClientTestBehavior test_type[] = { kWrite, kRead, kReadWrite };

INSTANTIATE_TEST_CASE_P(ClientBehavior, ClientFailoverParamITest,
                        ::testing::ValuesIn(test_type));


class ClientFailoverITest : public ExternalMiniClusterITestBase {
};

// Regression test for KUDU-1745: if the tablet servers and the master
// both experience some issue while writing, the client should not crash.
TEST_F(ClientFailoverITest, TestClusterCrashDuringWorkload) {
  // Start up with 1 tablet server and no special flags.
  NO_FATALS(StartCluster({}, {}, 1));

  // We have some VLOG messages in the client which have previously been
  // a source of crashes in this kind of error case. So, bump up
  // vlog for this test.
  if (FLAGS_v == 0) FLAGS_v = 3;

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);

  // When we shut down the servers, sometimes we get a timeout and sometimes
  // we get a network error, depending on the exact timing of where the shutdown
  // occurs. See KUDU-1466 for one possible reason why.
  workload.set_timeout_allowed(true);
  workload.set_network_error_allowed(true);

  // Set a short write timeout so that StopAndJoin below returns quickly
  // (the writes will retry as long as the timeout).
  workload.set_write_timeout_millis(1000);

  workload.Setup();
  workload.Start();
  // Wait until we've successfully written some rows, to ensure that we've
  // primed the meta cache with the tablet server before it crashes.
  while (workload.rows_inserted() == 0) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  cluster_->master()->Shutdown();
  cluster_->tablet_server(0)->Shutdown();
  workload.StopAndJoin();
}

class ClientFailoverTServerTimeoutITest : public ExternalMiniClusterITestBase {
 public:
  void SetUp() override {
    ExternalMiniClusterITestBase::SetUp();

    // Extra flags to speed up the test.
    const vector<string> extra_flags_tserver = {
      "--consensus_rpc_timeout_ms=250",
      "--heartbeat_interval_ms=10",
      "--raft_heartbeat_interval_ms=25",
      "--leader_failure_exp_backoff_max_delta_ms=1000",
    };
    const vector<string> extra_flags_master = {
      "--raft_heartbeat_interval_ms=25",
      "--leader_failure_exp_backoff_max_delta_ms=1000",
    };
    NO_FATALS(StartCluster(extra_flags_tserver, extra_flags_master, kTSNum));
  }

 protected:
  static const int kTSNum = 3;

  Status GetLeaderReplica(TServerDetails** leader) {
    string tablet_id;
    RETURN_NOT_OK(GetTabletId(&tablet_id));
    Status s;
    for (int i = 0; i < 128; ++i) {
      // FindTabletLeader tries to connect the the reported leader to verify
      // that it thinks it's the leader.
      s = itest::FindTabletLeader(
          ts_map_, tablet_id, MonoDelta::FromMilliseconds(100), leader);
      if (s.ok()) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(10L * (i + 1)));
    }
    return s;
  }

 private:
  // Get identifier of any tablet running on the tablet server with index 0.
  Status GetTabletId(string* tablet_id) {
    TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];
    vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
    RETURN_NOT_OK(itest::WaitForNumTabletsOnTS(
        ts, 1, MonoDelta::FromSeconds(32), &tablets));
    *tablet_id = tablets[0].tablet_status().tablet_id();

    return Status::OK();
  }
};

// Test that a client fails over to other available tablet replicas when a RPC
// with the former leader times out. This is a regression test for KUDU-1034.
TEST_F(ClientFailoverTServerTimeoutITest, FailoverOnLeaderTimeout) {
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kTSNum);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplica(&leader));
  ASSERT_NE(nullptr, leader);

  // Pause the leader: this will cause the client to get timeout errors
  // if trying to send RPCs to the corresponding tablet server.
  ExternalTabletServer* ts(cluster_->tablet_server_by_uuid(leader->uuid()));
  ASSERT_NE(nullptr, ts);
  ScopedResumeExternalDaemon leader_resumer(ts);
  ASSERT_OK(ts->Pause());

  // Write 100 more rows.
  int rows_target = workload.rows_inserted() + 100;
  workload.set_timeout_allowed(true);
  workload.set_write_timeout_millis(500);
  workload.Start();
  for (int i = 0; i < 1000; ++i) {
    if (workload.rows_inserted() >= rows_target) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Verify all rows have reached the destiation.
  EXPECT_GE(workload.rows_inserted(), rows_target);
}

// Series of tests to verify that client fails over to another available server
// if it experiences a timeout on connection negotiation with current server.
// The 'server' can be both a master and a tablet server.
class ClientFailoverOnNegotiationTimeoutITest : public KuduTest {
 public:
  ClientFailoverOnNegotiationTimeoutITest() {
    // Since we want to catch timeout during connection negotiation phase,
    // let's make the client re-establishing connections on every RPC.
    FLAGS_rpc_reopen_outbound_connections = true;
    // Set the connection negotiation timeout shorter than its default value
    // to run the test faster.
    FLAGS_rpc_negotiation_timeout_ms = 1000;

    cluster_opts_.extra_tserver_flags = {
        // Speed up Raft elections.
        "--raft_heartbeat_interval_ms=25",
        "--leader_failure_exp_backoff_max_delta_ms=1000",
        // Decreasing TS->master heartbeat interval speeds up the test.
        "--heartbeat_interval_ms=25",
    };
    cluster_opts_.extra_master_flags = {
        // Speed up Raft elections.
        "--raft_heartbeat_interval_ms=25",
        "--leader_failure_exp_backoff_max_delta_ms=1000",
    };
  }

  Status CreateAndStartCluster() {
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
    return cluster_->Start();
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }
 protected:
  ExternalMiniClusterOptions cluster_opts_;
  shared_ptr<ExternalMiniCluster> cluster_;
};

// Regression test for KUDU-1580: if a client times out on negotiating a connection
// to a tablet server, it should retry with other available tablet server.
TEST_F(ClientFailoverOnNegotiationTimeoutITest, Kudu1580ConnectToTServer) {
  static const int kNumTabletServers = 3;
  static const int kTimeoutMs = 5 * 60 * 1000;
  static const char* kTableName = "kudu1580";

  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  cluster_opts_.num_tablet_servers = kNumTabletServers;
  ASSERT_OK(CreateAndStartCluster());

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(
      &KuduClientBuilder()
          .default_admin_operation_timeout(MonoDelta::FromMilliseconds(kTimeoutMs))
          .default_rpc_timeout(MonoDelta::FromMilliseconds(kTimeoutMs)),
      &client));
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  KuduSchema schema(client::KuduSchemaFromSchema(CreateKeyValueTestSchema()));
  ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema)
      .add_hash_partitions({ "key" }, kNumTabletServers)
      .num_replicas(kNumTabletServers)
      .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));

  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(kTimeoutMs);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  // Running multiple iterations to cover possible variations of tablet leader
  // placement among tablet servers.
  for (int i = 0; i < 8 * kNumTabletServers; ++i) {
    vector<unique_ptr<ScopedResumeExternalDaemon>> resumers;
    for (int tsi = 0; tsi < kNumTabletServers; ++tsi) {
      ExternalTabletServer* ts(cluster_->tablet_server(tsi));
      ASSERT_NE(nullptr, ts);
      ASSERT_OK(ts->Pause());
      resumers.emplace_back(new ScopedResumeExternalDaemon(ts));
    }

    // Resume 2 out of 3 tablet servers (i.e. the majority), so the client
    // could eventially succeed with its write operations.
    thread resume_thread([&]() {
        const int idx0 = rand() % kNumTabletServers;
        unique_ptr<ScopedResumeExternalDaemon> r0(resumers[idx0].release());
        const int idx1 = (idx0 + 1) % kNumTabletServers;
        unique_ptr<ScopedResumeExternalDaemon> r1(resumers[idx1].release());
        SleepFor(MonoDelta::FromSeconds(1));
      });
    // An automatic clean-up to handle both success and failure cases
    // in the code below.
    auto cleanup = MakeScopedCleanup([&]() {
        resume_thread.join();
      });

    // Since the table is hash-partitioned with kNumTabletServer partitions,
    // hopefully three sequential numbers would go into different partitions.
    for (int ii = 0; ii < kNumTabletServers; ++ii) {
      unique_ptr<KuduInsert> ins(table->NewInsert());
      ASSERT_OK(ins->mutable_row()->SetInt32(0, kNumTabletServers * i + ii));
      ASSERT_OK(ins->mutable_row()->SetInt32(1, 0));
      ASSERT_OK(session->Apply(ins.release()));
    }
  }
}

} // namespace kudu
