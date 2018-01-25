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
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::client::CountTableRows;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduUpdate;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalTabletServer;
using kudu::cluster::ScopedResumeExternalDaemon;
using kudu::itest::TServerDetails;
using kudu::itest::DeleteTablet;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using std::set;
using std::string;
using std::unordered_map;
using std::vector;

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
  ASSERT_OK(DeleteTablet(leader, tablet_id, TABLET_DATA_TOMBSTONED, kTimeout));

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
  ASSERT_OK(RemoveServer(leader, tablet_id, old_leader, kTimeout));
  // Wait until the config is committed, otherwise AddServer() will fail.
  ASSERT_OK(WaitUntilCommittedConfigOpIdIndexIs(workload.batches_completed() + 4, leader, tablet_id,
                                                kTimeout));

  TServerDetails* to_add = ts_map_[cluster_->tablet_server(missing_replica_index)->uuid()];
  ASSERT_OK(AddServer(leader, tablet_id, to_add, consensus::RaftPeerPB::VOTER, kTimeout));
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

} // namespace kudu
