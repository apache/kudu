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
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/log_verifier.h"
#include "kudu/integration-tests/raft_consensus-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DECLARE_int64(client_inserts_per_thread);
DECLARE_int64(client_num_batches_per_thread);
DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_int32(num_client_threads);
DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);
DECLARE_int32(rpc_timeout);

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_counter(transaction_memory_pressure_rejections);

using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::MajoritySize;
using kudu::consensus::MakeOpId;
using kudu::consensus::OpId;
using kudu::consensus::RaftPeerAttrsPB;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::ReplicateMsg;
using kudu::itest::AddServer;
using kudu::itest::DONT_WAIT_FOR_LEADER;
using kudu::itest::GetInt64Metric;
using kudu::itest::LeaderStepDown;
using kudu::itest::RemoveServer;
using kudu::itest::StartElection;
using kudu::itest::TServerDetails;
using kudu::itest::TabletServerMap;
using kudu::itest::WAIT_FOR_LEADER;
using kudu::itest::WaitForReplicasReportedToMaster;
using kudu::itest::WaitUntilCommittedOpIdIndexIs;
using kudu::itest::WaitUntilLeader;
using kudu::itest::WriteSimpleTestRow;
using kudu::master::TabletLocationsPB;
using kudu::master::VOTER_REPLICA;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcController;
using kudu::server::SetFlagRequestPB;
using kudu::server::SetFlagResponsePB;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

static const int kConsensusRpcTimeoutForTests = 50;

static const int kTestRowKey = 1234;
static const int kTestRowIntVal = 5678;

// Integration test for the raft consensus implementation.
// Uses the whole tablet server stack with ExternalMiniCluster.
class RaftConsensusITest : public RaftConsensusITestBase {
 public:
  RaftConsensusITest() {
  }

  // Scan the given replica in a loop until the number of rows
  // is 'expected_count'. If it takes more than 10 seconds, then
  // fails the test.
  void WaitForRowCount(TabletServerServiceProxy* replica_proxy,
                       int expected_count,
                       vector<string>* results);

  // Add an Insert operation to the given consensus request.
  // The row to be inserted is generated based on the OpId.
  void AddOp(const OpId& id, ConsensusRequestPB* req);
  void AddOpWithTypeAndKey(const OpId& id,
                           RowOperationsPB::Type op_type,
                           int32_t key,
                           ConsensusRequestPB* req);

  string DumpToString(TServerDetails* leader,
                      const vector<string>& leader_results,
                      TServerDetails* replica,
                      const vector<string>& replica_results);

  // Insert 'num_rows' rows starting with row key 'start_row'.
  // Each row will have size 'payload_size'. A short (100ms) timeout is
  // used. If the flush generates any errors they will be ignored.
  void InsertPayloadIgnoreErrors(int start_row, int num_rows, int payload_size);

  // Brings Chaos to a MiniTabletServer by introducing random delays. Does this by
  // pausing the daemon a random amount of time.
  void DelayInjectorThread(ExternalTabletServer* tablet_server, int timeout_msec);

  // Thread which loops until '*finish' becomes true, trying to insert a row
  // on the given tablet server identified by 'replica_idx'.
  void StubbornlyWriteSameRowThread(int replica_idx, const AtomicBool* finish);

  // Stops the current leader of the configuration, runs leader election and then brings it back.
  // Before stopping the leader this pauses all follower nodes in regular intervals so that
  // we get an increased chance of stuff being pending.
  void StopOrKillLeaderAndElectNewOne();

  // Writes 'num_writes' operations to the current leader. Each of the operations
  // has a payload of around 128KB. Causes a gtest failure on error.
  void Write128KOpsToLeader(int num_writes);

  // Ensure that a majority of servers is required for elections and writes.
  // This is done by pausing a majority and asserting that writes and elections fail,
  // then unpausing the majority and asserting that elections and writes succeed.
  // If fails, throws a gtest assertion.
  // Note: This test assumes all tablet servers listed in tablet_servers are voters.
  void AssertMajorityRequiredForElectionsAndWrites(const TabletServerMap& tablet_servers,
                                                   const string& leader_uuid);

  void CreateClusterForCrashyNodesTests();
  void DoTestCrashyNodes(TestWorkload* workload, int max_rows_to_insert);

  // Prepare for a test where a single replica of a 3-server cluster is left
  // running as a follower.
  void SetupSingleReplicaTest(TServerDetails** replica_ts);

 protected:
  shared_ptr<KuduTable> table_;
  vector<scoped_refptr<kudu::Thread> > threads_;
};

void RaftConsensusITest::WaitForRowCount(TabletServerServiceProxy* replica_proxy,
                                         int expected_count,
                                         vector<string>* results) {
  LOG(INFO) << "Waiting for row count " << expected_count << "...";
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + MonoDelta::FromSeconds(90);
  while (true) {
    results->clear();
    NO_FATALS(ScanReplica(replica_proxy, results));
    if (results->size() == expected_count) {
      return;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
    if (MonoTime::Now() >= deadline) {
      break;
    }
  }
  FAIL() << "Did not reach expected row count " << expected_count
         << " after " << (MonoTime::Now() - start).ToString()
         << ": rows: " << *results;
}

void RaftConsensusITest::AddOp(const OpId& id, ConsensusRequestPB* req) {
  AddOpWithTypeAndKey(id, RowOperationsPB::INSERT,
                      id.index() * 10000 + id.term(), req);
}

void RaftConsensusITest::AddOpWithTypeAndKey(const OpId& id,
                                             RowOperationsPB::Type op_type,
                                             int32_t key,
                                             ConsensusRequestPB* req) {
  ReplicateMsg* msg = req->add_ops();
  msg->mutable_id()->CopyFrom(id);
  msg->set_timestamp(id.index());
  msg->set_op_type(consensus::WRITE_OP);
  WriteRequestPB* write_req = msg->mutable_write_request();
  CHECK_OK(SchemaToPB(schema_, write_req->mutable_schema()));
  write_req->set_tablet_id(tablet_id_);
  AddTestRowToPB(op_type, schema_, key, id.term(),
                 SecureShortDebugString(id), write_req->mutable_row_operations());
}

string RaftConsensusITest::DumpToString(TServerDetails* leader,
                                        const vector<string>& leader_results,
                                        TServerDetails* replica,
                                        const vector<string>& replica_results) {
  string ret = Substitute("Replica results did not match the leaders."
                          "\nLeader: $0\nReplica: $1. Results size "
                          "L: $2 R: $3",
                          leader->ToString(),
                          replica->ToString(),
                          leader_results.size(),
                          replica_results.size());

  StrAppend(&ret, "Leader Results: \n");
  for (const string& result : leader_results) {
    StrAppend(&ret, result, "\n");
  }

  StrAppend(&ret, "Replica Results: \n");
  for (const string& result : replica_results) {
    StrAppend(&ret, result, "\n");
  }

  return ret;
}

void RaftConsensusITest::InsertPayloadIgnoreErrors(int start_row,
                                                   int num_rows,
                                                   int payload_size) {
  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(kTableId, &table));
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(100);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  string payload(payload_size, 'x');
  for (int i = 0; i < num_rows; i++) {
    gscoped_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    CHECK_OK(row->SetInt32(0, i + start_row));
    CHECK_OK(row->SetInt32(1, 0));
    CHECK_OK(row->SetStringCopy(2, payload));
    CHECK_OK(session->Apply(insert.release()));
    ignore_result(session->Flush());
  }
}

void RaftConsensusITest::DelayInjectorThread(
    ExternalTabletServer* tablet_server, int timeout_msec) {

  while (inserters_.count() > 0) {
    // Adjust the value obtained from the normalized gauss. dist. so that we steal the lock
    // longer than the the timeout a small (~5%) percentage of the times.
    // (95% corresponds to 1.64485, in a normalized (0,1) gaussian distribution).
    double sleep_time_usec = 1000 *
        ((random_.Normal(0, 1) * timeout_msec) / 1.64485);

    if (sleep_time_usec < 0) sleep_time_usec = 0;

    // Additionally only cause timeouts at all 50% of the time, otherwise sleep.
    double val = (rand() * 1.0) / RAND_MAX;
    if (val < 0.5) {
      SleepFor(MonoDelta::FromMicroseconds(sleep_time_usec));
      continue;
    }

    ASSERT_OK(tablet_server->Pause());
    LOG_IF(INFO, sleep_time_usec > 0.0)
        << "Delay injector thread for TS " << tablet_server->instance_id().permanent_uuid()
        << " SIGSTOPped the ts, sleeping for " << sleep_time_usec << " usec...";
    SleepFor(MonoDelta::FromMicroseconds(sleep_time_usec));
    ASSERT_OK(tablet_server->Resume());
  }
}

void RaftConsensusITest::StubbornlyWriteSameRowThread(int replica_idx, const AtomicBool* finish) {
  vector<TServerDetails*> servers;
  AppendValuesFromMap(tablet_servers_, &servers);
  CHECK_LT(replica_idx, servers.size());
  TServerDetails* ts = servers[replica_idx];

  // Manually construct an RPC to our target replica. We expect most of the calls
  // to fail either with an "already present" or an error because we are writing
  // to a follower. That's OK, though - what we care about for this test is
  // just that the operations Apply() in the same order everywhere (even though
  // in this case the result will just be an error).
  WriteRequestPB req;
  WriteResponsePB resp;
  RpcController rpc;
  req.set_tablet_id(tablet_id_);
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, kTestRowKey, kTestRowIntVal,
                 "hello world", req.mutable_row_operations());

  while (!finish->Load()) {
    resp.Clear();
    rpc.Reset();
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ignore_result(ts->tserver_proxy->Write(req, &resp, &rpc));
    VLOG(1) << "Response from server " << replica_idx << ": "
            << SecureShortDebugString(resp);
  }
}

void RaftConsensusITest::StopOrKillLeaderAndElectNewOne() {
  TServerDetails* leader;
  vector<TServerDetails*> followers;
  CHECK_OK(GetTabletLeaderAndFollowers(tablet_id_, &leader, &followers));
  ExternalTabletServer* leader_ets = cluster_->tablet_server_by_uuid(leader->uuid());

  for (const auto* ts : followers) {
    ExternalTabletServer* ets = cluster_->tablet_server_by_uuid(ts->uuid());
    CHECK_OK(ets->Pause());
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  // When all are paused also pause or kill the current leader. Since we've waited a bit
  // the old leader is likely to have operations that must be aborted.
  const bool do_kill = rand() % 2 == 0;
  if (do_kill) {
    leader_ets->Shutdown();
  } else {
    CHECK_OK(leader_ets->Pause());
  }

  // Resume the replicas.
  for (const auto* ts : followers) {
    ExternalTabletServer* ets = cluster_->tablet_server_by_uuid(ts->uuid());
    CHECK_OK(ets->Resume());
  }

  // Get the new leader.
  TServerDetails* new_leader;
  CHECK_OK(GetLeaderReplicaWithRetries(tablet_id_, &new_leader));

  // Bring the old leader back.
  if (do_kill) {
    CHECK_OK(leader_ets->Restart());
    // Wait until we have the same number of followers.
    const auto initial_followers = followers.size();
    do {
      GetOnlyLiveFollowerReplicas(tablet_id_, &followers);
    } while (followers.size() < initial_followers);
  } else {
    CHECK_OK(leader_ets->Resume());
  }
}

void RaftConsensusITest::Write128KOpsToLeader(int num_writes) {
  TServerDetails* leader = nullptr;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

  WriteRequestPB req;
  req.set_tablet_id(tablet_id_);
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
  RowOperationsPB* data = req.mutable_row_operations();
  WriteResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(10000));
  int key = 0;

  // generate a 128Kb dummy payload
  string test_payload(128 * 1024, '0');
  for (int i = 0; i < num_writes; i++) {
    rpc.Reset();
    data->Clear();
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, key, key,
                   test_payload, data);
    key++;
    ASSERT_OK(leader->tserver_proxy->Write(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);
  }
}

void RaftConsensusITest::AssertMajorityRequiredForElectionsAndWrites(
    const TabletServerMap& tablet_servers, const string& leader_uuid) {

  TServerDetails* initial_leader = FindOrDie(tablet_servers, leader_uuid);

  // Calculate number of servers to leave unpaused (minority).
  // This math is a little unintuitive but works for cluster sizes including 2 and 1.
  // Note: We assume all of these TSes are voters.
  int config_size = tablet_servers.size();
  int minority_to_retain = MajoritySize(config_size) - 1;

  // Only perform this part of the test if we have some servers to pause, else
  // the failure assertions will throw.
  if (config_size > 1) {
    // Pause enough replicas to prevent a majority.
    int num_to_pause = config_size - minority_to_retain;
    LOG(INFO) << "Pausing " << num_to_pause << " tablet servers in config of size " << config_size;
    vector<string> paused_uuids;
    for (const TabletServerMap::value_type& entry : tablet_servers) {
      if (paused_uuids.size() == num_to_pause) {
        continue;
      }
      const string& replica_uuid = entry.first;
      if (replica_uuid == leader_uuid) {
        // Always leave this one alone.
        continue;
      }
      ExternalTabletServer* replica_ts = cluster_->tablet_server_by_uuid(replica_uuid);
      ASSERT_OK(replica_ts->Pause());
      paused_uuids.push_back(replica_uuid);
    }

    // Ensure writes timeout while only a minority is alive.
    Status s = WriteSimpleTestRow(initial_leader, tablet_id_, RowOperationsPB::UPDATE,
                                  kTestRowKey, kTestRowIntVal, "foo",
                                  MonoDelta::FromMilliseconds(100));
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

    // Step down.
    ASSERT_OK(LeaderStepDown(initial_leader, tablet_id_, MonoDelta::FromSeconds(10)));

    // Assert that elections time out without a live majority.
    // We specify a very short timeout here to keep the tests fast.
    ASSERT_OK(StartElection(initial_leader, tablet_id_, MonoDelta::FromSeconds(10)));
    s = WaitUntilLeader(initial_leader, tablet_id_, MonoDelta::FromMilliseconds(100));
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    LOG(INFO) << "Expected timeout encountered on election with weakened config: " << s.ToString();

    // Resume the paused servers.
    LOG(INFO) << "Resuming " << num_to_pause << " tablet servers in config of size " << config_size;
    for (const string& replica_uuid : paused_uuids) {
      ExternalTabletServer* replica_ts = cluster_->tablet_server_by_uuid(replica_uuid);
      ASSERT_OK(replica_ts->Resume());
    }
  }

  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(20), tablet_servers, tablet_id_, 1));

  // Now an election should succeed.
  ASSERT_OK(StartElection(initial_leader, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitUntilLeader(initial_leader, tablet_id_, MonoDelta::FromSeconds(10)));
  LOG(INFO) << "Successful election with full config of size " << config_size;

  // And a write should also succeed.
  ASSERT_OK(WriteSimpleTestRow(initial_leader, tablet_id_, RowOperationsPB::UPDATE,
                               kTestRowKey, kTestRowIntVal, Substitute("qsz=$0", config_size),
                               MonoDelta::FromSeconds(10)));
}

void RaftConsensusITest::CreateClusterForCrashyNodesTests() {
  if (AllowSlowTests()) {
    FLAGS_num_tablet_servers = 7;
    FLAGS_num_replicas = 7;
  }

  vector<string> ts_flags;

  // Crash 5% of the time just before sending an RPC. With 7 servers,
  // this means we crash about 30% of the time before we've fully
  // replicated the NO_OP at the start of the term.
  ts_flags.emplace_back("--fault_crash_on_leader_request_fraction=0.05");

  // Inject latency to encourage the replicas to fall out of sync
  // with each other.
  ts_flags.emplace_back("--log_inject_latency");
  ts_flags.emplace_back("--log_inject_latency_ms_mean=30");
  ts_flags.emplace_back("--log_inject_latency_ms_stddev=60");

  // Make leader elections faster so we get through more cycles of leaders.
  ts_flags.emplace_back("--raft_heartbeat_interval_ms=100");

  // Avoid preallocating segments since bootstrap is a little bit
  // faster if it doesn't have to scan forward through the preallocated
  // log area.
  ts_flags.emplace_back("--log_preallocate_segments=false");

  CreateCluster("raft_consensus-itest-crashy-nodes-cluster", ts_flags, {});
}

void RaftConsensusITest::DoTestCrashyNodes(TestWorkload* workload, int max_rows_to_insert) {
  int crashes_to_cause = 3;
  if (AllowSlowTests()) {
    crashes_to_cause = 15;
  }

  workload->set_num_replicas(FLAGS_num_replicas);
  // Set a really high write timeout so that even in the presence of many failures we
  // can verify an exact number of rows in the end, thanks to exactly once semantics.
  workload->set_write_timeout_millis(60 * 1000 /* 60 seconds */);
  workload->set_num_write_threads(10);
  workload->set_num_read_threads(2);
  workload->Setup();
  workload->Start();

  int num_crashes = 0;
  while (num_crashes < crashes_to_cause &&
      workload->rows_inserted() < max_rows_to_insert) {
    num_crashes += RestartAnyCrashedTabletServers();
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Writers are likely ongoing. To have some chance of completing all writes,
  // restart the tablets servers, otherwise they'll keep crashing and the writes
  // can never complete.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ExternalTabletServer* ts = cluster_->tablet_server(i);
    vector<string>* flags = ts->mutable_flags();
    bool removed_flag = false;
    for (auto it = flags->begin(); it != flags->end(); ++it) {
      if (HasPrefixString(*it, "--fault_crash")) {
        flags->erase(it);
        removed_flag = true;
        break;
      }
    }
    ASSERT_TRUE(removed_flag) << "could not remove flag from TS " << i
                              << "\nFlags:\n" << *flags;
    ts->Shutdown();
    ASSERT_OK(ts->Restart());
  }

  workload->StopAndJoin();

  // Ensure that the replicas converge.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload->table_name(),
                            ClusterVerifier::EXACTLY,
                            workload->rows_inserted()));
}

void RaftConsensusITest::SetupSingleReplicaTest(TServerDetails** replica_ts) {
  const vector<string> kTsFlags = {
    // Don't use the hybrid clock as we set logical timestamps on ops.
    "--use_hybrid_clock=false",
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false"
  };

  FLAGS_num_replicas = 3;
  FLAGS_num_tablet_servers = 3;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  // Kill all the servers but one.
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());

  // Elect server 2 as leader and wait for log index 1 to propagate to all servers.
  ASSERT_OK(StartElection(tservers[2], tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 1));

  cluster_->tablet_server_by_uuid(tservers[1]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(tservers[2]->uuid())->Shutdown();

  *replica_ts = tservers[0];
  LOG(INFO) << "================================== Cluster setup complete.";
}

// Test that we can retrieve the permanent uuid of a server running
// consensus service via RPC.
TEST_F(RaftConsensusITest, TestGetPermanentUuid) {
  NO_FATALS(BuildAndStart());

  RaftPeerPB peer;
  TServerDetails* leader = nullptr;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
  peer.mutable_last_known_addr()->CopyFrom(leader->registration.rpc_addresses(0));
  const string expected_uuid = leader->instance_id.permanent_uuid();

  rpc::MessengerBuilder builder("test builder");
  builder.set_num_reactors(1);
  std::shared_ptr<rpc::Messenger> messenger;
  ASSERT_OK(builder.Build(&messenger));

  ASSERT_OK(consensus::SetPermanentUuidForRemotePeer(messenger, &peer));
  ASSERT_EQ(expected_uuid, peer.permanent_uuid());
}

// TODO allow the scan to define an operation id, fetch the last id
// from the leader and then use that id to make the replica wait
// until it is done. This will avoid the sleeps below.
TEST_F(RaftConsensusITest, TestInsertAndMutateThroughConsensus) {
  NO_FATALS(BuildAndStart());

  int num_iters = AllowSlowTests() ? 10 : 1;

  for (int i = 0; i < num_iters; i++) {
    InsertTestRowsRemoteThread(i * FLAGS_client_inserts_per_thread,
                               FLAGS_client_inserts_per_thread,
                               FLAGS_client_num_batches_per_thread,
                               vector<CountDownLatch*>());
  }
  NO_FATALS(AssertAllReplicasAgree(FLAGS_client_inserts_per_thread * num_iters));
}

TEST_F(RaftConsensusITest, TestFailedTransaction) {
  NO_FATALS(BuildAndStart());

  // Wait until we have a stable leader.
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_,
                                  tablet_id_, 1));

  WriteRequestPB req;
  req.set_tablet_id(tablet_id_);
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));

  RowOperationsPB* data = req.mutable_row_operations();
  data->set_rows("some gibberish!");

  WriteResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));

  TServerDetails* leader = nullptr;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

  ASSERT_OK(DCHECK_NOTNULL(leader->tserver_proxy.get())->Write(req, &resp, &controller));
  ASSERT_TRUE(resp.has_error());

  // Add a proper row so that we can verify that all of the replicas continue
  // to process transactions after a failure. Additionally, this allows us to wait
  // for all of the replicas to finish processing transactions before shutting down,
  // avoiding a potential stall as we currently can't abort transactions (see KUDU-341).
  data->Clear();
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 0, 0, "original0", data);

  controller.Reset();
  controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));

  ASSERT_OK(DCHECK_NOTNULL(leader->tserver_proxy.get())->Write(req, &resp, &controller));
  SCOPED_TRACE(SecureShortDebugString(resp));
  ASSERT_FALSE(resp.has_error());

  NO_FATALS(AssertAllReplicasAgree(1));
}

// Inserts rows through consensus and also starts one delay injecting thread
// that steals consensus peer locks for a while. This is meant to test that
// even with timeouts and repeated requests consensus still works.
TEST_F(RaftConsensusITest, MultiThreadedMutateAndInsertThroughConsensus) {
  NO_FATALS(BuildAndStart());

  if (500 == FLAGS_client_inserts_per_thread) {
    if (AllowSlowTests()) {
      FLAGS_client_inserts_per_thread = FLAGS_client_inserts_per_thread * 10;
      FLAGS_client_num_batches_per_thread = FLAGS_client_num_batches_per_thread * 10;
    }
  }

  int num_threads = FLAGS_num_client_threads;
  for (int i = 0; i < num_threads; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", Substitute("ts-test$0", i),
                                  &RaftConsensusITest::InsertTestRowsRemoteThread,
                                  this, i * FLAGS_client_inserts_per_thread,
                                  FLAGS_client_inserts_per_thread,
                                  FLAGS_client_num_batches_per_thread,
                                  vector<CountDownLatch*>(),
                                  &new_thread));
    threads_.push_back(new_thread);
  }
  for (int i = 0; i < FLAGS_num_replicas; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", Substitute("chaos-test$0", i),
                                  &RaftConsensusITest::DelayInjectorThread,
                                  this, cluster_->tablet_server(i),
                                  kConsensusRpcTimeoutForTests,
                                  &new_thread));
    threads_.push_back(new_thread);
  }
  for (scoped_refptr<kudu::Thread> thr : threads_) {
   CHECK_OK(ThreadJoiner(thr.get()).Join());
  }

  NO_FATALS(AssertAllReplicasAgree(FLAGS_client_inserts_per_thread * FLAGS_num_client_threads));
}

TEST_F(RaftConsensusITest, TestInsertOnNonLeader) {
  NO_FATALS(BuildAndStart());

  // Wait for the initial leader election to complete.
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_,
                                  tablet_id_, 1));

  // Manually construct a write RPC to a replica and make sure it responds
  // with the correct error code.
  WriteRequestPB req;
  WriteResponsePB resp;
  RpcController rpc;
  req.set_tablet_id(tablet_id_);
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, kTestRowKey, kTestRowIntVal,
                 "hello world via RPC", req.mutable_row_operations());

  // Get the leader.
  vector<TServerDetails*> followers;
  GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

  ASSERT_OK(followers[0]->tserver_proxy->Write(req, &resp, &rpc));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_TRUE(resp.has_error());
  Status s = StatusFromPB(resp.error().status());
  EXPECT_TRUE(s.IsIllegalState());
  ASSERT_STR_CONTAINS(s.ToString(), "is not leader of this config. Role: FOLLOWER");
  // TODO(unknown): need to change the error code to be something like REPLICA_NOT_LEADER
  // so that the client can properly handle this case! plumbing this is a little difficult
  // so not addressing at the moment.
  NO_FATALS(AssertAllReplicasAgree(0));
}

// Test that when a follower is stopped for a long time, the log cache
// properly evicts operations, but still allows the follower to catch
// up when it comes back.
//
// Also asserts that the other replicas retain logs for the stopped
// follower to catch up from.
TEST_F(RaftConsensusITest, TestCatchupAfterOpsEvicted) {
  const vector<string> kTsFlags = {
    "--log_cache_size_limit_mb=1",
    "--consensus_max_batch_size_bytes=500000",
    // Use short and synchronous rolls so that we can test log segment retention.
    "--log_segment_size_mb=1",
    "--log_async_preallocate_segments=false",
    // Run the maintenance manager frequently and flush quickly,
    // so that we don't have to wait long for GC.
    "--maintenance_manager_polling_interval_ms=100",
    "--flush_threshold_secs=3",
    // We write 128KB cells in this test, so bump the limit.
    "--max_cell_size_bytes=1000000",
    // And disable WAL compression so the 128KB cells don't get compressed away.
    "--log_compression_codec=none"
  };

  NO_FATALS(BuildAndStart(kTsFlags));
  TServerDetails* replica = (*tablet_replicas_.begin()).second;
  ASSERT_TRUE(replica != nullptr);
  ExternalTabletServer* replica_ets = cluster_->tablet_server_by_uuid(replica->uuid());

  // Pause a replica
  ASSERT_OK(replica_ets->Pause());
  LOG(INFO)<< "Paused one of the replicas, starting to write.";

  // Insert 5MB worth of data.
  const int kNumWrites = 40;
  NO_FATALS(Write128KOpsToLeader(kNumWrites));

  // Sleep a bit to give the maintenance manager time to GC logs, if it were
  // going to.
  SleepFor(MonoDelta::FromSeconds(1));

  // Check that the leader and non-paused follower have not GCed any logs (since
  // the third peer needs them to catch up).
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    int num_wals = inspect_->CountFilesInWALDirForTS(i, tablet_id_, "wal-*");
    if (cluster_->tablet_server(i) == replica_ets) {
      ASSERT_EQ(1, num_wals) << "Replica should have only one segment";
    } else {
      ASSERT_EQ(6, num_wals)
          << "Other nodes should retain segments for the frozen replica to catch up";
    }
  }

  // Now unpause the replica, the lagging replica should eventually catch back up.
  ASSERT_OK(replica_ets->Resume());
  NO_FATALS(AssertAllReplicasAgree(kNumWrites));

  // Once the follower has caught up, all replicas should eventually GC the earlier
  // log segments that they were retaining.
  ASSERT_EVENTUALLY([&]() {
      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        SCOPED_TRACE(Substitute("TS $0", i));
        int num_wals = inspect_->CountFilesInWALDirForTS(i, tablet_id_, "wal-*");
        ASSERT_EQ(1, num_wals);
      }
    });
}

// Test that the leader doesn't crash if one of its followers has
// fallen behind so far that the logs necessary to catch it up
// have been GCed.
//
// In a real cluster, this will eventually cause the follower to be
// evicted/replaced. In any case, the leader should not crash.
//
// We also ensure that, when the leader stops writing to the follower,
// the follower won't disturb the other nodes when it attempts to elect
// itself.
//
// This is a regression test for KUDU-775 and KUDU-562.
TEST_F(RaftConsensusITest, TestFollowerFallsBehindLeaderGC) {
  vector<string> ts_flags = {
    // Disable follower eviction to maintain the original intent of this test.
    "--evict_failed_followers=false",
  };
  AddFlagsForLogRolls(&ts_flags); // For CauseFollowerToFallBehindLogGC().
  NO_FATALS(BuildAndStart(ts_flags));

  string leader_uuid;
  int64_t orig_term;
  string follower_uuid;
  NO_FATALS(CauseFollowerToFallBehindLogGC(
      tablet_servers_, &leader_uuid, &orig_term, &follower_uuid));
  SCOPED_TRACE(Substitute("leader: $0 follower: $1", leader_uuid, follower_uuid));

  // Wait for remaining majority to agree.
  TabletServerMap active_tablet_servers = tablet_servers_;
  ASSERT_EQ(3, active_tablet_servers.size());
  ASSERT_EQ(1, active_tablet_servers.erase(follower_uuid));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30), active_tablet_servers, tablet_id_,
                                  1));

  if (AllowSlowTests()) {
    // Sleep long enough that the "abandoned" server's leader election interval
    // will trigger several times. Then, verify that the term has not increased
    // on any of the servers.
    // This ensures that the other servers properly reject the pre-election requests
    // from the abandoned node, and that the abandoned node doesn't bump its term
    // either, since that would cause spurious leader elections upon the node coming back
    // to life.
    SleepFor(MonoDelta::FromSeconds(5));

    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      ExternalTabletServer* ts = cluster_->tablet_server(i);
      SCOPED_TRACE(ts->uuid());
      int64_t term_from_metric = -1;
      ASSERT_OK(GetTermMetricValue(ts, &term_from_metric));
      ASSERT_EQ(term_from_metric, orig_term);
    }
    OpId op_id;
    TServerDetails* leader = tablet_servers_[leader_uuid];
    ASSERT_OK(GetLastOpIdForReplica(tablet_id_, leader, consensus::RECEIVED_OPID,
                                    MonoDelta::FromSeconds(10), &op_id));
    ASSERT_EQ(orig_term, op_id.term())
      << "expected the leader to have not advanced terms but has op " << op_id;
  }
}

// This test starts several tablet servers, and configures them with
// fault injection so that the leaders frequently crash just before
// sending RPCs to followers.
//
// This can result in various scenarios where leaders crash right after
// being elected and never succeed in replicating their first operation.
// For example, KUDU-783 reproduces from this test approximately 5% of the
// time on a slow-test debug build.
TEST_F(RaftConsensusITest, InsertUniqueKeysWithCrashyNodes) {
  CreateClusterForCrashyNodesTests();

  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1);

  NO_FATALS(DoTestCrashyNodes(&workload, 100));
}

// The same crashy nodes test as above but inserts many duplicate keys.
// This emulates cases where there are many duplicate keys which, due to two phase
// locking, may cause deadlocks and other anomalies that cannot be observed when
// keys are unique.
TEST_F(RaftConsensusITest, InsertDuplicateKeysWithCrashyNodes) {
  CreateClusterForCrashyNodesTests();

  TestWorkload workload(cluster_.get());
  workload.set_write_pattern(TestWorkload::INSERT_WITH_MANY_DUP_KEYS);
  // Increase the number of rows per batch to get a higher chance of key collision.
  workload.set_write_batch_size(3);

  NO_FATALS(DoTestCrashyNodes(&workload, 300));
}


TEST_F(RaftConsensusITest, MultiThreadedInsertWithFailovers) {
  int kNumElections = FLAGS_num_replicas;

  if (AllowSlowTests()) {
    FLAGS_num_tablet_servers = 7;
    FLAGS_num_replicas = 7;
    kNumElections = 3 * FLAGS_num_replicas;
  }

  // Reset consensus rpc timeout to the default value or the election might fail often.
  FLAGS_consensus_rpc_timeout_ms = 1000;

  // Start a 7 node configuration cluster (since we can't bring leaders back we start with a
  // higher replica count so that we kill more leaders).

  NO_FATALS(BuildAndStart());

  OverrideFlagForSlowTests(
      "client_inserts_per_thread",
      Substitute("$0", (FLAGS_client_inserts_per_thread * 100)));
  OverrideFlagForSlowTests(
      "client_num_batches_per_thread",
      Substitute("$0", (FLAGS_client_num_batches_per_thread * 100)));

  int num_threads = FLAGS_num_client_threads;
  int64_t total_num_rows = num_threads * FLAGS_client_inserts_per_thread;

  // We create 2 * (kNumReplicas - 1) latches so that we kill the same node at least
  // twice.
  vector<CountDownLatch*> latches;
  for (int i = 1; i < kNumElections; i++) {
    latches.push_back(new CountDownLatch((i * total_num_rows)  / kNumElections));
  }

  for (int i = 0; i < num_threads; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", Substitute("ts-test$0", i),
                                  &RaftConsensusITest::InsertTestRowsRemoteThread,
                                  this, i * FLAGS_client_inserts_per_thread,
                                  FLAGS_client_inserts_per_thread,
                                  FLAGS_client_num_batches_per_thread,
                                  latches,
                                  &new_thread));
    threads_.push_back(new_thread);
  }

  for (const auto* latch : latches) {
    NO_FATALS(cluster_->AssertNoCrashes());
    latch->Wait();
    StopOrKillLeaderAndElectNewOne();
  }

  for (const auto& thr : threads_) {
    CHECK_OK(ThreadJoiner(thr.get()).Join());
  }

  NO_FATALS(AssertAllReplicasAgree(FLAGS_client_inserts_per_thread * FLAGS_num_client_threads));
  STLDeleteElements(&latches);
}

// Regression test for KUDU-597, an issue where we could mis-order operations on
// a machine if the following sequence occurred:
//  1) Replica is a FOLLOWER
//  2) A client request hits the machine
//  3) It receives some operations from the current leader
//  4) It gets elected LEADER
// In this scenario, it would incorrectly sequence the client request's PREPARE phase
// before the operations received in step (3), even though the correct behavior would be
// to either reject them or sequence them after those operations, because the operation
// index is higher.
//
// The test works by setting up three replicas and manually hammering them with write
// requests targeting a single row. If the bug exists, then TransactionOrderVerifier
// will trigger an assertion because the prepare order and the op indexes will become
// misaligned.
TEST_F(RaftConsensusITest, TestKUDU_597) {
  FLAGS_num_replicas = 3;
  FLAGS_num_tablet_servers = 3;
  NO_FATALS(BuildAndStart());

  AtomicBool finish(false);
  for (int i = 0; i < FLAGS_num_tablet_servers; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", Substitute("ts-test$0", i),
                                  &RaftConsensusITest::StubbornlyWriteSameRowThread,
                                  this, i, &finish, &new_thread));
    threads_.push_back(new_thread);
  }

  const int num_loops = AllowSlowTests() ? 10 : 1;
  for (int i = 0; i < num_loops; i++) {
    StopOrKillLeaderAndElectNewOne();
    SleepFor(MonoDelta::FromSeconds(1));
    ASSERT_OK(CheckTabletServersAreAlive(FLAGS_num_tablet_servers));
  }

  finish.Store(true);
  for (scoped_refptr<kudu::Thread> thr : threads_) {
    CHECK_OK(ThreadJoiner(thr.get()).Join());
  }
}

// Regression test for KUDU-1775: when a replica is restarted, and the first
// request it receives from a leader results in a LMP mismatch error, the
// replica should still respond with the correct 'last_committed_idx'.
TEST_F(RaftConsensusITest, TestLMPMismatchOnRestartedReplica) {
  TServerDetails* replica_ts;
  NO_FATALS(SetupSingleReplicaTest(&replica_ts));
  auto* replica_ets = cluster_->tablet_server_by_uuid(replica_ts->uuid());

  ConsensusServiceProxy* c_proxy = CHECK_NOTNULL(replica_ts->consensus_proxy.get());
  ConsensusRequestPB req;
  ConsensusResponsePB resp;
  RpcController rpc;

  req.set_tablet_id(tablet_id_);
  req.set_dest_uuid(replica_ts->uuid());
  req.set_caller_uuid("fake_caller");
  req.set_caller_term(2);
  req.set_all_replicated_index(0);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(1, 1));

  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // Send operations 2.1 through 2.3, committing through 2.2.
  AddOp(MakeOpId(2, 1), &req);
  AddOp(MakeOpId(2, 2), &req);
  AddOp(MakeOpId(2, 3), &req);
  req.set_committed_index(2);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // The COMMIT messages end up in the WAL asynchronously, so loop reading the
  // tablet server's WAL until it shows up.
  ASSERT_EVENTUALLY([&]() {
      LogVerifier lv(cluster_.get());
      OpId commit;
      ASSERT_OK(lv.ScanForHighestCommittedOpIdInLog(replica_ets, tablet_id_, &commit));
      ASSERT_EQ("2.2", OpIdToString(commit));
    });

  // Restart the replica.
  replica_ets->Shutdown();
  ASSERT_OK(replica_ets->Restart());

  // Send an operation 3.4 with preceding OpId 3.3.
  // We expect an LMP mismatch, since the replica has operation 2.3.
  // We use 'ASSERT_EVENTUALLY' here because the replica
  // may need a few retries while it's in BOOTSTRAPPING state.
  req.set_caller_term(3);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(3, 3));
  req.clear_ops();
  AddOp(MakeOpId(3, 4), &req);
  ASSERT_EVENTUALLY([&]() {
      rpc.Reset();
      ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
      ASSERT_EQ(resp.status().error().code(),
                consensus::ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH)
          << SecureDebugString(resp);
    });
  SCOPED_TRACE(SecureDebugString(resp));
  EXPECT_EQ(2, resp.status().last_committed_idx());
  EXPECT_EQ("0.0", OpIdToString(resp.status().last_received_current_leader()));
  // Even though the replica previously received operations through 2.3, the LMP mismatch
  // above causes us to truncate operation 2.3, so 2.2 remains.
  EXPECT_EQ("2.2", OpIdToString(resp.status().last_received()));
}

// Test a scenario where a replica has pending operations with lock
// dependencies on each other:
//   2.2: UPSERT row 1
//   2.3: UPSERT row 1
//   2.4: UPSERT row 1
// ...and a new leader tries to abort 2.4 in order to replace it with a new
// operation. Because the operations have a lock dependency, operation 2.4
// will be 'stuck' in the Prepare queue. This verifies that we can abort an
// operation even if it's stuck in the queue.
TEST_F(RaftConsensusITest, TestReplaceOperationStuckInPrepareQueue) {
  TServerDetails* replica_ts;
  NO_FATALS(SetupSingleReplicaTest(&replica_ts));

  ConsensusServiceProxy* c_proxy = CHECK_NOTNULL(replica_ts->consensus_proxy.get());
  ConsensusRequestPB req;
  ConsensusResponsePB resp;
  RpcController rpc;

  req.set_tablet_id(tablet_id_);
  req.set_dest_uuid(replica_ts->uuid());
  req.set_caller_uuid("fake_caller");
  req.set_caller_term(2);
  req.set_all_replicated_index(0);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(1, 1));
  AddOpWithTypeAndKey(MakeOpId(2, 2), RowOperationsPB::UPSERT, 1, &req);
  AddOpWithTypeAndKey(MakeOpId(2, 3), RowOperationsPB::UPSERT, 1, &req);
  AddOpWithTypeAndKey(MakeOpId(2, 4), RowOperationsPB::UPSERT, 1, &req);
  req.set_committed_index(2);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // Replace operation 2.4 with 3.4, add 3.5 (upsert of a new key)
  req.set_caller_term(3);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(2, 3));
  req.clear_ops();
  AddOpWithTypeAndKey(MakeOpId(3, 4), RowOperationsPB::UPSERT, 1, &req);
  AddOpWithTypeAndKey(MakeOpId(3, 5), RowOperationsPB::UPSERT, 2, &req);
  rpc.Reset();
  rpc.set_timeout(MonoDelta::FromSeconds(5));
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // Commit all ops.
  req.clear_ops();
  req.set_committed_index(5);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(3, 5));
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // Ensure we can read the data.
  // We need to ASSERT_EVENTUALLY here because otherwise it's possible to read the old value
  // of row '1', if the operation is still in flight.
  ASSERT_EVENTUALLY([&]() {
      vector<string> results;
      NO_FATALS(WaitForRowCount(replica_ts->tserver_proxy.get(), 2, &results));
      ASSERT_EQ("(int32 key=1, int32 int_val=3, string string_val=\"term: 3 index: 4\")",
                results[0]);
      ASSERT_EQ("(int32 key=2, int32 int_val=3, string string_val=\"term: 3 index: 5\")",
                results[1]);
    });
}

// Regression test for KUDU-644:
// Triggers some complicated scenarios on the replica involving aborting and
// replacing transactions.
TEST_F(RaftConsensusITest, TestReplicaBehaviorViaRPC) {
  TServerDetails* replica_ts;
  NO_FATALS(SetupSingleReplicaTest(&replica_ts));

  // Check that the 'term' metric is correctly exposed.
  {
    int64_t term_from_metric = -1;
    ASSERT_OK(GetTermMetricValue(cluster_->tablet_server_by_uuid(replica_ts->uuid()),
                                 &term_from_metric));
    ASSERT_EQ(term_from_metric, 1);
  }

  ConsensusServiceProxy* c_proxy = CHECK_NOTNULL(replica_ts->consensus_proxy.get());

  ConsensusRequestPB req;
  ConsensusResponsePB resp;
  RpcController rpc;

  // Send a simple request with no ops.
  req.set_tablet_id(tablet_id_);
  req.set_dest_uuid(replica_ts->uuid());
  req.set_caller_uuid("fake_caller");
  req.set_caller_term(2);
  req.set_all_replicated_index(0);
  req.set_committed_index(1);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(1, 1));

  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // Send some operations, but don't advance the commit index.
  // They should not commit.
  AddOp(MakeOpId(2, 2), &req);
  AddOp(MakeOpId(2, 3), &req);
  AddOp(MakeOpId(2, 4), &req);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // We shouldn't read anything yet, because the ops should be pending.
  {
    vector<string> results;
    NO_FATALS(ScanReplica(replica_ts->tserver_proxy.get(), &results));
    ASSERT_EQ(0, results.size()) << results;
  }

  // Send op 2.6, but set preceding OpId to 2.4. This is an invalid
  // request, and the replica should reject it.
  req.mutable_preceding_id()->CopyFrom(MakeOpId(2, 4));
  req.clear_ops();
  AddOp(MakeOpId(2, 6), &req);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error()) << SecureDebugString(resp);
  ASSERT_EQ(resp.error().status().message(),
            "New operation's index does not follow the previous op's index. "
            "Current: 2.6. Previous: 2.4");

  resp.Clear();
  req.clear_ops();
  // Send ops 3.5 and 2.6, then commit up to index 6, the replica
  // should fail because of the out-of-order terms.
  req.mutable_preceding_id()->CopyFrom(MakeOpId(2, 4));
  AddOp(MakeOpId(3, 5), &req);
  AddOp(MakeOpId(2, 6), &req);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error()) << SecureDebugString(resp);
  ASSERT_EQ(resp.error().status().message(),
            "New operation's term is not >= than the previous op's term."
            " Current: 2.6. Previous: 3.5");

  // Regression test for KUDU-639: if we send a valid request, but the
  // current commit index is higher than the data we're sending, we shouldn't
  // commit anything higher than the last op sent by the leader.
  //
  // To test, we re-send operation 2.3, with the correct preceding ID 2.2,
  // but we set the committed index to 2.4. This should only commit
  // 2.2 and 2.3.
  resp.Clear();
  req.clear_ops();
  req.mutable_preceding_id()->CopyFrom(MakeOpId(2, 2));
  AddOp(MakeOpId(2, 3), &req);
  req.set_committed_index(4);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);
  // Verify only 2.2 and 2.3 are committed.
  {
    vector<string> results;
    NO_FATALS(WaitForRowCount(replica_ts->tserver_proxy.get(), 2, &results));
    ASSERT_STR_CONTAINS(results[0], "term: 2 index: 2");
    ASSERT_STR_CONTAINS(results[1], "term: 2 index: 3");
  }

  resp.Clear();
  req.clear_ops();
  // Now send some more ops, and commit the earlier ones.
  req.set_committed_index(4);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(2, 4));
  AddOp(MakeOpId(2, 5), &req);
  AddOp(MakeOpId(2, 6), &req);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);

  // Verify they are committed.
  {
    vector<string> results;
    NO_FATALS(WaitForRowCount(replica_ts->tserver_proxy.get(), 3, &results));
    ASSERT_STR_CONTAINS(results[0], "term: 2 index: 2");
    ASSERT_STR_CONTAINS(results[1], "term: 2 index: 3");
    ASSERT_STR_CONTAINS(results[2], "term: 2 index: 4");
  }

  // At this point, we still have two operations which aren't committed. If we
  // try to perform a snapshot-consistent scan, we should time out rather than
  // hanging the RPC service thread.
  {
    ScanRequestPB req;
    ScanResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromMilliseconds(100));
    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(tablet_id_);
    scan->set_read_mode(READ_AT_SNAPSHOT);
    ASSERT_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

    // Send the call. We expect to get a timeout passed back from the server side
    // (i.e. not an RPC timeout)
    req.set_batch_size_bytes(0);
    SCOPED_TRACE(SecureDebugString(req));
    ASSERT_OK(replica_ts->tserver_proxy->Scan(req, &resp, &rpc));
    SCOPED_TRACE(SecureDebugString(resp));
    string err_str = StatusFromPB(resp.error().status()).ToString();
    ASSERT_STR_CONTAINS(err_str, "Timed out waiting for ts:");
    ASSERT_STR_CONTAINS(err_str, "to be safe");
  }

  resp.Clear();
  req.clear_ops();
  int leader_term = 2;
  const int kNumTerms = AllowSlowTests() ? 10000 : 100;
  while (leader_term < kNumTerms) {
    leader_term++;
    // Now pretend to be a new leader (term 3) and replace the earlier ops
    // without committing the new replacements.
    req.set_caller_term(leader_term);
    req.set_caller_uuid("new_leader");
    req.mutable_preceding_id()->CopyFrom(MakeOpId(2, 4));
    req.clear_ops();
    AddOp(MakeOpId(leader_term, 5), &req);
    AddOp(MakeOpId(leader_term, 6), &req);
    rpc.Reset();
    ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << "Req: " << SecureShortDebugString(req)
        << " Resp: " << SecureDebugString(resp);
  }

  // Send an empty request from the newest term which should commit
  // the earlier ops.
  {
    req.mutable_preceding_id()->CopyFrom(MakeOpId(leader_term, 6));
    req.set_committed_index(6);
    req.clear_ops();
    rpc.Reset();
    ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);
  }

  // Verify the new rows are committed.
  {
    vector<string> results;
    NO_FATALS(WaitForRowCount(replica_ts->tserver_proxy.get(), 5, &results));
    SCOPED_TRACE(results);
    ASSERT_STR_CONTAINS(results[3], Substitute("term: $0 index: 5", leader_term));
    ASSERT_STR_CONTAINS(results[4], Substitute("term: $0 index: 6", leader_term));
  }
}

// Basic test of adding and removing servers from a configuration.
TEST_F(RaftConsensusITest, TestAddRemoveServer) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--master_add_server_when_underreplicated=false",
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  // Elect server 0 as leader and wait for log index 1 to propagate to all servers.
  TServerDetails* leader_tserver = tservers[0];
  const string& leader_uuid = tservers[0]->uuid();
  ASSERT_OK(StartElection(leader_tserver, tablet_id_, kTimeout));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_tserver, tablet_id_, kTimeout));

  // Make sure the server rejects removal of itself from the configuration.
  Status s = RemoveServer(leader_tserver, tablet_id_, leader_tserver, kTimeout);
  ASSERT_TRUE(s.IsInvalidArgument()) << "Should not be able to remove self from config: "
                                     << s.ToString();

  // Insert the row that we will update throughout the test.
  ASSERT_OK(WriteSimpleTestRow(leader_tserver, tablet_id_, RowOperationsPB::INSERT,
                               kTestRowKey, kTestRowIntVal, "initial insert", kTimeout));

  // Kill the master, so we can change the config without interference.
  cluster_->master()->Shutdown();

  TabletServerMap active_tablet_servers = tablet_servers_;

  // Do majority correctness check for 3 servers.
  NO_FATALS(AssertMajorityRequiredForElectionsAndWrites(active_tablet_servers, leader_uuid));
  OpId opid;
  ASSERT_OK(GetLastOpIdForReplica(tablet_id_, leader_tserver, consensus::RECEIVED_OPID, kTimeout,
                                  &opid));
  int64_t cur_log_index = opid.index();

  // Go from 3 tablet servers down to 1 in the configuration.
  vector<int> remove_list = { 2, 1 };
  for (int to_remove_idx : remove_list) {
    int num_servers = active_tablet_servers.size();
    LOG(INFO) << "Remove: Going from " << num_servers << " to " << num_servers - 1 << " replicas";

    TServerDetails* tserver_to_remove = tservers[to_remove_idx];
    LOG(INFO) << "Removing tserver with uuid " << tserver_to_remove->uuid();
    ASSERT_OK(RemoveServer(leader_tserver, tablet_id_, tserver_to_remove, kTimeout));
    ASSERT_EQ(1, active_tablet_servers.erase(tserver_to_remove->uuid()));
    ASSERT_OK(WaitForServersToAgree(kTimeout, active_tablet_servers, tablet_id_, ++cur_log_index));

    // Do majority correctness check for each incremental decrease.
    NO_FATALS(AssertMajorityRequiredForElectionsAndWrites(active_tablet_servers, leader_uuid));
    ASSERT_OK(GetLastOpIdForReplica(tablet_id_, leader_tserver, consensus::RECEIVED_OPID, kTimeout,
                                    &opid));
    cur_log_index = opid.index();
  }

  // Add the tablet servers back, in reverse order, going from 1 to 3 servers in the configuration.
  vector<int> add_list = { 1, 2 };
  for (int to_add_idx : add_list) {
    int num_servers = active_tablet_servers.size();
    LOG(INFO) << "Add: Going from " << num_servers << " to " << num_servers + 1 << " replicas";

    TServerDetails* tserver_to_add = tservers[to_add_idx];
    LOG(INFO) << "Adding tserver with uuid " << tserver_to_add->uuid();
    ASSERT_OK(AddServer(leader_tserver, tablet_id_, tserver_to_add,
                        RaftPeerPB::VOTER, kTimeout));
    InsertOrDie(&active_tablet_servers, tserver_to_add->uuid(), tserver_to_add);
    ASSERT_OK(WaitForServersToAgree(kTimeout, active_tablet_servers, tablet_id_, ++cur_log_index));

    // Do majority correctness check for each incremental increase.
    NO_FATALS(AssertMajorityRequiredForElectionsAndWrites(active_tablet_servers, leader_uuid));
    ASSERT_OK(GetLastOpIdForReplica(tablet_id_, leader_tserver, consensus::RECEIVED_OPID, kTimeout,
                                    &opid));
    cur_log_index = opid.index();
  }
}

// Regression test for KUDU-1169: a crash when a Config Change operation is replaced
// by a later leader.
TEST_F(RaftConsensusITest, TestReplaceChangeConfigOperation) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--master_add_server_when_underreplicated=false",
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());


  // Elect server 0 as leader and wait for log index 1 to propagate to all servers.
  TServerDetails* leader_tserver = tservers[0];

  TabletServerMap original_followers = tablet_servers_;
  ASSERT_EQ(1, original_followers.erase(leader_tserver->uuid()));


  ASSERT_OK(StartElection(leader_tserver, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 1));

  // Shut down servers 1 and 2, so that server 0 can't replicate anything.
  cluster_->tablet_server_by_uuid(tservers[1]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(tservers[2]->uuid())->Shutdown();

  // Now try to replicate a ChangeConfig operation. This should get stuck and time out
  // because the server can't replicate any operations.
  Status s = RemoveServer(leader_tserver, tablet_id_, tservers[1],
                          MonoDelta::FromSeconds(1), -1);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // Pause the leader, and restart the other servers.
  ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[0]->uuid())->Pause());
  ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[1]->uuid())->Restart());
  ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[2]->uuid())->Restart());

  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), original_followers, tablet_id_, 1));

  // Elect one of the other servers.
  ASSERT_OK(StartElection(tservers[1], tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitUntilLeader(tservers[1], tablet_id_, MonoDelta::FromSeconds(10)));
  leader_tserver = tservers[1];

  // Resume the original leader. Its change-config operation will now be aborted
  // since it was never replicated to the majority, and the new leader will have
  // replaced the operation.
  ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[0]->uuid())->Resume());

  // Insert some data and verify that it propagates to all servers.
  NO_FATALS(InsertTestRowsRemoteThread(0, 10, 1, vector<CountDownLatch*>()));
  NO_FATALS(AssertAllReplicasAgree(10));

  // Try another config change.
  // This acts as a regression test for KUDU-1338, in which aborting the original
  // config change didn't properly unset the 'pending' configuration.
  ASSERT_OK(RemoveServer(leader_tserver, tablet_id_, tservers[2],
                         MonoDelta::FromSeconds(5), -1));
  NO_FATALS(InsertTestRowsRemoteThread(10, 10, 1, vector<CountDownLatch*>()));
}

// Test the atomic CAS arguments to ChangeConfig() add server and remove server.
TEST_F(RaftConsensusITest, TestAtomicAddRemoveServer) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--master_add_server_when_underreplicated=false",
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  // Elect server 0 as leader and wait for log index 1 to propagate to all servers.
  TServerDetails* leader_tserver = tservers[0];
  ASSERT_OK(StartElection(leader_tserver, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_tserver, tablet_id_,
                                          MonoDelta::FromSeconds(10)));
  int64_t cur_log_index = 1;

  TabletServerMap active_tablet_servers = tablet_servers_;

  TServerDetails* follower_ts = tservers[2];

  // Initial committed config should have opid_index == -1.
  // Server should reject request to change config from opid other than this.
  TabletServerErrorPB::Code error_code;
  Status s = RemoveServer(leader_tserver, tablet_id_, follower_ts,
                          MonoDelta::FromSeconds(10), 7, &error_code);
  ASSERT_EQ(TabletServerErrorPB::CAS_FAILED, error_code);
  ASSERT_STR_CONTAINS(s.ToString(), "of 7 but the committed config has opid_index of -1");

  // Specifying the correct committed opid index should work.
  int64_t committed_opid_index = -1;
  ASSERT_OK(RemoveServer(leader_tserver, tablet_id_, follower_ts,
                         MonoDelta::FromSeconds(10), committed_opid_index));

  ASSERT_EQ(1, active_tablet_servers.erase(follower_ts->uuid()));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10),
                                  active_tablet_servers, tablet_id_, ++cur_log_index));

  // Now, add the server back. Again, specifying something other than the
  // latest committed_opid_index should fail.
  s = AddServer(leader_tserver, tablet_id_, follower_ts, RaftPeerPB::VOTER,
                MonoDelta::FromSeconds(10), {}, -1, &error_code);
  ASSERT_EQ(TabletServerErrorPB::CAS_FAILED, error_code);
  ASSERT_STR_CONTAINS(s.ToString(), "of -1 but the committed config has opid_index of 2");

  // Specifying the correct committed opid index should work.
  // The previous config change op is the latest entry in the log.
  committed_opid_index = cur_log_index;
  ASSERT_OK(AddServer(leader_tserver, tablet_id_, follower_ts, RaftPeerPB::VOTER,
                      MonoDelta::FromSeconds(10), {}, committed_opid_index));

  InsertOrDie(&active_tablet_servers, follower_ts->uuid(), follower_ts);
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10),
                                  active_tablet_servers, tablet_id_, ++cur_log_index));
}

// Writes test rows in ascending order to a single tablet server.
// Essentially a poor-man's version of TestWorkload that only operates on a
// single tablet. Does not batch, does not tolerate timeouts, and does not
// interact with the Master. 'rows_inserted' is used to determine row id and is
// incremented prior to each successful insert. Since a write failure results in
// a crash, as long as there is no crash then 'rows_inserted' will have a
// correct count at the end of the run.
// Crashes on any failure, so 'write_timeout' should be high.
static void DoWriteTestRows(const TServerDetails* leader_tserver,
                            const string& tablet_id,
                            const MonoDelta& write_timeout,
                            AtomicInt<int32_t>* rows_inserted,
                            const AtomicBool* finish) {
  while (!finish->Load()) {
    int row_key = rows_inserted->Increment();
    CHECK_OK(WriteSimpleTestRow(leader_tserver, tablet_id, RowOperationsPB::INSERT,
                                row_key, row_key, Substitute("key=$0", row_key),
                                write_timeout));
  }
}

// Test that config change works while running a workload.
TEST_F(RaftConsensusITest, TestConfigChangeUnderLoad) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--master_add_server_when_underreplicated=false",
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  // Elect server 0 as leader and wait for log index 1 to propagate to all servers.
  TServerDetails* leader_tserver = tservers[0];
  ASSERT_OK(StartElection(leader_tserver, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 1));

  TabletServerMap active_tablet_servers = tablet_servers_;

  // Start a write workload.
  LOG(INFO) << "Starting write workload...";
  vector<scoped_refptr<Thread> > threads;
  AtomicInt<int32_t> rows_inserted(0);
  AtomicBool finish(false);
  int num_threads = FLAGS_num_client_threads;
  for (int i = 0; i < num_threads; i++) {
    scoped_refptr<Thread> thread;
    ASSERT_OK(Thread::Create(CURRENT_TEST_NAME(), Substitute("row-writer-$0", i),
                             &DoWriteTestRows,
                             leader_tserver, tablet_id_, MonoDelta::FromSeconds(10),
                             &rows_inserted, &finish,
                             &thread));
    threads.push_back(thread);
  }

  LOG(INFO) << "Removing servers...";
  // Go from 3 tablet servers down to 1 in the configuration.
  vector<int> remove_list = { 2, 1 };
  for (int to_remove_idx : remove_list) {
    int num_servers = active_tablet_servers.size();
    LOG(INFO) << "Remove: Going from " << num_servers << " to " << num_servers - 1 << " replicas";

    TServerDetails* tserver_to_remove = tservers[to_remove_idx];
    LOG(INFO) << "Removing tserver with uuid " << tserver_to_remove->uuid();
    ASSERT_OK(RemoveServer(leader_tserver, tablet_id_, tserver_to_remove,
                           MonoDelta::FromSeconds(10)));
    ASSERT_EQ(1, active_tablet_servers.erase(tserver_to_remove->uuid()));
    ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                  leader_tserver, tablet_id_,
                                                  MonoDelta::FromSeconds(10)));
  }

  LOG(INFO) << "Adding servers...";
  // Add the tablet servers back, in reverse order, going from 1 to 3 servers in the configuration.
  vector<int> add_list = { 1, 2 };
  for (int to_add_idx : add_list) {
    int num_servers = active_tablet_servers.size();
    LOG(INFO) << "Add: Going from " << num_servers << " to " << num_servers + 1 << " replicas";

    TServerDetails* tserver_to_add = tservers[to_add_idx];
    LOG(INFO) << "Adding tserver with uuid " << tserver_to_add->uuid();
    ASSERT_OK(AddServer(leader_tserver, tablet_id_, tserver_to_add,
                        RaftPeerPB::VOTER, MonoDelta::FromSeconds(10)));
    InsertOrDie(&active_tablet_servers, tserver_to_add->uuid(), tserver_to_add);
    ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                  leader_tserver, tablet_id_,
                                                  MonoDelta::FromSeconds(10)));
  }

  LOG(INFO) << "Joining writer threads...";
  finish.Store(true);
  for (const scoped_refptr<Thread>& thread : threads) {
    ASSERT_OK(ThreadJoiner(thread.get()).Join());
  }

  LOG(INFO) << "Waiting for replicas to agree...";
  // Wait for all servers to replicate everything up through the last write op.
  // Since we don't batch, there should be at least # rows inserted log entries,
  // plus the initial leader's no-op, plus 2 for the removed servers, plus 2 for
  // the added servers for a total of 5.
  int min_log_index = rows_inserted.Load() + 5;
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10),
                                  active_tablet_servers, tablet_id_,
                                  min_log_index));

  LOG(INFO) << "Number of rows inserted: " << rows_inserted.Load();
  NO_FATALS(AssertAllReplicasAgree(rows_inserted.Load()));
}

TEST_F(RaftConsensusITest, TestMasterNotifiedOnConfigChange) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  const vector<string> kMasterFlags = {
    "--master_add_server_when_underreplicated=false",
    "--allow_unsafe_replication_factor=true",
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 2;
  NO_FATALS(BuildAndStart({}, kMasterFlags));

  LOG(INFO) << "Finding tablet leader and waiting for things to start...";
  string tablet_id = tablet_replicas_.begin()->first;

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  for (itest::TabletReplicaMap::const_iterator iter = tablet_replicas_.find(tablet_id);
       iter != tablet_replicas_.end(); ++iter) {
    InsertOrDie(&active_tablet_servers, iter->second->uuid(), iter->second);
  }

  // Determine the server to add to the config.
  string uuid_to_add;
  for (const TabletServerMap::value_type& entry : tablet_servers_) {
    if (!ContainsKey(active_tablet_servers, entry.second->uuid())) {
      uuid_to_add = entry.second->uuid();
    }
  }
  ASSERT_FALSE(uuid_to_add.empty());

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            2, tablet_id, timeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(tablet_servers_, tablet_id, timeout, &leader_ts));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id, timeout));

  // Change the config.
  TServerDetails* tserver_to_add = tablet_servers_[uuid_to_add];
  LOG(INFO) << "Adding tserver with uuid " << tserver_to_add->uuid();
  ASSERT_OK(AddServer(leader_ts, tablet_id_, tserver_to_add, RaftPeerPB::VOTER,
                      timeout));
  ASSERT_OK(WaitForServersToAgree(timeout, tablet_servers_, tablet_id_, 2));

  // Wait for the master to be notified of the config change.
  // It should continue to have the same leader, even without waiting.
  LOG(INFO) << "Waiting for Master to see config change...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id, timeout,
                                            DONT_WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);

  // Change the config again.
  LOG(INFO) << "Removing tserver with uuid " << tserver_to_add->uuid();
  ASSERT_OK(RemoveServer(leader_ts, tablet_id_, tserver_to_add, timeout));
  active_tablet_servers = tablet_servers_;
  ASSERT_EQ(1, active_tablet_servers.erase(tserver_to_add->uuid()));
  ASSERT_OK(WaitForServersToAgree(timeout, active_tablet_servers, tablet_id_, 3));

  // Wait for the master to be notified of the removal.
  LOG(INFO) << "Waiting for Master to see config change...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            2, tablet_id, timeout,
                                            DONT_WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
}

// Test that even with memory pressure, a replica will still commit pending
// operations that the leader has committed.
TEST_F(RaftConsensusITest, TestEarlyCommitDespiteMemoryPressure) {
  // Enough operations to put us over our memory limit (defined below).
  const int kNumOps = 10000;

  // Set up a 3-node configuration with only one live follower so that we can
  // manipulate it directly via RPC.
  const vector<string> kTsFlags = {
    // Very low memory limit to ease testing.
    // When using tcmalloc, we set it to 30MB, since we can get accurate process memory
    // usage statistics. Otherwise, set to only 4MB, since we'll only be throttling based
    // on our tracked memory.
#ifdef TCMALLOC_ENABLED
    "--memory_limit_hard_bytes=30000000",
#else
    "--memory_limit_hard_bytes=4194304",
#endif
    "--enable_leader_failure_detection=false",
    // Don't let transaction memory tracking get in the way.
    "--tablet_transaction_memory_limit_mb=-1",
  };

  // If failure detection were on, a follower could be elected as leader after
  // we kill the leader below.
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  // Elect server 2 as leader, then kill it and server 1, leaving behind
  // server 0 as the sole follower.
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());
  ASSERT_OK(StartElection(tservers[2], tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 1));
  TServerDetails *replica_ts = tservers[0];
  cluster_->tablet_server_by_uuid(tservers[1]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(tservers[2]->uuid())->Shutdown();

  // Pretend to be the leader and send a request to replicate some operations.
  ConsensusRequestPB req;
  ConsensusResponsePB resp;
  RpcController rpc;
  req.set_dest_uuid(replica_ts->uuid());
  req.set_tablet_id(tablet_id_);
  req.set_caller_uuid(tservers[2]->instance_id.permanent_uuid());
  req.set_caller_term(1);
  req.set_committed_index(1);
  req.set_all_replicated_index(0);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(1, 1));
  for (int i = 0; i < kNumOps; i++) {
    AddOp(MakeOpId(1, 2 + i), &req);
  }
  OpId last_opid = MakeOpId(1, 2 + kNumOps - 1);
  ASSERT_OK(replica_ts->consensus_proxy->UpdateConsensus(req, &resp, &rpc));

  // At the time that the follower received our request it was still under the
  // tiny memory limit defined above, so the request should have succeeded.
  ASSERT_FALSE(resp.has_error()) << SecureDebugString(resp);
  ASSERT_TRUE(resp.has_status());
  ASSERT_TRUE(resp.status().has_last_committed_idx());
  ASSERT_EQ(last_opid.index(), resp.status().last_received().index());
  ASSERT_EQ(1, resp.status().last_committed_idx());

  // But no operations have been applied yet; there should be no data.
  vector<string> rows;
  WaitForRowCount(replica_ts->tserver_proxy.get(), 0, &rows);

  // Try again, but this time:
  // 1. Replicate just one new operation.
  // 2. Tell the follower that the previous set of operations were committed.
  req.mutable_preceding_id()->CopyFrom(last_opid);
  req.set_committed_index(last_opid.index());
  req.mutable_ops()->Clear();
  AddOp(MakeOpId(1, last_opid.index() + 1), &req);
  rpc.Reset();
  Status s = replica_ts->consensus_proxy->UpdateConsensus(req, &resp, &rpc);

  // Our memory limit was truly tiny, so we should be over it by now...
  ASSERT_TRUE(s.IsRemoteError());
  ASSERT_STR_CONTAINS(s.ToString(), "Soft memory limit exceeded");

  // ...but despite rejecting the request, we should have committed the
  // previous set of operations. That is, we should be able to see those rows.
  WaitForRowCount(replica_ts->tserver_proxy.get(), kNumOps, &rows);
}

// Test that we can create (vivify) a new tablet via tablet copy.
TEST_F(RaftConsensusITest, TestAutoCreateReplica) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
    "--log_cache_size_limit_mb=1",
    "--log_segment_size_mb=1",
    "--log_async_preallocate_segments=false",
    "--flush_threshold_mb=1",
    "--maintenance_manager_polling_interval_ms=300",
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
    "--allow_unsafe_replication_factor=true",
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 2;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  // 50K is enough to cause flushes & log rolls.
  const int num_rows_to_write = AllowSlowTests() ? 150000 : 50000;

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
  workload.set_num_replicas(FLAGS_num_replicas);
  workload.set_num_write_threads(10);
  workload.set_num_read_threads(2);
  workload.set_write_batch_size(100);
  workload.Setup();

  LOG(INFO) << "Starting write workload...";
  workload.Start();

  while (true) {
    int rows_inserted = workload.rows_inserted();
    if (rows_inserted >= num_rows_to_write) {
      break;
    }
    LOG(INFO) << "Only inserted " << rows_inserted << " rows so far, sleeping for 100ms";
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  LOG(INFO) << "Adding tserver with uuid " << new_node->uuid() << " as VOTER...";
  ASSERT_OK(AddServer(leader, tablet_id_, new_node, RaftPeerPB::VOTER,
                      MonoDelta::FromSeconds(10)));
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
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120),
                                  active_tablet_servers, tablet_id_,
                                  min_log_index));

  int rows_inserted = workload.rows_inserted();
  LOG(INFO) << "Number of rows inserted: " << rows_inserted;
  NO_FATALS(AssertAllReplicasAgree(rows_inserted));
}

TEST_F(RaftConsensusITest, TestMemoryRemainsConstantDespiteTwoDeadFollowers) {
  const int64_t kMinRejections = 100;
  const MonoDelta kMaxWaitTime = MonoDelta::FromSeconds(60);

  // Start the cluster with a low per-tablet transaction memory limit, so that
  // the test can complete faster.
  NO_FATALS(BuildAndStart({ "--tablet_transaction_memory_limit_mb=2" }));

  // Kill both followers.
  TServerDetails* details;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &details));
  int num_shutdown = 0;
  int leader_ts_idx = -1;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ExternalTabletServer* ts = cluster_->tablet_server(i);
    if (ts->instance_id().permanent_uuid() != details->uuid()) {
      ts->Shutdown();
      num_shutdown++;
    } else {
      leader_ts_idx = i;
    }
  }
  ASSERT_EQ(2, num_shutdown);
  ASSERT_NE(-1, leader_ts_idx);

  // Because the majority of the cluster is dead and because of this workload's
  // timeout behavior, more and more wedged transactions will accumulate in the
  // leader. To prevent memory usage from skyrocketing, the leader will
  // eventually reject new transactions. That's what we're testing for here.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.set_write_timeout_millis(150);
  workload.set_payload_bytes(30000);
  workload.Setup();
  workload.Start();

  // Run until the leader has rejected several transactions.
  MonoTime deadline = MonoTime::Now() + kMaxWaitTime;
  while (true) {
    int64_t num_rejections = 0;
    ASSERT_OK(GetInt64Metric(
        cluster_->tablet_server(leader_ts_idx)->bound_http_hostport(),
        &METRIC_ENTITY_tablet,
        nullptr,
        &METRIC_transaction_memory_pressure_rejections,
        "value",
        &num_rejections));
    if (num_rejections >= kMinRejections) {
      break;
    } else if (deadline < MonoTime::Now()) {
      FAIL() << "Ran for " << kMaxWaitTime.ToString() << ", deadline expired";
    }
    SleepFor(MonoDelta::FromMilliseconds(200));
  }
}

static void EnableLogLatency(server::GenericServiceProxy* proxy) {
  const unordered_map<string, string> kFlags = {
    { "log_inject_latency",         "true" },
    { "log_inject_latency_ms_mean", "1000" },
  };
  for (const auto& e : kFlags) {
    SetFlagRequestPB req;
    req.set_flag(e.first);
    req.set_value(e.second);
    SetFlagResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy->SetFlag(req, &resp, &rpc));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_EQ(SetFlagResponsePB::SUCCESS, resp.result());
  }
}

// Run a regular workload with a leader that's writing to its WAL slowly.
TEST_F(RaftConsensusITest, TestSlowLeader) {
  if (!AllowSlowTests()) return;

  NO_FATALS(BuildAndStart());

  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
  NO_FATALS(EnableLogLatency(leader->generic_proxy.get()));

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_num_read_threads(2);
  workload.Setup();
  workload.Start();
  SleepFor(MonoDelta::FromSeconds(60));
}

// Test write batches just below the maximum limit.
TEST_F(RaftConsensusITest, TestLargeBatches) {
  const vector<string> kTsFlags = {
    // We write 128KB cells in this test, so bump the limit, and disable compression.
    "--max_cell_size_bytes=1000000",
    "--log_segment_size_mb=1",
    "--log_compression_codec=none",
    "--log_min_segments_to_retain=100", // disable GC of logs.
  };

  NO_FATALS(BuildAndStart(kTsFlags));

  const int64_t kBatchSize = 40; // Write 40 * 128kb = 5MB per batch.
  const int64_t kNumBatchesToWrite = 100;
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_payload_bytes(128 * 1024); // Write ops of size 128KB.
  workload.set_write_batch_size(kBatchSize);
  workload.set_num_write_threads(1);
  workload.Setup();
  workload.Start();
  LOG(INFO) << "Waiting until we've written enough data...";
  while (workload.rows_inserted() < kBatchSize * kNumBatchesToWrite) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  workload.StopAndJoin();

  // Verify replication.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(),
                            ClusterVerifier::EXACTLY,
                            workload.rows_inserted()));

  int num_wals = inspect_->CountFilesInWALDirForTS(0, tablet_id_, "wal-*");
  int num_batches = workload.rows_inserted() / kBatchSize;
  // The number of WALs should be similar to 'num_batches'. We can't make
  // an exact assertion because async preallocation may take a small amount
  // of time, in which case it's possible to put more than one batch in a
  // single WAL.
  ASSERT_GE(num_wals, num_batches / 2);
  ASSERT_LE(num_wals, num_batches + 2);
}


// Regression test for KUDU-1469, a case in which a leader and follower could get "stuck"
// in a tight RPC loop, in which the leader would repeatedly send a batch of ops that the
// follower already had, the follower would fully de-dupe them, and yet the leader would
// never advance to the next batch.
//
// The 'perfect storm' reproduced here consists of:
// - the commit index has fallen far behind due to a slow log on the leader
//   and one of the three replicas being inaccessible
// - the other replica elects itself
// - before the old leader notices it has been ousted, it writes at least one more
//   operation to its local log.
// - before the replica can replicate anything to the old leader, it receives
//   more writes, such that the first batch's preceding_op_id is ahead of
//   the old leader's last written
//
// See the detailed comments below for more details.
TEST_F(RaftConsensusITest, TestCommitIndexFarBehindAfterLeaderElection) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);

  if (!AllowSlowTests()) return;

  // Set the batch size low so that, after the new leader takes
  // over below, the ops required to catch up from the committed index
  // to the newly replicated index don't fit into a single batch.
  NO_FATALS(BuildAndStart({"--consensus_max_batch_size_bytes=50000"}));

  // Get the leader and the two replica tablet servers.
  // These will have the following roles in this test:
  // 1) 'first_leader_ts' is the initial leader.
  // 2) 'second_leader_ts' will be forced to be elected as the second leader
  // 3) 'only_vote_ts' will simulate a heavily overloaded (or corrupted) TS
  //     which is far enough behind (or failed) such that it only participates
  //     by voting.
  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
  ExternalTabletServer* first_leader_ts = cluster_->tablet_server_by_uuid(leader->uuid());
  ExternalTabletServer* second_leader_ts = nullptr;
  ExternalTabletServer* only_vote_ts = nullptr;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ExternalTabletServer* ts = cluster_->tablet_server(i);
    if (ts->instance_id().permanent_uuid() != leader->uuid()) {
      if (second_leader_ts == nullptr) {
        second_leader_ts = ts;
      } else {
        only_vote_ts = ts;
      }
    }
  }

  // The 'only_vote' tablet server doesn't participate in replication.
  ASSERT_OK(cluster_->SetFlag(only_vote_ts, "follower_reject_update_consensus_requests", "true"));

  // Inject a long delay in the log of the first leader, and write 10 operations.
  // This delay ensures that it will replicate them to both itself and its follower,
  // but due to its log sync not completing, it won't know that it is safe to advance its
  // commit index until long after it has lost its leadership.
  ASSERT_OK(cluster_->SetFlag(first_leader_ts, "log_inject_latency_ms_mean", "6000"));
  ASSERT_OK(cluster_->SetFlag(first_leader_ts, "log_inject_latency", "true"));
  InsertPayloadIgnoreErrors(0, 10, 10000);

  // Write one more operation to the leader, but disable consensus on the follower so that
  // it doesn't get replicated.
  ASSERT_OK(cluster_->SetFlag(
      second_leader_ts, "follower_reject_update_consensus_requests", "true"));
  InsertPayloadIgnoreErrors(10, 1, 10000);

  // Pause the initial leader and wait for the replica to elect itself. The third TS participates
  // here by voting.
  ASSERT_OK(first_leader_ts->Pause());
  ASSERT_OK(WaitUntilLeader(tablet_servers_[second_leader_ts->uuid()], tablet_id_, kTimeout));

  // The voter TS has done its duty. Shut it down to avoid log spam where it tries to run
  // elections.
  only_vote_ts->Shutdown();

  // Perform one insert on the new leader. The new leader has not yet replicated its NO_OP to
  // the old leader, since the old leader is still paused.
  NO_FATALS(CreateClient(&client_));
  InsertPayloadIgnoreErrors(13, 1, 10000);

  // Now we expect to have the following logs:
  //
  // first_leader_ts         second_leader_ts
  // -------------------     ------------
  // 1.1  NO_OP      1.1     NO_OP
  // 1.2  WRITE_OP   1.2     WRITE_OP
  // ................................
  // 1.11 WRITE_OP   1.11    WRITE_OP
  // 1.12 WRITE_OP   2.12    NO_OP
  //                 2.13    WRITE_OP
  //
  // Both servers should have a committed_idx of 1.1 since the log was delayed.

  // Now, when we resume the original leader, we expect them to recover properly.
  // Previously this triggered KUDU-1469.
  ASSERT_OK(first_leader_ts->Resume());

  TabletServerMap active_tservers = tablet_servers_;
  active_tservers.erase(only_vote_ts->uuid());
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(60),
                                  active_tservers,
                                  tablet_id_, 13));
}

// Run a regular workload with one follower that's writing to its WAL slowly.
TEST_F(RaftConsensusITest, TestSlowFollower) {
  if (!AllowSlowTests()) return;

  NO_FATALS(BuildAndStart());

  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
  int num_reconfigured = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ExternalTabletServer* ts = cluster_->tablet_server(i);
    if (ts->instance_id().permanent_uuid() != leader->uuid()) {
      TServerDetails* follower;
      follower = GetReplicaWithUuidOrNull(tablet_id_, ts->instance_id().permanent_uuid());
      ASSERT_TRUE(follower);
      NO_FATALS(EnableLogLatency(follower->generic_proxy.get()));
      num_reconfigured++;
      break;
    }
  }
  ASSERT_EQ(1, num_reconfigured);

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_num_read_threads(2);
  workload.Setup();
  workload.Start();
  SleepFor(MonoDelta::FromSeconds(60));
}

// Run a special workload that constantly updates a single row on a cluster
// where every replica is writing to its WAL slowly.
TEST_F(RaftConsensusITest, TestHammerOneRow) {
  if (!AllowSlowTests()) return;

  NO_FATALS(BuildAndStart());

  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    const ExternalTabletServer* ts = cluster_->tablet_server(i);
    const TServerDetails* replica = GetReplicaWithUuidOrNull(
        tablet_id_, ts->instance_id().permanent_uuid());
    ASSERT_NE(nullptr, replica);
    NO_FATALS(EnableLogLatency(replica->generic_proxy.get()));
  }

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_write_pattern(TestWorkload::UPDATE_ONE_ROW);
  workload.set_write_timeout_millis(60000);
  workload.set_num_write_threads(20);
  workload.Setup();
  workload.Start();
  SleepFor(MonoDelta::FromSeconds(60));
  workload.StopAndJoin();

  // Ensure that the replicas converge.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
}

// Test that followers that fall behind the leader's log GC threshold are
// evicted from the config.
TEST_F(RaftConsensusITest, TestEvictAbandonedFollowers) {
  vector<string> ts_flags;
  AddFlagsForLogRolls(&ts_flags); // For CauseFollowerToFallBehindLogGC().
  vector<string> master_flags = {
    "--master_add_server_when_underreplicated=false",
  };

  NO_FATALS(BuildAndStart(ts_flags, master_flags));

  MonoDelta timeout = MonoDelta::FromSeconds(30);
  TabletServerMap active_tablet_servers = tablet_servers_;
  ASSERT_EQ(3, active_tablet_servers.size());

  string leader_uuid;
  int64_t orig_term;
  string follower_uuid;
  NO_FATALS(CauseFollowerToFallBehindLogGC(
      tablet_servers_, &leader_uuid, &orig_term, &follower_uuid));

  // Wait for the abandoned follower to be evicted.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(2, tablet_servers_[leader_uuid],
                                                tablet_id_, timeout));
  ASSERT_EQ(1, active_tablet_servers.erase(follower_uuid));
  ASSERT_OK(WaitForServersToAgree(timeout, active_tablet_servers, tablet_id_, 2));
}

// Test that, after followers are evicted from the config, the master re-adds a new
// replica for that follower and it eventually catches back up.
TEST_F(RaftConsensusITest, TestMasterReplacesEvictedFollowers) {
  vector<string> ts_flags;
  AddFlagsForLogRolls(&ts_flags); // For CauseFollowerToFallBehindLogGC().
  NO_FATALS(BuildAndStart(ts_flags));

  MonoDelta timeout = MonoDelta::FromSeconds(30);

  string leader_uuid;
  int64_t orig_term;
  string follower_uuid;
  NO_FATALS(CauseFollowerToFallBehindLogGC(
      tablet_servers_, &leader_uuid, &orig_term, &follower_uuid));

  // The follower will be evicted. Now wait for the master to cause it to be
  // copied.
  ASSERT_OK(WaitForServersToAgree(timeout, tablet_servers_, tablet_id_, 2));

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(kTableId, ClusterVerifier::AT_LEAST, 1));
}

// Test that a ChangeConfig() request is rejected unless the leader has
// replicated one of its own log entries during the current term.
// This is required for correctness of Raft config change. For details,
// see https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
TEST_F(RaftConsensusITest, TestChangeConfigRejectedUnlessNoopReplicated) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  MonoDelta timeout = MonoDelta::FromSeconds(30);

  int kLeaderIndex = 0;
  TServerDetails* leader_ts = tablet_servers_[cluster_->tablet_server(kLeaderIndex)->uuid()];

  // Prevent followers from accepting UpdateConsensus requests from the leader,
  // even though they will vote. This will allow us to get the distributed
  // system into a state where there is a valid leader (based on winning an
  // election) but that leader will be unable to commit any entries from its
  // own term, making it illegal to accept ChangeConfig() requests.
  for (int i = 1; i <= 2; i++) {
    ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i),
              "follower_reject_update_consensus_requests", "true"));
  }

  // Elect the leader.
  ASSERT_OK(StartElection(leader_ts, tablet_id_, timeout));
  ASSERT_OK(WaitUntilLeader(leader_ts, tablet_id_, timeout));

  // Now attempt to do a config change. It should be rejected because there
  // have not been any ops (notably the initial NO_OP) from the leader's term
  // that have been committed yet.
  Status s = RemoveServer(leader_ts, tablet_id_,
                          tablet_servers_[cluster_->tablet_server(1)->uuid()],
                          timeout);
  ASSERT_TRUE(!s.ok()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Leader has not yet committed an operation in its own term");
}

// Regression test for KUDU-1735, a crash in the case where a pending
// config change operation is aborted during tablet deletion when that config change
// was in fact already persisted to disk.
TEST_F(RaftConsensusITest, Test_KUDU_1735) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  vector<ExternalTabletServer*> external_tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  for (TServerDetails* ts : tservers) {
    external_tservers.push_back(cluster_->tablet_server_by_uuid(ts->uuid()));
  }

  // Elect server 0 as leader and wait for log index 1 to propagate to all servers.
  TServerDetails* leader_tserver = tservers[0];
  ASSERT_OK(StartElection(leader_tserver, tablet_id_, kTimeout));
  ASSERT_OK(WaitUntilLeader(leader_tserver, tablet_id_, kTimeout));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_,
                                  tablet_id_, 1));

  // Make follower tablet servers crash before writing a commit message.
  for (int i = 1; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(cluster_->SetFlag(external_tservers[i], "fault_crash_before_append_commit", "1.0"));
  }

  // Run a config change. This will cause the other servers to crash with pending config
  // change operations due to the above fault injection.
  ASSERT_OK(RemoveServer(leader_tserver, tablet_id_, tservers[1], kTimeout));
  for (int i = 1; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(external_tservers[i]->WaitForInjectedCrash(kTimeout));
  }

  // Delete the table, so that when we restart the crashed servers, they'll get RPCs to
  // delete tablets while config changes are pending.
  ASSERT_OK(client_->DeleteTable(kTableId));

  // Restart the crashed tservers and wait for them to delete their replicas.
  for (int i = 1; i < cluster_->num_tablet_servers(); i++) {
    auto* ts = external_tservers[i];
    ts->Shutdown();
    ASSERT_OK(ts->Restart());
    ASSERT_OK(WaitForNumTabletsOnTS(tservers[i], 0, kTimeout, nullptr));
  }
}

// Test that if for some reason none of the transactions can be prepared, that it will come
// back as an error in UpdateConsensus().
TEST_F(RaftConsensusITest, TestUpdateConsensusErrorNonePrepared) {
  const int kNumOps = 10;
  vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());

  // Shutdown the other servers so they don't get chatty.
  cluster_->tablet_server_by_uuid(tservers[1]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(tservers[2]->uuid())->Shutdown();

  // Configure the first server to fail all on prepare.
  TServerDetails *replica_ts = tservers[0];
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server_by_uuid(replica_ts->uuid()),
                "follower_fail_all_prepare", "true"));

  // Pretend to be the leader and send a request that should return an error.
  ConsensusRequestPB req;
  ConsensusResponsePB resp;
  RpcController rpc;
  req.set_dest_uuid(replica_ts->uuid());
  req.set_tablet_id(tablet_id_);
  req.set_caller_uuid(tservers[2]->instance_id.permanent_uuid());
  req.set_caller_term(0);
  req.set_committed_index(0);
  req.set_all_replicated_index(0);
  req.mutable_preceding_id()->CopyFrom(MakeOpId(0, 0));
  for (int i = 0; i < kNumOps; i++) {
    AddOp(MakeOpId(0, 1 + i), &req);
  }

  ASSERT_OK(replica_ts->consensus_proxy->UpdateConsensus(req, &resp, &rpc));
  LOG(INFO) << SecureShortDebugString(resp);
  ASSERT_TRUE(resp.status().has_error());
  ASSERT_EQ(consensus::ConsensusErrorPB::CANNOT_PREPARE, resp.status().error().code());
  ASSERT_STR_CONTAINS(SecureShortDebugString(resp), "Could not prepare a single transaction");
}

// Test that, if the raft metadata on a replica is corrupt, then the server
// doesn't crash, but instead marks the tablet as failed.
TEST_F(RaftConsensusITest, TestCorruptReplicaMetadata) {
  // Start cluster and wait until we have a stable leader.
  // Switch off tombstoning of evicted replicas to observe the failed tablet state.
  NO_FATALS(BuildAndStart({ "--consensus_rpc_timeout_ms=10000" }, // Ensure we are safe to evict.
                          { "--master_tombstone_evicted_tablet_replicas=false" }));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_,
                                  tablet_id_, 1));

  // Shut down one of the tablet servers, and then muck
  // with its consensus metadata to corrupt it.
  auto* ts = cluster_->tablet_server(0);
  ts->Shutdown();
  consensus::ConsensusMetadataPB cmeta_pb;
  ASSERT_OK(inspect_->ReadConsensusMetadataOnTS(0, tablet_id_, &cmeta_pb));
  cmeta_pb.set_current_term(cmeta_pb.current_term() - 1);
  ASSERT_OK(inspect_->WriteConsensusMetadataOnTS(0, tablet_id_, cmeta_pb));

  ASSERT_OK(ts->Restart());

  // The server should come up with a 'FAILED' status because of the corrupt
  // metadata.
  ASSERT_OK(WaitUntilTabletInState(tablet_servers_[ts->uuid()],
                                   tablet_id_,
                                   tablet::FAILED,
                                   MonoDelta::FromSeconds(30)));

  // Switch on deletion of evicted replicas (this is default behavior) to ensure
  // the tablet is reassigned.
  ASSERT_OK(cluster_->SetFlag(cluster_->master(0),
      "master_tombstone_evicted_tablet_replicas", "true"));

  // A new good copy should get created automatically.
  ASSERT_OK(WaitUntilTabletInState(tablet_servers_[ts->uuid()],
                                   tablet_id_,
                                   tablet::RUNNING,
                                   MonoDelta::FromSeconds(30)));
}

// Test that an IOError when writing to the write-ahead log is a fatal error.
// First, we test that failed replicates are fatal. Then, we test that failed
// commits are fatal.
TEST_F(RaftConsensusITest, TestLogIOErrorIsFatal) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
    // Disable core dumps since we will inject FATAL errors, and dumping
    // core can take a long time.
    "--disable_core_dumps",
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  FLAGS_num_replicas = 3;
  FLAGS_num_tablet_servers = 3;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());
  vector<ExternalTabletServer*> ext_tservers;
  for (auto* details : tservers) {
    ext_tservers.push_back(cluster_->tablet_server_by_uuid(details->uuid()));
  }

  // Test failed replicates.

  // Elect server 2 as leader and wait for log index 1 to propagate to all servers.
  ASSERT_OK(StartElection(tservers[2], tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 1));

  // Inject an IOError the next time servers 1 and 2 write to their WAL.
  // Then, cause server 0 to start and win a leader election.
  // This will cause servers 0 and 1 to crash.
  for (int i = 1; i <= 2; i++) {
    ASSERT_OK(cluster_->SetFlag(ext_tservers[i],
              "log_inject_io_error_on_append_fraction", "1.0"));
  }
  ASSERT_OK(StartElection(tservers[0], tablet_id_, MonoDelta::FromSeconds(10)));
  for (int i = 1; i <= 2; i++) {
    ASSERT_OK(ext_tservers[i]->WaitForFatal(MonoDelta::FromSeconds(10)));
  }

  // Now we know followers crash when they write to their log.
  // Let's verify the same for the leader (server 0).
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0],
            "log_inject_io_error_on_append_fraction", "1.0"));

  // Attempt to write to the leader, but with a short timeout.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.set_write_timeout_millis(100);
  workload.set_num_write_threads(1);
  workload.set_write_batch_size(1);
  workload.Setup();
  workload.Start();

  // Leader should crash as well.
  ASSERT_OK(ext_tservers[0]->WaitForFatal(MonoDelta::FromSeconds(10)));
  workload.StopAndJoin();

  LOG(INFO) << "Everything crashed!";

  // Test failed commits.

  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());
  NO_FATALS(WaitForTSAndReplicas());
  tservers.clear();
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());

  // Elect server 0 as leader, wait until writes are going through.
  ASSERT_OK(StartElection(tservers[0], tablet_id_, MonoDelta::FromSeconds(10)));
  workload.Start();
  int64_t prev_inserted = workload.rows_inserted();
  while (workload.rows_inserted() == prev_inserted) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 1));

  // Now shutdown servers 1 and 2 so that writes cannot commit. Write to the
  // leader, set flags so that commits crash the server, then bring the
  // followers back up.
  for (int i = 1; i <= 2; i++) {
    ext_tservers[i]->Shutdown();
  }

  OpId prev_opid, cur_opid;
  ASSERT_OK(GetLastOpIdForReplica(tablet_id_, tservers[0], consensus::RECEIVED_OPID,
                                  MonoDelta::FromSeconds(10), &prev_opid));
  VLOG(1) << "Previous OpId on server 0: " << OpIdToString(prev_opid);
  workload.Start();
  // Wait until we've got (uncommitted) entries into the leader's log.
  do {
    ASSERT_OK(GetLastOpIdForReplica(tablet_id_, tservers[0], consensus::RECEIVED_OPID,
                                    MonoDelta::FromSeconds(10), &cur_opid));
    VLOG(1) << "Current OpId on server 0: " << OpIdToString(cur_opid);
  } while (consensus::OpIdEquals(prev_opid, cur_opid));
  workload.StopAndJoin();
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0],
            "log_inject_io_error_on_append_fraction", "1.0"));
  for (int i = 1; i <= 2; i++) {
    ASSERT_OK(ext_tservers[i]->Restart());
  }
  // Leader will crash.
  ASSERT_OK(ext_tservers[0]->WaitForFatal(MonoDelta::FromSeconds(10)));
}

}  // namespace tserver
}  // namespace kudu
