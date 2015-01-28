// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <boost/assign/list_of.hpp>
#include <tr1/unordered_set>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_client_threads, 8,
             "Number of client threads to launch");
DEFINE_int64(client_inserts_per_thread, 50,
             "Number of rows inserted by each client thread");
DEFINE_int64(client_num_batches_per_thread, 5,
             "In how many batches to group the rows, for each client");
DECLARE_int32(consensus_rpc_timeout_ms);

#define ASSERT_ALL_REPLICAS_AGREE(count) \
  ASSERT_NO_FATAL_FAILURE(AssertAllReplicasAgree(count))

namespace kudu {
namespace tserver {

using client::FromInternalCompressionType;
using client::FromInternalDataType;
using client::FromInternalEncodingType;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduColumnStorageAttributes;
using client::KuduInsert;
using client::KuduSchema;
using client::KuduSession;
using client::KuduTable;
using master::TableIdentifierPB;
using master::TabletLocationsPB;
using master::TSInfoPB;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using rpc::RpcController;
using std::vector;
using std::tr1::shared_ptr;
using std::tr1::unordered_set;
using strings::Substitute;
using tablet::TabletPeer;
using tserver::TabletServer;

static const int kConsensusRpcTimeoutForTests = 50;

// Integration test for the raft consensus implementation.
// Uses the whole tablet server stack with ExternalMiniCluster.
class RaftConsensusITest : public TabletServerIntegrationTestBase {
 public:
  RaftConsensusITest()
      : inserters_(FLAGS_num_client_threads) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    FLAGS_consensus_rpc_timeout_ms = kConsensusRpcTimeoutForTests;
  }

  // Starts an external cluster with 'num_replicas'. The caller can pass
  // 'non_default_flags' to specify non-default values the flags used
  // to configure the external daemons.
  void BuildAndStart(const vector<std::string>& non_default_flags) {
    CreateCluster("raft_consensus-itest-cluster", non_default_flags);
    CreateClient(&client_);
    CreateTable();
    WaitForTSAndQuorum();
    CHECK_GT(tablet_replicas_.size(), 0);
    tablet_id_ = (*tablet_replicas_.begin()).first;
  }

  void CreateClient(shared_ptr<KuduClient>* client) {
    // Connect to the cluster.
    ASSERT_STATUS_OK(KuduClientBuilder()
                     .add_master_server_addr(cluster_->master()->bound_rpc_addr().ToString())
                     .Build(client));
  }

  // Create a table with a single tablet, with 'num_replicas'.
  void CreateTable() {
    // The tests here make extensive use of server schemas, but we need
    // a client schema to create the table.
    KuduSchema client_schema(GetClientSchema(schema_));
    ASSERT_OK(client_->NewTableCreator()
             ->table_name(kTableId)
             .schema(&client_schema)
             .num_replicas(FLAGS_num_replicas)
             // NOTE: this is quite high as a timeout, but the default (5 sec) does not
             // seem to be high enough in some cases (see KUDU-550). We should remove
             // this once that ticket is addressed.
             .timeout(MonoDelta::FromSeconds(20))
             .Create());
    ASSERT_STATUS_OK(client_->OpenTable(kTableId, &table_));
  }

  KuduSchema GetClientSchema(const Schema& server_schema) const {
    std::vector<KuduColumnSchema> client_cols;
    BOOST_FOREACH(const ColumnSchema& col, server_schema.columns()) {
      CHECK_EQ(col.has_read_default(), col.has_write_default());
      if (col.has_read_default()) {
        CHECK_EQ(col.read_default_value(), col.write_default_value());
      }
      KuduColumnStorageAttributes client_attrs(
          FromInternalEncodingType(col.attributes().encoding()),
          FromInternalCompressionType(col.attributes().compression()));
      KuduColumnSchema client_col(col.name(), FromInternalDataType(col.type_info()->type()),
                                  col.is_nullable(), col.read_default_value(),
                                  client_attrs);
      client_cols.push_back(client_col);
    }
    return KuduSchema(client_cols, server_schema.num_key_columns());
  }

  void ScanReplica(TabletServerServiceProxy* replica_proxy,
                   vector<string>* results) {

    ScanRequestPB req;
    ScanResponsePB resp;
    RpcController rpc;

    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(tablet_id_);
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

    // Send the call
    {
      req.set_batch_size_bytes(0);
      SCOPED_TRACE(req.DebugString());
      ASSERT_STATUS_OK(replica_proxy->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());
    }

    if (!resp.has_more_results())
      return;

    // Drain all the rows from the scanner.
    ASSERT_NO_FATAL_FAILURE(DrainScannerToStrings(resp.scanner_id(),
                                                  schema_,
                                                  results,
                                                  replica_proxy));

    std::sort(results->begin(), results->end());
  }

  string DumpToString(TServerDetails* leader,
                      const vector<string>& leader_results,
                      TServerDetails* replica,
                      const vector<string>& replica_results) {
    string ret = strings::Substitute("Replica results did not match the leaders."
                                     "\nLeader: $0\nReplica: $1. Results size "
                                     "L: $2 R: $3",
                                     leader->ToString(),
                                     replica->ToString(),
                                     leader_results.size(),
                                     replica_results.size());

    StrAppend(&ret, "Leader Results: \n");
    BOOST_FOREACH(const string& result, leader_results) {
      StrAppend(&ret, result, "\n");
    }

    StrAppend(&ret, "Replica Results: \n");
    BOOST_FOREACH(const string& result, replica_results) {
      StrAppend(&ret, result, "\n");
    }

    return ret;
  }

  void AssertAllReplicasAgree(int expected_result_count) {

    int counter = 0;
    while (true) {
      SleepFor(MonoDelta::FromMilliseconds(10 * counter));
      vector<string> leader_results;
      vector<string> replica_results;

      TServerDetails* leader;
      CHECK_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

      vector<TServerDetails*> followers;
      GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

      ScanReplica(leader->tserver_proxy.get(), &leader_results);

      if (expected_result_count != -1 && leader_results.size() != expected_result_count) {
        if (counter >= kMaxRetries) {
          FAIL() << "LEADER result size " << leader_results.size()
              << " did not match the expected count " << expected_result_count;
        }
        counter++;
        continue;
      }

      TServerDetails* last_replica;
      bool all_replicas_matched = true;
      BOOST_FOREACH(TServerDetails* replica, followers) {
        ScanReplica(replica->tserver_proxy.get(), &replica_results);
        last_replica = replica;
        SCOPED_TRACE(DumpToString(leader, leader_results,
                                  replica, replica_results));

        if (leader_results.size() != replica_results.size()) {
          all_replicas_matched = false;
          break;
        }

        // when the result sizes match the rows must match
        // TODO handle the case where we have compactions/flushes
        // (needs post-scan sorting or FT scans)
        ASSERT_EQ(leader_results.size(), replica_results.size());
        for (int i = 0; i < leader_results.size(); i++) {
          ASSERT_EQ(leader_results[i], replica_results[i]);
        }
        replica_results.clear();
      }

      if (all_replicas_matched) {
        break;
      } else {
        if (counter >= kMaxRetries) {
          SCOPED_TRACE(DumpToString(leader, leader_results,
                                    last_replica, replica_results));
          FAIL() << "LEADER results did not match one of the replicas.";
        }
        counter++;
      }
    }
  }

  void InsertTestRowsRemoteThread(uint64_t first_row,
                                  uint64_t count,
                                  uint64_t num_batches,
                                  const vector<CountDownLatch*>& latches) {
    scoped_refptr<KuduTable> table;
    CHECK_OK(client_->OpenTable(kTableId, &table));

    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(20000);
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

    for (int i = 0; i < num_batches; i++) {
      uint64_t first_row_in_batch = first_row + (i * count / num_batches);
      uint64_t last_row_in_batch = first_row_in_batch + count / num_batches;

      for (int j = first_row_in_batch; j < last_row_in_batch; j++) {
        gscoped_ptr<KuduInsert> insert = table->NewInsert();
        KuduPartialRow* row = insert->mutable_row();
        CHECK_OK(row->SetUInt32(0, j));
        CHECK_OK(row->SetUInt32(1, j * 2));
        CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello %d", j))));
        CHECK_OK(session->Apply(insert.Pass()));
      }

      // We don't handle write idempotency yet. (i.e making sure that when a leader fails
      // writes to it that were eventually committed by the new leader but un-ackd to the
      // client are not retried), so some errors are expected.
      // It's OK as long as the errors are Status::AlreadyPresent();

      int inserted = last_row_in_batch - first_row_in_batch;

      Status s = session->Flush();
      if (PREDICT_FALSE(!s.ok())) {
        std::vector<client::KuduError*> errors;
        ElementDeleter d(&errors);
        bool overflow;
        session->GetPendingErrors(&errors, &overflow);
        CHECK(!overflow);
        BOOST_FOREACH(const client::KuduError* e, errors) {
          CHECK(e->status().IsAlreadyPresent()) << "Unexpected error: " << e->status().ToString();
        }
        inserted -= errors.size();
      }

      BOOST_FOREACH(CountDownLatch* latch, latches) {
        latch->CountDown(inserted);
      }
    }

    inserters_.CountDown();
  }

  // Brings Chaos to a MiniTabletServer by introducing random delays. Does this by
  // pausing the daemon a random amount of time.
  void DelayInjectorThread(ExternalTabletServer* tablet_server, int timeout_msec) {
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

  // Stops the current leader of the quorum, runs leader election and then brings it back.
  // Before stopping the leader this pauses all follower nodes in regular intervals so that
  // we get an increased chance of stuff being pending.
  void StopOrKillLeaderAndElectNewOne() {
    bool kill = rand() % 2 == 0;

    TServerDetails* old_leader;
    CHECK_OK(GetLeaderReplicaWithRetries(tablet_id_, &old_leader));

    vector<TServerDetails*> followers;
    GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

    BOOST_FOREACH(TServerDetails* ts, followers) {
      CHECK_OK(ts->external_ts->Pause());
      SleepFor(MonoDelta::FromMilliseconds(100));
    }

    // When all are paused also pause or kill the current leader. Since we've waited a bit
    // the old leader is likely to have operations that must be aborted.
    if (kill) {
      old_leader->external_ts->Shutdown();
    } else {
      CHECK_OK(old_leader->external_ts->Pause());
    }

    // Resume the replicas.
    BOOST_FOREACH(TServerDetails* ts, followers) {
      CHECK_OK(ts->external_ts->Resume());
    }

    // Get the new leader.
    TServerDetails* new_leader;
    CHECK_OK(GetLeaderReplicaWithRetries(tablet_id_, &new_leader));

    // Bring the old leader back.
    if (kill) {
      CHECK_OK(old_leader->external_ts->Restart());
      // Wait until we have the same number of followers.
      int initial_followers = followers.size();
      do {
        GetOnlyLiveFollowerReplicas(tablet_id_, &followers);
      } while (followers.size() < initial_followers);
    } else {
      CHECK_OK(old_leader->external_ts->Resume());
    }
  }

 protected:
  shared_ptr<KuduClient> client_;
  scoped_refptr<KuduTable> table_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;
  CountDownLatch inserters_;
  string tablet_id_;
};

// Test that we can retrieve the permanent uuid of a server running
// consensus service via RPC.
TEST_F(RaftConsensusITest, TestGetPermanentUuid) {
  BuildAndStart(vector<string>());

  QuorumPeerPB peer;
  TServerDetails* leader = NULL;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
  peer.mutable_last_known_addr()->CopyFrom(leader->registration.rpc_addresses(0));
  const string expected_uuid = leader->instance_id.permanent_uuid();

  rpc::MessengerBuilder builder("test builder");
  builder.set_num_reactors(1);
  shared_ptr<rpc::Messenger> messenger;
  ASSERT_STATUS_OK(builder.Build(&messenger));

  ASSERT_STATUS_OK(consensus::SetPermanentUuidForRemotePeer(messenger, &peer));
  ASSERT_EQ(expected_uuid, peer.permanent_uuid());
}

// TODO allow the scan to define an operation id, fetch the last id
// from the leader and then use that id to make the replica wait
// until it is done. This will avoid the sleeps below.
TEST_F(RaftConsensusITest, TestInsertAndMutateThroughConsensus) {
  BuildAndStart(vector<string>());

  int num_iters = AllowSlowTests() ? 10 : 1;

  for (int i = 0; i < num_iters; i++) {
    InsertTestRowsRemoteThread(i * FLAGS_client_inserts_per_thread,
                               FLAGS_client_inserts_per_thread,
                               FLAGS_client_num_batches_per_thread,
                               vector<CountDownLatch*>());
  }
  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * num_iters);
}

TEST_F(RaftConsensusITest, TestFailedTransaction) {
  BuildAndStart(vector<string>());

  WriteRequestPB req;
  req.set_tablet_id(tablet_id_);
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

  RowOperationsPB* data = req.mutable_row_operations();
  data->set_rows("some gibberish!");

  WriteResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));

  TServerDetails* leader = NULL;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

  ASSERT_STATUS_OK(DCHECK_NOTNULL(leader->tserver_proxy.get())->Write(req, &resp, &controller));
  ASSERT_TRUE(resp.has_error());

  // Add a proper row so that we can verify that all of the replicas continue
  // to process transactions after a failure. Additionally, this allows us to wait
  // for all of the replicas to finish processing transactions before shutting down,
  // avoiding a potential stall as we currently can't abort transactions (see KUDU-341).
  data->Clear();
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 0, 0, "original0", data);

  controller.Reset();
  controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));

  ASSERT_STATUS_OK(DCHECK_NOTNULL(leader->tserver_proxy.get())->Write(req, &resp, &controller));
  SCOPED_TRACE(resp.ShortDebugString());
  ASSERT_FALSE(resp.has_error());

  ASSERT_ALL_REPLICAS_AGREE(1);
}

// Inserts rows through consensus and also starts one delay injecting thread
// that steals consensus peer locks for a while. This is meant to test that
// even with timeouts and repeated requests consensus still works.
TEST_F(RaftConsensusITest, MultiThreadedMutateAndInsertThroughConsensus) {
  BuildAndStart(vector<string>());

  if (500 == FLAGS_client_inserts_per_thread) {
    if (AllowSlowTests()) {
      FLAGS_client_inserts_per_thread = FLAGS_client_inserts_per_thread * 10;
      FLAGS_client_num_batches_per_thread = FLAGS_client_num_batches_per_thread * 10;
    }
  }

  int num_threads = FLAGS_num_client_threads;
  for (int i = 0; i < num_threads; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", strings::Substitute("ts-test$0", i),
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
    CHECK_OK(kudu::Thread::Create("test", strings::Substitute("chaos-test$0", i),
                                  &RaftConsensusITest::DelayInjectorThread,
                                  this, cluster_->tablet_server(i),
                                  kConsensusRpcTimeoutForTests,
                                  &new_thread));
    threads_.push_back(new_thread);
  }
  BOOST_FOREACH(scoped_refptr<kudu::Thread> thr, threads_) {
   CHECK_OK(ThreadJoiner(thr.get()).Join());
  }

  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * FLAGS_num_client_threads);
}

TEST_F(RaftConsensusITest, TestInsertOnNonLeader) {
  BuildAndStart(vector<string>());

  // Manually construct a write RPC to a replica and make sure it responds
  // with the correct error code.
  WriteRequestPB req;
  WriteResponsePB resp;
  RpcController rpc;
  req.set_tablet_id(tablet_id_);
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678,
                 "hello world via RPC", req.mutable_row_operations());

  // Get the leader.
  vector<TServerDetails*> followers;
  GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

  ASSERT_STATUS_OK(followers[0]->tserver_proxy->Write(req, &resp, &rpc));
  SCOPED_TRACE(resp.DebugString());
  ASSERT_TRUE(resp.has_error());
  Status s = StatusFromPB(resp.error().status());
  EXPECT_TRUE(s.IsIllegalState());
  ASSERT_STR_CONTAINS(s.ToString(), "is not leader of this quorum. Role: FOLLOWER");
  // TODO: need to change the error code to be something like REPLICA_NOT_LEADER
  // so that the client can properly handle this case! plumbing this is a little difficult
  // so not addressing at the moment.
  ASSERT_ALL_REPLICAS_AGREE(0);
}

TEST_F(RaftConsensusITest, TestRunLeaderElection) {
  // Reset consensus rpc timeout to the default value or the election might fail often.
  FLAGS_consensus_rpc_timeout_ms = 1000;

  BuildAndStart(vector<string>());

  int num_iters = AllowSlowTests() ? 10 : 1;

  InsertTestRowsRemoteThread(0,
                             FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_num_batches_per_thread,
                             vector<CountDownLatch*>());

  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * num_iters);

  // Select the last follower to be new leader.
  vector<TServerDetails*> followers;
  GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

  // Now shutdown the current leader.
  TServerDetails* leader = DCHECK_NOTNULL(GetLeaderReplicaOrNull(tablet_id_));
  leader->external_ts->Shutdown();

  TServerDetails* replica = followers.back();
  CHECK_NE(leader->instance_id.permanent_uuid(), replica->instance_id.permanent_uuid());

  // Make the new replica leader.
  consensus::RunLeaderElectionRequestPB request;
  request.set_tablet_id(tablet_id_);

  consensus::RunLeaderElectionResponsePB response;
  RpcController controller;

  ASSERT_OK(replica->consensus_proxy->RunLeaderElection(request, &response, &controller));
  ASSERT_FALSE(response.has_error()) << "Got an error back: " << response.DebugString();

  // Insert a bunch more rows.
  InsertTestRowsRemoteThread(FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_num_batches_per_thread,
                             vector<CountDownLatch*>());

  // Make sure the two remaining replicas agree.
  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * num_iters * 2);
}

TEST_F(RaftConsensusITest, TestInsertWhenTheQueueIsFull) {
  vector<string> extra_flags;
  extra_flags.push_back("--log_cache_size_soft_limit_mb=0");
  extra_flags.push_back("--log_cache_size_hard_limit_mb=1");
  BuildAndStart(extra_flags);
  TServerDetails* replica = (*tablet_replicas_.begin()).second;
  ASSERT_TRUE(replica != NULL);

  // generate a 128Kb dummy payload
  string test_payload(128 * 1024, '0');

  WriteRequestPB req;
  req.set_tablet_id(tablet_id_);
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));
  RowOperationsPB* data = req.mutable_row_operations();

  WriteResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
  Status s;

  int key = 0;
  int successful_writes_counter = 0;

  // Pause a replica
  ASSERT_OK(replica->external_ts->Pause());
  LOG(INFO)<< "Paused one of the replicas, starting to write.";

  TServerDetails* leader = NULL;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

  // .. and insert until insertions fail because the queue is full
  while (true) {
    rpc.Reset();
    data->Clear();
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, key, key,
                   test_payload, data);
    key++;
    ASSERT_OK(leader->tserver_proxy->Write(req, &resp, &rpc));

    if (resp.has_error()) {
      s = StatusFromPB(resp.error().status());
    }
    if (s.ok()) {
      successful_writes_counter++;
      continue;
    }
    break;
  }
  // Make sure the we get Status::ServiceUnavailable()
  ASSERT_TRUE(s.IsServiceUnavailable());

  // .. now unpause the replica, the lagging replica should eventually ACK the
  // pending request and allow to discard messages from the queue, thus
  // accepting new requests.
  // .. and insert until insertions succeed because the queue is is no longer full.

  ASSERT_OK(replica->external_ts->Resume());
  int failure_counter = 0;
  while (true) {
    rpc.Reset();
    data->Clear();
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, key, key,
                   test_payload, data);
    key++;
    ASSERT_OK(leader->tserver_proxy->Write(req, &resp, &rpc));
    if (resp.has_error()) {
      s = StatusFromPB(resp.error().status());
    } else {
      successful_writes_counter++;
      s = Status::OK();
    }
    if (s.IsServiceUnavailable()) {
      if (failure_counter++ == 1000) {
        FAIL() << "Wasn't able to write to the tablet.";
      }
      SleepFor(MonoDelta::FromMilliseconds(100));
      continue;
    }
    ASSERT_OK(s);
    break;
  }

  ASSERT_ALL_REPLICAS_AGREE(successful_writes_counter);
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

  // Start a 7 node quorum cluster (since we can't bring leaders back we start with a
  // higher replica count so that we kill more leaders).

  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=true");
  BuildAndStart(flags);

  OverrideFlagForSlowTests(
      "client_inserts_per_thread",
      strings::Substitute("$0", (FLAGS_client_inserts_per_thread * 100)));
  OverrideFlagForSlowTests(
      "client_num_batches_per_thread",
      strings::Substitute("$0", (FLAGS_client_num_batches_per_thread * 100)));

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
    CHECK_OK(kudu::Thread::Create("test", strings::Substitute("ts-test$0", i),
                                  &RaftConsensusITest::InsertTestRowsRemoteThread,
                                  this, i * FLAGS_client_inserts_per_thread,
                                  FLAGS_client_inserts_per_thread,
                                  FLAGS_client_num_batches_per_thread,
                                  latches,
                                  &new_thread));
    threads_.push_back(new_thread);
  }

  BOOST_FOREACH(CountDownLatch* latch, latches) {
    latch->Wait();
    StopOrKillLeaderAndElectNewOne();
  }

  BOOST_FOREACH(scoped_refptr<kudu::Thread> thr, threads_) {
   CHECK_OK(ThreadJoiner(thr.get()).Join());
  }

  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * FLAGS_num_client_threads);
  STLDeleteElements(&latches);
}

// Test automatic leader election by killing leaders.
TEST_F(RaftConsensusITest, TestAutomaticLeaderElection) {
  if (AllowSlowTests()) {
    FLAGS_num_tablet_servers = 5;
    FLAGS_num_replicas = 5;
  }
  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=true");
  BuildAndStart(flags);

  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

  unordered_set<string> killed_leader_uuids;

  const int kNumLeadersToKill = FLAGS_num_replicas / 2;
  const int kFinalNumReplicas = FLAGS_num_replicas / 2 + 1;

  for (int leaders_killed = 0; leaders_killed < kFinalNumReplicas; leaders_killed++) {
    LOG(INFO) << Substitute("Writing data to leader of $0-node quorum ($1 alive)...",
                            FLAGS_num_replicas, FLAGS_num_replicas - leaders_killed);

    InsertTestRowsRemoteThread(leaders_killed * FLAGS_client_inserts_per_thread,
                               FLAGS_client_inserts_per_thread,
                               FLAGS_client_num_batches_per_thread,
                               vector<CountDownLatch*>());

    // At this point, the writes are flushed but the commit index may not be
    // propagated to all replicas. We kill the leader anyway.
    if (leaders_killed < kNumLeadersToKill) {
      LOG(INFO) << "Killing current leader " << leader->instance_id.permanent_uuid() << "...";
      ASSERT_OK(KillServerWithUUID(leader->instance_id.permanent_uuid()));
      InsertOrDie(&killed_leader_uuids, leader->instance_id.permanent_uuid());

      LOG(INFO) << "Waiting for new guy to be elected leader.";
      ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
    }
  }

  // Verify the data on the remaining replicas.
  PruneFromReplicas(killed_leader_uuids);
  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * kFinalNumReplicas);
}

}  // namespace tserver
}  // namespace kudu

