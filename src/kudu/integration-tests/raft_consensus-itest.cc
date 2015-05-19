// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <glog/stl_logging.h>
#include <tr1/unordered_set>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/ts_itest-base.h"
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
  NO_FATALS(AssertAllReplicasAgree(count))

namespace kudu {
namespace tserver {

using boost::assign::list_of;
using consensus::ConsensusResponsePB;
using consensus::ConsensusRequestPB;
using consensus::ConsensusServiceProxy;
using consensus::MakeOpId;
using consensus::QuorumPB;
using consensus::QuorumPeerPB;
using consensus::ReplicateMsg;
using consensus::RunLeaderElectionResponsePB;
using consensus::RunLeaderElectionRequestPB;
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
    TabletServerIntegrationTestBase::SetUp();
    FLAGS_consensus_rpc_timeout_ms = kConsensusRpcTimeoutForTests;
  }

  // Starts an external cluster with a single tablet and a number of replicas equal
  // to 'FLAGS_num_replicas'. The caller can pass 'ts_flags' to specify non-default
  // flags to pass to the tablet servers.
  void BuildAndStart(const vector<string>& ts_flags,
                     const vector<string>& master_flags = vector<string>()) {
    CreateCluster("raft_consensus-itest-cluster", ts_flags, master_flags);
    CreateClient(&client_);
    CreateTable();
    WaitForTSAndQuorum();
    CHECK_GT(tablet_replicas_.size(), 0);
    tablet_id_ = (*tablet_replicas_.begin()).first;
  }

  void CreateClient(shared_ptr<KuduClient>* client) {
    // Connect to the cluster.
    ASSERT_OK(KuduClientBuilder()
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
    ASSERT_OK(client_->OpenTable(kTableId, &table_));
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
    rpc.set_timeout(MonoDelta::FromSeconds(10)); // Squelch warnings.

    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(tablet_id_);
    ASSERT_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

    // Send the call
    {
      req.set_batch_size_bytes(0);
      SCOPED_TRACE(req.DebugString());
      ASSERT_OK(replica_proxy->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      if (resp.has_error()) {
        ASSERT_OK(StatusFromPB(resp.error().status()));
      }
    }

    if (!resp.has_more_results())
      return;

    // Drain all the rows from the scanner.
    NO_FATALS(DrainScannerToStrings(resp.scanner_id(),
                                    schema_,
                                    results,
                                    replica_proxy));

    std::sort(results->begin(), results->end());
  }

  // Scan the given replica in a loop until the number of rows
  // is 'expected_count'. If it takes more than 10 seconds, then
  // fails the test.
  void WaitForRowCount(TabletServerServiceProxy* replica_proxy,
                       int expected_count,
                       vector<string>* results) {
    LOG(INFO) << "Waiting for row count " << expected_count << "...";
    MonoTime start = MonoTime::Now(MonoTime::FINE);
    MonoTime deadline = MonoTime::Now(MonoTime::FINE);
    deadline.AddDelta(MonoDelta::FromSeconds(10));
    while (true) {
      results->clear();
      NO_FATALS(ScanReplica(replica_proxy, results));
      if (results->size() == expected_count) {
        return;
      }
      SleepFor(MonoDelta::FromMilliseconds(10));
      if (!MonoTime::Now(MonoTime::FINE).ComesBefore(deadline)) {
        break;
      }
    }
    MonoTime end = MonoTime::Now(MonoTime::FINE);
    LOG(WARNING) << "Didn't reach row count " << expected_count;
    FAIL() << "Did not reach expected row count " << expected_count
           << " after " << end.GetDeltaSince(start).ToString()
           << ": rows: " << *results;
  }


  // Add an Insert operation to the given consensus request.
  // The row to be inserted is generated based on the OpId.
  void AddOp(const OpId& id, ConsensusRequestPB* req);

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
    ClusterVerifier v(cluster_.get());
    v.CheckCluster();
    v.CheckRowCount(kTableId, expected_result_count);
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
        CHECK_OK(row->SetInt32(0, j));
        CHECK_OK(row->SetInt32(1, j * 2));
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

  // Thread which loops until '*finish' becomes true, trying to insert a row
  // on the given tablet server identified by 'replica_idx'.
  void StubbornlyWriteSameRowThread(int replica_idx, const AtomicBool* finish);

  // Stops the current leader of the quorum, runs leader election and then brings it back.
  // Before stopping the leader this pauses all follower nodes in regular intervals so that
  // we get an increased chance of stuff being pending.
  void StopOrKillLeaderAndElectNewOne() {
    bool kill = rand() % 2 == 0;

    TServerDetails* old_leader;
    CHECK_OK(GetLeaderReplicaWithRetries(tablet_id_, &old_leader));
    ExternalTabletServer* old_leader_ets = cluster_->tablet_server_by_uuid(old_leader->uuid());

    vector<TServerDetails*> followers;
    GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

    BOOST_FOREACH(TServerDetails* ts, followers) {
      ExternalTabletServer* ets = cluster_->tablet_server_by_uuid(ts->uuid());
      CHECK_OK(ets->Pause());
      SleepFor(MonoDelta::FromMilliseconds(100));
    }

    // When all are paused also pause or kill the current leader. Since we've waited a bit
    // the old leader is likely to have operations that must be aborted.
    if (kill) {
      old_leader_ets->Shutdown();
    } else {
      CHECK_OK(old_leader_ets->Pause());
    }

    // Resume the replicas.
    BOOST_FOREACH(TServerDetails* ts, followers) {
      ExternalTabletServer* ets = cluster_->tablet_server_by_uuid(ts->uuid());
      CHECK_OK(ets->Resume());
    }

    // Get the new leader.
    TServerDetails* new_leader;
    CHECK_OK(GetLeaderReplicaWithRetries(tablet_id_, &new_leader));

    // Bring the old leader back.
    if (kill) {
      CHECK_OK(old_leader_ets->Restart());
      // Wait until we have the same number of followers.
      int initial_followers = followers.size();
      do {
        GetOnlyLiveFollowerReplicas(tablet_id_, &followers);
      } while (followers.size() < initial_followers);
    } else {
      CHECK_OK(old_leader_ets->Resume());
    }
  }

  // Start an election on the specified tserver
  Status StartElection(const TServerDetails* replica, const string& tablet_id);

  // Writes 'num_writes' operations to the current leader. Each of the operations
  // has a payload of around 128KB. Causes a gtest failure on error.
  void Write128KOpsToLeader(int num_writes);

  // Wait until the given tablet server has GCed its logs such that it has
  // 'max_expected' or fewer remaining segments.
  //
  // If the target number of segments is not achieved within 10 seconds,
  // causes a gtest failure.
  void WaitForLogGC(TServerDetails* leader, int max_expected);

 protected:
  shared_ptr<KuduClient> client_;
  scoped_refptr<KuduTable> table_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;
  CountDownLatch inserters_;
  string tablet_id_;
};

Status RaftConsensusITest::StartElection(const TServerDetails* replica, const string& tablet_id) {
  RunLeaderElectionRequestPB req;
  req.set_tablet_id(tablet_id);
  RunLeaderElectionResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  RETURN_NOT_OK(replica->consensus_proxy->RunLeaderElection(req, &resp, &rpc));
  Status s;
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status())
      .CloneAndPrepend(Substitute("Code $0", TabletServerErrorPB::Code_Name(resp.error().code())));
  }
  return Status::OK();
}

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
  ASSERT_OK(builder.Build(&messenger));

  ASSERT_OK(consensus::SetPermanentUuidForRemotePeer(messenger, &peer));
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
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));

  RowOperationsPB* data = req.mutable_row_operations();
  data->set_rows("some gibberish!");

  WriteResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));

  TServerDetails* leader = NULL;
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
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678,
                 "hello world via RPC", req.mutable_row_operations());

  // Get the leader.
  vector<TServerDetails*> followers;
  GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

  ASSERT_OK(followers[0]->tserver_proxy->Write(req, &resp, &rpc));
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
  ExternalTabletServer* leader_ets = cluster_->tablet_server_by_uuid(leader->uuid());
  leader_ets->Shutdown();

  TServerDetails* replica = followers.back();
  CHECK_NE(leader->instance_id.permanent_uuid(), replica->instance_id.permanent_uuid());

  // Make the new replica leader.
  ASSERT_OK(StartElection(replica, tablet_id_));

  // Insert a bunch more rows.
  InsertTestRowsRemoteThread(FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_num_batches_per_thread,
                             vector<CountDownLatch*>());

  // Restart the original replica and make sure they all agree.
  ASSERT_OK(leader_ets->Restart());

  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * num_iters * 2);
}

void RaftConsensusITest::Write128KOpsToLeader(int num_writes) {
  TServerDetails* leader = NULL;
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

    ASSERT_FALSE(resp.has_error()) << resp.DebugString();
  }
}

// Test that when a follower is stopped for a long time, the log cache
// properly evicts operations, but still allows the follower to catch
// up when it comes back.
TEST_F(RaftConsensusITest, TestCatchupAfterOpsEvicted) {
  vector<string> extra_flags;
  extra_flags.push_back("--log_cache_size_limit_mb=1");
  extra_flags.push_back("--consensus_max_batch_size_bytes=500000");
  BuildAndStart(extra_flags);
  TServerDetails* replica = (*tablet_replicas_.begin()).second;
  ASSERT_TRUE(replica != NULL);
  ExternalTabletServer* replica_ets = cluster_->tablet_server_by_uuid(replica->uuid());

  // Pause a replica
  ASSERT_OK(replica_ets->Pause());
  LOG(INFO)<< "Paused one of the replicas, starting to write.";

  // Insert 3MB worth of data.
  const int kNumWrites = 25;
  NO_FATALS(Write128KOpsToLeader(kNumWrites));

  // Now unpause the replica, the lagging replica should eventually catch back up.
  ASSERT_OK(replica_ets->Resume());

  ASSERT_ALL_REPLICAS_AGREE(kNumWrites);
}

void RaftConsensusITest::WaitForLogGC(TServerDetails* leader, int max_expected) {
  ExternalTabletServer* ets = cluster_->tablet_server_by_uuid(leader->uuid());
  MonoTime deadline = MonoTime::Now(MonoTime::COARSE);
  deadline.AddDelta(MonoDelta::FromSeconds(10));

  while (true) {
    vector<string> children;
    string wal_dir = JoinPathSegments(ets->data_dir(), "wals");
    ASSERT_OK(Env::Default()->GetChildren(wal_dir, &children));
    int num_wals = 0;
    BOOST_FOREACH(const string& child, children) {
      if (HasPrefixString(child, "wal-")) {
        num_wals++;
      }
    }
    if (num_wals <= max_expected) {
      return;
    }
    if (deadline.ComesBefore(MonoTime::Now(MonoTime::COARSE))) {
      FAIL() << "Never GCed down to expected number of WALs " << max_expected
             << ". Current contents of " << wal_dir << ":\n"
             << children;
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  LOG(FATAL) << "Should not reach here";
}

// Test that the leader doesn't crash if one of its followers has
// fallen behind so far that the logs necessary to catch it up
// have been GCed.
//
// In a real cluster, this will eventually cause the follower to be
// evicted/replaced. In any case, the leader should not crash.
//
// This is a regression test for KUDU-775.
TEST_F(RaftConsensusITest, TestFollowerFallsBehindLeaderGC) {
  // Configure a small log segment size so that we can roll
  // frequently. Additionally configure a small cache size so that
  // we evict data from the cache.
  vector<string> extra_flags;
  extra_flags.push_back("--log_cache_size_limit_mb=1");
  extra_flags.push_back("--log_segment_size_mb=1");
  BuildAndStart(extra_flags);

  SleepFor(MonoDelta::FromSeconds(1));
  // Pause one replica.
  TServerDetails* replica = (*tablet_replicas_.begin()).second;
  ASSERT_TRUE(replica != NULL);
  ExternalTabletServer* replica_ets = cluster_->tablet_server_by_uuid(replica->uuid());
  ASSERT_OK(replica_ets->Pause());

  // Write ~12.8MB worth of data to the leader, and wait for the logs
  // to GC.
  const int kNumWrites = 100;
  NO_FATALS(Write128KOpsToLeader(kNumWrites));

  TServerDetails* leader = NULL;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

  // Wait for the leader to GC its logs.
  NO_FATALS(WaitForLogGC(leader, 2));

  // Then wait another couple of seconds to be sure that it has bothered to try
  // to write to the paused peer.
  SleepFor(MonoDelta::FromSeconds(2));

  // Ensure that none of the tablet servers crashed.
  ASSERT_OK(replica_ets->Resume());
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    // Make sure it didn't crash.
    ASSERT_TRUE(cluster_->tablet_server(i)->IsProcessAlive())
      << "Tablet server " << i << " crashed";
  }

  // NOTE: we can't assert that they eventually converge, because the delayed
  // follower can never catch up!

  if (AllowSlowTests()) {
    // Sleep long enough that the "abandoned" server's leader election interval
    // will trigger several times. This ensures that the other servers properly
    // ignore the election requests from the abandoned node.
    SleepFor(MonoDelta::FromSeconds(5));
    vector<OpId> op_ids;
    ASSERT_OK(GetLastOpIdForEachReplica(tablet_id_,
                                        boost::assign::list_of(leader),
                                        &op_ids));
    ASSERT_EQ(1, op_ids.size());
    ASSERT_EQ(1, op_ids[0].term())
      << "expected the leader to have not advanced terms but has op " << op_ids[0];
  }
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

  unordered_set<TServerDetails*> killed_leaders;

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
      cluster_->tablet_server_by_uuid(leader->uuid())->Shutdown();
      InsertOrDie(&killed_leaders, leader);

      LOG(INFO) << "Waiting for new guy to be elected leader.";
      ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
    }
  }

  // Restart every node that was killed, and wait for the nodes to converge
  BOOST_FOREACH(TServerDetails* killed_node, killed_leaders) {
    CHECK_OK(cluster_->tablet_server_by_uuid(killed_node->uuid())->Restart());
  }
  // Verify the data on the remaining replicas.
  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * kFinalNumReplicas);
}

// Single-replica leader election test.
TEST_F(RaftConsensusITest, TestAutomaticLeaderElectionOneReplica) {
  FLAGS_num_tablet_servers = 1;
  FLAGS_num_replicas = 1;
  vector<string> ts_flags = list_of("--enable_leader_failure_detection=true");
  vector<string> master_flags = list_of("--catalog_manager_allow_local_consensus=false");
  BuildAndStart(ts_flags, master_flags);

  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
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
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678,
                 "hello world", req.mutable_row_operations());
  while (!finish->Load()) {
    resp.Clear();
    rpc.Reset();
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ignore_result(ts->tserver_proxy->Write(req, &resp, &rpc));
    VLOG(1) << "Response from server " << replica_idx << ": "
            << resp.ShortDebugString();
  }
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
  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=true");
  BuildAndStart(flags);

  AtomicBool finish(false);
  for (int i = 0; i < FLAGS_num_tablet_servers; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(kudu::Thread::Create("test", strings::Substitute("ts-test$0", i),
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
  BOOST_FOREACH(scoped_refptr<kudu::Thread> thr, threads_) {
    CHECK_OK(ThreadJoiner(thr.get()).Join());
  }
}

void RaftConsensusITest::AddOp(const OpId& id, ConsensusRequestPB* req) {
  ReplicateMsg* msg = req->add_ops();
  msg->mutable_id()->CopyFrom(id);
  msg->set_timestamp(id.index());
  msg->set_op_type(consensus::WRITE_OP);
  WriteRequestPB* write_req = msg->mutable_write_request();
  CHECK_OK(SchemaToPB(schema_, write_req->mutable_schema()));
  write_req->set_tablet_id(tablet_id_);
  int key = id.index() * 10000 + id.term();
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, key, id.term(),
                 id.ShortDebugString(), write_req->mutable_row_operations());
}

// Regression test for KUDU-644:
// Triggers some complicated scenarios on the replica involving aborting and
// replacing transactions.
TEST_F(RaftConsensusITest, TestReplicaBehaviorViaRPC) {
  FLAGS_num_replicas = 3;
  FLAGS_num_tablet_servers = 3;
  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=false");
  BuildAndStart(flags);

  // Kill all the servers but one.
  TServerDetails *replica_ts;
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());

  // Elect server 2 as leader and wait for log index 1 to propagate to all servers.
  ASSERT_OK(StartElection(tservers[2], tablet_id_));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 1));

  replica_ts = tservers[0];
  cluster_->tablet_server_by_uuid(tservers[1]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(tservers[2]->uuid())->Shutdown();


  LOG(INFO) << "================================== Cluster setup complete.";

  ConsensusServiceProxy* c_proxy = CHECK_NOTNULL(replica_ts->consensus_proxy.get());

  ConsensusRequestPB req;
  ConsensusResponsePB resp;
  RpcController rpc;

  // Send a simple request with no ops.
  req.set_tablet_id(tablet_id_);
  req.set_caller_uuid("fake_caller");
  req.set_caller_term(2);
  req.mutable_committed_index()->CopyFrom(MakeOpId(1, 1));
  req.mutable_preceding_id()->CopyFrom(MakeOpId(1, 1));

  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.DebugString();

  // Send some operations, but don't advance the commit index.
  // They should not commit.
  AddOp(MakeOpId(2, 2), &req);
  AddOp(MakeOpId(2, 3), &req);
  AddOp(MakeOpId(2, 4), &req);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.DebugString();

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
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
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
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
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
  req.mutable_committed_index()->CopyFrom(MakeOpId(2, 4));
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.DebugString();
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
  req.mutable_committed_index()->CopyFrom(MakeOpId(2, 4));
  req.mutable_preceding_id()->CopyFrom(MakeOpId(2, 4));
  AddOp(MakeOpId(2, 5), &req);
  AddOp(MakeOpId(2, 6), &req);
  rpc.Reset();
  ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.DebugString();

  // Verify they are committed.
  {
    vector<string> results;
    NO_FATALS(WaitForRowCount(replica_ts->tserver_proxy.get(), 3, &results));
    ASSERT_STR_CONTAINS(results[0], "term: 2 index: 2");
    ASSERT_STR_CONTAINS(results[1], "term: 2 index: 3");
    ASSERT_STR_CONTAINS(results[2], "term: 2 index: 4");
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
    ASSERT_FALSE(resp.has_error()) << "Req: " << req.ShortDebugString()
        << " Resp: " << resp.DebugString();
  }

  // Send an empty request from the newest term which should commit
  // the earlier ops.
  {
    req.mutable_preceding_id()->CopyFrom(MakeOpId(leader_term, 6));
    req.mutable_committed_index()->CopyFrom(MakeOpId(leader_term, 6));
    req.clear_ops();
    rpc.Reset();
    ASSERT_OK(c_proxy->UpdateConsensus(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.DebugString();
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

}  // namespace tserver
}  // namespace kudu

