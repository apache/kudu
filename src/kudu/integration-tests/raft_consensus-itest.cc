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
#include "kudu/gutil/strings/strcat.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/master/master.proxy.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/util/random.h"
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
using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
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

static const int kMaxRetries = 20;
static const int kNumReplicas = 3;
static const int kConsensusRpcTimeoutForTests = 50;

// Integration test for the raft consensus implementation.
// Uses the whole tablet server stack with ExternalMiniCluster.
class RaftConsensusITest : public TabletServerTest {
 public:
  RaftConsensusITest()
      : random_(SeedRandom()),
        inserters_(FLAGS_num_client_threads) {
  }

  struct TServerDetails {
    master::TSInfoPB ts_info;
    gscoped_ptr<TabletServerServiceProxy> tserver_proxy;
    gscoped_ptr<TabletServerAdminServiceProxy> tserver_admin_proxy;
    gscoped_ptr<consensus::ConsensusServiceProxy> consensus_proxy;
    ExternalTabletServer* external_ts;
  };

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    FLAGS_consensus_rpc_timeout_ms = kConsensusRpcTimeoutForTests;
  }

  // Starts an external cluster with 'num_replicas'. The caller can pass
  // 'non_default_flags' to specify non-default values the flags used
  // to configure the external daemons.
  void BuildAndStart(int num_replicas,
                     const vector<std::string>& non_default_flags) {
    CreateCluster(num_replicas, non_default_flags);
    CreateClient(&client_);
    CreateTable(num_replicas);
    WaitForTSAndQuorum(num_replicas);
  }

  void CreateCluster(int num_replicas, const vector<std::string>& non_default_flags) {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_replicas;
    opts.data_root = GetTestPath("raft_consensus-itest-cluster");

    // If the caller passed no flags use the default ones.
    if (non_default_flags.empty()) {
      opts.extra_tserver_flags.push_back("--log_cache_size_soft_limit_mb=5");
      opts.extra_tserver_flags.push_back("--log_cache_size_hard_limit_mb=10");
      opts.extra_tserver_flags.push_back(strings::Substitute("--consensus_rpc_timeout_ms=$0",
                                                             FLAGS_consensus_rpc_timeout_ms));
    } else {
      BOOST_FOREACH(const std::string& flag, non_default_flags) {
        opts.extra_tserver_flags.push_back(flag);
      }
    }

    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_STATUS_OK(cluster_->Start());
    ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(num_replicas, MonoDelta::FromSeconds(5)));
  }

  void CreateClient(shared_ptr<KuduClient>* client) {
    // Connect to the cluster.
    ASSERT_STATUS_OK(KuduClientBuilder()
                     .add_master_server_addr(cluster_->master()->bound_rpc_addr().ToString())
                     .Build(client));
  }

  // Create a table with a single tablet, with 'num_replicas'.
  void CreateTable(int num_replicas) {
    // The tests here make extensive use of server schemas, but we need
    // a client schema to create the table.
    KuduSchema client_schema(GetClientSchema(schema_));
    ASSERT_OK(client_->NewTableCreator()
             ->table_name(kTableId)
             .schema(&client_schema)
             .num_replicas(num_replicas)
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

  void CreateReplicaProxies(const TabletLocationsPB& locations) {
    STLDeleteElements(&replicas_);

    BOOST_FOREACH(const TabletLocationsPB::ReplicaPB& replica_pb, locations.replicas()) {
      HostPort host_port;
      ASSERT_STATUS_OK(HostPortFromPB(replica_pb.ts_info().rpc_addresses(0), &host_port));
      vector<Sockaddr> addresses;
      host_port.ResolveAddresses(&addresses);

      gscoped_ptr<TServerDetails> peer(new TServerDetails());
      peer->ts_info.CopyFrom(replica_pb.ts_info());

      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        if (cluster_->tablet_server(i)->instance_id().permanent_uuid() ==
            replica_pb.ts_info().permanent_uuid()) {
          peer->external_ts = cluster_->tablet_server(i);
        }
      }

      ASSERT_OK(CreateClientProxies(addresses[0],
                                    &peer->tserver_proxy,
                                    &peer->tserver_admin_proxy,
                                    &peer->consensus_proxy));

      replicas_.push_back(peer.release());
    }
  }

  // Gets the the locations of the quorum and waits until 1 LEADER and kNumReplicas - 1
  // FOLLOWERS are reported.
  void WaitForTSAndQuorum(int num_replicas) {
    int num_retries = 0;
    // make sure the replicas are up and find the leader
    while (true) {
      if (num_retries >= kMaxRetries) {
        FAIL() << " Reached max. retries while looking up the quorum.";
      }

      Status status = cluster_->WaitForTabletServerCount(num_replicas, MonoDelta::FromSeconds(5));
      if (status.IsTimedOut()) {
        LOG(WARNING)<< "Timeout waiting for all replicas to be online, retrying...";
        num_retries++;
        continue;
      }
      break;
    }
    WaitForReplicasAndUpdateLocations(num_replicas);
  }

  void WaitForReplicasAndUpdateLocations(int num_replicas) {
    int num_retries = 0;
    while (true) {
      if (num_retries >= kMaxRetries) {
        FAIL() << " Reached max. retries while looking up the quorum.";
      }
      GetTableLocationsRequestPB req;
      GetTableLocationsResponsePB resp;
      RpcController controller;
      req.mutable_table()->set_table_name(kTableId);
      TabletLocationsPB locations;

      CHECK_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));
      CHECK(resp.tablet_locations_size() > 0);
      tablet_id_ = resp.tablet_locations(0).tablet_id();
      locations = resp.tablet_locations(0);

      CreateReplicaProxies(locations);

      if (replicas_.size() < num_replicas) {
        LOG(WARNING)<< "Couldn't find the leader and/or replicas. Locations: "
        << locations.ShortDebugString();
        sleep(1);
        num_retries++;
        continue;
      }
      break;
    }
  }

  // Removes a set of servers from the replicas_ list.
  // Handy for controlling who to validate against after killing servers.
  void PruneFromReplicas(const unordered_set<std::string>& uuids) {
    vector<TServerDetails*> pruned_replicas;
    BOOST_FOREACH(TServerDetails* replica, replicas_) {
      if (ContainsKey(uuids, replica->ts_info.permanent_uuid())) {
        delete replica;
      } else {
        pruned_replicas.push_back(replica);
      }
    }
    replicas_ = pruned_replicas;
  }

  TServerDetails* GetReplicaWithUuidOrNull(const string& uuid) {
    BOOST_FOREACH(TServerDetails* replica, replicas_) {
      if (replica->ts_info.permanent_uuid() == uuid) {
        return replica;
      }
    }
    return NULL;
  }

  // Returns:
  // Status::OK() if the replica is alive and leader of the quorum.
  // Status::NotFound() if the replica is not part of the quorum or is dead.
  // Status::IllegalState() if the replica is live but not the leader.
  Status GetReplicaStatusAndCheckIfLeader(TServerDetails* replica) {
    consensus::GetCommittedQuorumRequestPB req;
    consensus::GetCommittedQuorumResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(100));
    req.set_tablet_id(tablet_id_);
    if (!replica->consensus_proxy->GetCommittedQuorum(req, &resp, &controller).ok() ||
        resp.has_error()) {
      VLOG(1) << "Error getting quorum from replica: " << replica->ts_info.permanent_uuid();
      return Status::NotFound("Error connecting to replica");
    }
    if (consensus::GetRoleInQuorum(replica->ts_info.permanent_uuid(),
                                   resp.quorum()) == QuorumPeerPB::LEADER) {
      return Status::OK();
    }
    VLOG(1) << "Replica not leader of quorum: " << replica->ts_info.permanent_uuid();
    return Status::IllegalState("Replica found but not leader");
  }

   Status GetLeaderReplicaWithRetries(TServerDetails** leader, int max_attempts = 100) {
    int attempts = 0;
    while (attempts < max_attempts) {
      *leader = GetLeaderReplicaOrNull();
      if (*leader) {
        return Status::OK();
      }
      attempts++;
      usleep(100 * attempts * 1000);
    }
    return Status::NotFound("Leader replica not found");
  }

  // Returns the last committed leader of the quorum. Tries to get it from master
  // but then actually tries to the get the committed quorum to make sure.
  TServerDetails* GetLeaderReplicaOrNull() {
    string leader_uuid;
    Status master_found_leader_result = GetTabletLeaderUUIDFromMaster(tablet_id_, &leader_uuid);

    // See if the master is up to date. I.e. if it does report a leader and if the
    // replica it reports as leader is still alive and (at least thinks) its still
    // the leader.
    TServerDetails* leader;
    if (master_found_leader_result.ok()) {
      leader = GetReplicaWithUuidOrNull(leader_uuid);
      if (leader && GetReplicaStatusAndCheckIfLeader(leader).ok()) {
        return leader;
      }
    }

    // The replica we got from the master (if any) is either dead or not the leader.
    // Find the actual leader.
    vector<TServerDetails*> replicas_copy = replicas_;
    std::random_shuffle(replicas_copy.begin(), replicas_copy.end());
    BOOST_FOREACH(TServerDetails* replica, replicas_copy) {
      if (GetReplicaStatusAndCheckIfLeader(replica).ok()) {
        return replica;
      }
    }
    return NULL;
  }

  void GetOnlyLiveFollowerReplicas(vector<TServerDetails*>* followers) {
    followers->clear();
    TServerDetails* leader = GetLeaderReplicaOrNull();
    BOOST_FOREACH(TServerDetails* replica, replicas_) {
      if (leader != NULL && replica->ts_info.permanent_uuid() == leader->ts_info.permanent_uuid()) {
        continue;
      }
      if (GetReplicaStatusAndCheckIfLeader(replica).IsIllegalState()) {
        followers->push_back(replica);
      }
    }
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

  string DumpToString(const master::TSInfoPB& leader_info,
                      const vector<string>& leader_results,
                      const master::TSInfoPB& replica_info,
                      const vector<string>& replica_results) {
    string ret = strings::Substitute("Replica results did not match the leaders."
                                     "\nLeader: $0\nReplica: $1. Results size "
                                     "L: $2 R: $3",
                                     leader_info.ShortDebugString(),
                                     replica_info.ShortDebugString(),
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
      usleep(10000 * counter);
      vector<string> leader_results;
      vector<string> replica_results;

      TServerDetails* leader;
      CHECK_OK(GetLeaderReplicaWithRetries(&leader));

      vector<TServerDetails*> followers;
      GetOnlyLiveFollowerReplicas(&followers);

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
        SCOPED_TRACE(DumpToString(leader->ts_info, leader_results,
                                  replica->ts_info, replica_results));

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
          SCOPED_TRACE(DumpToString(leader->ts_info, leader_results,
                                    last_replica->ts_info, replica_results));
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
        usleep(sleep_time_usec);
        continue;
      }

      ASSERT_OK(tablet_server->Pause());
      LOG_IF(INFO, sleep_time_usec > 0.0)
          << "Delay injector thread for TS " << tablet_server->instance_id().permanent_uuid()
          << " SIGSTOPped the ts, sleeping for " << sleep_time_usec << " usec...";
      usleep(sleep_time_usec);
      ASSERT_OK(tablet_server->Resume());
    }
  }

  // Returns the index of the replica who is farthest ahead.
  int GetFurthestAheadReplicaIdx(const vector<TServerDetails*>& replicas) {
    consensus::GetLastOpIdRequestPB opid_req;
    consensus::GetLastOpIdResponsePB opid_resp;
    opid_req.set_tablet_id(tablet_id_);
    RpcController controller;

    uint64 max_index = 0;
    int max_replica_index = -1;

    for (int i = 0; i < replicas.size(); i++) {
      controller.Reset();
      opid_resp.Clear();
      opid_req.set_tablet_id(tablet_id_);
      CHECK_OK(replicas[i]->consensus_proxy->GetLastOpId(opid_req, &opid_resp, &controller));
      if (opid_resp.opid().index() > max_index) {
        max_index = opid_resp.opid().index();
        max_replica_index = i;
      }
    }

    CHECK_NE(max_replica_index, -1);

    return max_replica_index;
  }

  Status GetTabletLeaderUUIDFromMaster(const std::string& tablet_id, std::string* leader_uuid) {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(100));
    req.mutable_table()->set_table_name(kTableId);

    RETURN_NOT_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));
    BOOST_FOREACH(const TabletLocationsPB& loc, resp.tablet_locations()) {
      if (loc.tablet_id() == tablet_id) {
        BOOST_FOREACH(const TabletLocationsPB::ReplicaPB& replica, loc.replicas()) {
          if (replica.role() == QuorumPeerPB::LEADER) {
            *leader_uuid = replica.ts_info().permanent_uuid();
            return Status::OK();
          }
        }
      }
    }
    return Status::NotFound("Unable to find leader for tablet", tablet_id);
  }

  Status KillServerWithUUID(const std::string& uuid) {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      ExternalTabletServer* ts = cluster_->tablet_server(i);
      if (ts->instance_id().permanent_uuid() == uuid) {
        ts->Shutdown();
        return Status::OK();
      }
    }
    return Status::NotFound("Unable to find server with UUID", uuid);
  }

  // Stops the current leader of the quorum, runs leader election and then brings it back.
  // Before stopping the leader this pauses all follower nodes in regular intervals so that
  // we get an increased chance of stuff being pending.
  void StopOrKillLeaderAndElectNewOne() {
    bool kill = rand() % 2 == 0;

    TServerDetails* old_leader;
    CHECK_OK(GetLeaderReplicaWithRetries(&old_leader));

    vector<TServerDetails*> followers;
    GetOnlyLiveFollowerReplicas(&followers);

    BOOST_FOREACH(TServerDetails* ts, followers) {
      CHECK_OK(ts->external_ts->Pause());
      usleep(100 * 1000); // 100 ms
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
    CHECK_OK(GetLeaderReplicaWithRetries(&new_leader));

    // Bring the old leader back.
    if (kill) {
      CHECK_OK(old_leader->external_ts->Restart());
      // Wait until we have the same number of followers.
      int initial_followers = followers.size();
      do {
        GetOnlyLiveFollowerReplicas(&followers);
      } while (followers.size() < initial_followers);
    } else {
      CHECK_OK(old_leader->external_ts->Resume());
    }
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
    STLDeleteElements(&replicas_);
  }

 protected:
  gscoped_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  scoped_refptr<KuduTable> table_;
  vector<TServerDetails*> replicas_;

  ThreadSafeRandom random_;
  QuorumPB quorum_;
  string tablet_id_;

  std::vector<scoped_refptr<kudu::Thread> > threads_;
  CountDownLatch inserters_;
};

// Test that we can retrieve the permanent uuid of a server running
// consensus service via RPC.
TEST_F(RaftConsensusITest, TestGetPermanentUuid) {
  BuildAndStart(kNumReplicas, vector<string>());

  QuorumPeerPB peer;
  TServerDetails* leader = NULL;
  ASSERT_OK(GetLeaderReplicaWithRetries(&leader));
  peer.mutable_last_known_addr()->CopyFrom(leader->ts_info.rpc_addresses(0));
  const string expected_uuid = leader->ts_info.permanent_uuid();

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
  BuildAndStart(kNumReplicas, vector<string>());

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
  BuildAndStart(kNumReplicas, vector<string>());

  WriteRequestPB req;
  req.set_tablet_id(tablet_id_);
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

  RowOperationsPB* data = req.mutable_row_operations();
  data->set_rows("some gibberish!");

  WriteResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));

  TServerDetails* leader = NULL;
  ASSERT_OK(GetLeaderReplicaWithRetries(&leader));

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
  BuildAndStart(kNumReplicas, vector<string>());

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
  for (int i = 0; i < kNumReplicas; i++) {
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
  BuildAndStart(kNumReplicas, vector<string>());

  // Manually construct a write RPC to a replica and make sure it responds
  // with the correct error code.
  WriteRequestPB req;
  WriteResponsePB resp;
  RpcController rpc;
  req.set_tablet_id(tablet_id_);
  ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1234, 5678,
                 "hello world via RPC", req.mutable_row_operations());
  ASSERT_STATUS_OK(replicas_[0]->tserver_proxy->Write(req, &resp, &rpc));
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

  BuildAndStart(kNumReplicas, vector<string>());

  int num_iters = AllowSlowTests() ? 10 : 1;

  InsertTestRowsRemoteThread(0,
                             FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_num_batches_per_thread,
                             vector<CountDownLatch*>());

  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * num_iters);

  // Select the last follower to be new leader.
  vector<TServerDetails*> followers;
  GetOnlyLiveFollowerReplicas(&followers);

  // Now shutdown the current leader.
  TServerDetails* leader = DCHECK_NOTNULL(GetLeaderReplicaOrNull());
  leader->external_ts->Shutdown();

  TServerDetails* replica = followers.back();
  CHECK_NE(leader->ts_info.permanent_uuid(), replica->ts_info.permanent_uuid());

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
  BuildAndStart(kNumReplicas, extra_flags);
  TServerDetails* replica = replicas_[0];
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
  ASSERT_OK(GetLeaderReplicaWithRetries(&leader));

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
      usleep(100 * 1000);
      continue;
    }
    ASSERT_OK(s);
    break;
  }

  ASSERT_ALL_REPLICAS_AGREE(successful_writes_counter);
}

TEST_F(RaftConsensusITest, MultiThreadedInsertWithFailovers) {
  int kNumReplicas = 3;
  int kNumElections = kNumReplicas;

  if (AllowSlowTests()) {
    kNumReplicas = 7;
    kNumElections = 3 * kNumReplicas;
  }

  // Reset consensus rpc timeout to the default value or the election might fail often.
  FLAGS_consensus_rpc_timeout_ms = 1000;

  // Start a 7 node quorum cluster (since we can't bring leaders back we start with a
  // higher replica count so that we kill more leaders).

  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=true");
  BuildAndStart(kNumReplicas, flags);

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
  int kNumReplicas = 3;
  if (AllowSlowTests()) {
    kNumReplicas = 5;
  }
  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=true");
  BuildAndStart(kNumReplicas, flags);

  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(&leader));

  unordered_set<string> killed_leader_uuids;

  const int kNumLeadersToKill = kNumReplicas / 2;
  const int kFinalNumReplicas = kNumReplicas / 2 + 1;

  for (int leaders_killed = 0; leaders_killed < kFinalNumReplicas; leaders_killed++) {
    LOG(INFO) << Substitute("Writing data to leader of $0-node quorum ($1 alive)...",
                            kNumReplicas, kNumReplicas - leaders_killed);

    InsertTestRowsRemoteThread(leaders_killed * FLAGS_client_inserts_per_thread,
                               FLAGS_client_inserts_per_thread,
                               FLAGS_client_num_batches_per_thread,
                               vector<CountDownLatch*>());

    // At this point, the writes are flushed but the commit index may not be
    // propagated to all replicas. We kill the leader anyway.
    if (leaders_killed < kNumLeadersToKill) {
      LOG(INFO) << "Killing current leader " << leader->ts_info.permanent_uuid() << "...";
      ASSERT_OK(KillServerWithUUID(leader->ts_info.permanent_uuid()));
      InsertOrDie(&killed_leader_uuids, leader->ts_info.permanent_uuid());

      LOG(INFO) << "Waiting for new guy to be elected leader.";
      ASSERT_OK(GetLeaderReplicaWithRetries(&leader));
    }
  }

  // Verify the data on the remaining replicas.
  PruneFromReplicas(killed_leader_uuids);
  ASSERT_ALL_REPLICAS_AGREE(FLAGS_client_inserts_per_thread * kFinalNumReplicas);
}

}  // namespace tserver
}  // namespace kudu

