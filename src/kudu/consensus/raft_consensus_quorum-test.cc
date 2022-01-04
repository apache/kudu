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
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/logical_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_bool(enable_leader_failure_detection);

METRIC_DECLARE_entity(tablet);

using kudu::log::Log;
using kudu::log::LogEntryPB;
using kudu::log::LogOptions;
using kudu::log::LogReader;
using kudu::pb_util::SecureShortDebugString;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using strings::SubstituteAndAppend;

namespace kudu {
namespace consensus {

const char* kTestTablet = "TestTablet";

void DoNothing(const string& s) {
}

// Test suite for tests that focus on multiple peer interaction, but
// without integrating with other components, such as ops.
class RaftConsensusQuorumTest : public KuduTest {
 public:
  typedef vector<unique_ptr<LogEntryPB>> LogEntries;

  RaftConsensusQuorumTest()
      : clock_(Timestamp(1)),
        metric_entity_(METRIC_ENTITY_tablet.Instantiate(&metric_registry_, "raft-test")),
        schema_(GetSimpleTestSchema()) {
    options_.tablet_id = kTestTablet;
    FLAGS_enable_leader_failure_detection = false;
    CHECK_OK(ThreadPoolBuilder("raft").Build(&raft_pool_));
  }


  // Builds an initial configuration of 'num' elements.
  // All of the peers start as followers.
  void BuildInitialRaftConfigPB(int num) {
    config_ = BuildRaftConfigPB(num);
    config_.set_opid_index(kInvalidOpIdIndex);
    peers_.reset(new TestPeerMapManager(config_));
  }

  Status BuildFsManagersAndLogs(int num) {
    // Build the fsmanagers and logs
    for (int i = 0; i < num; i++) {
      shared_ptr<MemTracker> parent_mem_tracker =
          MemTracker::CreateTracker(-1, Substitute("peer-$0", i));
      parent_mem_trackers_.push_back(parent_mem_tracker);
      string test_path = GetTestPath(Substitute("peer-$0-root", i));
      FsManagerOpts opts;
      opts.parent_mem_tracker = parent_mem_tracker;
      opts.wal_root = test_path;
      opts.data_roots = { test_path };
      unique_ptr<FsManager> fs_manager(new FsManager(env_, opts));
      RETURN_NOT_OK(fs_manager->CreateInitialFileSystemLayout());
      RETURN_NOT_OK(fs_manager->Open());

      scoped_refptr<Log> log;
      SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
      RETURN_NOT_OK(Log::Open(LogOptions(),
                              fs_manager.get(),
                              /*file_cache*/nullptr,
                              kTestTablet,
                              schema_ptr,
                              0, // schema_version
                              /*metric_entity*/nullptr,
                              &log));
      logs_.emplace_back(std::move(log));
      fs_managers_.push_back(std::move(fs_manager));
    }
    return Status::OK();
  }

  // Builds a configuration of 'num' voters.
  RaftConfigPB BuildRaftConfigPB(int num) {
    RaftConfigPB raft_config;
    for (int i = 0; i < num; i++) {
      RaftPeerPB* peer_pb = raft_config.add_peers();
      peer_pb->set_member_type(RaftPeerPB::VOTER);
      peer_pb->set_permanent_uuid(fs_managers_[i]->uuid());
      HostPortPB* hp = peer_pb->mutable_last_known_addr();
      hp->set_host(Substitute("peer-$0.fake-domain-for-tests", i));
      hp->set_port(0);
    }
    return raft_config;
  }

  Status BuildPeers() {
    CHECK_EQ(config_.peers_size(), fs_managers_.size());
    for (int i = 0; i < config_.peers_size(); i++) {
      FsManager* fs = fs_managers_[i].get();
      scoped_refptr<ConsensusMetadataManager> cmeta_manager(
          new ConsensusMetadataManager(fs));

      RETURN_NOT_OK(cmeta_manager->Create(kTestTablet, config_, kMinimumTerm));

      RaftPeerPB* local_peer_pb;
      RETURN_NOT_OK(GetRaftConfigMember(&config_, fs->uuid(), &local_peer_pb));

      shared_ptr<RaftConsensus> peer;
      ServerContext ctx({ /*quiescing*/nullptr,
                          /*num_leaders*/nullptr,
                          raft_pool_.get() });
      RETURN_NOT_OK(RaftConsensus::Create(options_,
                                          config_.peers(i),
                                          std::move(cmeta_manager),
                                          std::move(ctx),
                                          &peer));
      peers_->AddPeer(config_.peers(i).permanent_uuid(), peer);
    }
    return Status::OK();
  }

  Status StartPeers() {
    ConsensusBootstrapInfo boot_info;

    TestPeerMap all_peers = peers_->GetPeerMapCopy();
    for (int i = 0; i < config_.peers_size(); i++) {
      shared_ptr<RaftConsensus> peer;
      RETURN_NOT_OK(peers_->GetPeerByIdx(i, &peer));

      unique_ptr<PeerProxyFactory> proxy_factory(
          new LocalTestPeerProxyFactory(peers_.get()));
      unique_ptr<TimeManager> time_manager(
          new TimeManager(&clock_, Timestamp::kMin));
      unique_ptr<TestOpFactory> op_factory(
          new TestOpFactory(logs_[i].get()));
      op_factory->SetConsensus(peer.get());
      op_factories_.emplace_back(std::move(op_factory));

      RETURN_NOT_OK(peer->Start(
          boot_info,
          std::move(proxy_factory),
          logs_[i],
          std::move(time_manager),
          op_factories_.back().get(),
          metric_entity_,
          &DoNothing));
    }
    return Status::OK();
  }

  Status BuildConfig(int num) {
    RETURN_NOT_OK(BuildFsManagersAndLogs(num));
    BuildInitialRaftConfigPB(num);
    RETURN_NOT_OK(BuildPeers());
    return Status::OK();
  }

  Status BuildAndStartConfig(int num) {
    RETURN_NOT_OK(BuildConfig(num));
    RETURN_NOT_OK(StartPeers());

    // Automatically elect the last node in the list.
    const int kLeaderIdx = num - 1;
    shared_ptr<RaftConsensus> leader;
    RETURN_NOT_OK(peers_->GetPeerByIdx(kLeaderIdx, &leader));
    RETURN_NOT_OK(leader->EmulateElectionForTests());
    return Status::OK();
  }

  LocalTestPeerProxy* GetLeaderProxyToPeer(int peer_idx, int leader_idx) {
    shared_ptr<RaftConsensus> follower;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &follower));
    shared_ptr<RaftConsensus> leader;
    CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));
    for (LocalTestPeerProxy* proxy : down_cast<LocalTestPeerProxyFactory*>(
        leader->peer_proxy_factory_.get())->GetProxies()) {
      if (proxy->GetTarget() == follower->peer_uuid()) {
        return proxy;
      }
    }
    CHECK(false) << "Proxy not found";
    return nullptr;
  }

  Status AppendDummyMessage(int peer_idx,
                            scoped_refptr<ConsensusRound>* round) {
    unique_ptr<ReplicateMsg> msg(new ReplicateMsg());
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request();
    msg->set_timestamp(clock_.Now().ToUint64());

    shared_ptr<RaftConsensus> peer;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &peer));

    // Use a latch in place of a op callback.
    unique_ptr<Synchronizer> sync(new Synchronizer());
    *round = peer->NewRound(std::move(msg), sync->AsStatusCallback());
    EmplaceOrDie(&syncs_, round->get(), std::move(sync));
    RETURN_NOT_OK_PREPEND(peer->Replicate(round->get()),
                          Substitute("Unable to replicate to peer $0", peer_idx));
    return Status::OK();
  }

  static void FireSharedSynchronizer(const shared_ptr<Synchronizer>& sync, const Status& s) {
    sync->StatusCB(s);
  }

  Status CommitDummyMessage(int peer_idx,
                            ConsensusRound* round,
                            shared_ptr<Synchronizer>* commit_sync = nullptr) {
    StatusCallback commit_callback;
    if (commit_sync != nullptr) {
      shared_ptr<Synchronizer> sync(std::make_shared<Synchronizer>());
      commit_callback = [sync](const Status& s) { FireSharedSynchronizer(sync, s); };
      *commit_sync = std::move(sync);
    } else {
      commit_callback = &DoNothingStatusCB;
    }

    CommitMsg msg;
    msg.set_op_type(NO_OP);
    msg.mutable_commited_op_id()->CopyFrom(round->id());
    CHECK_OK(logs_[peer_idx]->AsyncAppendCommit(msg, commit_callback));
    return Status::OK();
  }

  Status WaitForReplicate(ConsensusRound* round) {
    return FindOrDie(syncs_, round)->Wait();
  }

  Status TimedWaitForReplicate(ConsensusRound* round, const MonoDelta& delta) {
    return FindOrDie(syncs_, round)->WaitFor(delta);
  }

  void WaitForReplicateIfNotAlreadyPresent(const OpId& to_wait_for, int peer_idx) {
    shared_ptr<RaftConsensus> peer;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &peer));
    while (true) {
      if (OpIdCompare(peer->queue_->GetLastOpIdInLog(), to_wait_for) >= 0) {
        return;
      }
      SleepFor(MonoDelta::FromMilliseconds(1));
    }
  }

  // Waits for an operation to be (database) committed in the replica at index
  // 'peer_idx'. If the operation was already committed this returns immediately.
  void WaitForCommitIfNotAlreadyPresent(int64_t to_wait_for,
                                        int peer_idx,
                                        int leader_idx) {
    MonoDelta timeout(MonoDelta::FromSeconds(10));
    MonoTime start(MonoTime::Now());

    shared_ptr<RaftConsensus> peer;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &peer));

    int backoff_exp = 0;
    const int kMaxBackoffExp = 8;
    OpId committed = MinimumOpId();
    while (true) {
      boost::optional<OpId> opt_committed = peer->GetLastOpId(COMMITTED_OPID);
      if (opt_committed) {
        committed = *opt_committed;
        if (committed.index() >= to_wait_for) {
          return;
        }
      }
      if (MonoTime::Now() > (start + timeout)) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(1LL << backoff_exp));
      backoff_exp = std::min(backoff_exp + 1, kMaxBackoffExp);
    }

    LOG(ERROR) << "Max timeout reached (" << timeout.ToString() << ") while waiting for commit of "
               << "op " << to_wait_for << " on replica. Last committed op on replica: "
               << committed.index() << ". Dumping state and quitting.";
    vector<string> lines;
    shared_ptr<RaftConsensus> leader;
    CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));
    for (const string& line : lines) {
      LOG(ERROR) << line;
    }

    // Gather the replica and leader operations for printing
    LogEntries replica_ops;
    GatherLogEntries(peer_idx, logs_[peer_idx], &replica_ops);
    LogEntries leader_ops;
    GatherLogEntries(leader_idx, logs_[leader_idx], &leader_ops);
    SCOPED_TRACE(PrintOnError(replica_ops, Substitute("local peer ($0)", peer->peer_uuid())));
    SCOPED_TRACE(PrintOnError(leader_ops, Substitute("leader (peer-$0)", leader_idx)));
    FAIL() << "Replica did not commit.";
  }

  // Used in ReplicateSequenceOfMessages() to specify whether
  // we should wait for all replicas to have replicated the
  // sequence or just a majority.
  enum ReplicateWaitMode {
    WAIT_FOR_ALL_REPLICAS,
    WAIT_FOR_MAJORITY
  };

  // Used in ReplicateSequenceOfMessages() to specify whether
  // we should also commit the messages in the sequence
  enum CommitMode {
    DONT_COMMIT,
    COMMIT_ONE_BY_ONE
  };

  // Replicates a sequence of messages to the peer passed as leader.
  // Optionally waits for the messages to be replicated to followers.
  // 'last_op_id' is set to the id of the last replicated operation.
  // The operations are only committed if 'commit_one_by_one' is true.
  void ReplicateSequenceOfMessages(int seq_size,
                                   int leader_idx,
                                   ReplicateWaitMode wait_mode,
                                   CommitMode commit_mode,
                                   OpId* last_op_id,
                                   vector<scoped_refptr<ConsensusRound>>* rounds,
                                   shared_ptr<Synchronizer>* commit_sync = nullptr) {
    for (int i = 0; i < seq_size; i++) {
      scoped_refptr<ConsensusRound> round;
      ASSERT_OK(AppendDummyMessage(leader_idx, &round));
      ASSERT_OK(WaitForReplicate(round.get()));
      last_op_id->CopyFrom(round->id());
      if (commit_mode == COMMIT_ONE_BY_ONE) {
        CommitDummyMessage(leader_idx, round.get(), commit_sync);
      }
      rounds->push_back(round);
    }

    if (wait_mode == WAIT_FOR_ALL_REPLICAS) {
      shared_ptr<RaftConsensus> leader;
      CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));

      TestPeerMap all_peers = peers_->GetPeerMapCopy();
      int i = 0;
      for (const TestPeerMap::value_type& entry : all_peers) {
        if (entry.second->peer_uuid() != leader->peer_uuid()) {
          WaitForReplicateIfNotAlreadyPresent(*last_op_id, i);
        }
        i++;
      }
    }
  }

  void GatherLogEntries(int idx, const scoped_refptr<Log>& log,
                        LogEntries* entries) {
    ASSERT_OK(log->WaitUntilAllFlushed());
    log->Close();
    shared_ptr<LogReader> log_reader;
    ASSERT_OK(log::LogReader::Open(fs_managers_[idx].get(),
                                   /*index*/nullptr,
                                   kTestTablet,
                                   metric_entity_.get(),
                                   /*file_cache*/nullptr,
                                   &log_reader));
    log::SegmentSequence segments;
    log_reader->GetSegmentsSnapshot(&segments);

    LogEntries ret;
    for (const log::SegmentSequence::value_type& entry : segments) {
      ASSERT_OK(entry->ReadEntries(&ret));
    }
    *entries = std::move(ret);
  }

  // Verifies that the replica's log match the leader's. This deletes the
  // peers (so we're sure that no further writes occur) and closes the logs
  // so it must be the very last thing to run, in a test.
  void VerifyLogs(int leader_idx, int first_replica_idx, int last_replica_idx) {
    // Wait for in-flight ops to be done. We're destroying the
    // peers next and leader ops won't be able to commit anymore.
    for (const auto& factory : op_factories_) {
      factory->WaitDone();
    }

    // Shut down all the peers.
    TestPeerMap all_peers = peers_->GetPeerMapCopy();
    for (const TestPeerMap::value_type& entry : all_peers) {
      entry.second->Shutdown();
    }

    LogEntries leader_entries;
    GatherLogEntries(leader_idx, logs_[leader_idx], &leader_entries);
    shared_ptr<RaftConsensus> leader;
    CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));

    for (int replica_idx = first_replica_idx; replica_idx < last_replica_idx; replica_idx++) {
      LogEntries replica_entries;
      GatherLogEntries(replica_idx, logs_[replica_idx], &replica_entries);

      shared_ptr<RaftConsensus> replica;
      CHECK_OK(peers_->GetPeerByIdx(replica_idx, &replica));
      VerifyReplica(leader_entries,
                    replica_entries,
                    leader->peer_uuid(),
                    replica->peer_uuid());
    }
  }

  void ExtractReplicateIds(const LogEntries& entries,
                           vector<OpId>* ids) {
    ids->reserve(entries.size() / 2);
    for (const auto& entry : entries) {
      if (entry->has_replicate()) {
        ids->push_back(entry->replicate().id());
      }
    }
  }

  void VerifyReplicateOrderMatches(const LogEntries& leader_entries,
                                   const LogEntries& replica_entries) {
    vector<OpId> leader_ids, replica_ids;
    ExtractReplicateIds(leader_entries, &leader_ids);
    ExtractReplicateIds(replica_entries, &replica_ids);
    ASSERT_EQ(leader_ids.size(), replica_ids.size());
    for (int i = 0; i < leader_ids.size(); i++) {
      ASSERT_EQ(SecureShortDebugString(leader_ids[i]),
                SecureShortDebugString(replica_ids[i]));
    }
  }

  void VerifyNoCommitsBeforeReplicates(const LogEntries& entries) {
    std::unordered_set<OpId, OpIdHashFunctor, OpIdEqualsFunctor> replication_ops;

    for (const auto& entry : entries) {
      if (entry->has_replicate()) {
        ASSERT_TRUE(InsertIfNotPresent(&replication_ops, entry->replicate().id()))
          << "REPLICATE op id showed up twice: " << SecureShortDebugString(*entry);
      } else if (entry->has_commit()) {
        ASSERT_EQ(1, replication_ops.erase(entry->commit().commited_op_id()))
          << "COMMIT came before associated REPLICATE: " << SecureShortDebugString(*entry);
      }
    }
  }

  void VerifyReplica(const LogEntries& leader_entries,
                     const LogEntries& replica_entries,
                     const string& leader_name,
                     const string& replica_name) {
    SCOPED_TRACE(PrintOnError(leader_entries, Substitute("Leader: $0", leader_name)));
    SCOPED_TRACE(PrintOnError(replica_entries, Substitute("Replica: $0", replica_name)));

    // Check that the REPLICATE messages come in the same order on both nodes.
    VerifyReplicateOrderMatches(leader_entries, replica_entries);

    // Check that no COMMIT precedes its related REPLICATE on both the replica
    // and leader.
    VerifyNoCommitsBeforeReplicates(replica_entries);
    VerifyNoCommitsBeforeReplicates(leader_entries);
  }

  string PrintOnError(const LogEntries& replica_entries,
                      const string& replica_id) {
    string ret = "";
    SubstituteAndAppend(&ret, "$1 log entries for replica $0:\n",
                        replica_id, replica_entries.size());
    for (const auto& replica_entry : replica_entries) {
      StrAppend(&ret, "Replica log entry: ", SecureShortDebugString(*replica_entry), "\n");
    }
    return ret;
  }

  // Read the ConsensusMetadata for the given peer from disk.
  scoped_refptr<ConsensusMetadata> ReadConsensusMetadataFromDisk(int peer_index) {
    FsManager* fs = fs_managers_[peer_index].get();
    scoped_refptr<ConsensusMetadata> cmeta;
    CHECK_OK(ConsensusMetadata::Load(fs, kTestTablet, fs->uuid(), &cmeta));
    return cmeta;
  }

  // Assert that the durable term == term and that the peer that got the vote == voted_for.
  void AssertDurableTermAndVote(int peer_index, int64_t term, const std::string& voted_for) {
    scoped_refptr<ConsensusMetadata> cmeta = ReadConsensusMetadataFromDisk(peer_index);
    ASSERT_EQ(term, cmeta->current_term());
    ASSERT_EQ(voted_for, cmeta->voted_for());
  }

  // Assert that the durable term == term and that the peer has not yet voted.
  void AssertDurableTermWithoutVote(int peer_index, int64_t term) {
    scoped_refptr<ConsensusMetadata> cmeta = ReadConsensusMetadataFromDisk(peer_index);
    ASSERT_EQ(term, cmeta->current_term());
    ASSERT_FALSE(cmeta->has_voted_for());
  }

  ~RaftConsensusQuorumTest() {
    peers_->Clear();
  }

 protected:
  ConsensusOptions options_;
  RaftConfigPB config_;
  OpId initial_id_;
  vector<shared_ptr<MemTracker>> parent_mem_trackers_;
  vector<unique_ptr<FsManager>> fs_managers_;
  vector<scoped_refptr<Log> > logs_;
  unique_ptr<ThreadPool> raft_pool_;
  unique_ptr<TestPeerMapManager> peers_;
  vector<unique_ptr<TestOpFactory>> op_factories_;
  clock::LogicalClock clock_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  const Schema schema_;
  std::unordered_map<ConsensusRound*, unique_ptr<Synchronizer>> syncs_;
};

// Tests Replicate/Commit a single message through the leader.
TEST_F(RaftConsensusQuorumTest, TestFollowersReplicateAndCommitMessage) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderIdx = 2;

  ASSERT_OK(BuildAndStartConfig(3));

  OpId last_op_id;
  vector<scoped_refptr<ConsensusRound>> rounds;
  shared_ptr<Synchronizer> commit_sync;
  NO_FATALS(ReplicateSequenceOfMessages(
      1, kLeaderIdx, WAIT_FOR_ALL_REPLICAS, DONT_COMMIT,
      &last_op_id, &rounds, &commit_sync));

  // Commit the operation
  ASSERT_OK(CommitDummyMessage(kLeaderIdx, rounds[0].get(), &commit_sync));

  // Wait for everyone to commit the operations.

  // We need to make sure the CommitMsg lands on the leaders log or the
  // verification will fail. Since CommitMsgs are appended to the replication
  // queue there is a scenario where they land in the followers log before
  // landing on the leader's log. However we know that they are durable
  // on the leader when the commit callback is triggered.
  // We thus wait for the commit callback to trigger, ensuring durability
  // on the leader and then for the commits to be present on the replicas.
  ASSERT_OK(commit_sync->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower0Idx, kLeaderIdx);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower1Idx, kLeaderIdx);
  VerifyLogs(2, 0, 1);
}

// Tests Replicate/Commit a sequence of messages through the leader.
// First a sequence of replicates and then a sequence of commits.
TEST_F(RaftConsensusQuorumTest, TestFollowersReplicateAndCommitSequence) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderIdx = 2;

  int seq_size = AllowSlowTests() ? 1000 : 100;

  ASSERT_OK(BuildAndStartConfig(3));

  OpId last_op_id;
  vector<scoped_refptr<ConsensusRound>> rounds;
  shared_ptr<Synchronizer> commit_sync;

  NO_FATALS(ReplicateSequenceOfMessages(
      seq_size, kLeaderIdx, WAIT_FOR_ALL_REPLICAS, DONT_COMMIT,
      &last_op_id, &rounds, &commit_sync));

  // Commit the operations, but wait for the replicates to finish first
  for (const scoped_refptr<ConsensusRound>& round : rounds) {
    ASSERT_OK(CommitDummyMessage(kLeaderIdx, round.get(), &commit_sync));
  }

  // See comment at the end of TestFollowersReplicateAndCommitMessage
  // for an explanation on this waiting sequence.
  ASSERT_OK(commit_sync->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower0Idx, kLeaderIdx);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower1Idx, kLeaderIdx);
  VerifyLogs(2, 0, 1);
}

TEST_F(RaftConsensusQuorumTest, TestConsensusContinuesIfAMinorityFallsBehind) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderIdx = 2;

  ASSERT_OK(BuildAndStartConfig(3));

  OpId last_replicate;
  vector<scoped_refptr<ConsensusRound>> rounds;
  {
    // lock one of the replicas down by obtaining the state lock
    // and never letting it go.
    shared_ptr<RaftConsensus> follower0;
    CHECK_OK(peers_->GetPeerByIdx(kFollower0Idx, &follower0));

    RaftConsensus::LockGuard l(follower0->lock_);

    // If the locked replica would stop consensus we would hang here
    // as we wait for operations to be replicated to a majority.
    NO_FATALS(ReplicateSequenceOfMessages(
        10, kLeaderIdx, WAIT_FOR_MAJORITY, COMMIT_ONE_BY_ONE,
        &last_replicate, &rounds));

    // Follower 1 should be fine (Were we to wait for follower0's replicate
    // this would hang here). We know he must have replicated but make sure
    // by calling Wait().
    WaitForReplicateIfNotAlreadyPresent(last_replicate, kFollower1Idx);
    WaitForCommitIfNotAlreadyPresent(last_replicate.index(), kFollower1Idx, kLeaderIdx);
  }

  // After we let the lock go the remaining follower should get up-to-date
  WaitForReplicateIfNotAlreadyPresent(last_replicate, kFollower0Idx);
  WaitForCommitIfNotAlreadyPresent(last_replicate.index(), kFollower0Idx, kLeaderIdx);
  VerifyLogs(2, 0, 1);
}

TEST_F(RaftConsensusQuorumTest, TestConsensusStopsIfAMajorityFallsBehind) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderIdx = 2;

  ASSERT_OK(BuildAndStartConfig(3));

  OpId last_op_id;

  scoped_refptr<ConsensusRound> round;
  {
    // lock two of the replicas down by obtaining the state locks
    // and never letting them go.
    shared_ptr<RaftConsensus> follower0;
    CHECK_OK(peers_->GetPeerByIdx(kFollower0Idx, &follower0));
    RaftConsensus::LockGuard l_0(follower0->lock_);

    shared_ptr<RaftConsensus> follower1;
    CHECK_OK(peers_->GetPeerByIdx(kFollower1Idx, &follower1));
    RaftConsensus::LockGuard l_1(follower1->lock_);

    // Append a single message to the queue
    ASSERT_OK(AppendDummyMessage(kLeaderIdx, &round));
    last_op_id.CopyFrom(round->id());
    // This should timeout.
    Status status = TimedWaitForReplicate(round.get(), MonoDelta::FromMilliseconds(500));
    ASSERT_TRUE(status.IsTimedOut());
  }

  // After we release the locks the operation should replicate to all replicas
  // and we commit.
  ASSERT_OK(WaitForReplicate(round.get()));
  CommitDummyMessage(kLeaderIdx, round.get());

  // Assert that everything was ok
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower0Idx);
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower1Idx);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower0Idx, kLeaderIdx);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower1Idx, kLeaderIdx);
  VerifyLogs(2, 0, 1);
}

// If some communication error happens the leader will resend the request to the
// peers. This tests that the peers handle repeated requests.
TEST_F(RaftConsensusQuorumTest, TestReplicasHandleCommunicationErrors) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderIdx = 2;

  ASSERT_OK(BuildAndStartConfig(3));

  OpId last_op_id;

  // Append a dummy message, with faults injected on the first attempt
  // to send the message.
  scoped_refptr<ConsensusRound> round;
  GetLeaderProxyToPeer(kFollower0Idx, kLeaderIdx)->InjectCommFaultLeaderSide();
  GetLeaderProxyToPeer(kFollower1Idx, kLeaderIdx)->InjectCommFaultLeaderSide();
  ASSERT_OK(AppendDummyMessage(kLeaderIdx, &round));

  // We should successfully replicate it due to retries.
  ASSERT_OK(WaitForReplicate(round.get()));

  GetLeaderProxyToPeer(kFollower0Idx, kLeaderIdx)->InjectCommFaultLeaderSide();
  GetLeaderProxyToPeer(kFollower1Idx, kLeaderIdx)->InjectCommFaultLeaderSide();
  ASSERT_OK(CommitDummyMessage(kLeaderIdx, round.get()));

  // The commit should eventually reach both followers as well.
  last_op_id = round->id();
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower0Idx, kLeaderIdx);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower1Idx, kLeaderIdx);

  // Append a sequence of messages, and keep injecting errors into the
  // replica proxies.
  vector<scoped_refptr<ConsensusRound>> rounds;
  shared_ptr<Synchronizer> commit_sync;
  for (int i = 0; i < 100; i++) {
    scoped_refptr<ConsensusRound> round;
    ASSERT_OK(AppendDummyMessage(kLeaderIdx, &round));
    ConsensusRound* round_ptr = round.get();
    last_op_id.CopyFrom(round->id());
    rounds.push_back(round);

    // inject comm faults
    if (i % 2 == 0) {
      GetLeaderProxyToPeer(kFollower0Idx, kLeaderIdx)->InjectCommFaultLeaderSide();
    } else {
      GetLeaderProxyToPeer(kFollower1Idx, kLeaderIdx)->InjectCommFaultLeaderSide();
    }

    ASSERT_OK(WaitForReplicate(round_ptr));
    ASSERT_OK(CommitDummyMessage(kLeaderIdx, round_ptr, &commit_sync));
  }

  // Assert last operation was correctly replicated and committed.
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower0Idx);
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower1Idx);

  // See comment at the end of TestFollowersReplicateAndCommitMessage
  // for an explanation on this waiting sequence.
  ASSERT_OK(commit_sync->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower0Idx, kLeaderIdx);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), kFollower1Idx, kLeaderIdx);
  VerifyLogs(2, 0, 1);
}

// In this test we test the ability of the leader to send heartbeats
// to replicas by simply pushing nothing after the configuration round
// and still expecting for the replicas' Update() method to be called.
TEST_F(RaftConsensusQuorumTest, TestLeaderHeartbeats) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderIdx = 2;

  ASSERT_OK(BuildConfig(3));

  shared_ptr<RaftConsensus> follower0;
  CHECK_OK(peers_->GetPeerByIdx(kFollower0Idx, &follower0));
  shared_ptr<RaftConsensus> follower1;
  CHECK_OK(peers_->GetPeerByIdx(kFollower1Idx, &follower1));

  ASSERT_OK(StartPeers());

  shared_ptr<RaftConsensus> leader;
  CHECK_OK(peers_->GetPeerByIdx(kLeaderIdx, &leader));
  ASSERT_OK(leader->EmulateElectionForTests());

  // Wait for the config round to get committed and count the number
  // of update calls, calls after that will be heartbeats.
  OpId config_round;
  config_round.set_term(1);
  config_round.set_index(1);
  WaitForCommitIfNotAlreadyPresent(config_round.index(), kFollower0Idx, kLeaderIdx);
  WaitForCommitIfNotAlreadyPresent(config_round.index(), kFollower1Idx, kLeaderIdx);

  int repl0_init_count = follower0->update_calls_for_tests();
  int repl1_init_count = follower1->update_calls_for_tests();

  // Now wait for about 4 times the heartbeat period; the counters
  // should have increased between 3 to 8 times.
  //
  // Why the variance? Heartbeat timing is jittered such that the period
  // between heartbeats can be anywhere from 3/4 to 5/4 the interval.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 4LL));

  int repl0_final_count = follower0->update_calls_for_tests();
  int repl1_final_count = follower1->update_calls_for_tests();

  ASSERT_GE(repl0_final_count - repl0_init_count, 3);
  ASSERT_LE(repl0_final_count - repl0_init_count, 8);
  ASSERT_GE(repl1_final_count - repl1_init_count, 3);
  ASSERT_LE(repl1_final_count - repl1_init_count, 8);

  VerifyLogs(2, 0, 1);
}

// After creating the initial configuration, this test writes a small sequence
// of messages to the initial leader. It then shuts down the current
// leader, makes another peer become leader and writes a sequence of
// messages to it. The new leader and the follower should agree on the
// sequence of messages.
TEST_F(RaftConsensusQuorumTest, TestLeaderElectionWithQuiescedQuorum) {
  const int kInitialNumPeers = 5;
  ASSERT_OK(BuildAndStartConfig(kInitialNumPeers));

  OpId last_op_id;
  shared_ptr<Synchronizer> last_commit_sync;
  vector<scoped_refptr<ConsensusRound>> rounds;

  // Loop twice, successively shutting down the previous leader.
  for (int current_config_size = kInitialNumPeers;
       current_config_size >= kInitialNumPeers - 1;
       current_config_size--) {
    NO_FATALS(ReplicateSequenceOfMessages(
        10, current_config_size - 1, WAIT_FOR_ALL_REPLICAS, COMMIT_ONE_BY_ONE,
        &last_op_id, &rounds, &last_commit_sync));

    // Make sure the last operation is committed everywhere
    ASSERT_OK(last_commit_sync->Wait());
    for (int i = 0; i < current_config_size - 1; i++) {
      WaitForCommitIfNotAlreadyPresent(last_op_id.index(), i, current_config_size - 1);
    }

    // Now shutdown the current leader.
    LOG(INFO) << "Shutting down current leader with index " << (current_config_size - 1);
    shared_ptr<RaftConsensus> current_leader;
    CHECK_OK(peers_->GetPeerByIdx(current_config_size - 1, &current_leader));
    current_leader->Shutdown();
    peers_->RemovePeer(current_leader->peer_uuid());

    // ... and make the peer before it become leader.
    shared_ptr<RaftConsensus> new_leader;
    CHECK_OK(peers_->GetPeerByIdx(current_config_size - 2, &new_leader));

    // This will force an election in which we expect to make the last
    // non-shutdown peer in the list become leader.
    int64_t flush_count_before =
        new_leader->consensus_metadata_for_tests()->flush_count_for_tests();
    LOG(INFO) << "Running election for future leader with index " << (current_config_size - 1);
    ASSERT_OK(new_leader->StartElection(RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE,
                                        RaftConsensus::EXTERNAL_REQUEST));
    ASSERT_OK(new_leader->WaitUntilLeader(MonoDelta::FromSeconds(15)));
    LOG(INFO) << "Election won";
    int64_t flush_count_after =
        new_leader->consensus_metadata_for_tests()->flush_count_for_tests();
    ASSERT_EQ(flush_count_after, flush_count_before + 1)
        << "Expected only one consensus metadata flush for a leader election";

    // ... replicating a set of messages to the new leader should now be possible.
    NO_FATALS(ReplicateSequenceOfMessages(
        10, current_config_size - 2, WAIT_FOR_MAJORITY, COMMIT_ONE_BY_ONE,
        &last_op_id, &rounds, &last_commit_sync));

    // Make sure the last operation is committed everywhere
    ASSERT_OK(last_commit_sync->Wait());
    for (int i = 0; i < current_config_size - 2; i++) {
      WaitForCommitIfNotAlreadyPresent(last_op_id.index(), i, current_config_size - 2);
    }
  }
  // We can only verify the logs of the peers that were not killed, due to the
  // old leaders being out-of-date now.
  VerifyLogs(2, 0, 1);
}

TEST_F(RaftConsensusQuorumTest, TestReplicasEnforceTheLogMatchingProperty) {
  ASSERT_OK(BuildAndStartConfig(3));

  OpId last_op_id;
  shared_ptr<Synchronizer> last_commit_sync;
  vector<scoped_refptr<ConsensusRound>> rounds;
  NO_FATALS(ReplicateSequenceOfMessages(
      10, 2, WAIT_FOR_ALL_REPLICAS, COMMIT_ONE_BY_ONE,
      &last_op_id, &rounds, &last_commit_sync));

  // Make sure the last operation is committed everywhere
  ASSERT_OK(last_commit_sync->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), 0, 2);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), 1, 2);

  // Now replicas should only accept operations with
  // 'last_id' as the preceding id.
  ConsensusRequestPB req;
  ConsensusResponsePB resp;

  shared_ptr<RaftConsensus> leader;
  CHECK_OK(peers_->GetPeerByIdx(2, &leader));

  shared_ptr<RaftConsensus> follower;
  CHECK_OK(peers_->GetPeerByIdx(0, &follower));

  req.set_caller_uuid(leader->peer_uuid());
  req.set_caller_term(last_op_id.term());
  req.mutable_preceding_id()->CopyFrom(last_op_id);
  req.set_committed_index(last_op_id.index());
  req.set_all_replicated_index(0);

  // Send a request with the next index.
  ReplicateMsg* replicate = req.add_ops();
  replicate->set_timestamp(clock_.Now().ToUint64());
  OpId* id = replicate->mutable_id();
  id->set_term(last_op_id.term());
  id->set_index(last_op_id.index() + 1);
  replicate->set_op_type(NO_OP);

  // Since the req adds the next op, the leader must have also appended it.
  req.set_last_idx_appended_to_leader(id->index());

  // Appending this message to peer0 should work and update
  // its 'last_received' to 'id'.
  ASSERT_OK(follower->Update(&req, &resp));
  ASSERT_TRUE(OpIdEquals(resp.status().last_received(), *id));
  ASSERT_EQ(0, follower->queue_->metrics_.num_ops_behind_leader->value());

  // Now skip one message in the same term. The replica should
  // complain with the right error message.
  req.mutable_preceding_id()->set_index(id->index() + 1);
  id->set_index(id->index() + 2);
  // Appending this message to peer0 should return a Status::OK
  // but should contain an error referring to the log matching property.
  ASSERT_OK(follower->Update(&req, &resp));
  ASSERT_TRUE(resp.has_status());
  ASSERT_TRUE(resp.status().has_error());
  ASSERT_EQ(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH, resp.status().error().code());
  ASSERT_STR_CONTAINS(resp.status().error().status().message(),
                      "Log matching property violated");
}

// Test that RequestVote performs according to "spec".
TEST_F(RaftConsensusQuorumTest, TestRequestVote) {
  ASSERT_OK(BuildAndStartConfig(3));

  OpId last_op_id;
  shared_ptr<Synchronizer> last_commit_sync;
  vector<scoped_refptr<ConsensusRound>> rounds;
  NO_FATALS(ReplicateSequenceOfMessages(
      10, 2, WAIT_FOR_ALL_REPLICAS, COMMIT_ONE_BY_ONE,
      &last_op_id, &rounds, &last_commit_sync));

  // Make sure the last operation is committed everywhere
  ASSERT_OK(last_commit_sync->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), 0, 2);
  WaitForCommitIfNotAlreadyPresent(last_op_id.index(), 1, 2);

  // Ensure last-logged OpId is > (0,0).
  ASSERT_TRUE(OpIdLessThan(MinimumOpId(), last_op_id));

  const int kPeerIndex = 1;
  shared_ptr<RaftConsensus> peer;
  CHECK_OK(peers_->GetPeerByIdx(kPeerIndex, &peer));
  auto flush_count = [&]() {
    return peer->consensus_metadata_for_tests()->flush_count_for_tests();
  };

  VoteRequestPB request;
  request.set_tablet_id(kTestTablet);
  request.mutable_candidate_status()->mutable_last_received()->CopyFrom(last_op_id);

  // Test that the replica won't vote since it has recently heard from
  // a valid leader.
  int flush_count_before = flush_count();
  VoteResponsePB response;
  request.set_candidate_uuid(fs_managers_[0]->uuid());
  request.set_candidate_term(last_op_id.term() + 1);
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_EQ(ConsensusErrorPB::LEADER_IS_ALIVE, response.consensus_error().code());
  ASSERT_EQ(0, flush_count() - flush_count_before)
      << "A rejected vote should not flush metadata";

  // Test that replicas only vote yes for a single peer per term.

  // Indicate that replicas should vote even if they think another leader is alive.
  // This will allow the rest of the requests in the test to go through.
  flush_count_before = flush_count();
  request.set_ignore_live_leader(true);
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_TRUE(response.vote_granted());
  ASSERT_EQ(last_op_id.term() + 1, response.responder_term());
  NO_FATALS(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 1,
                                     fs_managers_[0]->uuid()));
  ASSERT_EQ(1, flush_count() - flush_count_before)
      << "A granted vote should flush only once";

  // Ensure we get same response for same term and same UUID.
  response.Clear();
  flush_count_before = flush_count();
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_TRUE(response.vote_granted());
  ASSERT_EQ(0, flush_count() - flush_count_before)
      << "Confirming a previous vote should not flush";

  // Ensure we get a "no" for a different candidate UUID for that term.
  flush_count_before = flush_count();
  response.Clear();
  request.set_candidate_uuid(fs_managers_[2]->uuid());
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_TRUE(response.has_consensus_error());
  ASSERT_EQ(ConsensusErrorPB::ALREADY_VOTED, response.consensus_error().code());
  ASSERT_EQ(last_op_id.term() + 1, response.responder_term());
  NO_FATALS(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 1,
                                     fs_managers_[0]->uuid()));
  ASSERT_EQ(0, flush_count() - flush_count_before)
      << "Rejected votes for same term should not flush";

  //
  // Test that replicas refuse votes for an old term.
  //

  // Increase the term of our candidate, which will cause the voter replica to
  // increase its own term to match.
  flush_count_before = flush_count();
  request.set_candidate_uuid(fs_managers_[0]->uuid());
  request.set_candidate_term(last_op_id.term() + 2);
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_TRUE(response.vote_granted());
  ASSERT_EQ(last_op_id.term() + 2, response.responder_term());
  NO_FATALS(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 2,
                                     fs_managers_[0]->uuid()));
  ASSERT_EQ(1, flush_count() - flush_count_before)
      << "Accepted votes with increased term should flush once";

  // Now try the old term.
  // Note: Use the peer who "won" the election on the previous term (peer 0),
  // although in practice the impl does not store historical vote data.
  flush_count_before = flush_count();
  request.set_candidate_term(last_op_id.term() + 1);
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_TRUE(response.has_consensus_error());
  ASSERT_EQ(ConsensusErrorPB::INVALID_TERM, response.consensus_error().code());
  ASSERT_EQ(last_op_id.term() + 2, response.responder_term());
  NO_FATALS(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 2,
                                     fs_managers_[0]->uuid()));
  ASSERT_EQ(0, flush_count() - flush_count_before)
      << "Rejected votes for old terms should not flush";

  // Ensure that replicas don't change term or flush any metadata for a pre-election
  // request, even when they vote "yes".
  flush_count_before = flush_count();
  request.set_candidate_term(last_op_id.term() + 3);
  request.set_is_pre_election(true);
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_TRUE(response.vote_granted());
  ASSERT_FALSE(response.has_consensus_error());
  ASSERT_EQ(last_op_id.term() + 2, response.responder_term());
  NO_FATALS(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 2,
                                     fs_managers_[0]->uuid()));
  ASSERT_EQ(0, flush_count() - flush_count_before)
      << "Pre-elections should not flush";
  request.set_is_pre_election(false);

  //
  // Ensure replicas vote no for an old op index.
  //

  flush_count_before = flush_count();
  request.set_candidate_uuid(fs_managers_[0]->uuid());
  request.set_candidate_term(last_op_id.term() + 3);
  request.mutable_candidate_status()->mutable_last_received()->CopyFrom(MinimumOpId());
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request,
                              TabletVotingState(boost::none, tablet::TABLET_DATA_READY),
                              &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_TRUE(response.has_consensus_error());
  ASSERT_EQ(ConsensusErrorPB::LAST_OPID_TOO_OLD, response.consensus_error().code());
  ASSERT_EQ(last_op_id.term() + 3, response.responder_term());
  NO_FATALS(AssertDurableTermWithoutVote(kPeerIndex, last_op_id.term() + 3));
  ASSERT_EQ(1, flush_count() - flush_count_before)
      << "Rejected votes for old op index but new term should flush once.";


  // Send a "heartbeat" to the peer. It should be rejected.
  ConsensusRequestPB req;
  req.set_caller_term(last_op_id.term());
  req.set_caller_uuid(fs_managers_[0]->uuid());
  req.set_committed_index(last_op_id.index());
  req.set_all_replicated_index(0);
  ConsensusResponsePB res;
  Status s = peer->Update(&req, &res);
  ASSERT_EQ(last_op_id.term() + 3, res.responder_term());
  ASSERT_TRUE(res.status().has_error());
  ASSERT_EQ(ConsensusErrorPB::INVALID_TERM, res.status().error().code());
  LOG(INFO) << "Follower rejected old heartbeat, as expected: " << SecureShortDebugString(res);
}

}  // namespace consensus
}  // namespace kudu
