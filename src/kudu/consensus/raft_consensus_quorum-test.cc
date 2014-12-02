// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/peer_manager.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/raft_consensus_state.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/metadata.h"
#include "kudu/server/logical_clock.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(leader_heartbeat_interval_ms);
DECLARE_bool(enable_leader_failure_detection);

#define REPLICATE_SEQUENCE_OF_MESSAGES(a, b, c, d, e, f, g) \
  ASSERT_NO_FATAL_FAILURE(ReplicateSequenceOfMessages(a, b, c, d, e, f, g))

namespace kudu {

namespace rpc {
class RpcContext;
}
namespace consensus {

using log::Log;
using log::LogEntryPB;
using log::LogOptions;
using log::LogReader;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using rpc::RpcContext;
using strings::Substitute;
using strings::SubstituteAndAppend;

const char* kTestTablet = "TestTablet";

Status WaitUntilLeaderForTests(RaftConsensus* raft) {
  while (raft->GetActiveRole() != QuorumPeerPB::LEADER) {
    usleep(10000); // 10ms.
  }
  return Status::OK();
}

// A simple wrapper around a latch callback that can be used a consensus
// commit continuation.
class CommitContinuationLatchCallback : public ConsensusCommitContinuation {
 public:

  virtual void ReplicationFinished(const Status& status) {
    if (status.ok()) {
      callback_.OnSuccess();
    } else {
      callback_.OnFailure(status);
    }
  }

  LatchCallback callback_;
};

// Test suite for tests that focus on multiple peer interaction, but
// without integrating with other components, such as transactions.
class RaftConsensusQuorumTest : public KuduTest {
 public:
  RaftConsensusQuorumTest()
    : clock_(server::LogicalClock::CreateStartingAt(Timestamp(0))),
      metric_context_(&metric_registry_, "raft-test"),
      schema_(GetSimpleTestSchema()) {
    options_.tablet_id = kTestTablet;
    FLAGS_enable_leader_failure_detection = false;
  }

  // Builds an initial quorum of 'num' elements.
  // Since we don't have leader election yet, the initial roles are pre-assigned.
  // The last peer (index 'num - 1') always starts out as CANDIDATE.
  void BuildInitialQuorumPB(int num) {
    BuildQuorumPBForTests(&quorum_, num);
    peers_.reset(new TestPeerMapManager(quorum_));
  }

  Status BuildFsManagersAndLogs() {
    // Build the fsmanagers and logs
    for (int i = 0; i < quorum_.peers_size(); i++) {
      LogOptions options;
      string test_path = GetTestPath(Substitute("peer-$0-root", i));
      env_->CreateDir(test_path);
      FsManager* fs_manager = new FsManager(env_.get(), test_path);
      fs_managers_.push_back(fs_manager);

      gscoped_ptr<Log> log;
      RETURN_NOT_OK(Log::Open(options,
                              fs_manager,
                              kTestTablet,
                              schema_,
                              NULL,
                              &log));
      Log* log_ptr = log.release();
      logs_.push_back(log_ptr);
    }
    return Status::OK();
  }

  void BuildPeers() {
    vector<LocalTestPeerProxyFactory*> proxy_factories;
    for (int i = 0; i < quorum_.peers_size(); i++) {
      LocalTestPeerProxyFactory* proxy_factory = new LocalTestPeerProxyFactory(peers_.get());
      proxy_factories.push_back(proxy_factory);

      TestTransactionFactory* txn_factory = new TestTransactionFactory();

      gscoped_ptr<ConsensusMetadata> cmeta;
      CHECK_OK(ConsensusMetadata::Create(fs_managers_[i], kTestTablet, quorum_,
                                         consensus::kMinimumTerm, &cmeta));


      string peer_uuid = Substitute("peer-$0", i);
      MetricContext metrics(metric_context_, peer_uuid);

      gscoped_ptr<PeerMessageQueue> queue(new PeerMessageQueue(metrics,
                                                               logs_[i],
                                                               peer_uuid,
                                                               kTestTablet));

      gscoped_ptr<PeerManager> peer_manager(
          new PeerManager(options_.tablet_id,
                              quorum_.peers(i).permanent_uuid(),
                              proxy_factory,
                              queue.get(),
                              logs_[i]));

      scoped_refptr<RaftConsensus> peer(
          new RaftConsensus(options_,
                            cmeta.Pass(),
                            gscoped_ptr<PeerProxyFactory>(proxy_factory).Pass(),
                            queue.Pass(),
                            peer_manager.Pass(),
                            metrics,
                            quorum_.peers(i).permanent_uuid(),
                            clock_,
                            txn_factory,
                            logs_[i]));

      txn_factory->SetConsensus(peer.get());
      txn_factories_.push_back(txn_factory);
      peers_->AddPeer(quorum_.peers(i).permanent_uuid(), peer);
    }
  }

  Status StartPeers() {
    ConsensusBootstrapInfo boot_info;

    TestPeerMap all_peers = peers_->GetPeerMapCopy();
    BOOST_FOREACH(const TestPeerMap::value_type& entry, all_peers) {
      RETURN_NOT_OK(entry.second->Start(boot_info));
    }
    return Status::OK();
  }

  Status BuildQuorum(int num) {
    BuildInitialQuorumPB(num);
    RETURN_NOT_OK(BuildFsManagersAndLogs());
    BuildPeers();
    return Status::OK();
  }

  Status BuildAndStartQuorum(int num) {
    RETURN_NOT_OK(BuildQuorum(num));
    RETURN_NOT_OK(StartPeers());
    return Status::OK();
  }

  LocalTestPeerProxy* GetLeaderProxyToPeer(int peer_idx, int leader_idx) {
    scoped_refptr<RaftConsensus> follower;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &follower));
    scoped_refptr<RaftConsensus> leader;
    CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));
    BOOST_FOREACH(LocalTestPeerProxy* proxy, down_cast<LocalTestPeerProxyFactory*>(
        leader->peer_proxy_factory_.get())->GetProxies()) {
      if (proxy->GetTarget() == follower->peer_uuid()) {
        return proxy;
      }
    }
    CHECK(false) << "Proxy not found";
    return NULL;
  }

  Status AppendDummyMessage(int peer_idx,
                            gscoped_ptr<ConsensusRound>* round,
                            shared_ptr<LatchCallback>* req_commit_clbk = NULL) {
    gscoped_ptr<ReplicateMsg> msg(new ReplicateMsg());
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request();
    // These would normally be transactions with their own lifecycle, but
    // here we just add them to the autorelease pool.
    CommitContinuationLatchCallback* continuation = pool_.Add(new CommitContinuationLatchCallback);

    shared_ptr<LatchCallback> commit_clbk(new LatchCallback);
    if (req_commit_clbk != NULL) {
      *req_commit_clbk = commit_clbk;
    }

    scoped_refptr<RaftConsensus> peer;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &peer));
    round->reset(peer->NewRound(msg.Pass(), continuation, commit_clbk));
    RETURN_NOT_OK_PREPEND(peer->Replicate(round->get()),
                          Substitute("Unable to replicate to peer $0", peer_idx));
    return Status::OK();
  }

  Status CommitDummyMessage(ConsensusRound* round) {
    gscoped_ptr<CommitMsg> msg(new CommitMsg());
    msg->set_op_type(NO_OP);
    msg->set_timestamp(clock_->Now().ToUint64());
    round->Commit(msg.Pass());
    return Status::OK();
  }

  Status WaitForReplicate(ConsensusRound* round) {
    return down_cast<CommitContinuationLatchCallback*>(
        round->continuation_)->callback_.Wait();
  }
  Status TimedWaitForReplicate(ConsensusRound* round, const MonoDelta& delta) {
    return down_cast<CommitContinuationLatchCallback*>(
        round->continuation_)->callback_.WaitFor(delta);
  }

  void WaitForReplicateIfNotAlreadyPresent(const OpId& to_wait_for, int peer_idx) {
    scoped_refptr<RaftConsensus> peer;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &peer));
    ReplicaState* state = peer->GetReplicaStateForTests();
    while (true) {
      {
        ReplicaState::UniqueLock lock;
        CHECK_OK(state->LockForRead(&lock));
        if (OpIdCompare(state->GetLastReceivedOpIdUnlocked(), to_wait_for) >= 0) {
          return;
        }
      }
      usleep(1000);
    }
  }

  // Waits for an operation to be (database) committed in the replica at index
  // 'peer_idx'. If the operation was already committed this returns immediately.
  void WaitForCommitIfNotAlreadyPresent(const OpId& to_wait_for,
                                        int peer_idx,
                                        int leader_idx) {
    scoped_refptr<RaftConsensus> peer;
    CHECK_OK(peers_->GetPeerByIdx(peer_idx, &peer));
    ReplicaState* state = peer->GetReplicaStateForTests();

    for (int i = 0; i < 1000; i++) {
      {
        ReplicaState::UniqueLock lock;
        CHECK_OK(state->LockForRead(&lock));
        if (OpIdCompare(state->GetCommittedOpIdUnlocked(), to_wait_for) >= 0) {
          return;
        }
      }
      usleep(1000);
    }

    LOG(ERROR) << "Max timeout attempts reached while waiting for commit: "
               << to_wait_for.ShortDebugString() << " on replica. Dumping state and quitting.";
    vector<string> lines;
    scoped_refptr<RaftConsensus> leader;
    CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));
    BOOST_FOREACH(const string& line, lines) {
      LOG(ERROR) << line;
    }

    // Gather the replica and leader operations for printing
    vector<LogEntryPB*> replica_ops;
    ElementDeleter repl0_deleter(&replica_ops);
    GatherLogEntries(peer_idx, logs_[peer_idx], &replica_ops);
    vector<LogEntryPB*> leader_ops;
    ElementDeleter leader_deleter(&leader_ops);
    GatherLogEntries(leader_idx, logs_[leader_idx], &leader_ops);
    SCOPED_TRACE(PrintOnError(leader_ops, "leader"));
    SCOPED_TRACE(PrintOnError(replica_ops, peer->peer_uuid()));
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
                                   vector<ConsensusRound*>* rounds,
                                   shared_ptr<LatchCallback>* commit_clbk = NULL) {
    for (int i = 0; i < seq_size; i++) {
      gscoped_ptr<ConsensusRound> round;
      ASSERT_STATUS_OK(AppendDummyMessage(leader_idx, &round, commit_clbk));
      ASSERT_STATUS_OK(WaitForReplicate(round.get()));
      last_op_id->CopyFrom(round->id());
      if (commit_mode == COMMIT_ONE_BY_ONE) {
        CommitDummyMessage(round.get());
      }
      rounds->push_back(round.release());
    }

    if (wait_mode == WAIT_FOR_ALL_REPLICAS) {
      scoped_refptr<RaftConsensus> leader;
      CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));

      TestPeerMap all_peers = peers_->GetPeerMapCopy();
      int i = 0;
      BOOST_FOREACH(const TestPeerMap::value_type& entry, all_peers) {
        if (entry.second->peer_uuid() != leader->peer_uuid()) {
          WaitForReplicateIfNotAlreadyPresent(*last_op_id, i);
        }
        i++;
      }
    }
  }

  void GatherLogEntries(int idx, Log* log, vector<LogEntryPB* >* entries) {
    ASSERT_STATUS_OK(log->WaitUntilAllFlushed());
    log->Close();
    gscoped_ptr<LogReader> log_reader;
    ASSERT_STATUS_OK(log::LogReader::Open(fs_managers_[idx],
                                          kTestTablet,
                                          &log_reader));
    vector<LogEntryPB*> ret;
    ElementDeleter deleter(&ret);
    log::SegmentSequence segments;
    ASSERT_STATUS_OK(log_reader->GetSegmentsSnapshot(&segments));

    BOOST_FOREACH(const log::SegmentSequence::value_type& entry, segments) {
      ASSERT_STATUS_OK(entry->ReadEntries(&ret));
    }

    entries->swap(ret);
  }

  // Verifies that the replica's log match the leader's. This deletes the
  // peers (so we're sure that no further writes occur) and closes the logs
  // so it must be the very last thing to run, in a test.
  void VerifyLogs(int leader_idx, int first_replica_idx, int last_replica_idx) {
    // Wait for in-flight transactions to be done. We're destroying the
    // peers next and leader transactions won't be able to commit anymore.
    BOOST_FOREACH(TestTransactionFactory* factory, txn_factories_) {
      factory->WaitDone();
    }

    // Shut down all the peers.
    TestPeerMap all_peers = peers_->GetPeerMapCopy();
    BOOST_FOREACH(const TestPeerMap::value_type& entry, all_peers) {
      entry.second->Shutdown();
    }

    vector<LogEntryPB*> leader_entries;
    ElementDeleter leader_entry_deleter(&leader_entries);
    GatherLogEntries(leader_idx, logs_[leader_idx], &leader_entries);
    scoped_refptr<RaftConsensus> leader;
    CHECK_OK(peers_->GetPeerByIdx(leader_idx, &leader));

    for (int replica_idx = first_replica_idx; replica_idx < last_replica_idx; replica_idx++) {
      vector<LogEntryPB*> replica_entries;
      ElementDeleter replica_entry_deleter(&replica_entries);
      GatherLogEntries(replica_idx, logs_[replica_idx], &replica_entries);

      scoped_refptr<RaftConsensus> replica;
      CHECK_OK(peers_->GetPeerByIdx(replica_idx, &replica));
      VerifyReplica(leader_entries,
                    replica_entries,
                    leader->peer_uuid(),
                    replica->peer_uuid());
    }
  }

  void ExtractReplicateIds(const vector<LogEntryPB*>& entries,
                           vector<OpId>* ids) {
    ids->reserve(entries.size() / 2);
    BOOST_FOREACH(const LogEntryPB* entry, entries) {
      if (entry->has_replicate()) {
        ids->push_back(entry->replicate().id());
      }
    }
  }

  void VerifyReplicateOrderMatches(const vector<LogEntryPB*>& leader_entries,
                                   const vector<LogEntryPB*>& replica_entries) {
    vector<OpId> leader_ids, replica_ids;
    ExtractReplicateIds(leader_entries, &leader_ids);
    ExtractReplicateIds(replica_entries, &replica_ids);
    ASSERT_EQ(leader_ids.size(), replica_ids.size());
    for (int i = 0; i < leader_ids.size(); i++) {
      ASSERT_EQ(leader_ids[i].ShortDebugString(),
                replica_ids[i].ShortDebugString());
    }
  }

  void VerifyNoCommitsBeforeReplicates(const vector<LogEntryPB*>& entries) {
    unordered_set<consensus::OpId,
                  OpIdHashFunctor,
                  OpIdEqualsFunctor> replication_ops;

    BOOST_FOREACH(const LogEntryPB* entry, entries) {
      if (entry->has_replicate()) {
        ASSERT_TRUE(InsertIfNotPresent(&replication_ops, entry->replicate().id()))
          << "REPLICATE op id showed up twice: " << entry->ShortDebugString();
      } else if (entry->has_commit()) {
        ASSERT_EQ(1, replication_ops.erase(entry->commit().commited_op_id()))
          << "COMMIT came before associated REPLICATE: " << entry->ShortDebugString();
      }
    }
  }

  void VerifyReplica(const vector<LogEntryPB*>& leader_entries,
                     const vector<LogEntryPB*>& replica_entries,
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

  string PrintOnError(const vector<LogEntryPB*>& replica_entries,
                      const string& replica_id) {
    string ret = "";
    SubstituteAndAppend(&ret, "$1 log entries for replica $0:\n",
                        replica_id, replica_entries.size());
    BOOST_FOREACH(LogEntryPB* replica_entry, replica_entries) {
      StrAppend(&ret, "Replica log entry: ", replica_entry->ShortDebugString(), "\n");
    }
    return ret;
  }

  // Read the ConsensusMetadata for the given peer from disk.
  gscoped_ptr<ConsensusMetadata> ReadConsensusMetadataFromDisk(int peer_index) {
    gscoped_ptr<ConsensusMetadata> cmeta;
    CHECK_OK(ConsensusMetadata::Load(fs_managers_[peer_index], kTestTablet, &cmeta));
    return cmeta.Pass();
  }

  // Assert that the durable term == term and that the peer that got the vote == voted_for.
  void AssertDurableTermAndVote(int peer_index, uint64_t term, const std::string& voted_for) {
    gscoped_ptr<ConsensusMetadata> cmeta = ReadConsensusMetadataFromDisk(peer_index);
    ASSERT_EQ(term, cmeta->pb().current_term());
    ASSERT_EQ(voted_for, cmeta->pb().voted_for());
  }

  // Assert that the durable term == term and that the peer has not yet voted.
  void AssertDurableTermWithoutVote(int peer_index, uint64_t term) {
    gscoped_ptr<ConsensusMetadata> cmeta = ReadConsensusMetadataFromDisk(peer_index);
    ASSERT_EQ(term, cmeta->pb().current_term());
    ASSERT_FALSE(cmeta->pb().has_voted_for());
  }

  ~RaftConsensusQuorumTest() {
    peers_->Clear();
    STLDeleteElements(&txn_factories_);
    STLDeleteElements(&logs_);
    STLDeleteElements(&fs_managers_);
  }

 protected:
  ConsensusOptions options_;
  QuorumPB quorum_;
  OpId initial_id_;
  vector<FsManager*> fs_managers_;
  vector<Log*> logs_;
  gscoped_ptr<TestPeerMapManager> peers_;
  vector<TestTransactionFactory*> txn_factories_;
  scoped_refptr<server::Clock> clock_;
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  const Schema schema_;
  AutoReleasePool pool_;
};

// Tests Replicate/Commit a single message through the leader.
TEST_F(RaftConsensusQuorumTest, TestFollowersReplicateAndCommitMessage) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;
  vector<ConsensusRound*> rounds;
  ElementDeleter deleter(&rounds);
  shared_ptr<LatchCallback> commit_clbk;
  REPLICATE_SEQUENCE_OF_MESSAGES(1,
                                 kLeaderidx,
                                 WAIT_FOR_ALL_REPLICAS,
                                 DONT_COMMIT,
                                 &last_op_id,
                                 &rounds,
                                 &commit_clbk);

  // Commit the operation
  ASSERT_STATUS_OK(CommitDummyMessage(rounds[0]));

  // Wait for everyone to commit the operations.

  // We need to make sure the CommitMsg lands on the leaders log or the
  // verification will fail. Since CommitMsgs are appended to the replication
  // queue there is a scenario where they land in the followers log before
  // landing on the leader's log. However we know that they are durable
  // on the leader when the commit callback is triggered.
  // We thus wait for the commit callback to trigger, ensuring durability
  // on the leader and then for the commits to be present on the replicas.
  ASSERT_STATUS_OK(commit_clbk.get()->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower1Idx, kLeaderidx);
  VerifyLogs(2, 0, 1);
}

// Tests Replicate/Commit a sequence of messages through the leader.
// First a sequence of replicates and then a sequence of commits.
TEST_F(RaftConsensusQuorumTest, TestFollowersReplicateAndCommitSequence) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  int seq_size = AllowSlowTests() ? 1000 : 100;

  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;
  vector<ConsensusRound*> rounds;
  ElementDeleter deleter(&rounds);
  shared_ptr<LatchCallback> commit_clbk;

  REPLICATE_SEQUENCE_OF_MESSAGES(seq_size,
                                 kLeaderidx,
                                 WAIT_FOR_ALL_REPLICAS,
                                 DONT_COMMIT,
                                 &last_op_id,
                                 &rounds,
                                 &commit_clbk);

  // Commit the operations, but wait for the replicates to finish first
  BOOST_FOREACH(ConsensusRound* round, rounds) {
    ASSERT_STATUS_OK(CommitDummyMessage(round));
  }

  // See comment at the end of TestFollowersReplicateAndCommitMessage
  // for an explanation on this waiting sequence.
  ASSERT_STATUS_OK(commit_clbk.get()->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower1Idx, kLeaderidx);
  VerifyLogs(2, 0, 1);
}

TEST_F(RaftConsensusQuorumTest, TestConsensusContinuesIfAMinorityFallsBehind) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_replicate;
  vector<ConsensusRound*> rounds;
  ElementDeleter deleter(&rounds);
  {
    // lock one of the replicas down by obtaining the state lock
    // and never letting it go.
    scoped_refptr<RaftConsensus> follower0;
    CHECK_OK(peers_->GetPeerByIdx(kFollower0Idx, &follower0));

    ReplicaState* follower0_rs = follower0->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock;
    ASSERT_STATUS_OK(follower0_rs->LockForRead(&lock));

    // If the locked replica would stop consensus we would hang here
    // as we wait for operations to be replicated to a majority.
    ASSERT_NO_FATAL_FAILURE(ReplicateSequenceOfMessages(
                              10,
                              kLeaderidx,
                              WAIT_FOR_MAJORITY,
                              COMMIT_ONE_BY_ONE,
                              &last_replicate,
                              &rounds));

    // Follower 1 should be fine (Were we to wait for follower0's replicate
    // this would hang here). We know he must have replicated but make sure
    // by calling Wait().
    WaitForReplicateIfNotAlreadyPresent(last_replicate, kFollower1Idx);
    WaitForCommitIfNotAlreadyPresent(last_replicate, kFollower1Idx, kLeaderidx);
  }

  // After we let the lock go the remaining follower should get up-to-date
  WaitForReplicateIfNotAlreadyPresent(last_replicate, kFollower0Idx);
  WaitForCommitIfNotAlreadyPresent(last_replicate, kFollower0Idx, kLeaderidx);
  VerifyLogs(2, 0, 1);
}

TEST_F(RaftConsensusQuorumTest, TestConsensusStopsIfAMajorityFallsBehind) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;

  gscoped_ptr<ConsensusRound> round;
  {
    // lock two of the replicas down by obtaining the state locks
    // and never letting them go.
    scoped_refptr<RaftConsensus> follower0;
    CHECK_OK(peers_->GetPeerByIdx(kFollower0Idx, &follower0));
    ReplicaState* follower0_rs = follower0->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock0;
    ASSERT_STATUS_OK(follower0_rs->LockForRead(&lock0));

    scoped_refptr<RaftConsensus> follower1;
    CHECK_OK(peers_->GetPeerByIdx(kFollower1Idx, &follower1));
    ReplicaState* follower1_rs = follower1->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock1;
    ASSERT_STATUS_OK(follower1_rs->LockForRead(&lock1));

    // Append a single message to the queue
    ASSERT_STATUS_OK(AppendDummyMessage(kLeaderidx, &round));
    last_op_id.CopyFrom(round->id());
    // This should timeout.
    Status status = TimedWaitForReplicate(round.get(), MonoDelta::FromMilliseconds(500));
    ASSERT_TRUE(status.IsTimedOut());
  }

  // After we release the locks the operation should replicate to all replicas
  // and we commit.
  ASSERT_OK(WaitForReplicate(round.get()));
  CommitDummyMessage(round.get());

  // Assert that everything was ok
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower0Idx);
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower1Idx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower1Idx, kLeaderidx);
  VerifyLogs(2, 0, 1);
}

// If some communication error happens the leader will resend the request to the
// peers. This tests that the peers handle repeated requests.
TEST_F(RaftConsensusQuorumTest, TestReplicasHandleCommunicationErrors) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;

  // Append a dummy message, with faults injected on the first attempt
  // to send the message.
  gscoped_ptr<ConsensusRound> round;
  GetLeaderProxyToPeer(kFollower0Idx, kLeaderidx)->InjectCommFaultLeaderSide();
  GetLeaderProxyToPeer(kFollower1Idx, kLeaderidx)->InjectCommFaultLeaderSide();
  ASSERT_STATUS_OK(AppendDummyMessage(kLeaderidx, &round));

  // We should successfully replicate it due to retries.
  ASSERT_STATUS_OK(WaitForReplicate(round.get()));

  GetLeaderProxyToPeer(kFollower0Idx, kLeaderidx)->InjectCommFaultLeaderSide();
  GetLeaderProxyToPeer(kFollower1Idx, kLeaderidx)->InjectCommFaultLeaderSide();
  ASSERT_STATUS_OK(CommitDummyMessage(round.get()));

  // The commit should eventually reach both followers as well.
  last_op_id = round->id();
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower1Idx, kLeaderidx);

  // Append a sequence of messages, and keep injecting errors into the
  // replica proxies.
  vector<ConsensusRound*> contexts;
  ElementDeleter deleter(&contexts);
  shared_ptr<LatchCallback> commit_clbk;
  for (int i = 0; i < 100; i++) {
    gscoped_ptr<ConsensusRound> round;
    ASSERT_STATUS_OK(AppendDummyMessage(kLeaderidx, &round, &commit_clbk));
    ConsensusRound* round_ptr = round.get();
    last_op_id.CopyFrom(round->id());
    contexts.push_back(round.release());

    // inject comm faults
    if (i % 2 == 0) {
      GetLeaderProxyToPeer(kFollower0Idx, kLeaderidx)->InjectCommFaultLeaderSide();
    } else {
      GetLeaderProxyToPeer(kFollower1Idx, kLeaderidx)->InjectCommFaultLeaderSide();
    }

    ASSERT_OK(WaitForReplicate(round_ptr));
    ASSERT_STATUS_OK(CommitDummyMessage(round_ptr));
  }

  // Assert last operation was correctly replicated and committed.
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower0Idx);
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower1Idx);

  // See comment at the end of TestFollowersReplicateAndCommitMessage
  // for an explanation on this waiting sequence.
  ASSERT_STATUS_OK(commit_clbk.get()->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower1Idx, kLeaderidx);
  VerifyLogs(2, 0, 1);
}

// In this test we test the ability of the leader to send heartbeats
// to replicas by simply pushing nothing after the configuration round
// and still expecting for the replicas Update() hooks to be called.
TEST_F(RaftConsensusQuorumTest, TestLeaderHeartbeats) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  ASSERT_STATUS_OK(BuildQuorum(3));
  scoped_refptr<RaftConsensus> follower0;
  CHECK_OK(peers_->GetPeerByIdx(kFollower0Idx, &follower0));
  scoped_refptr<RaftConsensus> follower1;
  CHECK_OK(peers_->GetPeerByIdx(kFollower1Idx, &follower1));

  shared_ptr<CounterHooks> counter_hook_rpl0(
      new CounterHooks(follower0->GetFaultHooks()));
  shared_ptr<CounterHooks> counter_hook_rpl1(
      new CounterHooks(follower1->GetFaultHooks()));

  // Replace the default fault hooks on the replicas with counter hooks
  // before we start the quorum.
  follower0->SetFaultHooks(counter_hook_rpl0);
  follower1->SetFaultHooks(counter_hook_rpl1);

  ASSERT_STATUS_OK(StartPeers());

  // Wait for the config round to get committed and count the number
  // of update calls, calls after that will be heartbeats.
  OpId config_round;
  config_round.set_term(1);
  config_round.set_index(1);
  WaitForCommitIfNotAlreadyPresent(config_round, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(config_round, kFollower1Idx, kLeaderidx);

  int repl0_init_count = counter_hook_rpl0->num_pre_update_calls();
  int repl1_init_count = counter_hook_rpl1->num_pre_update_calls();

  // Now wait for about 4 times the hearbeat period the counters
  // should have increased 3/4 times.
  usleep(FLAGS_leader_heartbeat_interval_ms * 4 * 1000);

  int repl0_final_count = counter_hook_rpl0->num_pre_update_calls();
  int repl1_final_count = counter_hook_rpl1->num_pre_update_calls();

  ASSERT_GE(repl0_final_count - repl0_init_count, 3);
  ASSERT_LE(repl0_final_count - repl0_init_count, 4);
  ASSERT_GE(repl1_final_count - repl1_init_count, 3);
  ASSERT_LE(repl1_final_count - repl1_init_count, 4);

  VerifyLogs(2, 0, 1);
}

// After creating the initial quorum, this test writes a small sequence
// of messages to the initial leader. It then shuts down the current
// leader, makes another peer become leader and writes a sequence of
// messages to it. The new leader and the follower should agree on the
// sequence of messages.
TEST_F(RaftConsensusQuorumTest, TestLeaderElectionWithQuiescedQuorum) {
  const int kInitialQuorumSize = 5;
  ASSERT_STATUS_OK(BuildAndStartQuorum(kInitialQuorumSize));

  OpId last_op_id;
  shared_ptr<LatchCallback> last_commit_clbk;
  vector<ConsensusRound*> rounds;
  ElementDeleter deleter(&rounds);

  // Loop twice, successively shutting down the previous leader.
  for (int current_quorum_size = kInitialQuorumSize;
       current_quorum_size >= kInitialQuorumSize - 1;
       current_quorum_size--) {
    REPLICATE_SEQUENCE_OF_MESSAGES(10,
                                   current_quorum_size - 1, // The index of the leader.
                                   WAIT_FOR_ALL_REPLICAS,
                                   COMMIT_ONE_BY_ONE,
                                   &last_op_id,
                                   &rounds,
                                   &last_commit_clbk);

    // Make sure the last operation is committed everywhere
    ASSERT_STATUS_OK(last_commit_clbk.get()->Wait());
    for (int i = 0; i < current_quorum_size - 1; i++) {
      WaitForCommitIfNotAlreadyPresent(last_op_id, i, current_quorum_size - 1);
    }

    // Now shutdown the current leader.
    LOG(INFO) << "Shutting down current leader with index " << (current_quorum_size - 1);
    scoped_refptr<RaftConsensus> current_leader;
    CHECK_OK(peers_->GetPeerByIdx(current_quorum_size - 1, &current_leader));
    current_leader->Shutdown();
    peers_->RemovePeer(current_leader->peer_uuid());

    // ... and make the peer before it become leader.
    scoped_refptr<RaftConsensus> new_leader;
    CHECK_OK(peers_->GetPeerByIdx(current_quorum_size - 2, &new_leader));

    // This will force an election in which we expect to make the last
    // non-shutdown peer in the list become leader.
    LOG(INFO) << "Running election for future leader with index " << (current_quorum_size - 1);
    ASSERT_STATUS_OK(new_leader->StartElection());
    WaitUntilLeaderForTests(new_leader.get());
    LOG(INFO) << "Election won";

    // ... replicating a set of messages to the new leader should now be possible.
    REPLICATE_SEQUENCE_OF_MESSAGES(10,
                                   current_quorum_size - 2, // The index of the new leader.
                                   WAIT_FOR_MAJORITY,
                                   COMMIT_ONE_BY_ONE,
                                   &last_op_id,
                                   &rounds,
                                   &last_commit_clbk);

    // Make sure the last operation is committed everywhere
    ASSERT_STATUS_OK(last_commit_clbk.get()->Wait());
    for (int i = 0; i < current_quorum_size - 2; i++) {
      WaitForCommitIfNotAlreadyPresent(last_op_id, i, current_quorum_size - 2);
    }
  }
  // We can only verify the logs of the peers that were not killed, due to the
  // old leaders being out-of-date now.
  VerifyLogs(2, 0, 1);
}

TEST_F(RaftConsensusQuorumTest, TestReplicasEnforceTheLogMatchingProperty) {
  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;
  shared_ptr<LatchCallback> last_commit_clbk;
  vector<ConsensusRound*> rounds;
  ElementDeleter deleter(&rounds);
  REPLICATE_SEQUENCE_OF_MESSAGES(10,
                                 2, // The index of the initial leader.
                                 WAIT_FOR_ALL_REPLICAS,
                                 COMMIT_ONE_BY_ONE,
                                 &last_op_id,
                                 &rounds,
                                 &last_commit_clbk);

  // Make sure the last operation is committed everywhere
  ASSERT_STATUS_OK(last_commit_clbk->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, 0, 2);
  WaitForCommitIfNotAlreadyPresent(last_op_id, 1, 2);

  // Now replicas should only accept operations with
  // 'last_id' as the preceding id.
  ConsensusRequestPB req;
  ConsensusResponsePB resp;

  scoped_refptr<RaftConsensus> leader;
  CHECK_OK(peers_->GetPeerByIdx(2, &leader));

  scoped_refptr<RaftConsensus> follower;
  CHECK_OK(peers_->GetPeerByIdx(0, &follower));


  req.set_caller_uuid(leader->peer_uuid());
  req.set_caller_term(last_op_id.term());
  req.mutable_preceding_id()->CopyFrom(last_op_id);
  req.mutable_committed_index()->CopyFrom(last_op_id);

  ReplicateMsg* replicate = req.add_ops();
  OpId* id = replicate->mutable_id();
  id->set_term(last_op_id.term());
  id->set_index(last_op_id.index() + 1);
  replicate->set_op_type(NO_OP);

  // Appending this message to peer0 should work and update
  // its 'last_received' to 'id'.
  ASSERT_OK(follower->Update(&req, &resp));
  ASSERT_TRUE(OpIdEquals(resp.status().last_received(), *id));

  // Now skip one message in the same term. The replica should
  // complain with the right error message.
  req.mutable_preceding_id()->set_index(id->index() + 1);
  id->set_index(id->index() + 2);
  // Appending this message to peer0 should return a Status::OK
  // but should contain an error referring to the log matching property.
  ASSERT_OK(follower->Update(&req, &resp));
  ASSERT_TRUE(resp.has_status());
  ASSERT_TRUE(resp.status().has_error());
  ASSERT_EQ(resp.status().error().code(), ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
  ASSERT_STR_CONTAINS(resp.status().error().status().message(),
                      "Log matching property violated");
}

// Test that RequestVote performs according to "spec".
TEST_F(RaftConsensusQuorumTest, TestRequestVote) {
  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;
  shared_ptr<LatchCallback> last_commit_clbk;
  vector<ConsensusRound*> rounds;
  ElementDeleter deleter(&rounds);
  REPLICATE_SEQUENCE_OF_MESSAGES(10,
                                 2, // The index of the initial leader.
                                 WAIT_FOR_ALL_REPLICAS,
                                 COMMIT_ONE_BY_ONE,
                                 &last_op_id,
                                 &rounds,
                                 &last_commit_clbk);

  // Make sure the last operation is committed everywhere
  ASSERT_STATUS_OK(last_commit_clbk->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, 0, 2);
  WaitForCommitIfNotAlreadyPresent(last_op_id, 1, 2);

  // Ensure last-logged OpId is > (0,0).
  ASSERT_TRUE(OpIdLessThan(MinimumOpId(), last_op_id));

  const int kPeerIndex = 1;
  scoped_refptr<RaftConsensus> peer;
  CHECK_OK(peers_->GetPeerByIdx(kPeerIndex, &peer));

  VoteRequestPB request;
  request.set_tablet_id(kTestTablet);
  request.mutable_candidate_status()->mutable_last_received()->CopyFrom(last_op_id);

  //
  // Test that replicas only vote yes for a single peer per term.
  //

  // Our first vote should be a yes.
  VoteResponsePB response;
  request.set_candidate_uuid("peer-0");
  request.set_candidate_term(last_op_id.term() + 1);
  ASSERT_OK(peer->RequestVote(&request, &response));
  ASSERT_TRUE(response.vote_granted());
  ASSERT_EQ(last_op_id.term() + 1, response.responder_term());
  ASSERT_NO_FATAL_FAILURE(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 1, "peer-0"));

  // Ensure we get same response for same term and same UUID.
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request, &response));
  ASSERT_TRUE(response.vote_granted());

  // Ensure we get a "no" for a different candidate UUID for that term.
  response.Clear();
  request.set_candidate_uuid("peer-2");
  ASSERT_OK(peer->RequestVote(&request, &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_TRUE(response.has_consensus_error());
  ASSERT_EQ(ConsensusErrorPB::ALREADY_VOTED, response.consensus_error().code());
  ASSERT_EQ(last_op_id.term() + 1, response.responder_term());
  ASSERT_NO_FATAL_FAILURE(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 1, "peer-0"));

  //
  // Test that replicas refuse votes for an old term.
  //

  // Increase the term of our candidate, which will cause the voter replica to
  // increase its own term to match.
  request.set_candidate_uuid("peer-0");
  request.set_candidate_term(last_op_id.term() + 2);
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request, &response));
  ASSERT_TRUE(response.vote_granted());
  ASSERT_EQ(last_op_id.term() + 2, response.responder_term());
  ASSERT_NO_FATAL_FAILURE(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 2, "peer-0"));

  // Now try the old term.
  // Note: Use the peer who "won" the election on the previous term (peer-0),
  // although in practice the impl does not store historical vote data.
  request.set_candidate_term(last_op_id.term() + 1);
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request, &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_TRUE(response.has_consensus_error());
  ASSERT_EQ(ConsensusErrorPB::INVALID_TERM, response.consensus_error().code());
  ASSERT_EQ(last_op_id.term() + 2, response.responder_term());
  ASSERT_NO_FATAL_FAILURE(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 2, "peer-0"));

  //
  // Ensure replicas vote no for someone who does not claim to be a member of
  // the quorum.
  //

  request.set_candidate_uuid("unknown-replica");
  request.set_candidate_term(last_op_id.term() + 3);
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request, &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_TRUE(response.has_consensus_error());
  ASSERT_EQ(ConsensusErrorPB::NOT_IN_QUORUM, response.consensus_error().code());
  // Also should not rev the term to match a non-member.
  ASSERT_EQ(last_op_id.term() + 2, response.responder_term());
  ASSERT_NO_FATAL_FAILURE(AssertDurableTermAndVote(kPeerIndex, last_op_id.term() + 2, "peer-0"));

  //
  // Ensure replicas vote no for an old op index.
  //

  request.set_candidate_uuid("peer-0");
  request.set_candidate_term(last_op_id.term() + 3);
  request.mutable_candidate_status()->mutable_last_received()->CopyFrom(MinimumOpId());
  response.Clear();
  ASSERT_OK(peer->RequestVote(&request, &response));
  ASSERT_FALSE(response.vote_granted());
  ASSERT_TRUE(response.has_consensus_error());
  ASSERT_EQ(ConsensusErrorPB::LAST_OPID_TOO_OLD, response.consensus_error().code());
  ASSERT_EQ(last_op_id.term() + 3, response.responder_term());
  ASSERT_NO_FATAL_FAILURE(AssertDurableTermWithoutVote(kPeerIndex, last_op_id.term() + 3));

  // Send a "heartbeat" to the peer. It should be rejected.
  ConsensusRequestPB req;
  req.set_caller_term(last_op_id.term());
  req.set_caller_uuid("peer-0");
  req.mutable_committed_index()->CopyFrom(last_op_id);
  ConsensusResponsePB res;
  Status s = peer->Update(&req, &res);
  ASSERT_EQ(last_op_id.term() + 3, res.responder_term());
  ASSERT_TRUE(res.status().has_error());
  ASSERT_EQ(ConsensusErrorPB::INVALID_TERM, res.status().error().code());
  LOG(INFO) << "Follower rejected old heartbeat, as expected: " << res.ShortDebugString();
}

}  // namespace consensus
}  // namespace kudu
