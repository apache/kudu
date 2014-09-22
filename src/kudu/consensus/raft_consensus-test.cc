// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/raft_consensus_state.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/metadata.h"
#include "kudu/server/logical_clock.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(leader_heartbeat_interval_ms);

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
const int kDontVerify = -1;

class RaftConsensusTest : public KuduTest {
 public:
  RaftConsensusTest()
    : clock_(server::LogicalClock::CreateStartingAt(Timestamp(0))),
      metric_context_(&metric_registry_, "raft-test"),
      schema_(GetSimpleTestSchema()) {}

  // Builds an initial quorum of 'num' elements.
  // Since we don't have leader election yet, the initial roles are pre-assigned.
  // The last peer (index 'num - 1') always starts out as CANDIDATE.
  void BuildInitialQuorumPB(int num) {
    for (int i = 0; i < num; i++) {
      QuorumPeerPB* peer_pb = quorum_.add_peers();
      peer_pb->set_permanent_uuid(Substitute("peer-$0", i));
      if (i == num - 1) {
        peer_pb->set_role(QuorumPeerPB::CANDIDATE);
      } else {
        peer_pb->set_role(QuorumPeerPB::FOLLOWER);
      }
      HostPortPB* hp = peer_pb->mutable_last_known_addr();
      hp->set_host("0");
      hp->set_port(0);
    }
    quorum_.set_local(false);
    quorum_.set_seqno(0);
    options_.tablet_id = kTestTablet;
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
      LocalTestPeerProxyFactory* proxy_factory = new LocalTestPeerProxyFactory();
      proxy_factories.push_back(proxy_factory);

      TestTransactionFactory* txn_factory = new TestTransactionFactory();

      gscoped_ptr<ConsensusMetadata> cmeta;
      CHECK_OK(ConsensusMetadata::Create(fs_managers_[i], kTestTablet, quorum_,
                                         consensus::kMinimumTerm, &cmeta));

      RaftConsensus* peer =
          new RaftConsensus(options_,
                            cmeta.Pass(),
                            gscoped_ptr<PeerProxyFactory>(proxy_factory).Pass(),
                            MetricContext(metric_context_, Substitute("peer-$0", i)),
                            quorum_.peers(i).permanent_uuid(),
                            clock_,
                            txn_factory,
                            logs_[i]);

      txn_factory->SetConsensus(peer);
      txn_factories_.push_back(txn_factory);
      peers_.push_back(peer);
    }

    // Add each peer to the other peer's proxy factories so that they can communicate
    // with each other.
    for (int i = 0; i < quorum_.peers_size(); i++) {
      LocalTestPeerProxyFactory* proxy_factory = proxy_factories[i];
      for (int j = 0; j < quorum_.peers_size(); j++) {
        proxy_factory->AddPeer(quorum_.peers(j), peers_[j]);
      }
    }
  }

  Status StartPeers() {
    ConsensusBootstrapInfo boot_info;

    for (int i = 0; i < quorum_.peers_size(); i++) {
      RaftConsensus* peer = peers_[i];
      RETURN_NOT_OK(peer->Start(boot_info));
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

  RaftConsensus* GetPeer(int peer_idx) {
    CHECK_GE(peer_idx, 0);
    CHECK_LT(peer_idx, peers_.size());
    return DCHECK_NOTNULL(peers_[peer_idx]);
  }

  LocalTestPeerProxy* GetLeaderProxyToPeer(int peer_idx, int leader_idx) {
    RaftConsensus* follower = GetPeer(peer_idx);
    BOOST_FOREACH(LocalTestPeerProxy* proxy, down_cast<LocalTestPeerProxyFactory*>(
        GetPeer(leader_idx)->peer_proxy_factory_.get())->GetProxies()) {
      if (proxy->GetTarget()->peer_uuid() == follower->peer_uuid()) {
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
    shared_ptr<FutureCallback> replicate_clbk(new LatchCallback);
    shared_ptr<LatchCallback> commit_clbk(new LatchCallback);
    if (req_commit_clbk != NULL) {
      *req_commit_clbk = commit_clbk;
    }
    RaftConsensus* peer = GetPeer(peer_idx);
    round->reset(peer->NewRound(msg.Pass(), replicate_clbk, commit_clbk));
    RETURN_NOT_OK(peer->Replicate(round->get()));
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
    return down_cast<LatchCallback*, FutureCallback>(round->replicate_callback().get())->Wait();
  }
  Status TimedWaitForReplicate(ConsensusRound* round, const MonoDelta& delta) {
    return down_cast<LatchCallback*, FutureCallback>(
        round->replicate_callback().get())->WaitFor(delta);
  }

  void WaitForReplicateIfNotAlreadyPresent(const OpId& to_wait_for, int peer_idx) {
    RaftConsensus* peer = GetPeer(peer_idx);
    shared_ptr<LatchCallback> clbk(new LatchCallback);
    Status s = peer->RegisterOnReplicateCallback(to_wait_for, clbk);
    if (s.IsAlreadyPresent()) {
      return;
    }
    ASSERT_STATUS_OK(s);
    clbk->Wait();
  }

  // Waits for an operation to be (database) committed in the replica at index
  // 'peer_idx'. If the operation was already committed this returns immediately.
  void WaitForCommitIfNotAlreadyPresent(const OpId& to_wait_for,
                                        int peer_idx,
                                        int leader_idx) {
    shared_ptr<LatchCallback> clbk(new LatchCallback);
    RaftConsensus* replica = GetPeer(peer_idx);
    Status s = replica->RegisterOnCommitCallback(to_wait_for, clbk);
    if (s.IsAlreadyPresent()) {
      return;
    }
    ASSERT_STATUS_OK(s);
    int num_attempts = 0;

    while (true) {
      Status s = clbk->WaitFor(MonoDelta::FromMilliseconds(1000));
      if (s.ok()) {
        return;
      }
      CHECK(s.IsTimedOut());
      LOG(INFO) << "Waiting for " << to_wait_for.ShortDebugString()
          << " to be committed. Already timedout " << num_attempts << " times.";
      num_attempts++;
      if (num_attempts == 10) {
        LOG(ERROR) << "Max timeout attempts reached while waiting for commit: "
            << to_wait_for.ShortDebugString() << " on replica. Dumping state and quitting.";
        vector<string> lines;
        GetPeer(leader_idx)->queue_.DumpToStrings(&lines);
        BOOST_FOREACH(const string& line, lines) {
          LOG(ERROR) << line;
        }

        // Gather the replica and leader operations for printing
        vector<OperationPB*> replica_ops;
        ElementDeleter repl0_deleter(&replica_ops);
        GatherOperations(peer_idx, replica->log(), &replica_ops);
        vector<OperationPB*> leader_ops;
        ElementDeleter leader_deleter(&leader_ops);
        GatherOperations(leader_idx, logs_[leader_idx], &leader_ops);
        SCOPED_TRACE(PrintOnError(leader_ops, "leader"));
        SCOPED_TRACE(PrintOnError(replica_ops, replica->peer_uuid()));
        FAIL() << "Replica did not commit.";
      }
    }
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
      for (int i = 0; i < peers_.size(); i++) {
        if (i != leader_idx) {
          WaitForReplicateIfNotAlreadyPresent(*last_op_id, 0);
        }
      }
    }
  }

  void GatherOperations(int idx, Log* log, vector<OperationPB* >* operations) {
    ASSERT_STATUS_OK(log->WaitUntilAllFlushed());
    log->Close();
    gscoped_ptr<LogReader> log_reader;
    ASSERT_STATUS_OK(log::LogReader::Open(fs_managers_[idx],
                                          kTestTablet,
                                          &log_reader));
    vector<LogEntryPB*> entries;
    ElementDeleter deleter(&entries);
    log::SegmentSequence segments;
    ASSERT_STATUS_OK(log_reader->GetSegmentsSnapshot(&segments));

    BOOST_FOREACH(const log::SegmentSequence::value_type& entry, segments) {
      ASSERT_STATUS_OK(entry->ReadEntries(&entries));
    }
    BOOST_FOREACH(LogEntryPB* entry, entries) {
      operations->push_back(entry->release_operation());
    }
  }

  // Verifies that the replica's log match the leader's. This deletes the
  // peers (so we're sure that no further writes occur) and closes the logs
  // so it must be the very last thing to run, in a test.
  void VerifyLogs(int leader_idx = 2, int replica0_idx = 0, int replica1_idx = 1) {
    // Wait for in-flight transactions to be done. We're destroying the
    // peers next and leader transactions won't be able to commit anymore.

    string leader_name = GetPeer(leader_idx)->peer_uuid();
    string replica0_name = GetPeer(replica0_idx)->peer_uuid();
    string replica1_name = replica1_idx != kDontVerify ? GetPeer(replica1_idx)->peer_uuid() : "";
    BOOST_FOREACH(TestTransactionFactory* factory, txn_factories_) {
      factory->WaitDone();
    }
    BOOST_FOREACH(RaftConsensus* peer, peers_) {
      peer->Shutdown();
    }
    vector<OperationPB*> replica0_ops;
    ElementDeleter repl0_deleter(&replica0_ops);
    GatherOperations(replica0_idx, logs_[replica0_idx], &replica0_ops);
    vector<OperationPB*> replica1_ops;
    ElementDeleter repl1_deleter(&replica1_ops);
    if (replica1_idx != kDontVerify) {
      GatherOperations(replica1_idx, logs_[replica1_idx], &replica1_ops);
    }
    vector<OperationPB*> leader_ops;
    ElementDeleter leader_deleter(&leader_ops);
    GatherOperations(leader_idx, logs_[leader_idx], &leader_ops);

    // gather the leader's replicates and commits, the replicas
    // must have seen the same operations in the same order.
    vector<OpId> leader_replicates;
    vector<OpId> leader_commits;
    BOOST_FOREACH(OperationPB* operation, leader_ops) {
      if (operation->has_replicate()) {
        leader_replicates.push_back(operation->id());
      } else {
        leader_commits.push_back(operation->id());
      }
    }

    VerifyReplica(leader_replicates,
                  leader_commits,
                  leader_ops,
                  replica0_ops,
                  leader_name,
                  replica0_name);
    if (replica1_idx != kDontVerify) {
      VerifyReplica(leader_replicates,
                    leader_commits,
                    leader_ops,
                    replica1_ops,
                    leader_name,
                    replica1_name);
    }
  }

  void VerifyReplica(const vector<OpId>& leader_replicates,
                     const vector<OpId>& leader_commits,
                     const vector<OperationPB*>& leader_ops,
                     const vector<OperationPB*>& replica_ops,
                     string leader_name,
                     string replica_name) {
    int l_repl_idx = 0;
    int l_comm_idx = 0;
    OpId last_repl_op = MinimumOpId();
    unordered_set<consensus::OpId,
                  OpIdHashFunctor,
                  OpIdEqualsFunctor> replication_ops;

    SCOPED_TRACE(PrintOnError(leader_ops, Substitute("Leader: $0", leader_name)));
    SCOPED_TRACE(PrintOnError(replica_ops, Substitute("Replica: $0", replica_name)));
    for (int i = 0; i < replica_ops.size(); i++) {
      OperationPB* operation = replica_ops[i];
      // check that the operations appear in the same order as the leaders
      // additionally check that no commit op appears before its replicate op
      // and that replicate op ids are monotonically increasing
      if (operation->has_replicate()) {
        ASSERT_LT(l_repl_idx, leader_replicates.size());
        ASSERT_TRUE(OpIdEquals(leader_replicates[l_repl_idx], operation->id()))
            << "Expected Leader Replicate: " << leader_replicates[l_repl_idx].ShortDebugString()
            << " To match replica: " << operation->id().ShortDebugString();
        ASSERT_TRUE(OpIdCompare(operation->id(), last_repl_op) > 0);
        last_repl_op = operation->id();
        replication_ops.insert(last_repl_op);
        l_repl_idx++;
      } else {
        CHECK(operation->has_commit());
        ASSERT_LT(l_comm_idx, leader_commits.size());
        ASSERT_EQ(replication_ops.erase(operation->commit().commited_op_id()), 1);
        l_comm_idx++;
      }
    }
  }

  string PrintOnError(const vector<OperationPB*>& replica_ops,
                      string replica_id) {
    string ret = "";
    SubstituteAndAppend(&ret, "Replica Ops for replica: $0 Num. Replica Ops: $1\n",
                        replica_id,
                        replica_ops.size());
    BOOST_FOREACH(OperationPB* replica_op, replica_ops) {
      StrAppend(&ret, "Replica Operation: ", replica_op->ShortDebugString(), "\n");
    }
    return ret;
  }

  ~RaftConsensusTest() {
    STLDeleteElements(&txn_factories_);
    STLDeleteElements(&peers_);
    STLDeleteElements(&logs_);
    STLDeleteElements(&fs_managers_);
  }

 protected:
  ConsensusOptions options_;
  QuorumPB quorum_;
  OpId initial_id_;
  vector<FsManager*> fs_managers_;
  vector<RaftConsensus*> peers_;
  vector<Log*> logs_;
  vector<TestTransactionFactory*> txn_factories_;
  scoped_refptr<server::Clock> clock_;
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  const Schema schema_;
};

// Tests Replicate/Commit a single message through the leader.
TEST_F(RaftConsensusTest, TestFollowersReplicateAndCommitMessage) {
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
  ReplicateSequenceOfMessages(1,
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
  VerifyLogs();
}

// Tests Replicate/Commit a sequence of messages through the leader.
// First a sequence of replicates and then a sequence of commits.
TEST_F(RaftConsensusTest, TestFollowersReplicateAndCommitSequence) {
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

  ReplicateSequenceOfMessages(seq_size,
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
  VerifyLogs();
}

TEST_F(RaftConsensusTest, TestConsensusContinuesIfAMinorityFallsBehind) {
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
    ReplicaState* follower0_rs = GetPeer(kFollower0Idx)->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock;
    ASSERT_STATUS_OK(follower0_rs->LockForRead(&lock));

    // If the locked replica would stop consensus we would hang here
    // as we wait for operations to be replicated to a majority.
    ReplicateSequenceOfMessages(10,
                                kLeaderidx,
                                WAIT_FOR_MAJORITY,
                                COMMIT_ONE_BY_ONE,
                                &last_replicate,
                                &rounds);

    // Follower 1 should be fine (Were we to wait for follower0's replicate
    // this would hang here). We know he must have replicated but make sure
    // by calling Wait().
    WaitForReplicateIfNotAlreadyPresent(last_replicate, kFollower1Idx);
    WaitForCommitIfNotAlreadyPresent(last_replicate, kFollower1Idx, kLeaderidx);
  }

  // After we let the lock go the remaining follower should get up-to-date
  WaitForReplicateIfNotAlreadyPresent(last_replicate, kFollower0Idx);
  WaitForCommitIfNotAlreadyPresent(last_replicate, kFollower0Idx, kLeaderidx);
  VerifyLogs();
}

TEST_F(RaftConsensusTest, TestConsensusStopsIfAMajorityFallsBehind) {
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
    ReplicaState* follower0_rs = GetPeer(kFollower0Idx)->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock0;
    ASSERT_STATUS_OK(follower0_rs->LockForRead(&lock0));

    ReplicaState* follower1_rs = GetPeer(1)->GetReplicaStateForTests();
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
  CommitDummyMessage(round.get());
  // Assert that everything was ok
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower0Idx);
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower1Idx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower1Idx, kLeaderidx);
  VerifyLogs();
}

// If some communication error happens the leader will resend the request to the
// peers. This tests that the peers handle repeated requests.
TEST_F(RaftConsensusTest, TestReplicasHandleCommunicationErrors) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;

  // Append a dummy message, make sure it gets to everyone (if both
  // replicas get the commit the leader is guaranteed to also
  // have gotten it)
  gscoped_ptr<ConsensusRound> round;
  ASSERT_STATUS_OK(AppendDummyMessage(kLeaderidx, &round));
  GetLeaderProxyToPeer(kFollower0Idx, kLeaderidx)->InjectCommFaultLeaderSide();
  GetLeaderProxyToPeer(kFollower1Idx, kLeaderidx)->InjectCommFaultLeaderSide();

  last_op_id = round->id();

  ASSERT_STATUS_OK(CommitDummyMessage(round.get()));
  GetLeaderProxyToPeer(kFollower0Idx, kLeaderidx)->InjectCommFaultLeaderSide();
  GetLeaderProxyToPeer(kFollower1Idx, kLeaderidx)->InjectCommFaultLeaderSide();
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
    ASSERT_STATUS_OK(CommitDummyMessage(round.get()));
    last_op_id.CopyFrom(round->id());
    contexts.push_back(round.release());

    // inject comm faults
    if (i % 2 == 0) {
      GetLeaderProxyToPeer(kFollower0Idx, kLeaderidx)->InjectCommFaultLeaderSide();
    } else {
      GetLeaderProxyToPeer(kFollower1Idx, kLeaderidx)->InjectCommFaultLeaderSide();
    }
  }

  // Assert last operation was correctly replicated and committed.
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower0Idx);
  WaitForReplicateIfNotAlreadyPresent(last_op_id, kFollower1Idx);

  // See comment at the end of TestFollowersReplicateAndCommitMessage
  // for an explanation on this waiting sequence.
  ASSERT_STATUS_OK(commit_clbk.get()->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower0Idx, kLeaderidx);
  WaitForCommitIfNotAlreadyPresent(last_op_id, kFollower1Idx, kLeaderidx);
  VerifyLogs();
}

// In this test we test the ability of the leader to send heartbeats
// to replicas by simply pushing nothing after the configuration round
// and still expecting for the replicas Update() hooks to be called.
TEST_F(RaftConsensusTest, TestLeaderHeartbeats) {
  // Constants with the indexes of peers with certain roles,
  // since peers don't change roles in this test.
  const int kFollower0Idx = 0;
  const int kFollower1Idx = 1;
  const int kLeaderidx = 2;

  ASSERT_STATUS_OK(BuildQuorum(3));
  RaftConsensus* replica0 = GetPeer(kFollower0Idx);
  RaftConsensus* replica1 = GetPeer(kFollower1Idx);

  shared_ptr<CounterHooks> counter_hook_rpl0(
      new CounterHooks(replica0->GetFaultHooks()));
  shared_ptr<CounterHooks> counter_hook_rpl1(
      new CounterHooks(replica1->GetFaultHooks()));

  // Replace the default fault hooks on the replicas with counter hooks
  // before we start the quorum.
  replica0->SetFaultHooks(counter_hook_rpl0);
  replica1->SetFaultHooks(counter_hook_rpl1);

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
}

// After creating the initial quorum, this test writes a small sequence
// of messages to the initial leader. It then shuts down the current
// leader, makes another peer become leader and writes a sequence of
// messages to it. The new leader and the follower should agree on the
// sequence of messages.
TEST_F(RaftConsensusTest, TestLeaderPromotionWithQuiescedQuorum) {
  ASSERT_STATUS_OK(BuildAndStartQuorum(3));

  OpId last_op_id;
  shared_ptr<LatchCallback> last_commit_clbk;
  vector<ConsensusRound*> rounds;
  ElementDeleter deleter(&rounds);
  ReplicateSequenceOfMessages(10,
                              2, // The index of the initial leader.
                              WAIT_FOR_ALL_REPLICAS,
                              COMMIT_ONE_BY_ONE,
                              &last_op_id,
                              &rounds,
                              &last_commit_clbk);

  // Make sure the last operation is committed everywhere
  ASSERT_STATUS_OK(last_commit_clbk.get()->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, 0, 2);
  WaitForCommitIfNotAlreadyPresent(last_op_id, 1, 2);

  // Now shutdown the current leader
  RaftConsensus* current_leader = GetPeer(2);
  current_leader->Shutdown();

  // ... and make peer 1 become leader
  RaftConsensus* new_leader = GetPeer(1);

  // This will make peer-1 change itself to leader, change peer-2 to
  // follower and submit a config change.
  ASSERT_STATUS_OK(new_leader->EmulateElection());

  // ... replicating a set of messages to peer 1 should now be possible.
  ReplicateSequenceOfMessages(10,
                              1, // The index of the new leader.
                              WAIT_FOR_MAJORITY,
                              COMMIT_ONE_BY_ONE,
                              &last_op_id,
                              &rounds,
                              &last_commit_clbk);

  // Make sure the last operation is committed everywhere
  ASSERT_STATUS_OK(last_commit_clbk.get()->Wait());
  WaitForCommitIfNotAlreadyPresent(last_op_id, 0, 1);
  VerifyLogs(1, 0, kDontVerify);
}

}  // namespace consensus
}  // namespace kudu
