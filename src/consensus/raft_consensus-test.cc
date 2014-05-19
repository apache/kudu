// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "common/wire_protocol-test-util.h"
#include "consensus/consensus.pb.h"
#include "consensus/consensus-test-util.h"
#include "consensus/raft_consensus.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/stl_util.h"
#include "consensus/log_reader.h"
#include "rpc/rpc_context.h"
#include "server/metadata.h"
#include "server/logical_clock.h"
#include "util/test_macros.h"
#include "util/test_util.h"

namespace kudu {

namespace rpc {
class RpcContext;
}
namespace consensus {

using log::Log;
using log::LogEntryPB;
using log::LogOptions;
using log::LogReader;
using log::ReadableLogSegment;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using metadata::TabletSuperBlockPB;
using rpc::RpcContext;
using strings::Substitute;

const char* kTestTablet = "TestTablet";

class RaftConsensusTest : public KuduTest {
 public:
  RaftConsensusTest()
    : clock_(server::LogicalClock::CreateStartingAt(Timestamp(0))) {}

  // Builds an initial quorum of 'num' elements where the first
  // element is the leader.
  void BuildInitialQuorumPB(int num) {
    for (int i = 0; i < num; i++) {
      QuorumPeerPB* peer_pb = quorum_.add_peers();
      peer_pb->set_permanent_uuid(Substitute("peer-$0", i));
      if (i == num - 1) {
        peer_pb->set_role(QuorumPeerPB::LEADER);
      } else {
        peer_pb->set_role(QuorumPeerPB::FOLLOWER);
      }
      HostPortPB* hp = peer_pb->mutable_last_known_addr();
      hp->set_host("0");
      hp->set_port(0);
    }
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
      RaftConsensus* peer = new RaftConsensus(options_,
                                              gscoped_ptr<PeerProxyFactory>(proxy_factory).Pass());

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

  Status InitPeers() {
    for (int i = 0; i < quorum_.peers_size(); i++) {
      RaftConsensus* peer = peers_[i];
      TestTransactionFactory* txn_factory = new TestTransactionFactory();
      txn_factories_.push_back(txn_factory);
      RETURN_NOT_OK(peer->Init(quorum_.peers(i), clock_, txn_factory, logs_[i]));
    }
    return Status::OK();
  }

  Status StartPeers() {
    for (int i = 0; i < quorum_.peers_size(); i++) {
      RaftConsensus* peer = peers_[i];
      gscoped_ptr<metadata::QuorumPB> running_quorum;
      RETURN_NOT_OK(peer->Start(quorum_, &running_quorum));
    }
    return Status::OK();
  }

  Status BuildQuorum(int num) {
    BuildInitialQuorumPB(num);
    RETURN_NOT_OK(BuildFsManagersAndLogs());
    BuildPeers();
    RETURN_NOT_OK(InitPeers());
    RETURN_NOT_OK(StartPeers());
    return Status::OK();
  }

  RaftConsensus* GetLeader() {
    return peers_[peers_.size() - 1];
  }

  RaftConsensus* GetFollower(int idx) {
    return peers_[idx];
  }

  LocalTestPeerProxy* GetFollowerProxy(int idx) {
    RaftConsensus* follower = GetFollower(idx);
    BOOST_FOREACH(LocalTestPeerProxy* proxy, down_cast<LocalTestPeerProxyFactory*>(
        GetLeader()->peer_proxy_factory_.get())->GetProxies()) {
      if (proxy->GetTarget()->peer_uuid() == follower->peer_uuid()) {
        return proxy;
      }
    }
    CHECK(false) << "Proxy not found";
    return NULL;
  }

  Status AppendDummyMessage(RaftConsensus* peer, gscoped_ptr<ConsensusRound>* round) {
    gscoped_ptr<ReplicateMsg> msg(new ReplicateMsg());
    msg->set_op_type(NO_OP);
    msg->mutable_no_op();
    shared_ptr<FutureCallback> replicate_clbk(new LatchCallback);
    shared_ptr<FutureCallback> commit_clbk(new LatchCallback);
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
        round->replicate_callback().get())->TimedWait(delta);
  }

  void WaitForReplicateIfNotAlreadyPresent(const OpId& to_wait_for, RaftConsensus* replica) {
    shared_ptr<LatchCallback> clbk(new LatchCallback);
    Status s = replica->RegisterOnReplicateCallback(to_wait_for, clbk);
    if (s.IsAlreadyPresent()) {
      return;
    }
    ASSERT_STATUS_OK(s);
    clbk->Wait();
  }

  void WaitForCommitIfNotAlreadyPresent(const OpId& to_wait_for, RaftConsensus* replica, int idx) {
    shared_ptr<LatchCallback> clbk(new LatchCallback);
    Status s = replica->RegisterOnCommitCallback(to_wait_for, clbk);
    if (s.IsAlreadyPresent()) {
      return;
    }
    ASSERT_STATUS_OK(s);
    int num_attempts = 0;
    // Because commit's are asynchonous we need to keep making the leader
    // send status updates.
    // TODO heartbeats should do this.
    while (true) {
      Status s = clbk->TimedWait(MonoDelta::FromMilliseconds(1000));
      if (s.ok()) {
        return;
      }
      CHECK(s.IsTimedOut());
      GetLeader()->SignalRequestToPeers(true);
      num_attempts++;
      if (num_attempts == 10) {
        LOG(ERROR) << "Max timeout attempts reached while waiting for commit: "
            << to_wait_for.ShortDebugString() << " on replica. Dumping state and quitting.";
        GetLeader()->queue_.DumpToLog();

        // Gather the replica and leader operations for printing
        vector<OperationPB*> replica_ops;
        ElementDeleter repl0_deleter(&replica_ops);
        GatherOperations(idx, replica->log(), &replica_ops);
        vector<OperationPB*> leader_ops;
        ElementDeleter leader_deleter(&leader_ops);
        int leader_idx = peers_.size() - 1;
        GatherOperations(leader_idx, logs_[leader_idx], &leader_ops);

        SCOPED_TRACE(PrintOnError(leader_ops, "leader"));
        SCOPED_TRACE(PrintOnError(replica_ops, replica->peer_uuid()));
        FAIL() << "Replica did not commit.";
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
    BOOST_FOREACH(const shared_ptr<ReadableLogSegment> segment, log_reader->segments()) {
      ASSERT_STATUS_OK(log_reader->ReadEntries(segment, &entries));
    }
    BOOST_FOREACH(LogEntryPB* entry, entries) {
      operations->push_back(entry->release_operation());
    }
  }

  // Verifies that the replica's log match the leader's. This closes the log
  // so if this is not the very last thing to be executed the log must be reopened
  // after this.
  void VerifyLogs() {
    vector<OperationPB*> replica0_ops;
    ElementDeleter repl0_deleter(&replica0_ops);
    GatherOperations(0, logs_[0], &replica0_ops);
    vector<OperationPB*> replica1_ops;
    ElementDeleter repl1_deleter(&replica1_ops);
    GatherOperations(1, logs_[1], &replica1_ops);
    vector<OperationPB*> leader_ops;
    ElementDeleter leader_deleter(&leader_ops);
    GatherOperations(2, logs_[2], &leader_ops);

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

    VerifyReplica(leader_replicates, leader_commits, leader_ops, replica0_ops, "peer-0");
    VerifyReplica(leader_replicates, leader_commits, leader_ops, replica1_ops, "peer-1");
  }

  void VerifyReplica(const vector<OpId>& leader_replicates,
                     const vector<OpId>& leader_commits,
                     const vector<OperationPB*>& leader_ops,
                     const vector<OperationPB*>& replica_ops,
                     string replica_id) {
    int l_repl_idx = 0;
    int l_comm_idx = 0;
    OpId last_repl_op = log::MinimumOpId();
    unordered_set<consensus::OpId,
                  log::OpIdHashFunctor,
                  log::OpIdEqualsFunctor> replication_ops;

    SCOPED_TRACE(PrintOnError(leader_ops, "leader"));
    SCOPED_TRACE(PrintOnError(replica_ops, replica_id));
    for (int i = 0; i < replica_ops.size(); i++) {
      OperationPB* operation = replica_ops[i];
      // check that the operations appear in the same order as the leaders
      // additionally check that no commit op appears before its replicate op
      // and that replicate op ids are monotonically increasing
      if (operation->has_replicate()) {
        ASSERT_LT(l_repl_idx, leader_replicates.size());
        ASSERT_TRUE(log::OpIdEquals(leader_replicates[l_repl_idx], operation->id()))
            << "Expected Leader Replicate: " << leader_replicates[l_repl_idx].ShortDebugString()
            << " To match replica: " << operation->id().ShortDebugString();
        ASSERT_TRUE(log::OpIdCompare(operation->id(), last_repl_op) > 0);
        last_repl_op = operation->id();
        replication_ops.insert(last_repl_op);
        l_repl_idx++;
      } else {
        ASSERT_LT(l_comm_idx, leader_commits.size());
        ASSERT_TRUE(log::OpIdEquals(leader_commits[l_comm_idx], operation->id()))
            << "Expected Leader Commit: " << leader_commits[l_comm_idx].ShortDebugString()
            << " To match replica: " << operation->id().ShortDebugString();
        ASSERT_EQ(replication_ops.erase(operation->commit().commited_op_id()), 1);
        l_comm_idx++;
      }
    }
  }

  string PrintOnError(const vector<OperationPB*>& replica_ops,
                      string replica_id) {
    string ret = "";
    StrAppend(&ret, Substitute("Replica Ops for replica: $0 Num. Replica Ops: $1\n",
                               replica_id,
                               replica_ops.size()));
    BOOST_FOREACH(OperationPB* replica_op, replica_ops) {
      StrAppend(&ret, "Replica Operation: ", replica_op->ShortDebugString(), "\n");
    }
    return ret;
  }

  ~RaftConsensusTest() {
    STLDeleteElements(&peers_);
    STLDeleteElements(&txn_factories_);
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
};

// Tests Replicate/Commit a single message through the leader.
TEST_F(RaftConsensusTest, TestFollowersReplicateAndCommitMessage) {
  ASSERT_STATUS_OK(BuildQuorum(3));

  OpId last_op_id;
  gscoped_ptr<ConsensusRound> round;
  ASSERT_STATUS_OK(AppendDummyMessage(GetLeader(), &round));
  ASSERT_STATUS_OK(WaitForReplicate(round.get()));
  last_op_id.CopyFrom(round->id());

  // Wait for the followers replicate the operations.
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(0));
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(1));
  // Commit the operation
  ASSERT_STATUS_OK(CommitDummyMessage(round.get()));

  // Wait for the followers commit the operations.
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(0), 0);
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(1), 1);
  VerifyLogs();
}

// Tests Replicate/Commit a sequence of messages through the leader.
// First a sequence of replicates and then a sequence of commits.
TEST_F(RaftConsensusTest, TestFollowersReplicateAndCommitSequence) {

  int seq_size = AllowSlowTests() ? 1000 : 100;

  ASSERT_STATUS_OK(BuildQuorum(3));

  // We'll append seq_size messages so we register the callback for op seq_size + 1
  // (to account for the initial configuration op).
  OpId last_op_id;

  vector<ConsensusRound*> contexts;
  ElementDeleter deleter(&contexts);
  for (int i = 0; i < seq_size; i++) {
    gscoped_ptr<ConsensusRound> round;
    ASSERT_STATUS_OK(AppendDummyMessage(GetLeader(), &round));
    ASSERT_STATUS_OK(WaitForReplicate(round.get()));
    last_op_id.CopyFrom(round->id());
    contexts.push_back(round.release());
  }

  // Wait for the followers replicate the operations.
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(0));
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(1));

  // Commit the operations, but wait for the replicates to finish first
  BOOST_FOREACH(ConsensusRound* round, contexts) {
    ASSERT_STATUS_OK(CommitDummyMessage(round));
  }

  // Wait for the followers commit the operations.
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(0), 0);
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(1), 1);
  VerifyLogs();
}

TEST_F(RaftConsensusTest, TestConsensusContinuesIfAMinorityFallsBehind) {
  ASSERT_STATUS_OK(BuildQuorum(3));

  OpId last_replicate;
  vector<ConsensusRound*> contexts;
  ElementDeleter deleter(&contexts);
  {
    // lock one of the replicas down by obtaining the state lock
    // and never letting it go.
    ReplicaState* follower0_rs = GetFollower(0)->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock;
    follower0_rs->LockForRead(&lock);

    // replicate and commit 10 messages
    for (int i = 0; i < 10; i++) {
      gscoped_ptr<ConsensusRound> round;
      ASSERT_STATUS_OK(AppendDummyMessage(GetLeader(), &round));
      last_replicate.CopyFrom(round->id());
      // if the locked replica was stopping consensus we would hang here
      WaitForReplicate(round.get());
      CommitDummyMessage(round.get());
      contexts.push_back(round.release());
    }
    // Follower 1 should be fine (Were we to wait for follower0's replicate
    // this would hang here). We know he must have replicated but make sure
    // by calling Wait().
    WaitForReplicateIfNotAlreadyPresent(last_replicate, GetFollower(1));
    WaitForCommitIfNotAlreadyPresent(last_replicate, GetFollower(1), 1);
  }

  // After we let the lock go the remaining follower should get up-to-date
  WaitForReplicateIfNotAlreadyPresent(last_replicate, GetFollower(0));
  WaitForCommitIfNotAlreadyPresent(last_replicate, GetFollower(0), 0);
  VerifyLogs();
}

TEST_F(RaftConsensusTest, TestConsensusStopsIfAMajorityFallsBehind) {
  ASSERT_STATUS_OK(BuildQuorum(3));

  OpId last_op_id;

  gscoped_ptr<ConsensusRound> round;
  {
    // lock two of the replicas down by obtaining the state locks
    // and never letting them go.
    ReplicaState* follower0_rs = GetFollower(0)->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock0;
    follower0_rs->LockForRead(&lock0);

    ReplicaState* follower1_rs = GetFollower(1)->GetReplicaStateForTests();
    ReplicaState::UniqueLock lock1;
    follower1_rs->LockForRead(&lock1);

    // Append a single message to the queue
    ASSERT_STATUS_OK(AppendDummyMessage(GetLeader(), &round));
    last_op_id.CopyFrom(round->id());
    // This should timeout.
    Status status = TimedWaitForReplicate(round.get(), MonoDelta::FromMilliseconds(500));
    ASSERT_TRUE(status.IsTimedOut());
  }
  // After we release the locks the operation should replicate to all replicas
  // and we commit.
  CommitDummyMessage(round.get());
  // Assert that everything was ok
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(0));
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(1));
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(0), 0);
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(1), 1);
  VerifyLogs();
}

// If some communication error happens the leader will resend the request to the
// peers. This tests that the peers handle repeated requests.
TEST_F(RaftConsensusTest, TestReplicasHandleCommunicationErrors) {
  ASSERT_STATUS_OK(BuildQuorum(3));

  OpId last_op_id;

  // Append a dummy message, make sure it gets to everyone (if both
  // replicas get the commit the leader is guaranteed to also
  // have gotten it)
  gscoped_ptr<ConsensusRound> round;
  ASSERT_STATUS_OK(AppendDummyMessage(GetLeader(), &round));

  last_op_id = round->id();

  ASSERT_STATUS_OK(CommitDummyMessage(round.get()));
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(0), 0);
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(1), 1);

  // Append a sequence of messages, and keep injecting errors into the
  // replica proxies.
  vector<ConsensusRound*> contexts;
  ElementDeleter deleter(&contexts);
  for (int i = 0; i < 100; i++) {
    gscoped_ptr<ConsensusRound> round;
    ASSERT_STATUS_OK(AppendDummyMessage(GetLeader(), &round));
    ASSERT_STATUS_OK(CommitDummyMessage(round.get()));
    last_op_id.CopyFrom(round->id());
    contexts.push_back(round.release());

    // inject comm faults
    if (i % 2 == 0) {
      GetFollowerProxy(0)->InjectCommFaultLeaderSide();
    } else {
      GetFollowerProxy(1)->InjectCommFaultLeaderSide();
    }
  }

  // Assert las operation was correctly committed
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(0));
  WaitForReplicateIfNotAlreadyPresent(last_op_id, GetFollower(1));
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(0), 0);
  WaitForCommitIfNotAlreadyPresent(last_op_id, GetFollower(1), 1);
  VerifyLogs();
}

}  // namespace consensus
}  // namespace kudu
