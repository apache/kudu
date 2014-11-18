// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/peer_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/server/logical_clock.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using log::Log;
using log::LogOptions;
using metadata::QuorumPB;
using std::string;
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::InSequence;
using ::testing::Return;

const char* kTestTablet = "TestTablet";

class MockQueue : public PeerMessageQueue {
 public:
  explicit MockQueue(const MetricContext& metric_ctx, log::Log* log)
    : PeerMessageQueue(metric_ctx, log) {}
  MOCK_METHOD4(Init, void(RaftConsensusQueueIface* consensus,
                          const OpId& committed_index,
                          uint64_t current_term,
                          int majority_size));
  virtual Status AppendOperation(gscoped_ptr<ReplicateMsg> replicate) OVERRIDE {
    VLOG(1) << "Appending to mock queue: " << replicate->ShortDebugString();
    return AppendOperationMock(replicate.release());
  }
  MOCK_METHOD1(AppendOperationMock, Status(ReplicateMsg* replicate));
  MOCK_METHOD1(TrackPeer, Status(const std::string& uuid));
  MOCK_METHOD1(UntrackPeer, void(const std::string& uuid));
  MOCK_METHOD2(RequestForPeer, void(const std::string& uuid,
                                    ConsensusRequestPB* request));
  MOCK_METHOD2(ResponseFromPeer, void(const ConsensusResponsePB& response,
                                      bool* more_pending));
  MOCK_METHOD0(Clear, void());
  MOCK_METHOD0(Close, void());
};

class MockPeerManager : public PeerManager {
 public:
  MockPeerManager() : PeerManager("", "", NULL, NULL, NULL) {}
  MOCK_METHOD1(UpdateQuorum, Status(const metadata::QuorumPB& quorum));
  MOCK_METHOD1(SignalRequest, void(bool force_if_queue_empty));
  MOCK_METHOD0(Close, void());
};

class RaftConsensusTest : public KuduTest {
 public:
  RaftConsensusTest()
      : clock_(server::LogicalClock::CreateStartingAt(Timestamp(0))),
        metric_context_(&metric_registry_, "raft-test"),
        schema_(GetSimpleTestSchema()) {

    options_.tablet_id = kTestTablet;
  }

  virtual void SetUp() OVERRIDE {
    LogOptions options;
    string test_path = GetTestPath("test-peer-root");
    env_->CreateDir(test_path);

    // TODO mock the Log too, since we're gonna mock the queue
    // monitors and pretty much everything else.
    fs_manager_.reset(new FsManager(env_.get(), test_path));

    CHECK_OK(Log::Open(LogOptions(),
                       fs_manager_.get(),
                       kTestTablet,
                       schema_,
                       NULL,
                       &log_));

    queue_ = new MockQueue(metric_context_, log_.get());
    peer_manager_ = new MockPeerManager;
    txn_factory_.reset(new MockTransactionFactory);

    ON_CALL(*txn_factory_, StartReplicaTransactionMock(_))
        .WillByDefault(Invoke(this, &RaftConsensusTest::StartReplicaTransaction));
    ON_CALL(*queue_, AppendOperationMock(_))
        .WillByDefault(Invoke(this, &RaftConsensusTest::AppendToLog));

  }

  void SetUpConsensus(QuorumPeerPB::Role initial_role = QuorumPeerPB::LEADER,
                      int64_t initial_term = consensus::kMinimumTerm) {
    BuildQuorumPBForTests(&quorum_, 1);
    quorum_.mutable_peers(0)->set_role(initial_role);

    gscoped_ptr<PeerProxyFactory> proxy_factory(new LocalTestPeerProxyFactory());

    gscoped_ptr<ConsensusMetadata> cmeta;
    CHECK_OK(ConsensusMetadata::Create(fs_manager_.get(), kTestTablet, quorum_,
                                       initial_term, &cmeta));

    consensus_.reset(new RaftConsensus(options_,
                                       cmeta.Pass(),
                                       proxy_factory.Pass(),
                                       gscoped_ptr<PeerMessageQueue>(queue_),
                                       gscoped_ptr<PeerManager>(peer_manager_),
                                       metric_context_,
                                       "peer-0",
                                       clock_,
                                       txn_factory_.get(),
                                       log_.get()));
  }

  Status AppendToLog(ReplicateMsg* replicate) {
    return log_->AsyncAppendReplicates(&replicate,
                                       1,
                                       Bind(LogAppendCallback, Owned(replicate)));
  }

  static void LogAppendCallback(ReplicateMsg* repl, const Status& s) {
    CHECK_OK(s);
  }

  Status StartReplicaTransaction(ConsensusRound* round) {
    shared_ptr<LatchCallback> commit_callback(new LatchCallback());
    commit_callbacks_.push_back(commit_callback);
    round->SetCommitCallback(commit_callback);
    rounds_.push_back(round);
    return Status::OK();
  }

  void SetUpGeneralExpectations() {
    EXPECT_CALL(*peer_manager_, SignalRequest(_))
        .Times(AnyNumber());
    EXPECT_CALL(*peer_manager_, Close())
        .Times(1);
    EXPECT_CALL(*queue_, Close())
            .Times(1);
  }

  ConsensusRound* CreateRound(gscoped_ptr<ReplicateMsg> replicate) {
    shared_ptr<LatchCallback> commit_callback(new LatchCallback());
    commit_callbacks_.push_back(commit_callback);
    ConsensusRound* round = new ConsensusRound(consensus_.get(),
                                               replicate.Pass(),
                                               NULL,
                                               commit_callback);
    rounds_.push_back(round);
    return round;
  }

  ConsensusRound* AppendNoOpRound() {
    gscoped_ptr<ReplicateMsg> replicate(new ReplicateMsg);
    replicate->set_op_type(NO_OP);
    ConsensusRound* round = CreateRound(replicate.Pass());
    CHECK_OK(consensus_->Replicate(round));
    return round;
  }

  void CommitRound(ConsensusRound* round) {
    // Need to commit, otherwise consensus will wait for these to finish.
    gscoped_ptr<CommitMsg> commit(new CommitMsg);
    commit->set_op_type(round->replicate_msg()->op_type());
    CHECK_OK(round->Commit(commit.Pass()));
  }

  void CommitRemainingRounds() {
    BOOST_FOREACH(ConsensusRound* round, rounds_) {
      CommitRound(round);
    }
  }

  ~RaftConsensusTest() {
    // Wait for all rounds to be done.
    BOOST_FOREACH(const shared_ptr<LatchCallback>& callback, commit_callbacks_) {
      callback->Wait();
    }
    STLDeleteElements(&rounds_);
  }

 protected:
  ConsensusOptions options_;
  QuorumPB quorum_;
  OpId initial_id_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<Log> log_;
  gscoped_ptr<PeerProxyFactory> proxy_factory_;
  scoped_refptr<server::Clock> clock_;
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  const Schema schema_;
  scoped_refptr<RaftConsensus> consensus_;

  vector<ConsensusRound*> rounds_;
  vector<shared_ptr<LatchCallback> > commit_callbacks_;

  // Mocks.
  // NOTE: The first two belong to 'consensus_' and may be deleted before
  // the test is.
  MockQueue* queue_;
  MockPeerManager* peer_manager_;
  gscoped_ptr<MockTransactionFactory> txn_factory_;
};

// Tests that the committed index moves along with the majority replicated
// index when the terms are the same.
TEST_F(RaftConsensusTest, TestCommittedIndexWhenInSameTerm) {
  SetUpConsensus();
  SetUpGeneralExpectations();
  EXPECT_CALL(*peer_manager_, UpdateQuorum(_))
      .Times(1)
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*queue_, Init(_, _, _, _))
      .Times(1);
  EXPECT_CALL(*txn_factory_, StartReplicaTransactionMock(_))
      .Times(1);
  EXPECT_CALL(*queue_, AppendOperationMock(_))
      .Times(11);


  ConsensusBootstrapInfo info;
  ASSERT_OK(consensus_->Start(info));

  // Commit the first config round, created on Start();
  OpId committed_index;
  consensus_->UpdateMajorityReplicated(rounds_[0]->id(), &committed_index);
  CommitRound(rounds_[0]);

  ASSERT_OPID_EQ(rounds_[0]->id(), committed_index);

  // Append 10 rounds
  for (int i = 0; i < 10; i++) {
    ConsensusRound* round = AppendNoOpRound();
    // queue reports majority replicated index in the leader's term
    // committed index should move accordingly.
    consensus_->UpdateMajorityReplicated(round->id(), &committed_index);
    ASSERT_OPID_EQ(round->id(), committed_index);

    CommitRound(round);
  }
}

// Tests that, when terms change, the commit index only advances when the majority
// replicated index is in the current term.
TEST_F(RaftConsensusTest, TestCommittedIndexWhenTermsChange) {
  SetUpConsensus();
  SetUpGeneralExpectations();
  EXPECT_CALL(*peer_manager_, UpdateQuorum(_))
      .Times(2)
      .WillRepeatedly(Return(Status::OK()));
  EXPECT_CALL(*queue_, Init(_, _, _, _))
      .Times(2);
  EXPECT_CALL(*txn_factory_, StartReplicaTransactionMock(_))
      .Times(2);
  EXPECT_CALL(*queue_, AppendOperationMock(_))
      .Times(4);

  ConsensusBootstrapInfo info;
  ASSERT_OK(consensus_->Start(info));

  OpId committed_index;
  consensus_->UpdateMajorityReplicated(rounds_[0]->id(), &committed_index);
  CommitRound(rounds_[0]);
  ASSERT_OPID_EQ(rounds_[0]->id(), committed_index);

  // Append another round in the current term (besides the original config round).
  ConsensusRound* round = AppendNoOpRound();

  // Now emulate an election, the same guy will be leader but the term
  // will change.
  ASSERT_OK(consensus_->EmulateElection());

  // Now tell consensus that 'round' has been majority replicated, this _shouldn't_
  // advance the committed index, since that belongs to a previous term.
  OpId new_committed_index;
  consensus_->UpdateMajorityReplicated(round->id(), &new_committed_index);
  ASSERT_OPID_EQ(committed_index, new_committed_index);

  ConsensusRound* last_config_round = rounds_[2];

  // Now notify that the last change config was committed, this should advance the
  // commit index to the id of the last change config.
  consensus_->UpdateMajorityReplicated(last_config_round->id(), &committed_index);
  ASSERT_OPID_EQ(last_config_round->id(), committed_index);
  // Since these became "consensus committed" before changing the actual
  // txn commitment order shouldn't matter.
  CommitRound(last_config_round);
  CommitRound(round);
}

// Tests that consensus is able to handle pending operations. It tests this in two ways:
// - It tests that consensus does the right thing with pending transactions from the the WAL.
// - It tests that when a follower gets promoted to leader it does the right thing
//   with the pending operations.
TEST_F(RaftConsensusTest, TestPendingTransactions) {
  // Start as follower, we'll promote the peer later.
  SetUpConsensus(QuorumPeerPB::FOLLOWER, 10);

  // Emulate a stateful system by having a bunch of operations in flight when consensus starts.
  // Specifically we emulate we're on term 10, with 5 operations before the last known
  // committed operation, 10.104, which should be committed immediately, and 5 operations after the
  // last known committed operation, which should be pending but not yet committed.
  ConsensusBootstrapInfo info;
  info.last_id.set_term(10);
  for (int i = 0; i < 10; i++) {
    ReplicateMsg* replicate = new ReplicateMsg();
    replicate->set_op_type(NO_OP);
    info.last_id.set_index(100 + i);
    replicate->mutable_id()->CopyFrom(info.last_id);
    info.orphaned_replicates.push_back(replicate);
  }

  info.last_committed_id.set_term(10);
  info.last_committed_id.set_index(104);

  {
    InSequence dummy;
    // On start we expect 10 regular transactions to be started, with 5 of those having
    // their commit continuation called immediately.
    EXPECT_CALL(*txn_factory_, StartReplicaTransactionMock(_))
        .Times(10);
    // Queue gets cleared when the peer becomes follower.
    EXPECT_CALL(*queue_, Clear())
        .Times(1);
  }

  ASSERT_OK(consensus_->Start(info));

  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(queue_));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(txn_factory_.get()));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(peer_manager_));

  // Now we test what this peer does with the pending operations once it's elected leader.
  {
    InSequence dummy;
    // Queue gets initted when the peer becomes leader.
    EXPECT_CALL(*queue_, Init(_, _, _, _))
    .Times(1);
    // Peer manager gets updated with the new set of peers to send stuff to.
    EXPECT_CALL(*peer_manager_, UpdateQuorum(_))
    .Times(1).WillOnce(Return(Status::OK()));
    // The 5 pending operations should all be appended to the queue for replication
    // with an additional change config operation for a total of 6 appended ops.
    EXPECT_CALL(*queue_, AppendOperationMock(_))
    .Times(6);
    // One more transaction is started in the factory, for the config change.
    EXPECT_CALL(*txn_factory_, StartReplicaTransactionMock(_))
    .Times(1);
  }

  // Emulate an election, this will make this peer become leader and trigger the
  // above set expectations.
  ASSERT_OK(consensus_->EmulateElection());

  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(queue_));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(txn_factory_.get()));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(peer_manager_));

  EXPECT_CALL(*peer_manager_, SignalRequest(_))
      .Times(AnyNumber());
  // In the end peer manager and the queue get closed.
  EXPECT_CALL(*peer_manager_, Close())
      .Times(1);
  EXPECT_CALL(*queue_, Close())
      .Times(1);

  // Now tell consensus all original orphaned replicates were majority replicated.
  // This should not advance the committed index because we haven't replicated
  // anything in the current term.
  OpId committed_index;
  consensus_->UpdateMajorityReplicated(info.orphaned_replicates.back()->id(),
                                       &committed_index);
  // Should still be the last committed in the the wal.
  ASSERT_OPID_EQ(committed_index, info.last_committed_id);

  // Now mark the last operation (the config round) as committed.
  // This should advance the committed index, since that round in on our current term,
  // and we should be able to commit all previous rounds.
  OpId cc_round_id = info.orphaned_replicates.back()->id();
  cc_round_id.set_term(11);
  cc_round_id.set_index(cc_round_id.index() + 1);
  consensus_->UpdateMajorityReplicated(cc_round_id,
                                       &committed_index);

  ASSERT_OPID_EQ(committed_index, cc_round_id);
  CommitRemainingRounds();
}


}  // namespace consensus
}  // namespace kudu
