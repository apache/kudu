// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/log.h"
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
using ::testing::Return;

const char* kTestTablet = "TestTablet";

class MockQueue : public PeerMessageQueue {
 public:
  explicit MockQueue(const MetricContext& metric_ctx) : PeerMessageQueue(metric_ctx) {}
  MOCK_METHOD4(Init, void(RaftConsensusQueueIface* consensus,
                          const OpId& committed_index,
                          uint64_t current_term,
                          int majority_size));
  virtual Status AppendOperation(gscoped_ptr<ReplicateMsg> replicate) OVERRIDE {
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
    BuildQuorumPBForTests(&quorum_, 1);
    options_.tablet_id = kTestTablet;
  }

  virtual void SetUp() OVERRIDE {
    LogOptions options;
    string test_path = GetTestPath("test-peer-root");
    env_->CreateDir(test_path);

    // TODO mock the Log too, since we're gonna mock the queue
    // monitors and pretty much everything else.
    fs_manager_.reset(new FsManager(env_.get(), test_path));
    gscoped_ptr<ConsensusMetadata> cmeta;
    CHECK_OK(ConsensusMetadata::Create(fs_manager_.get(), kTestTablet, quorum_,
                                       consensus::kMinimumTerm, &cmeta));
    CHECK_OK(Log::Open(LogOptions(),
                       fs_manager_.get(),
                       kTestTablet,
                       schema_,
                       NULL,
                       &log_));

    gscoped_ptr<PeerProxyFactory> proxy_factory(new LocalTestPeerProxyFactory());
    queue_ = new MockQueue(metric_context_);
    peer_manager_ = new MockPeerManager;
    txn_factory_.reset(new MockTransactionFactory);

    ON_CALL(*txn_factory_, StartReplicaTransactionMock(_))
        .WillByDefault(Invoke(this, &RaftConsensusTest::StartReplicaTransaction));
    ON_CALL(*queue_, AppendOperationMock(_))
        .WillByDefault(Invoke(this, &RaftConsensusTest::AppendToLog));

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
    EXPECT_CALL(*queue_, AppendOperationMock(_))
        .Times(AnyNumber());
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
  SetUpGeneralExpectations();
  EXPECT_CALL(*peer_manager_, UpdateQuorum(_))
      .Times(1)
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*queue_, Init(_, _, _, _))
      .Times(1);
  EXPECT_CALL(*txn_factory_, StartReplicaTransactionMock(_))
      .Times(1);

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
  SetUpGeneralExpectations();
  EXPECT_CALL(*peer_manager_, UpdateQuorum(_))
      .Times(2)
      .WillRepeatedly(Return(Status::OK()));
  EXPECT_CALL(*queue_, Init(_, _, _, _))
      .Times(2);
  EXPECT_CALL(*txn_factory_, StartReplicaTransactionMock(_))
      .Times(2);

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

}  // namespace consensus
}  // namespace kudu
