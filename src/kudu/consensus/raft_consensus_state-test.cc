// Copyright (c) 2014 Cloudera, Inc.
#include "kudu/consensus/raft_consensus_state.h"

#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using metadata::QuorumPB;

// TODO: Share a test harness with ConsensusMetadataTest?
const char* kTabletId = "TestTablet";
const int64_t kInitialSeqno = 0;
const uint64_t kInitialTerm = 0;

class RaftConsensusStateTest : public KuduTest {
 public:
  RaftConsensusStateTest()
    : fs_manager_(env_.get(), test_dir_),
      txn_factory_(new MockTransactionFactory()) {
    CHECK_OK(ThreadPoolBuilder("state-pool").Build(&pool_));
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test quorum.
    quorum_.set_local(true);
    quorum_.set_seqno(kInitialSeqno);
    quorum_.add_peers()->set_permanent_uuid(fs_manager_.uuid());

    gscoped_ptr<ConsensusMetadata> cmeta;
    ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, quorum_, kInitialTerm, &cmeta));
    state_.reset(new ReplicaState(ConsensusOptions(), pool_.get(), fs_manager_.uuid(), cmeta.Pass(),
                                  txn_factory_.get()));
  }

 protected:
  FsManager fs_manager_;
  QuorumPB quorum_;
  gscoped_ptr<ThreadPool> pool_;
  gscoped_ptr<MockTransactionFactory> txn_factory_;
  gscoped_ptr<ReplicaState> state_;
};

// Test that we can transition a new quorum from a pending state into a
// persistent state.
TEST_F(RaftConsensusStateTest, TestPendingPersistent) {
  ReplicaState::UniqueLock lock;
  ASSERT_OK(state_->LockForConfigChange(&lock));

  quorum_.set_seqno(1);

  ASSERT_OK(state_->SetPendingQuorumUnlocked(quorum_));
  ASSERT_TRUE(state_->IsQuorumChangePendingUnlocked());
  ASSERT_EQ(1, state_->GetPendingQuorumUnlocked().seqno());
  ASSERT_EQ(0, state_->GetCommittedQuorumUnlocked().seqno());

  ASSERT_OK(state_->SetCommittedQuorumUnlocked(quorum_));
  ASSERT_FALSE(state_->IsQuorumChangePendingUnlocked());
  ASSERT_EQ(1, state_->GetCommittedQuorumUnlocked().seqno());
}

// Ensure that we can set persistent quorums directly.
TEST_F(RaftConsensusStateTest, TestPersistentWrites) {
  ReplicaState::UniqueLock lock;
  ASSERT_OK(state_->LockForConfigChange(&lock));

  ASSERT_FALSE(state_->IsQuorumChangePendingUnlocked());
  ASSERT_EQ(0, state_->GetCommittedQuorumUnlocked().seqno());

  quorum_.set_seqno(1);
  ASSERT_OK(state_->SetCommittedQuorumUnlocked(quorum_));
  ASSERT_EQ(1, state_->GetCommittedQuorumUnlocked().seqno());

  quorum_.set_seqno(2);
  ASSERT_OK(state_->SetCommittedQuorumUnlocked(quorum_));
  ASSERT_EQ(2, state_->GetCommittedQuorumUnlocked().seqno());
}

// Death tests.
typedef RaftConsensusStateTest RaftConsensusStateDeathTest;

// Assert that we get a process crash when we set a pending quorum and then
// attempt to set a different persistent quorum. See KUDU-513.
TEST_F(RaftConsensusStateDeathTest, TestCrashIfPersistentDiffersFromPending) {
  ReplicaState::UniqueLock lock;
  ASSERT_OK(state_->LockForConfigChange(&lock));

  quorum_.set_seqno(1);
  ASSERT_OK(state_->SetPendingQuorumUnlocked(quorum_));
  ASSERT_TRUE(state_->IsQuorumChangePendingUnlocked());

  quorum_.set_seqno(2);

  // Should trigger a CHECK failure.
  ASSERT_DEATH(state_->SetCommittedQuorumUnlocked(quorum_),
               "Attempting to persist quorum change while a different one is pending");
}

}  // namespace consensus
}  // namespace kudu
