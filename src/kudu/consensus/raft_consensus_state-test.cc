// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/raft_consensus_state.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <vector>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using boost::assign::list_of;
using std::vector;

// TODO: Share a test harness with ConsensusMetadataTest?
const char* kTabletId = "TestTablet";

class RaftConsensusStateTest : public KuduTest {
 public:
  RaftConsensusStateTest()
    : fs_manager_(env_.get(), GetTestPath("fs_root")),
      txn_factory_(new MockTransactionFactory()) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test quorum.
    quorum_.set_local(true);
    quorum_.add_peers()->set_permanent_uuid(fs_manager_.uuid());
    quorum_.set_opid_index(kInvalidOpIdIndex);

    gscoped_ptr<ConsensusMetadata> cmeta;
    ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, fs_manager_.uuid(),
                                        quorum_, kMinimumTerm, &cmeta));
    state_.reset(new ReplicaState(ConsensusOptions(), fs_manager_.uuid(), cmeta.Pass(),
                                  txn_factory_.get()));

    // Start up the ReplicaState.
    ReplicaState::UniqueLock lock;
    ASSERT_OK(state_->LockForStart(&lock));
    ASSERT_OK(state_->StartUnlocked(MinimumOpId()));
  }

 protected:
  FsManager fs_manager_;
  QuorumPB quorum_;
  gscoped_ptr<MockTransactionFactory> txn_factory_;
  gscoped_ptr<ReplicaState> state_;
};

// Test that we can transition a new quorum from a pending state into a
// persistent state.
TEST_F(RaftConsensusStateTest, TestPendingPersistent) {
  ReplicaState::UniqueLock lock;
  ASSERT_OK(state_->LockForConfigChange(&lock));

  quorum_.clear_opid_index();
  ASSERT_OK(state_->SetPendingQuorumUnlocked(quorum_));
  ASSERT_TRUE(state_->IsQuorumChangePendingUnlocked());
  ASSERT_FALSE(state_->GetPendingQuorumUnlocked().has_opid_index());
  ASSERT_TRUE(state_->GetCommittedQuorumUnlocked().has_opid_index());

  ASSERT_FALSE(state_->SetCommittedQuorumUnlocked(quorum_).ok());
  quorum_.set_opid_index(1);
  ASSERT_TRUE(state_->SetCommittedQuorumUnlocked(quorum_).ok());

  ASSERT_FALSE(state_->IsQuorumChangePendingUnlocked());
  ASSERT_EQ(1, state_->GetCommittedQuorumUnlocked().opid_index());
}

// Ensure that we can set persistent quorums directly.
TEST_F(RaftConsensusStateTest, TestPersistentWrites) {
  ReplicaState::UniqueLock lock;
  ASSERT_OK(state_->LockForConfigChange(&lock));

  ASSERT_FALSE(state_->IsQuorumChangePendingUnlocked());
  ASSERT_EQ(kInvalidOpIdIndex, state_->GetCommittedQuorumUnlocked().opid_index());

  quorum_.set_opid_index(1);
  ASSERT_OK(state_->SetCommittedQuorumUnlocked(quorum_));
  ASSERT_EQ(1, state_->GetCommittedQuorumUnlocked().opid_index());

  quorum_.set_opid_index(2);
  ASSERT_OK(state_->SetCommittedQuorumUnlocked(quorum_));
  ASSERT_EQ(2, state_->GetCommittedQuorumUnlocked().opid_index());
}

}  // namespace consensus
}  // namespace kudu
