// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/consensus_meta.h"

#include <vector>

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

#define ASSERT_VALUES_EQUAL(cmeta, opid_index, uuid, term) \
  ASSERT_NO_FATAL_FAILURE(AssertValuesEqual(cmeta, opid_index, uuid, term))

namespace kudu {
namespace consensus {

using boost::assign::list_of;
using std::string;
using std::vector;

const char* kTabletId = "test-consensus-metadata";
const uint64_t kInitialTerm = 3;

class ConsensusMetadataTest : public KuduTest {
 public:
  ConsensusMetadataTest()
    : fs_manager_(env_.get(), GetTestPath("fs_root")) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test quorum.
    quorum_.set_local(true);
    quorum_.add_peers()->set_permanent_uuid(fs_manager_.uuid());
    quorum_.set_opid_index(kInvalidOpIdIndex);
  }

 protected:
  // Assert that the given cmeta has a single quorum with the given metadata values.
  void AssertValuesEqual(const ConsensusMetadata& cmeta,
                         int64_t opid_index, const string& permanant_uuid, uint64_t term);

  FsManager fs_manager_;
  QuorumPB quorum_;
};

void ConsensusMetadataTest::AssertValuesEqual(const ConsensusMetadata& cmeta,
                                              int64_t opid_index,
                                              const string& permanant_uuid,
                                              uint64_t term) {
  // Sanity checks.
  ASSERT_TRUE(cmeta.committed_quorum().local());
  ASSERT_EQ(1, cmeta.committed_quorum().peers_size());

  // Value checks.
  ASSERT_EQ(opid_index, cmeta.committed_quorum().opid_index());
  ASSERT_EQ(permanant_uuid, cmeta.committed_quorum().peers().begin()->permanent_uuid());
  ASSERT_EQ(term, cmeta.current_term());
}

// Test the basic "happy case" of creating and then loading a file.
TEST_F(ConsensusMetadataTest, TestCreateLoad) {
  // Create the file.
  {
    gscoped_ptr<ConsensusMetadata> cmeta;
    ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, fs_manager_.uuid(),
                                        quorum_, kInitialTerm, &cmeta));
  }

  // Load the file.
  gscoped_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Load(&fs_manager_, kTabletId, fs_manager_.uuid(), &cmeta));
  ASSERT_VALUES_EQUAL(*cmeta, kInvalidOpIdIndex, fs_manager_.uuid(), kInitialTerm);
}

// Ensure that we get an error when loading a file that doesn't exist.
TEST_F(ConsensusMetadataTest, TestFailedLoad) {
  gscoped_ptr<ConsensusMetadata> cmeta;
  Status s = ConsensusMetadata::Load(&fs_manager_, kTabletId, fs_manager_.uuid(), &cmeta);
  ASSERT_TRUE(s.IsNotFound()) << "Unexpected status: " << s.ToString();
  LOG(INFO) << "Expected failure: " << s.ToString();
}

// Check that changes are not written to disk until Flush() is called.
TEST_F(ConsensusMetadataTest, TestFlush) {
  const uint64_t kNewTerm = 4;
  gscoped_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, fs_manager_.uuid(),
                                      quorum_, kInitialTerm, &cmeta));
  cmeta->set_current_term(kNewTerm);

  // We are sort of "breaking the rules" by having multiple ConsensusMetadata
  // objects in flight that point to the same file, but for a test this is fine
  // since it's read-only.
  {
    gscoped_ptr<ConsensusMetadata> cmeta_read;
    ASSERT_OK(ConsensusMetadata::Load(&fs_manager_, kTabletId, fs_manager_.uuid(), &cmeta_read));
    ASSERT_VALUES_EQUAL(*cmeta_read, kInvalidOpIdIndex, fs_manager_.uuid(), kInitialTerm);
  }

  ASSERT_OK(cmeta->Flush());

  {
    gscoped_ptr<ConsensusMetadata> cmeta_read;
    ASSERT_OK(ConsensusMetadata::Load(&fs_manager_, kTabletId, fs_manager_.uuid(), &cmeta_read));
    ASSERT_VALUES_EQUAL(*cmeta_read, kInvalidOpIdIndex, fs_manager_.uuid(), kNewTerm);
  }
}

// Builds a distributed quorum of voters with the given uuids.
QuorumPB BuildQuorum(const vector<string>& uuids) {
  QuorumPB quorum;
  quorum.set_local(false);
  BOOST_FOREACH(const string& uuid, uuids) {
    QuorumPeerPB* peer = quorum.add_peers();
    peer->set_permanent_uuid(uuid);
    peer->set_member_type(QuorumPeerPB::VOTER);
    CHECK_OK(HostPortToPB(HostPort("255.255.255.255", 0), peer->mutable_last_known_addr()));
  }
  return quorum;
}

// Test ConsensusMetadata active role calculation.
TEST_F(ConsensusMetadataTest, TestActiveRole) {
  vector<string> uuids = list_of("a")("b")("c")("d");
  string peer_uuid = "e";
  QuorumPB quorum1 = BuildQuorum(uuids); // We aren't a member of this quorum...
  quorum1.set_opid_index(1);

  gscoped_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, peer_uuid,
                                      quorum1, kInitialTerm, &cmeta));

  // Not a participant.
  ASSERT_EQ(QuorumPeerPB::NON_PARTICIPANT, cmeta->active_role());

  // Follower.
  uuids.push_back(peer_uuid);
  QuorumPB quorum2 = BuildQuorum(uuids); // But we are a member of this one.
  quorum2.set_opid_index(1);
  cmeta->set_committed_quorum(quorum2);
  ASSERT_EQ(QuorumPeerPB::FOLLOWER, cmeta->active_role());

  // Pending should mask committed.
  cmeta->set_pending_quorum(quorum1);
  ASSERT_EQ(QuorumPeerPB::NON_PARTICIPANT, cmeta->active_role());
  cmeta->clear_pending_quorum();
  ASSERT_EQ(QuorumPeerPB::FOLLOWER, cmeta->active_role());

  // Leader.
  cmeta->set_leader_uuid(peer_uuid);
  ASSERT_EQ(QuorumPeerPB::LEADER, cmeta->active_role());

  // Again, pending should mask committed.
  cmeta->set_pending_quorum(quorum1);
  ASSERT_EQ(QuorumPeerPB::NON_PARTICIPANT, cmeta->active_role());
  cmeta->set_pending_quorum(quorum2); // pending == committed.
  ASSERT_EQ(QuorumPeerPB::LEADER, cmeta->active_role());
  cmeta->set_committed_quorum(quorum1); // committed now excludes this node, but is masked...
  ASSERT_EQ(QuorumPeerPB::LEADER, cmeta->active_role());

  // ... until we clear pending, then we find committed now excludes us.
  cmeta->clear_pending_quorum();
  ASSERT_EQ(QuorumPeerPB::NON_PARTICIPANT, cmeta->active_role());
}

// Ensure that invocations of ToConsensusStatePB() return the expected state
// in the returned object.
TEST_F(ConsensusMetadataTest, TestToConsensusStatePB) {
  vector<string> uuids = list_of("a")("b")("c")("d");
  string peer_uuid = "e";

  QuorumPB committed_quorum = BuildQuorum(uuids); // We aren't a member of this quorum...
  committed_quorum.set_opid_index(1);
  gscoped_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, peer_uuid,
                                      committed_quorum, kInitialTerm, &cmeta));

  uuids.push_back(peer_uuid);
  QuorumPB pending_quorum = BuildQuorum(uuids);

  // Set the pending quorum to be one containing the current leader (who is not
  // in the committed quorum). Ensure that the leader shows up when we ask for
  // the active consensus state.
  cmeta->set_pending_quorum(pending_quorum);
  cmeta->set_leader_uuid(peer_uuid);
  ConsensusStatePB active_cstate = cmeta->ToConsensusStatePB(ConsensusMetadata::ACTIVE);
  ASSERT_TRUE(active_cstate.has_leader_uuid());
  ASSERT_OK(VerifyConsensusState(active_cstate, UNCOMMITTED_QUORUM));

  // Without changing anything, ask for the committed consensus state.
  // Since the current leader is not a voter in the committed quorum, the
  // returned consensus state should not list a leader.
  ConsensusStatePB committed_cstate = cmeta->ToConsensusStatePB(ConsensusMetadata::COMMITTED);
  ASSERT_FALSE(committed_cstate.has_leader_uuid());
  ASSERT_OK(VerifyConsensusState(committed_cstate, COMMITTED_QUORUM));

  // Set a new leader to be a member of the committed quorum. Now the committed
  // consensus state should list a leader.
  cmeta->set_leader_uuid("a");
  ConsensusStatePB new_committed_cstate = cmeta->ToConsensusStatePB(ConsensusMetadata::COMMITTED);
  ASSERT_TRUE(new_committed_cstate.has_leader_uuid());
  ASSERT_OK(VerifyConsensusState(new_committed_cstate, COMMITTED_QUORUM));
}

} // namespace consensus
} // namespace kudu
