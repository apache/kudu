// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/quorum_util.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using std::vector;

class QuorumUtilTest : public KuduTest {
};

// Sanity check for the IsVotingRole() helper.
TEST_F(QuorumUtilTest, TestIsVotingRole) {
  ASSERT_TRUE(IsVotingRole(QuorumPeerPB::LEADER));
  ASSERT_TRUE(IsVotingRole(QuorumPeerPB::CANDIDATE));
  ASSERT_TRUE(IsVotingRole(QuorumPeerPB::FOLLOWER));
  ASSERT_FALSE(IsVotingRole(QuorumPeerPB::LEARNER));
  ASSERT_FALSE(IsVotingRole(QuorumPeerPB::NON_PARTICIPANT));
  ASSERT_FALSE(IsVotingRole(QuorumPeerPB::UNKNOWN));
}

// Test to ensure SetAllQuorumVotersToFollower() sets all voting roles to
// follower and leaves the rest alone.
TEST_F(QuorumUtilTest, TestSetAllQuorumVotersToFollower) {
  QuorumPB quorum;
  vector<QuorumPeerPB::Role> roles = boost::assign::list_of<QuorumPeerPB::Role>
      (QuorumPeerPB::LEADER)(QuorumPeerPB::CANDIDATE)(QuorumPeerPB::FOLLOWER)
      (QuorumPeerPB::LEARNER)(QuorumPeerPB::NON_PARTICIPANT)(QuorumPeerPB::UNKNOWN);
  BOOST_FOREACH(QuorumPeerPB::Role role, roles) {
    QuorumPeerPB* peer = quorum.add_peers();
    peer->set_role(role);
  }

  QuorumPB leaderless_quorum;
  SetAllQuorumVotersToFollower(quorum, &leaderless_quorum);

  ASSERT_EQ(6, leaderless_quorum.peers_size());
  ASSERT_EQ(QuorumPeerPB::FOLLOWER, leaderless_quorum.peers(0).role());
  ASSERT_EQ(QuorumPeerPB::FOLLOWER, leaderless_quorum.peers(1).role());
  ASSERT_EQ(QuorumPeerPB::FOLLOWER, leaderless_quorum.peers(2).role());
  ASSERT_EQ(QuorumPeerPB::LEARNER, leaderless_quorum.peers(3).role());
  ASSERT_EQ(QuorumPeerPB::NON_PARTICIPANT, leaderless_quorum.peers(4).role());
  ASSERT_EQ(QuorumPeerPB::UNKNOWN, leaderless_quorum.peers(5).role());
}

} // namespace consensus
} // namespace kudu
