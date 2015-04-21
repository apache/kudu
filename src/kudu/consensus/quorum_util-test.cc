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

using boost::assign::list_of;
using std::vector;

class QuorumUtilTest : public KuduTest {
};

// Sanity check for the IsVotingRole() helper.
TEST_F(QuorumUtilTest, TestIsVotingRole) {
  ASSERT_TRUE(IsVotingRole(QuorumPeerPB::LEADER));
  ASSERT_TRUE(IsVotingRole(QuorumPeerPB::FOLLOWER));
  ASSERT_FALSE(IsVotingRole(QuorumPeerPB::LEARNER));
  ASSERT_FALSE(IsVotingRole(QuorumPeerPB::NON_PARTICIPANT));
  ASSERT_FALSE(IsVotingRole(QuorumPeerPB::UNKNOWN));
}

// Test to ensure SetAllQuorumVotersToFollower() sets all voting roles to
// follower and leaves the rest alone.
TEST_F(QuorumUtilTest, TestSetAllQuorumVotersToFollower) {
  QuorumPB quorum;
  vector<QuorumPeerPB::Role> roles = list_of<QuorumPeerPB::Role>
      (QuorumPeerPB::LEADER)(QuorumPeerPB::FOLLOWER)
      (QuorumPeerPB::LEARNER)(QuorumPeerPB::NON_PARTICIPANT)(QuorumPeerPB::UNKNOWN);
  BOOST_FOREACH(QuorumPeerPB::Role role, roles) {
    QuorumPeerPB* peer = quorum.add_peers();
    peer->set_role(role);
  }

  QuorumPB leaderless_quorum;
  SetAllQuorumVotersToFollower(quorum, &leaderless_quorum);
  ASSERT_EQ(5, leaderless_quorum.peers_size());

  vector<QuorumPeerPB::Role> expected = list_of<QuorumPeerPB::Role>
      (QuorumPeerPB::FOLLOWER)(QuorumPeerPB::FOLLOWER)
      (QuorumPeerPB::LEARNER)(QuorumPeerPB::NON_PARTICIPANT)(QuorumPeerPB::UNKNOWN);
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(expected[i], leaderless_quorum.peers(i).role());
  }
}

} // namespace consensus
} // namespace kudu
