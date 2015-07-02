// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include "kudu/consensus/quorum_util.h"

#include "kudu/consensus/opid_util.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using std::string;

static void SetPeerInfo(const string& uuid,
                        RaftPeerPB::MemberType type,
                        RaftPeerPB* peer) {
  peer->set_permanent_uuid(uuid);
  peer->set_member_type(type);
}

TEST(QuorumUtilTest, TestMemberExtraction) {
  RaftConfigPB config;
  SetPeerInfo("A", RaftPeerPB::VOTER, config.add_peers());
  SetPeerInfo("B", RaftPeerPB::VOTER, config.add_peers());
  SetPeerInfo("C", RaftPeerPB::VOTER, config.add_peers());

  // Basic test for GetRaftConfigMember().
  RaftPeerPB peer_pb;
  Status s = GetRaftConfigMember(config, "invalid", &peer_pb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(GetRaftConfigMember(config, "A", &peer_pb));
  ASSERT_EQ("A", peer_pb.permanent_uuid());

  // Basic test for GetRaftConfigLeader().
  ConsensusStatePB cstate;
  *cstate.mutable_config() = config;
  s = GetRaftConfigLeader(cstate, &peer_pb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  cstate.set_leader_uuid("B");
  ASSERT_OK(GetRaftConfigLeader(cstate, &peer_pb));
  ASSERT_EQ("B", peer_pb.permanent_uuid());
}

} // namespace consensus
} // namespace kudu
