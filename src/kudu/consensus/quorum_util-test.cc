// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <string>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace consensus {

using std::string;

// Add a consensus peer into the specified configuration.
static void AddPeer(RaftConfigPB* config,
                    const string& uuid,
                    RaftPeerPB::MemberType type) {
  RaftPeerPB* peer = config->add_peers();
  peer->set_permanent_uuid(uuid);
  peer->mutable_last_known_addr()->set_host(uuid + ".example.com");
  peer->set_member_type(type);
}

TEST(QuorumUtilTest, TestMemberExtraction) {
  RaftConfigPB config;
  AddPeer(&config, "A", RaftPeerPB::VOTER);
  AddPeer(&config, "B", RaftPeerPB::VOTER);
  AddPeer(&config, "C", RaftPeerPB::VOTER);

  // Basic test for GetRaftConfigMember().
  RaftPeerPB* peer_pb;
  Status s = GetRaftConfigMember(&config, "invalid", &peer_pb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(GetRaftConfigMember(&config, "A", &peer_pb));
  ASSERT_EQ("A", peer_pb->permanent_uuid());

  // Basic test for GetRaftConfigLeader().
  ConsensusStatePB cstate;
  *cstate.mutable_committed_config() = config;
  s = GetRaftConfigLeader(&cstate, &peer_pb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  cstate.set_leader_uuid("B");
  ASSERT_OK(GetRaftConfigLeader(&cstate, &peer_pb));
  ASSERT_EQ("B", peer_pb->permanent_uuid());
}

TEST(QuorumUtilTest, TestDiffConsensusStates) {
  ConsensusStatePB old_cs;
  AddPeer(old_cs.mutable_committed_config(), "A", RaftPeerPB::VOTER);
  AddPeer(old_cs.mutable_committed_config(), "B", RaftPeerPB::VOTER);
  AddPeer(old_cs.mutable_committed_config(), "C", RaftPeerPB::VOTER);
  old_cs.set_current_term(1);
  old_cs.set_leader_uuid("A");
  old_cs.mutable_committed_config()->set_opid_index(1);

  // Simple case of no change.
  EXPECT_EQ("no change",
            DiffConsensusStates(old_cs, old_cs));

  // Simulate a leader change.
  {
    auto new_cs = old_cs;
    new_cs.set_leader_uuid("B");
    new_cs.set_current_term(2);

    EXPECT_EQ("term changed from 1 to 2, "
              "leader changed from A (A.example.com) to B (B.example.com)",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate eviction of a peer.
  {
    auto new_cs = old_cs;
    new_cs.mutable_committed_config()->set_opid_index(2);
    new_cs.mutable_committed_config()->mutable_peers()->RemoveLast();

    EXPECT_EQ("config changed from index 1 to 2, "
              "VOTER C (C.example.com) evicted",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate addition of a peer.
  {
    auto new_cs = old_cs;
    new_cs.mutable_committed_config()->set_opid_index(2);
    AddPeer(new_cs.mutable_committed_config(), "D", RaftPeerPB::NON_VOTER);

    EXPECT_EQ("config changed from index 1 to 2, "
              "NON_VOTER D (D.example.com) added",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate change of a peer's member type.
  {
    auto new_cs = old_cs;
    new_cs.mutable_committed_config()->set_opid_index(2);
    new_cs.mutable_committed_config()
      ->mutable_peers()->Mutable(2)->set_member_type(RaftPeerPB::NON_VOTER);

    EXPECT_EQ("config changed from index 1 to 2, "
              "C (C.example.com) changed from VOTER to NON_VOTER",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate change from no leader to a leader
  {
    auto no_leader_cs = old_cs;
    no_leader_cs.clear_leader_uuid();
    auto new_cs = old_cs;
    new_cs.set_current_term(2);

    EXPECT_EQ("term changed from 1 to 2, "
              "leader changed from <none> to A (A.example.com)",
              DiffConsensusStates(no_leader_cs, new_cs));
  }

  // Simulate gaining a pending config
  {
    auto pending_config_cs = old_cs;
    pending_config_cs.mutable_pending_config();
    EXPECT_EQ("now has a pending config: ", DiffConsensusStates(old_cs, pending_config_cs));
  }

  // Simulate losing a pending config
  {
    auto pending_config_cs = old_cs;
    pending_config_cs.mutable_pending_config();
    EXPECT_EQ("no longer has a pending config: ", DiffConsensusStates(pending_config_cs, old_cs));
  }

  // Simulate a change in a pending config
  {
    auto before_cs = old_cs;
    AddPeer(before_cs.mutable_pending_config(), "A", RaftPeerPB::VOTER);
    auto after_cs = before_cs;
    after_cs.mutable_pending_config()
      ->mutable_peers()->Mutable(0)->set_member_type(RaftPeerPB::NON_VOTER);

    EXPECT_EQ("pending config changed, A (A.example.com) changed from VOTER to NON_VOTER",
              DiffConsensusStates(before_cs, after_cs));
  }
}

TEST(QuorumUtilTest, TestIsRaftConfigVoter) {
  RaftConfigPB config;
  AddPeer(&config, "A", RaftPeerPB::VOTER);
  AddPeer(&config, "B", RaftPeerPB::NON_VOTER);
  AddPeer(&config, "C", RaftPeerPB::UNKNOWN_MEMBER_TYPE);

  // The case when membership type is not specified. That sort of configuration
  // would not pass VerifyRaftConfig(), though. Anyway, that should result
  // in non-voter since the member_type is initialized with UNKNOWN_MEMBER_TYPE.
  const string no_member_type_peer_uuid = "D";
  RaftPeerPB* no_member_type_peer = config.add_peers();
  no_member_type_peer->set_permanent_uuid(no_member_type_peer_uuid);
  no_member_type_peer->mutable_last_known_addr()->set_host(
      no_member_type_peer_uuid + ".example.com");

  ASSERT_TRUE(IsRaftConfigVoter("A", config));
  ASSERT_FALSE(IsRaftConfigVoter("B", config));
  ASSERT_FALSE(IsRaftConfigVoter("C", config));
  ASSERT_FALSE(IsRaftConfigVoter(no_member_type_peer_uuid, config));

  RaftPeerPB* peer_a;
  ASSERT_OK(GetRaftConfigMember(&config, "A", &peer_a));
  RaftPeerPB* peer_b;
  ASSERT_OK(GetRaftConfigMember(&config, "B", &peer_b));
  ASSERT_FALSE(ReplicaTypesEqual(*peer_a, *peer_b));
  ASSERT_TRUE(ReplicaTypesEqual(*peer_b, *peer_b));
  RaftPeerPB* peer_c;
  ASSERT_OK(GetRaftConfigMember(&config, "C", &peer_c));
  ASSERT_FALSE(ReplicaTypesEqual(*peer_b, *peer_c));
}

} // namespace consensus
} // namespace kudu
