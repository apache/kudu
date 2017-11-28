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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace consensus {

// Handy notation of membership types used by AddPeer(), etc.
constexpr auto N = RaftPeerPB::NON_VOTER;
constexpr auto U = RaftPeerPB::UNKNOWN_MEMBER_TYPE;
constexpr auto V = RaftPeerPB::VOTER;

typedef std::pair<string, bool> Attr;

// Add a consensus peer into the specified configuration.
static void AddPeer(RaftConfigPB* config,
                    const string& uuid,
                    RaftPeerPB::MemberType type,
                    boost::optional<char> overall_health = boost::none,
                    vector<Attr> attrs = {}) {
  RaftPeerPB* peer = config->add_peers();
  peer->set_permanent_uuid(uuid);
  peer->mutable_last_known_addr()->set_host(uuid + ".example.com");
  peer->set_member_type(type);
  if (overall_health) {
    unique_ptr<HealthReportPB> health_report(new HealthReportPB);
    switch (*overall_health) {
      case '+':
        health_report->set_overall_health(HealthReportPB::HEALTHY);
        break;
      case '-':
        health_report->set_overall_health(HealthReportPB::FAILED);
        break;
      case '?':
        health_report->set_overall_health(HealthReportPB::UNKNOWN);
        break;
      default:
        FAIL() << *overall_health << ": unexpected replica health status";
        break;
    }
    peer->set_allocated_health_report(health_report.release());
  }
  if (!attrs.empty()) {
    unique_ptr<RaftPeerAttrsPB> attrs_pb(new RaftPeerAttrsPB);
    for (const auto& attr : attrs) {
      if (attr.first == "PROMOTE") {
        attrs_pb->set_promote(attr.second);
      } else if (attr.first == "REPLACE") {
        attrs_pb->set_replace(attr.second);
      } else {
        FAIL() << attr.first << ": unexpected attribute to set";
      }
    }
    peer->set_allocated_attrs(attrs_pb.release());
  }
}

TEST(QuorumUtilTest, TestMemberExtraction) {
  RaftConfigPB config;
  AddPeer(&config, "A", V);
  AddPeer(&config, "B", V);
  AddPeer(&config, "C", V);

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
  AddPeer(old_cs.mutable_committed_config(), "A", V);
  AddPeer(old_cs.mutable_committed_config(), "B", V);
  AddPeer(old_cs.mutable_committed_config(), "C", V);
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
    AddPeer(new_cs.mutable_committed_config(), "D", N);

    EXPECT_EQ("config changed from index 1 to 2, "
              "NON_VOTER D (D.example.com) added",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate change of a peer's member type.
  {
    auto new_cs = old_cs;
    new_cs.mutable_committed_config()->set_opid_index(2);
    new_cs.mutable_committed_config()
      ->mutable_peers()->Mutable(2)->set_member_type(N);

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
    AddPeer(before_cs.mutable_pending_config(), "A", V);
    auto after_cs = before_cs;
    after_cs.mutable_pending_config()
      ->mutable_peers()->Mutable(0)->set_member_type(N);

    EXPECT_EQ("pending config changed, A (A.example.com) changed from VOTER to NON_VOTER",
              DiffConsensusStates(before_cs, after_cs));
  }
}

TEST(QuorumUtilTest, TestIsRaftConfigVoter) {
  RaftConfigPB config;
  AddPeer(&config, "A", V);
  AddPeer(&config, "B", N);
  AddPeer(&config, "C", U);

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

// Verify logic of the kudu::consensus::IsUnderReplicated.
TEST(QuorumUtilTest, IsUnderReplicated) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V);
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", V);
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_FALSE(IsUnderReplicated(config, 3));
    EXPECT_TRUE(IsUnderReplicated(config, 4));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '?');
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_FALSE(IsUnderReplicated(config, 3));
    EXPECT_TRUE(IsUnderReplicated(config, 4));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '-');
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", N, '+');
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", N, '+');
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", N, '+');
    EXPECT_FALSE(IsUnderReplicated(config, 1));
    EXPECT_TRUE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(IsUnderReplicated(config, 1));
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", N, '-', {{"PROMOTE", true}});
    EXPECT_FALSE(IsUnderReplicated(config, 1));
    EXPECT_TRUE(IsUnderReplicated(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    EXPECT_TRUE(IsUnderReplicated(config, 3));
    EXPECT_FALSE(IsUnderReplicated(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", N, '+');
    EXPECT_TRUE(IsUnderReplicated(config, 4));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
    EXPECT_FALSE(IsUnderReplicated(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_TRUE(IsUnderReplicated(config, 4));
    EXPECT_FALSE(IsUnderReplicated(config, 3));
    EXPECT_FALSE(IsUnderReplicated(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", N, '-');
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", N, '+');
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    // The replica does not have the PROMOTE attribute, so a new one is needed.
    EXPECT_TRUE(IsUnderReplicated(config, 3));
    EXPECT_TRUE(IsUnderReplicated(config, 4));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_FALSE(IsUnderReplicated(config, 3));
    EXPECT_TRUE(IsUnderReplicated(config, 4));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", N, '-', {{"PROMOTE", true}});
    EXPECT_FALSE(IsUnderReplicated(config, 2));
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '-', {{"PROMOTE", true}});
    AddPeer(&config, "E", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(IsUnderReplicated(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '-', {{"PROMOTE", true}});
    AddPeer(&config, "E", N, '+', {{"PROMOTE", false}});
    EXPECT_TRUE(IsUnderReplicated(config, 3));
  }
}

// Verify logic of the kudu::consensus::CanEvictReplica(), anticipating
// removal of a voter replica.
TEST(QuorumUtilTest, CanEvictReplicaVoters) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V);
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", V);
    EXPECT_FALSE(CanEvictReplica(config, 3));
    EXPECT_FALSE(CanEvictReplica(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '-');
    EXPECT_FALSE(CanEvictReplica(config, 3));
    EXPECT_FALSE(CanEvictReplica(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, '+');
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
    EXPECT_EQ("B", uuid_to_evict);
    EXPECT_FALSE(CanEvictReplica(config, 3));
    EXPECT_FALSE(CanEvictReplica(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", V);
    EXPECT_FALSE(CanEvictReplica(config, 3));
    EXPECT_FALSE(CanEvictReplica(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+', {{"REPLACE", false}});
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", V, '+');
    EXPECT_FALSE(CanEvictReplica(config, 4));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 3, &uuid_to_evict));
    EXPECT_EQ("C", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    EXPECT_FALSE(CanEvictReplica(config, 4));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 3, &uuid_to_evict));
    EXPECT_EQ("B", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", V, '?', {{"REPLACE", true}});
    EXPECT_FALSE(CanEvictReplica(config, 3));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 2, &uuid_to_evict));
    EXPECT_EQ("D", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", V, '-', {{"REPLACE", true}});
    EXPECT_FALSE(CanEvictReplica(config, 3));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 2, &uuid_to_evict));
    EXPECT_EQ("D", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", V, '+', {{"REPLACE", true}});
    EXPECT_FALSE(CanEvictReplica(config, 3));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 2, &uuid_to_evict));
    EXPECT_EQ("D", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '?');
    AddPeer(&config, "D", V, '+', {{"REPLACE", true}});
    EXPECT_FALSE(CanEvictReplica(config, 3));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 2, &uuid_to_evict));
    EXPECT_EQ("D", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    EXPECT_FALSE(CanEvictReplica(config, 4));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 3, &uuid_to_evict));
    EXPECT_EQ("B", uuid_to_evict);
  }
}

// Verify logic of the kudu::consensus::CanEvictReplica(), anticipating
// removal of a non-voter replica.
TEST(QuorumUtilTest, CanEvictReplicaNonVoters) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V);
    EXPECT_FALSE(CanEvictReplica(config, 1));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    EXPECT_FALSE(CanEvictReplica(config, 1));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N);
    EXPECT_FALSE(CanEvictReplica(config, 2));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
    EXPECT_EQ("B", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", N, '+');
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 2, &uuid_to_evict));
    EXPECT_EQ("C", uuid_to_evict);
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
    EXPECT_EQ("C", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N, '-', {{"PROMOTE", true}});
    EXPECT_FALSE(CanEvictReplica(config, 2));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
    EXPECT_EQ("B", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N, '-');
    AddPeer(&config, "C", N);
    EXPECT_FALSE(CanEvictReplica(config, 2));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
    EXPECT_EQ("B", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N, '?');
    AddPeer(&config, "C", N, '+');
    EXPECT_FALSE(CanEvictReplica(config, 2));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
    EXPECT_EQ("B", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", N);
    EXPECT_FALSE(CanEvictReplica(config, 2));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
    EXPECT_EQ("C", uuid_to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", N);
    EXPECT_FALSE(CanEvictReplica(config, 2));
    EXPECT_FALSE(CanEvictReplica(config, 1));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", N, '-', {{"PROMOTE", true}});
    EXPECT_FALSE(CanEvictReplica(config, 2));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 1, &uuid_to_evict));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(CanEvictReplica(config, 3));
    string uuid_to_evict;
    ASSERT_TRUE(CanEvictReplica(config, 2, &uuid_to_evict));
    EXPECT_EQ("D", uuid_to_evict);
  }
}

} // namespace consensus
} // namespace kudu
