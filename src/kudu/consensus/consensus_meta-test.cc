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

#include "kudu/consensus/consensus_meta.h"

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(encrypt_data_at_rest);

namespace kudu {
namespace consensus {

using std::string;
using std::unique_ptr;
using std::vector;

const char* kTabletId = "test-consensus-metadata";
const int64_t kInitialTerm = 3;

class ConsensusMetadataTest : public KuduTest, public ::testing::WithParamInterface<bool> {
 public:
  ConsensusMetadataTest() {
    SetEncryptionFlags(GetParam());
    fs_manager_ = std::make_unique<FsManager>(env_, FsManagerOpts(GetTestPath("fs_root")));
  }

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());

    // Initialize test configuration.
    config_.set_opid_index(kInvalidOpIdIndex);
    RaftPeerPB* peer = config_.add_peers();
    peer->set_permanent_uuid(fs_manager_->uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

 protected:
  // Assert that the given cmeta has a single configuration with the given metadata values.
  void AssertValuesEqual(const scoped_refptr<ConsensusMetadata>& cmeta,
                         int64_t opid_index, const string& permanant_uuid, int64_t term);


  std::unique_ptr<FsManager> fs_manager_;
  RaftConfigPB config_;
};

void ConsensusMetadataTest::AssertValuesEqual(const scoped_refptr<ConsensusMetadata>& cmeta,
                                              int64_t opid_index,
                                              const string& permanant_uuid,
                                              int64_t term) {
  // Sanity checks.
  ASSERT_EQ(1, cmeta->CommittedConfig().peers_size());

  // Value checks.
  ASSERT_EQ(opid_index, cmeta->CommittedConfig().opid_index());
  ASSERT_EQ(permanant_uuid, cmeta->CommittedConfig().peers().begin()->permanent_uuid());
  ASSERT_EQ(term, cmeta->current_term());
}

INSTANTIATE_TEST_SUITE_P(, ConsensusMetadataTest, ::testing::Values(false, true));

// Test the basic "happy case" of creating and then loading a file.
TEST_P(ConsensusMetadataTest, TestCreateLoad) {
  // Create the file.
  {
    ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTabletId, fs_manager_->uuid(),
                                        config_, kInitialTerm));
  }

  // Load the file.
  scoped_refptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Load(fs_manager_.get(), kTabletId, fs_manager_->uuid(), &cmeta));
  NO_FATALS(AssertValuesEqual(cmeta, kInvalidOpIdIndex, fs_manager_->uuid(), kInitialTerm));
  ASSERT_GT(cmeta->on_disk_size(), 0);
}

// Test deferred creation.
TEST_P(ConsensusMetadataTest, TestDeferredCreateLoad) {
  // Create the cmeta object, but not the file.
  scoped_refptr<ConsensusMetadata> writer;
  ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTabletId, fs_manager_->uuid(),
                                      config_, kInitialTerm,
                                      ConsensusMetadataCreateMode::NO_FLUSH_ON_CREATE,
                                      &writer));

  // Try to load the file: it should not be there.
  scoped_refptr<ConsensusMetadata> reader;
  Status s = ConsensusMetadata::Load(fs_manager_.get(), kTabletId, fs_manager_->uuid(), &reader);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // Flush; now the file will be there.
  ASSERT_OK(writer->Flush());
  ASSERT_OK(ConsensusMetadata::Load(fs_manager_.get(), kTabletId, fs_manager_->uuid(), &reader));
  NO_FATALS(AssertValuesEqual(reader, kInvalidOpIdIndex, fs_manager_->uuid(), kInitialTerm));
}

// Ensure that Create() will not overwrite an existing file.
TEST_P(ConsensusMetadataTest, TestCreateNoOverwrite) {
  // Create the consensus metadata file.
  ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTabletId, fs_manager_->uuid(),
                                      config_, kInitialTerm));
  // Try to create it again.
  Status s = ConsensusMetadata::Create(fs_manager_.get(), kTabletId, fs_manager_->uuid(),
                                       config_, kInitialTerm);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "Unable to write consensus meta file.*already exists");
}

// Ensure that we get an error when loading a file that doesn't exist.
TEST_P(ConsensusMetadataTest, TestFailedLoad) {
  Status s = ConsensusMetadata::Load(fs_manager_.get(), kTabletId, fs_manager_->uuid());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  LOG(INFO) << "Expected failure: " << s.ToString();
}

// Check that changes are not written to disk until Flush() is called.
TEST_P(ConsensusMetadataTest, TestFlush) {
  const int64_t kNewTerm = 4;
  scoped_refptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTabletId, fs_manager_->uuid(),
                                      config_, kInitialTerm,
                                      ConsensusMetadataCreateMode::FLUSH_ON_CREATE,
                                      &cmeta));
  cmeta->set_current_term(kNewTerm);

  // We are sort of "breaking the rules" by having multiple ConsensusMetadata
  // objects in flight that point to the same file, but for a test this is fine
  // since it's read-only.
  {
    scoped_refptr<ConsensusMetadata> cmeta_read;
    ASSERT_OK(ConsensusMetadata::Load(
        fs_manager_.get(), kTabletId, fs_manager_->uuid(), &cmeta_read));
    NO_FATALS(AssertValuesEqual(cmeta_read, kInvalidOpIdIndex, fs_manager_->uuid(), kInitialTerm));
    ASSERT_GT(cmeta->on_disk_size(), 0);
  }

  ASSERT_OK(cmeta->Flush());
  size_t cmeta_size = cmeta->on_disk_size();

  {
    scoped_refptr<ConsensusMetadata> cmeta_read;
    ASSERT_OK(ConsensusMetadata::Load(
        fs_manager_.get(), kTabletId, fs_manager_->uuid(), &cmeta_read));
    NO_FATALS(AssertValuesEqual(cmeta_read, kInvalidOpIdIndex, fs_manager_->uuid(), kNewTerm));
    ASSERT_EQ(cmeta_size, cmeta_read->on_disk_size());
  }
}

// Test ConsensusMetadata active role calculation.
TEST_P(ConsensusMetadataTest, TestActiveRole) {
  vector<string> uuids = { "a", "b", "c", "d" };
  string peer_uuid = "e";
  RaftConfigPB config1 = BuildConfig(uuids); // We aren't a member of this config...
  config1.set_opid_index(0);

  scoped_refptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTabletId, peer_uuid,
                                      config1, kInitialTerm,
                                      ConsensusMetadataCreateMode::FLUSH_ON_CREATE,
                                      &cmeta));

  ASSERT_EQ(4, cmeta->CountVotersInConfig(COMMITTED_CONFIG));
  ASSERT_EQ(0, cmeta->GetConfigOpIdIndex(COMMITTED_CONFIG));

  // Not a participant.
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());
  ASSERT_FALSE(cmeta->IsMemberInConfig(peer_uuid, COMMITTED_CONFIG));
  ASSERT_FALSE(cmeta->IsVoterInConfig(peer_uuid, COMMITTED_CONFIG));

  // Follower.
  uuids.push_back(peer_uuid);
  RaftConfigPB config2 = BuildConfig(uuids); // But we are a member of this one.
  config2.set_opid_index(1);
  cmeta->set_committed_config(config2);

  ASSERT_EQ(5, cmeta->CountVotersInConfig(COMMITTED_CONFIG));
  ASSERT_EQ(1, cmeta->GetConfigOpIdIndex(COMMITTED_CONFIG));

  ASSERT_EQ(RaftPeerPB::FOLLOWER, cmeta->active_role());
  ASSERT_TRUE(cmeta->IsVoterInConfig(peer_uuid, COMMITTED_CONFIG));

  // Pending should mask committed.
  cmeta->set_pending_config(config1);
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());

  ASSERT_TRUE(cmeta->IsMemberInConfig(peer_uuid, COMMITTED_CONFIG));
  ASSERT_TRUE(cmeta->IsVoterInConfig(peer_uuid, COMMITTED_CONFIG));
  for (auto config_state : {ACTIVE_CONFIG, PENDING_CONFIG}) {
    ASSERT_FALSE(cmeta->IsMemberInConfig(peer_uuid, config_state));
    ASSERT_FALSE(cmeta->IsVoterInConfig(peer_uuid, config_state));
  }
  cmeta->clear_pending_config();
  ASSERT_EQ(RaftPeerPB::FOLLOWER, cmeta->active_role());
  ASSERT_TRUE(cmeta->IsMemberInConfig(peer_uuid, ACTIVE_CONFIG));
  ASSERT_TRUE(cmeta->IsVoterInConfig(peer_uuid, ACTIVE_CONFIG));

  // Leader.
  cmeta->set_leader_uuid(peer_uuid);
  ASSERT_EQ(RaftPeerPB::LEADER, cmeta->active_role());

  // Again, pending should mask committed.
  cmeta->set_pending_config(config1);
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());
  cmeta->set_pending_config(config2); // pending == committed.
  ASSERT_EQ(RaftPeerPB::LEADER, cmeta->active_role());
  cmeta->set_committed_config(config1); // committed now excludes this node, but is masked...
  ASSERT_EQ(RaftPeerPB::LEADER, cmeta->active_role());

  // ... until we clear pending, then we find committed now excludes us.
  cmeta->clear_pending_config();
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());
}

// Ensure that invocations of ToConsensusStatePB() return the expected state
// in the returned object.
TEST_P(ConsensusMetadataTest, TestToConsensusStatePB) {
  vector<string> uuids = { "a", "b", "c", "d" };
  string peer_uuid = "e";

  RaftConfigPB committed_config = BuildConfig(uuids); // We aren't a member of this config...
  committed_config.set_opid_index(1);
  scoped_refptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTabletId, peer_uuid,
                                      committed_config, kInitialTerm,
                                      ConsensusMetadataCreateMode::FLUSH_ON_CREATE,
                                      &cmeta));

  uuids.push_back(peer_uuid);
  RaftConfigPB pending_config = BuildConfig(uuids);
  pending_config.set_opid_index(2);

  // Set the pending configuration to be one containing the current leader (who is not
  // in the committed configuration). Ensure that the leader shows up in the pending
  // configuration.
  cmeta->set_pending_config(pending_config);
  cmeta->set_leader_uuid(peer_uuid);
  ConsensusStatePB cstate = cmeta->ToConsensusStatePB();
  ASSERT_OK(VerifyConsensusState(cstate));

  // Set a new leader to be a member of the committed configuration.
  cmeta->set_leader_uuid("a");
  ConsensusStatePB new_cstate = cmeta->ToConsensusStatePB();
  ASSERT_FALSE(new_cstate.leader_uuid().empty());
  ASSERT_OK(VerifyConsensusState(new_cstate));

  // An empty leader UUID means no leader and we should not set the
  // corresponding PB field in that case. Regression test for KUDU-2147.
  cmeta->clear_pending_config();
  cmeta->set_leader_uuid("");
  new_cstate = cmeta->ToConsensusStatePB();
  ASSERT_TRUE(new_cstate.leader_uuid().empty());
  ASSERT_OK(VerifyConsensusState(new_cstate));
}

// Helper for TestMergeCommittedConsensusStatePB.
static void AssertConsensusMergeExpected(const scoped_refptr<ConsensusMetadata>& cmeta,
                                         const ConsensusStatePB& cstate,
                                         int64_t expected_term,
                                         const string& expected_voted_for) {
  // See header docs for ConsensusMetadata::MergeCommittedConsensusStatePB() for
  // a "spec" of these assertions.
  ASSERT_TRUE(!cmeta->has_pending_config());
  ASSERT_EQ(pb_util::SecureShortDebugString(cmeta->CommittedConfig()),
            pb_util::SecureShortDebugString(cstate.committed_config()));
  ASSERT_EQ("", cmeta->leader_uuid());
  ASSERT_EQ(expected_term, cmeta->current_term());
  if (expected_voted_for.empty()) {
    ASSERT_FALSE(cmeta->has_voted_for());
  } else {
    ASSERT_EQ(expected_voted_for, cmeta->voted_for());
  }
}

// Ensure that MergeCommittedConsensusStatePB() works as advertised.
TEST_P(ConsensusMetadataTest, TestMergeCommittedConsensusStatePB) {
  vector<string> uuids = { "a", "b", "c", "d" };

  RaftConfigPB committed_config = BuildConfig(uuids); // We aren't a member of this config...
  committed_config.set_opid_index(1);
  scoped_refptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTabletId, "e",
                                      committed_config, 1,
                                      ConsensusMetadataCreateMode::FLUSH_ON_CREATE,
                                      &cmeta));

  uuids.emplace_back("e");
  RaftConfigPB pending_config = BuildConfig(uuids);
  cmeta->set_pending_config(pending_config);
  cmeta->set_leader_uuid("e");
  cmeta->set_voted_for("e");

  // Keep the term and votes because the merged term is lower.
  ConsensusStatePB remote_state;
  remote_state.set_current_term(0);
  *remote_state.mutable_committed_config() = BuildConfig({ "x", "y", "z" });
  cmeta->MergeCommittedConsensusStatePB(remote_state);
  NO_FATALS(AssertConsensusMergeExpected(cmeta, remote_state, 1, "e"));

  // Same as above because the merged term is the same as the cmeta term.
  remote_state.set_current_term(1);
  *remote_state.mutable_committed_config() = BuildConfig({ "f", "g", "h" });
  cmeta->MergeCommittedConsensusStatePB(remote_state);
  NO_FATALS(AssertConsensusMergeExpected(cmeta, remote_state, 1, "e"));

  // Higher term, so wipe out the prior state.
  remote_state.set_current_term(2);
  *remote_state.mutable_committed_config() = BuildConfig({ "i", "j", "k" });
  cmeta->set_pending_config(pending_config);
  cmeta->MergeCommittedConsensusStatePB(remote_state);
  NO_FATALS(AssertConsensusMergeExpected(cmeta, remote_state, 2, ""));
}

} // namespace consensus
} // namespace kudu
