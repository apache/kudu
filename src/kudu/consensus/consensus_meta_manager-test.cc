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

#include <cstdint>
#include <string>

#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

using google::protobuf::util::MessageDifferencer;

namespace kudu {
namespace consensus {

static constexpr const char* kTabletId = "cmeta-mgr-test";
static const int64_t kInitialTerm = 1;

// Functional tests for the cmeta manager.
class ConsensusMetadataManagerTest : public KuduTest {
 public:
  ConsensusMetadataManagerTest()
      : fs_manager_(env_, GetTestPath("fs_root")),
        cmeta_manager_(new ConsensusMetadataManager(&fs_manager_)) {
  }

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test configuration.
    config_.set_opid_index(kInvalidOpIdIndex);
    RaftPeerPB* peer = config_.add_peers();
    peer->set_permanent_uuid(fs_manager_.uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

 protected:
  FsManager fs_manager_;
  scoped_refptr<ConsensusMetadataManager> cmeta_manager_;
  RaftConfigPB config_;
};

// Test the basic "happy case" of creating and then loading a file.
TEST_F(ConsensusMetadataManagerTest, TestCreateLoad) {
  // Try to load a nonexistent instance.
  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->Load(kTabletId, &cmeta);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // Create a new ConsensusMetadata instance.
  ASSERT_OK(cmeta_manager_->Create(kTabletId, config_, kInitialTerm, &cmeta));

  // Load it back.
  ASSERT_OK(cmeta_manager_->Load(kTabletId, &cmeta));

  // Ensure we got what we expected.
  ASSERT_EQ(kInitialTerm, cmeta->current_term());
  ASSERT_TRUE(MessageDifferencer::Equals(config_, cmeta->CommittedConfig()))
      << DiffRaftConfigs(config_, cmeta->CommittedConfig());
}

// Test Delete.
TEST_F(ConsensusMetadataManagerTest, TestDelete) {
  // Create a ConsensusMetadata instance.
  scoped_refptr<ConsensusMetadata> cmeta;
  ASSERT_OK(cmeta_manager_->Create(kTabletId, config_, kInitialTerm, &cmeta));

  // Now delete it.
  ASSERT_OK(cmeta_manager_->Delete(kTabletId));

  // Can't load it because it's gone.
  Status s = cmeta_manager_->Load(kTabletId, &cmeta);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

// Test that we can't clobber (overwrite) an existing cmeta.
TEST_F(ConsensusMetadataManagerTest, TestNoClobber) {
  // Create a ConsensusMetadata instance.
  {
    scoped_refptr<ConsensusMetadata> cmeta;
    ASSERT_OK(cmeta_manager_->Create(kTabletId, config_, kInitialTerm, &cmeta));
  }

  // Creating it again should fail.
  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->Create(kTabletId, config_, kInitialTerm, &cmeta);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "already exists");
}

} // namespace consensus
} // namespace kudu
