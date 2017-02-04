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

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/mini_master.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/crypto.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

DECLARE_int64(tsk_validity_seconds);
DECLARE_int64(tsk_rotation_seconds);

using std::string;
using std::unique_ptr;
using std::vector;
using kudu::security::TokenSigningPublicKeyPB;
using kudu::security::SignedTokenPB;
using kudu::security::TokenPB;

namespace kudu {
namespace master {

class TokenSignerITest : public KuduTest {
 public:
  TokenSignerITest() {
    FLAGS_tsk_validity_seconds = 60;
    FLAGS_tsk_rotation_seconds = 20;

    // Hard-coded ports for the masters. This is safe, as this unit test
    // runs under a resource lock (see CMakeLists.txt in this directory).
    // TODO(aserbin): we should have a generic method to obtain n free ports.
    opts_.master_rpc_ports = { 11010, 11011, 11012 };

    opts_.num_masters = num_masters_ = opts_.master_rpc_ports.size();
  }

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new MiniCluster(env_, opts_));
    ASSERT_OK(cluster_->Start());
  }

  Status RestartCluster() {
    cluster_->Shutdown();
    return cluster_->Start();
  }

  Status ShutdownLeader() {
    int leader_idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    MiniMaster* leader_master = cluster_->mini_master(leader_idx);
    leader_master->Shutdown();
    return Status::OK();
  }

  // Check the leader is found on the cluster.
  Status WaitForNewLeader() {
    return cluster_->GetLeaderMasterIndex(nullptr);
  }

  static SignedTokenPB MakeToken() {
    SignedTokenPB ret;
    TokenPB token;
    token.set_expire_unix_epoch_seconds(WallTime_Now() + 600);
    CHECK(token.SerializeToString(ret.mutable_token_data()));
    return ret;
  }

  Status SignToken(SignedTokenPB* token) {
    int leader_idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    MiniMaster* leader = cluster_->mini_master(leader_idx);
    Master* master = leader->master();
    RETURN_NOT_OK(master->token_signer()->SignToken(token));
    return Status::OK();
  }

  Status GetPublicKeys(int idx, vector<TokenSigningPublicKeyPB>* public_keys) {
    CHECK_GE(idx, 0);
    CHECK_LT(idx, num_masters_);
    MiniMaster* mm = cluster_->mini_master(idx);
    vector<TokenSigningPublicKeyPB> keys =
        mm->master()->token_signer()->verifier().ExportKeys();
    public_keys->swap(keys);
    return Status::OK();
  }

  Status GetLeaderPublicKeys(vector<TokenSigningPublicKeyPB>* public_keys) {
    int leader_idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    return GetPublicKeys(leader_idx, public_keys);
  }

 protected:
  int num_masters_;
  MiniClusterOptions opts_;
  unique_ptr<MiniCluster> cluster_;
};

// Check that once cluster has started, the TSK for signing is available at the
// leader master and nothing is available at master-followers. The follower
// masters do not poll the system table for TSK entries nor TSK information
// is transferred to them from the leader master in any other way.
TEST_F(TokenSignerITest, TskAtLeaderMaster) {
  // Check the leader can sign tokens: this guarantees at least one TSK has been
  // generated and is available for token signing.
  SignedTokenPB t(MakeToken());
  ASSERT_OK(SignToken(&t));

  // Get the public part of the signing key from the leader.
  vector<TokenSigningPublicKeyPB> leader_public_keys;
  ASSERT_OK(GetLeaderPublicKeys(&leader_public_keys));
  ASSERT_EQ(1, leader_public_keys.size());
  EXPECT_EQ(t.signing_key_seq_num(), leader_public_keys[0].key_seq_num());
}

// New TSK is generated upon start of the leader master and persisted in
// the system catalog table. So, check that appropriate TSK
// information is available upon start of the cluster and it's persistent
// for some time after restart (until the expiration time, actually;
// the removal of expired TSK info is not covered by this scenario).
TEST_F(TokenSignerITest, TskClusterRestart) {
  // Check the leader can sign tokens just after start.
  SignedTokenPB t_pre(MakeToken());
  ASSERT_OK(SignToken(&t_pre));

  vector<TokenSigningPublicKeyPB> public_keys_before;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys_before));
  ASSERT_EQ(1, public_keys_before.size());

  ASSERT_OK(RestartCluster());

  // Check the leader can sign tokens after the restart.
  SignedTokenPB t_post(MakeToken());
  ASSERT_OK(SignToken(&t_post));
  EXPECT_EQ(t_post.signing_key_seq_num(), t_pre.signing_key_seq_num());

  vector<TokenSigningPublicKeyPB> public_keys_after;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys_after));
  ASSERT_EQ(1, public_keys_after.size());
  EXPECT_EQ(public_keys_before[0].SerializeAsString(),
            public_keys_after[0].SerializeAsString());
}

// Test that if leadership changes, the new leader has the same TSK information
// for token verification as the former leader
// (it's assumed no new TSK generation happened in between).
TEST_F(TokenSignerITest, TskMasterLeadershipChange) {
  SignedTokenPB t_former_leader(MakeToken());
  ASSERT_OK(SignToken(&t_former_leader));

  vector<TokenSigningPublicKeyPB> public_keys;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys));
  ASSERT_EQ(1, public_keys.size());

  ASSERT_OK(ShutdownLeader());
  ASSERT_OK(WaitForNewLeader());

  // The new leader should use the same signing key.
  SignedTokenPB t_new_leader(MakeToken());
  ASSERT_OK(SignToken(&t_new_leader));
  EXPECT_EQ(t_new_leader.signing_key_seq_num(),
            t_former_leader.signing_key_seq_num());

  vector<TokenSigningPublicKeyPB> public_keys_new_leader;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys_new_leader));
  ASSERT_EQ(1, public_keys_new_leader.size());
  EXPECT_EQ(public_keys[0].SerializeAsString(),
            public_keys_new_leader[0].SerializeAsString());
}

// TODO(aserbin): add a test case which corresponds to multiple signing keys
// right after cluster start-up.

} // namespace master
} // namespace kudu
