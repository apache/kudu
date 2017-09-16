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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/walltime.h"
#include "kudu/integration-tests/internal_mini_cluster.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/rpc/messenger.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int64(authn_token_validity_seconds);
DECLARE_int64(tsk_rotation_seconds);
DECLARE_int32(heartbeat_interval_ms);

using std::string;
using std::unique_ptr;
using std::vector;
using kudu::security::TokenPB;
using kudu::security::TokenSigningPublicKeyPB;
using kudu::security::SignedTokenPB;
using kudu::security::VerificationResult;

namespace kudu {
namespace master {

class TokenSignerITest : public KuduTest {
 public:
  TokenSignerITest() {
    FLAGS_authn_token_validity_seconds = authn_token_validity_seconds_;
    FLAGS_tsk_rotation_seconds = tsk_rotation_seconds_;

    // Hard-coded ports for the masters. This is safe, as this unit test
    // runs under a resource lock (see CMakeLists.txt in this directory).
    // TODO(aserbin): we should have a generic method to obtain n free ports.
    opts_.master_rpc_ports = { 11010, 11011, 11012 };

    opts_.num_masters = num_masters_ = opts_.master_rpc_ports.size();
    opts_.num_tablet_servers = num_tablet_servers_;
  }

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new InternalMiniCluster(env_, opts_));
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

  Status MakeSignedToken(SignedTokenPB* token) {
    static const string kUserName = "test";
    int leader_idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    MiniMaster* leader = cluster_->mini_master(leader_idx);
    Master* master = leader->master();
    RETURN_NOT_OK(master->token_signer()->GenerateAuthnToken(kUserName, token));
    return Status::OK();
  }

  Status GetPublicKeys(int idx, vector<TokenSigningPublicKeyPB>* public_keys) {
    CHECK_GE(idx, 0);
    CHECK_LT(idx, num_masters_);
    MiniMaster* mm = cluster_->mini_master(idx);
    vector<TokenSigningPublicKeyPB> keys =
        mm->master()->token_verifier().ExportKeys();
    public_keys->swap(keys);
    return Status::OK();
  }

  Status GetLeaderPublicKeys(vector<TokenSigningPublicKeyPB>* public_keys) {
    int leader_idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    return GetPublicKeys(leader_idx, public_keys);
  }

 protected:
  const int64_t authn_token_validity_seconds_ = 20;
  const int64_t tsk_rotation_seconds_ = 20;

  int num_masters_;
  const int num_tablet_servers_ = 3;
  InternalMiniClusterOptions opts_;
  unique_ptr<InternalMiniCluster> cluster_;
};

// Check that once cluster has started, the TSK for signing is available at the
// leader master and nothing is available at master-followers. The follower
// masters do not poll the system table for TSK entries nor TSK information
// is transferred to them from the leader master in any other way.
TEST_F(TokenSignerITest, TskAtLeaderMaster) {
  // Check the leader can sign tokens: this guarantees at least one TSK has been
  // generated and is available for token signing.
  SignedTokenPB t;
  ASSERT_OK(MakeSignedToken(&t));

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
  SignedTokenPB t_pre;
  ASSERT_OK(MakeSignedToken(&t_pre));

  vector<TokenSigningPublicKeyPB> public_keys_before;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys_before));
  ASSERT_EQ(1, public_keys_before.size());

  ASSERT_OK(RestartCluster());

  // Check the leader can sign tokens after the restart.
  SignedTokenPB t_post;
  ASSERT_OK(MakeSignedToken(&t_post));
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
  SignedTokenPB t_former_leader;
  ASSERT_OK(MakeSignedToken(&t_former_leader));

  vector<TokenSigningPublicKeyPB> public_keys;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys));
  ASSERT_EQ(1, public_keys.size());

  ASSERT_OK(ShutdownLeader());
  ASSERT_OK(WaitForNewLeader());

  // The new leader should use the same signing key.
  SignedTokenPB t_new_leader;
  ASSERT_OK(MakeSignedToken(&t_new_leader));
  EXPECT_EQ(t_new_leader.signing_key_seq_num(),
            t_former_leader.signing_key_seq_num());

  vector<TokenSigningPublicKeyPB> public_keys_new_leader;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys_new_leader));
  ASSERT_EQ(1, public_keys_new_leader.size());
  EXPECT_EQ(public_keys[0].SerializeAsString(),
            public_keys_new_leader[0].SerializeAsString());
}

// Check for authn token verification results during and past its lifetime.
//
// Tablet servers should be able to successfully verify the authn token
// up to the very end of the token validity interval. Past the duration of authn
// token's validity interval the token is considered invalid and an attempt to
// verify such a token should fail.
//
// This test exercises the edge case for the token validity interval:
//   * Generate an authn token at the very end of TSK activity interval.
//   * Make sure the TSK stays valid and can be used for token verification
//     up to the very end of the token validity interval.
TEST_F(TokenSignerITest, AuthnTokenLifecycle) {
  using std::all_of;
  using std::bind;
  using std::equal_to;
  using std::placeholders::_1;

  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }
  vector<TokenSigningPublicKeyPB> public_keys;
  ASSERT_OK(GetLeaderPublicKeys(&public_keys));
  ASSERT_EQ(1, public_keys.size());
  const TokenSigningPublicKeyPB& public_key = public_keys[0];
  ASSERT_TRUE(public_key.has_key_seq_num());
  const int64_t key_seq_num = public_key.key_seq_num();
  ASSERT_TRUE(public_key.has_expire_unix_epoch_seconds());
  const int64_t key_expire = public_key.expire_unix_epoch_seconds();
  const int64_t key_active_end = key_expire - authn_token_validity_seconds_;

  SignedTokenPB stoken;
  ASSERT_OK(MakeSignedToken(&stoken));
  ASSERT_EQ(key_seq_num, stoken.signing_key_seq_num());

  for (int i = 0; i < num_tablet_servers_; ++i) {
    const tserver::TabletServer* ts = cluster_->mini_tablet_server(i)->server();
    ASSERT_NE(nullptr, ts);
    TokenPB token;
    // There might be some delay due to parallel OS activity, but the public
    // part of TSK should reach all tablet servers in a few heartbeat intervals.
    AssertEventually([&] {
        const VerificationResult res = ts->messenger()->token_verifier().
            VerifyTokenSignature(stoken, &token);
        ASSERT_EQ(VerificationResult::VALID, res);
    }, MonoDelta::FromMilliseconds(5L * FLAGS_heartbeat_interval_ms));
    NO_PENDING_FATALS();
  }

  // Get closer to the very end of the TSK's activity interval and generate
  // another authn token. Such token has the expiration time close to the
  // expiration time of the TSK itself. Make sure the authn token can be
  // successfully verified up to its expiry.
  //
  // It's necessary to keep some margin due to double-to-int truncation issues
  // (that's about WallTime_Now()) and to allow generating authn token before
  // current TSK is replaced with the next one (that's to make this test stable
  // on slow VMs). The margin should be relatively small compared with the
  // TSK rotation interval.
  const int64_t margin = tsk_rotation_seconds_ / 5;
  while (true) {
    if (key_active_end - margin <= WallTime_Now()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(500));
  }

  SignedTokenPB stoken_eotai; // end-of-TSK-activity-interval authn token
  ASSERT_OK(MakeSignedToken(&stoken_eotai));
  const int64_t key_seq_num_token_eotai = stoken_eotai.signing_key_seq_num();
  ASSERT_EQ(key_seq_num, key_seq_num_token_eotai);

  vector<bool> expired_at_tserver(num_tablet_servers_, false);
  vector<bool> valid_at_tserver(num_tablet_servers_, false);
  while (true) {
    for (int i = 0; i < num_tablet_servers_; ++i) {
      if (expired_at_tserver[i]) {
        continue;
      }
      const tserver::TabletServer* ts = cluster_->mini_tablet_server(i)->server();
      ASSERT_NE(nullptr, ts);
      const int64_t time_pre = WallTime_Now();
      TokenPB token;
      const VerificationResult res = ts->messenger()->token_verifier().
          VerifyTokenSignature(stoken_eotai, &token);
      if (res == VerificationResult::VALID) {
        // Both authn token and its TSK should be valid.
        valid_at_tserver[i] = true;
        ASSERT_GE(token.expire_unix_epoch_seconds(), time_pre);
        ASSERT_GE(key_expire, time_pre);
      } else {
        expired_at_tserver[i] = true;
        // The only expected error here is EXPIRED_TOKEN.
        ASSERT_EQ(VerificationResult::EXPIRED_TOKEN, res);
        const int64_t time_post = WallTime_Now();
        ASSERT_LT(token.expire_unix_epoch_seconds(), time_post);
      }
    }
    if (all_of(expired_at_tserver.begin(), expired_at_tserver.end(),
               bind(equal_to<bool>(), _1, true))) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(500));
  }

  // The end-of-TSK-activity-interval authn token should have been successfully
  // validated by all tablet servers.
  ASSERT_TRUE(all_of(valid_at_tserver.begin(), valid_at_tserver.end(),
              bind(equal_to<bool>(), _1, true)));

  while (WallTime_Now() < key_expire) {
    SleepFor(MonoDelta::FromMilliseconds(500));
  }
  // Wait until current TSK expires and try to verify the token again.
  // The token verification result should be EXPIRED_TOKEN.
  for (int i = 0; i < num_tablet_servers_; ++i) {
    const tserver::TabletServer* ts = cluster_->mini_tablet_server(i)->server();
    ASSERT_NE(nullptr, ts);
    TokenPB token;
    ASSERT_EQ(VerificationResult::EXPIRED_TOKEN,
              ts->messenger()->token_verifier().
              VerifyTokenSignature(stoken_eotai, &token));
  }
}

// TODO(aserbin): add a test case which corresponds to multiple signing keys
// right after cluster start-up.

} // namespace master
} // namespace kudu
