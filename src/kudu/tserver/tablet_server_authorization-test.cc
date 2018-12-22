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

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::shared_ptr;
using std::string;
using std::vector;

DECLARE_bool(tserver_enforce_access_control);
DECLARE_double(tserver_inject_invalid_authz_token_ratio);

namespace kudu {

class Schema;

using pb_util::SecureShortDebugString;
using rpc::ErrorStatusPB;
using rpc::RpcController;
using security::PrivateKey;
using security::SignedTokenPB;
using security::TablePrivilegePB;
using security::TokenSigner;
using security::TokenSigningPrivateKeyPB;
using security::TokenSigningPublicKeyPB;
using security::TokenVerifier;

namespace tserver {

namespace {

// Verifies the expected response for an invalid/malformed token.
void CheckInvalidAuthzToken(const Status& s, const RpcController& rpc) {
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
  ASSERT_TRUE(rpc.error_response()) << "Expected an error response";
  ASSERT_TRUE(rpc.error_response()->code() == ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN)
      << SecureShortDebugString(*rpc.error_response());
}

// Gets a private key for the given sequence number.
TokenSigningPrivateKeyPB GetTokenSigningPrivateKey(int seq_num) {
  TokenSigningPrivateKeyPB tsk;
  PrivateKey private_key;
  CHECK_OK(GeneratePrivateKey(/*num_bits=*/512, &private_key));
  string private_key_str_der;
  CHECK_OK(private_key.ToString(&private_key_str_der, security::DataFormat::DER));
  tsk.set_rsa_key_der(private_key_str_der);
  tsk.set_key_seq_num(seq_num);
  tsk.set_expire_unix_epoch_seconds(WallTime_Now() + 3600);
  return tsk;
}

// Test-param argument to instantiate various tserver requests and send the
// appropriate proxy calls.
typedef std::function<Status(const Schema&, const SignedTokenPB*, TabletServerServiceProxy*,
                             RpcController*)> RequestorFunc;

Status WriteGenerator(const Schema& schema, const SignedTokenPB* token,
                      TabletServerServiceProxy* proxy, RpcController* rpc) {
  WriteRequestPB req;
  req.set_tablet_id(TabletServerTestBase::kTabletId);
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema, 1234, 5678, "hello world",
                 req.mutable_row_operations());
  if (token) {
    *req.mutable_authz_token() = *token;
  }
  WriteResponsePB resp;
  LOG(INFO) << "Sending write request";
  return proxy->Write(req, &resp, rpc);
}

Status ScanGenerator(const Schema& schema, const SignedTokenPB* token,
                     TabletServerServiceProxy* proxy, RpcController* rpc) {
  ScanRequestPB req;
  req.set_call_seq_id(0);
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(TabletServerTestBase::kTabletId);
  RETURN_NOT_OK(SchemaToColumnPBs(schema, scan->mutable_projected_columns()));
  if (token) {
    *scan->mutable_authz_token() = *token;
  }
  ScanResponsePB resp;
  LOG(INFO) << "Sending scan request";
  return proxy->Scan(req, &resp, rpc);
}

Status SplitKeyRangeGenerator(const Schema& /*schema*/, const SignedTokenPB* token,
                              TabletServerServiceProxy* proxy, RpcController* rpc) {
  SplitKeyRangeRequestPB req;
  req.set_tablet_id(TabletServerTestBase::kTabletId);
  if (token) {
    *req.mutable_authz_token() = *token;
  }
  SplitKeyRangeResponsePB resp;
  LOG(INFO) << "Sending split-key-range request";
  return proxy->SplitKeyRange(req, &resp, rpc);
}

Status ChecksumGenerator(const Schema& schema, const SignedTokenPB* token,
                         TabletServerServiceProxy* proxy, RpcController* rpc) {
  ChecksumRequestPB req;
  NewScanRequestPB* scan = req.mutable_new_request();
  scan->set_tablet_id(TabletServerTestBase::kTabletId);
  RETURN_NOT_OK(SchemaToColumnPBs(schema, scan->mutable_projected_columns()));
  if (token) {
    *scan->mutable_authz_token() = *token;
  }
  ChecksumResponsePB resp;
  LOG(INFO) << "Sending checksum scan request";
  return proxy->Checksum(req, &resp, rpc);
}

} // anonymous namespace

class AuthzTabletServerTest : public TabletServerTestBase,
                              public testing::WithParamInterface<RequestorFunc> {
 public:
  void SetUp() override {
    NO_FATALS(TabletServerTestBase::SetUp());
    NO_FATALS(StartTabletServer(/*num_data_dirs=*/1));
  }
};

TEST_P(AuthzTabletServerTest, TestInvalidAuthzTokens) {
  FLAGS_tserver_enforce_access_control = true;
  rpc::UserCredentials user;
  const string kUser = "dan";
  user.set_real_user(kUser);
  proxy_->set_user_credentials(user);

  TokenSigningPrivateKeyPB tsk = GetTokenSigningPrivateKey(1);
  shared_ptr<TokenVerifier> verifier(new TokenVerifier());
  // We're going to manually tamper with the tokens to make them invalid, so
  // pass in arbitrary expiration values.
  TokenSigner signer(3600, 3600, 3600, verifier);
  ASSERT_OK(signer.ImportKeys({ tsk }));
  vector<TokenSigningPublicKeyPB> public_keys = verifier->ExportKeys();
  ASSERT_OK(mini_server_->server()->mutable_token_verifier()->ImportKeys(public_keys));

  // Set up a privilege that permits everything. Even with these privileges,
  // invalid authz tokens will prevent access.
  TablePrivilegePB privilege;
  privilege.set_table_id(kTableId);
  privilege.set_scan_privilege(true);
  privilege.set_insert_privilege(true);
  privilege.set_update_privilege(true);
  privilege.set_delete_privilege(true);

  // Test various "invalid token" scenarios.
  typedef std::function<SignedTokenPB(void)> TokenCreator;
  vector<TokenCreator> token_creators;
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating token with a bad signature";
    SignedTokenPB token;
    CHECK_OK(signer.GenerateAuthzToken(kUser, privilege, &token));
    string bad_signature = token.signature();
    // Flip the bits in the signature.
    for (int i = 0; i < bad_signature.length(); i++) {
      char* byte = &bad_signature[i];
      *byte = ~*byte;
    }
    token.set_token_data(std::move(bad_signature));
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating token with no signature";
    SignedTokenPB token;
    CHECK_OK(signer.GenerateAuthzToken(kUser, privilege, &token));
    token.clear_signature();
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating token for a different user";
    SignedTokenPB token;
    CHECK_OK(signer.GenerateAuthzToken("bad-dan", privilege, &token));
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating authn token instead of authz token";
    SignedTokenPB token;
    CHECK_OK(signer.GenerateAuthnToken(kUser, &token));
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating expired authz token";
    TokenSigningPrivateKeyPB tsk = GetTokenSigningPrivateKey(2);
    shared_ptr<TokenVerifier> verifier(new TokenVerifier());
    TokenSigner expired_signer(3600, /*authz_token_validity_seconds=*/1, 3600, verifier);
    CHECK_OK(expired_signer.ImportKeys({ tsk }));
    vector<TokenSigningPublicKeyPB> expired_public_keys = verifier->ExportKeys();
    CHECK_OK(mini_server_->server()->mutable_token_verifier()->ImportKeys(public_keys));

    SignedTokenPB token;
    CHECK_OK(expired_signer.GenerateAuthzToken(kUser, privilege, &token));
    // Wait for the token to expire.
    SleepFor(MonoDelta::FromSeconds(3));
    return token;
  });

  const auto& send_req = GetParam();
  // Run all of the above "invalid token" scenarios against the above
  // requests.
  for (const auto& token_creator : token_creators) {
    RpcController rpc;
    const SignedTokenPB token = token_creator();
    Status s = send_req(schema_, &token, proxy_.get(), &rpc);
    NO_FATALS(CheckInvalidAuthzToken(s, rpc));
  }

  // Send a request with no token. This is also considered an "invalid token".
  {
    LOG(INFO) << "Generating request with no authz token";
    RpcController rpc;
    Status s = send_req(schema_, nullptr, proxy_.get(), &rpc);
    NO_FATALS(CheckInvalidAuthzToken(s, rpc));
  }
  // Now test a valid token that has no privileges. This is flat-out
  // disallowed and "fatal".
  {
    LOG(INFO) << "Generating request with no privileges";
    SignedTokenPB token;
    TablePrivilegePB empty;
    ASSERT_OK(signer.GenerateAuthzToken(kUser, empty, &token));
    RpcController rpc;
    Status s = send_req(schema_, &token, proxy_.get(), &rpc);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
    ASSERT_TRUE(rpc.error_response());
    ASSERT_TRUE(rpc.error_response()->code() == ErrorStatusPB::FATAL_UNAUTHORIZED)
        << SecureShortDebugString(*rpc.error_response());
  }
  // Create a healthy token but inject an error.
  {
    LOG(INFO) << "Generating healthy request but injecting error";
    google::FlagSaver saver;
    FLAGS_tserver_inject_invalid_authz_token_ratio = 1.0;
    SignedTokenPB token;
    ASSERT_OK(signer.GenerateAuthzToken(kUser, privilege, &token));
    RpcController rpc;
    Status s = send_req(schema_, &token, proxy_.get(), &rpc);
    NO_FATALS(CheckInvalidAuthzToken(s, rpc));
  }
  // Create a healthy token.
  {
    LOG(INFO) << "Generating healthy request";
    SignedTokenPB token;
    ASSERT_OK(signer.GenerateAuthzToken(kUser, privilege, &token));
    RpcController rpc;
    ASSERT_OK(send_req(schema_, &token, proxy_.get(), &rpc));
    ASSERT_FALSE(rpc.error_response());
  }
}

INSTANTIATE_TEST_CASE_P(RequestorFuncs, AuthzTabletServerTest,
    ::testing::Values(&WriteGenerator, &ScanGenerator,
                      &SplitKeyRangeGenerator, &ChecksumGenerator));

} // namespace tserver
} // namespace kudu
