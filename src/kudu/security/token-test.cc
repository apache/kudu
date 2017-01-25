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

#include "kudu/util/test_util.h"

#include "kudu/gutil/walltime.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"

DECLARE_int32(token_signing_key_num_rsa_bits);
DECLARE_int64(token_signing_key_validity_seconds);


namespace kudu {
namespace security {

namespace {

SignedTokenPB MakeUnsignedToken(int64_t expiration) {
  SignedTokenPB ret;
  TokenPB token;
  token.set_expire_unix_epoch_seconds(expiration);
  CHECK(token.SerializeToString(ret.mutable_token_data()));
  return ret;
}

SignedTokenPB MakeIncompatibleToken() {
  SignedTokenPB ret;
  TokenPB token;
  token.set_expire_unix_epoch_seconds(WallTime_Now() + 100);
  token.add_incompatible_features(TokenPB::Feature_MAX + 1);
  CHECK(token.SerializeToString(ret.mutable_token_data()));
  return ret;
}

} // anonymous namespace

class TokenTest : public KuduTest {
  void SetUp() override {
    KuduTest::SetUp();
    // Set the keylength smaller to make tests run faster.
    FLAGS_token_signing_key_num_rsa_bits = 512;
  }
};

TEST_F(TokenTest, TestSigner) {
  SignedTokenPB token = MakeUnsignedToken(WallTime_Now());

  const int kStartingSeqNum = 123;
  TokenSigner signer(kStartingSeqNum);

  // Should start off with no signing keys.
  ASSERT_TRUE(signer.GetTokenSigningPublicKeys(0).empty());

  // Trying to sign a token when there is no TSK should give
  // an error.
  Status s = signer.SignToken(&token);
  ASSERT_TRUE(s.IsIllegalState());

  // Rotate the TSK once.
  ASSERT_OK(signer.RotateSigningKey());

  // We should see the key now if we request TSKs starting at a
  // lower sequence number.
  ASSERT_EQ(signer.GetTokenSigningPublicKeys(0).size(), 1);
  // We should not see the key if we ask for the sequence number
  // that it is assigned.
  ASSERT_EQ(signer.GetTokenSigningPublicKeys(kStartingSeqNum).size(), 0);

  // We should be able to sign a token, even though we have
  // just one key.
  ASSERT_OK(signer.SignToken(&token));
  ASSERT_TRUE(token.has_signature());
  ASSERT_EQ(token.signing_key_seq_num(), kStartingSeqNum);

  // Rotate again and check that we return the right keys.
  ASSERT_OK(signer.RotateSigningKey());
  ASSERT_EQ(signer.GetTokenSigningPublicKeys(0).size(), 2);
  ASSERT_EQ(signer.GetTokenSigningPublicKeys(kStartingSeqNum).size(), 1);
  ASSERT_EQ(signer.GetTokenSigningPublicKeys(kStartingSeqNum + 1).size(), 0);

  // We still use the original key for signing (we always use the second-to-latest
  // key).
  token = MakeUnsignedToken(WallTime_Now());
  ASSERT_OK(signer.SignToken(&token));
  ASSERT_TRUE(token.has_signature());
  ASSERT_EQ(token.signing_key_seq_num(), kStartingSeqNum);

  // If we rotate one more time, we should start using the second key.
  ASSERT_OK(signer.RotateSigningKey());
  token = MakeUnsignedToken(WallTime_Now());
  ASSERT_OK(signer.SignToken(&token));
  ASSERT_TRUE(token.has_signature());
  ASSERT_EQ(token.signing_key_seq_num(), kStartingSeqNum + 1);
}

// Test that the TokenSigner can export its public keys in protobuf form.
TEST_F(TokenTest, TestExportKeys) {
  // Test that the exported public keys don't contain private key material,
  // and have an appropriate expiration.
  TokenSigner signer(1);
  ASSERT_OK(signer.RotateSigningKey());
  auto keys = signer.GetTokenSigningPublicKeys(0);
  ASSERT_EQ(keys.size(), 1);
  const TokenSigningPublicKeyPB& key = keys[0];
  ASSERT_TRUE(key.has_rsa_key_der());
  ASSERT_EQ(key.key_seq_num(), 1);
  ASSERT_TRUE(key.has_expire_unix_epoch_seconds());
  ASSERT_GT(key.expire_unix_epoch_seconds(), WallTime_Now());
}

// Test that the TokenVerifier can import keys exported by the TokenSigner
// and then verify tokens signed by it.
TEST_F(TokenTest, TestEndToEnd_Valid) {
  TokenSigner signer(1);
  ASSERT_OK(signer.RotateSigningKey());

  // Make and sign a token.
  SignedTokenPB token = MakeUnsignedToken(WallTime_Now() + 600);
  ASSERT_OK(signer.SignToken(&token));

  // Try to verify it.
  TokenVerifier verifier;
  verifier.ImportPublicKeys(signer.GetTokenSigningPublicKeys(0));
  ASSERT_EQ(VerificationResult::VALID, verifier.VerifyTokenSignature(token));
}

// Test all of the possible cases covered by token verification.
// See VerificationResult.
TEST_F(TokenTest, TestEndToEnd_InvalidCases) {
  TokenSigner signer(1);
  ASSERT_OK(signer.RotateSigningKey());

  TokenVerifier verifier;
  verifier.ImportPublicKeys(signer.GetTokenSigningPublicKeys(0));

  // Make and sign a token, but corrupt the data in it.
  {
    SignedTokenPB token = MakeUnsignedToken(WallTime_Now() + 600);
    ASSERT_OK(signer.SignToken(&token));
    token.set_token_data("xyz");
    ASSERT_EQ(VerificationResult::INVALID_TOKEN, verifier.VerifyTokenSignature(token));
  }

  // Make and sign a token, but corrupt the signature.
  {
    SignedTokenPB token = MakeUnsignedToken(WallTime_Now() + 600);
    ASSERT_OK(signer.SignToken(&token));
    token.set_signature("xyz");
    ASSERT_EQ(VerificationResult::INVALID_SIGNATURE, verifier.VerifyTokenSignature(token));
  }

  // Make and sign a token, but set it to be already expired.
  {
    SignedTokenPB token = MakeUnsignedToken(WallTime_Now() - 10);
    ASSERT_OK(signer.SignToken(&token));
    ASSERT_EQ(VerificationResult::EXPIRED_TOKEN, verifier.VerifyTokenSignature(token));
  }

  // Make and sign a token which uses an incompatible feature flag.
  {
    SignedTokenPB token = MakeIncompatibleToken();
    ASSERT_OK(signer.SignToken(&token));
    ASSERT_EQ(VerificationResult::INCOMPATIBLE_FEATURE, verifier.VerifyTokenSignature(token));
  }

  // Rotate to a new key, but don't inform the verifier of it yet. When we
  // verify, we expect the verifier to complain the key is unknown.
  ASSERT_OK(signer.RotateSigningKey());
  ASSERT_OK(signer.RotateSigningKey());
  {
    SignedTokenPB token = MakeUnsignedToken(WallTime_Now() + 600);
    ASSERT_OK(signer.SignToken(&token));
    ASSERT_EQ(VerificationResult::UNKNOWN_SIGNING_KEY, verifier.VerifyTokenSignature(token));
  }

  // Rotate to a signing key which is already expired, and inform the verifier
  // of all of the current keys. The verifier should recognize the key but
  // know that it's expired.
  FLAGS_token_signing_key_validity_seconds = -10;
  ASSERT_OK(signer.RotateSigningKey());
  ASSERT_OK(signer.RotateSigningKey());
  verifier.ImportPublicKeys(signer.GetTokenSigningPublicKeys(
      verifier.GetMaxKnownKeySequenceNumber()));
  {
    SignedTokenPB token = MakeUnsignedToken(WallTime_Now() + 600);
    ASSERT_OK(signer.SignToken(&token));
    ASSERT_EQ(VerificationResult::EXPIRED_SIGNING_KEY, verifier.VerifyTokenSignature(token));
  }
}

} // namespace security
} // namespace kudu
