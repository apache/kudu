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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/walltime.h"
#include "kudu/security/crypto.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/test_util.h"

DECLARE_int32(tsk_num_rsa_bits);

using std::string;
using std::make_shared;
using std::unique_ptr;
using std::vector;

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

// Generate public key as a string in DER format for tests.
Status GeneratePublicKeyStrDer(string* ret) {
  PrivateKey private_key;
  RETURN_NOT_OK(GeneratePrivateKey(512, &private_key));
  PublicKey public_key;
  RETURN_NOT_OK(private_key.GetPublicKey(&public_key));
  string public_key_str_der;
  RETURN_NOT_OK(public_key.ToString(&public_key_str_der, DataFormat::DER));
  *ret = public_key_str_der;
  return Status::OK();
}

// Generate token signing key with the specified parameters.
Status GenerateTokenSigningKey(int64_t seq_num,
                               int64_t expire_time_seconds,
                               unique_ptr<TokenSigningPrivateKey>* tsk) {
  {
    unique_ptr<PrivateKey> private_key(new PrivateKey);
    RETURN_NOT_OK(GeneratePrivateKey(512, private_key.get()));
    tsk->reset(new TokenSigningPrivateKey(
        seq_num, expire_time_seconds, std::move(private_key)));
  }
  return Status::OK();
}

void CheckAndAddNextKey(int iter_num,
                        TokenSigner* signer,
                        int64_t* key_seq_num) {
  ASSERT_NE(nullptr, signer);
  ASSERT_NE(nullptr, key_seq_num);
  int64_t seq_num;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer->CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    seq_num = key->key_seq_num();
  }

  for (int i = 0; i < iter_num; ++i) {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer->CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_EQ(seq_num, key->key_seq_num());
    if (i + 1 == iter_num) {
      // Finally, add the key to the TokenSigner.
      ASSERT_OK(signer->AddKey(std::move(key)));
    }
  }
  *key_seq_num = seq_num;
}

} // anonymous namespace

class TokenTest : public KuduTest {
};

TEST_F(TokenTest, TestInit) {
  TokenSigner signer(10, 10);
  const TokenVerifier& verifier(signer.verifier());

  SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
  Status s = signer.SignToken(&token);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  static const int64_t kKeySeqNum = 100;
  PrivateKey private_key;
  ASSERT_OK(GeneratePrivateKey(512, &private_key));
  string private_key_str_der;
  ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
  TokenSigningPrivateKeyPB pb;
  pb.set_rsa_key_der(private_key_str_der);
  pb.set_key_seq_num(kKeySeqNum);
  pb.set_expire_unix_epoch_seconds(WallTime_Now() + 120);

  ASSERT_OK(signer.ImportKeys({pb}));
  vector<TokenSigningPublicKeyPB> public_keys(verifier.ExportKeys());
  ASSERT_EQ(1, public_keys.size());
  ASSERT_EQ(kKeySeqNum, public_keys[0].key_seq_num());

  // It should be possible to sign tokens once the signer is initialized.
  ASSERT_OK(signer.SignToken(&token));
  ASSERT_TRUE(token.has_signature());
}

// Verify that TokenSigner does not allow 'holes' in the sequence numbers
// of the generated keys. The idea is to not allow sequences like '1, 5, 6'.
// In general, calling the CheckNeedKey() method multiple times and then calling
// the AddKey() method once should advance the key sequence number only by 1
// regardless of number CheckNeedKey() calls.
//
// This is to make sure that the sequence numbers are not sparse in case if
// running scenarios CheckNeedKey()-try-to-store-key-AddKey() over and over
// again, given that the 'try-to-store-key' part can fail sometimes.
TEST_F(TokenTest, TestTokenSignerNonSparseSequenceNumbers) {
  static const int kIterNum = 3;
  static const int64_t kAuthnTokenValiditySeconds = 1;
  static const int64_t kKeyRotationSeconds = 1;

  TokenSigner signer(kAuthnTokenValiditySeconds, kKeyRotationSeconds);

  int64_t seq_num_first_key;
  NO_FATALS(CheckAndAddNextKey(kIterNum, &signer, &seq_num_first_key));

  SleepFor(MonoDelta::FromSeconds(kKeyRotationSeconds + 1));

  int64_t seq_num_second_key;
  NO_FATALS(CheckAndAddNextKey(kIterNum, &signer, &seq_num_second_key));

  ASSERT_EQ(seq_num_first_key + 1, seq_num_second_key);
}

// Verify the behavior of the TokenSigner::ImportKeys() method. In general,
// it should tolerate mix of expired and non-expired keys, even if their
// sequence numbers are intermixed: keys with greater sequence numbers could
// be already expired but keys with lesser sequence numbers could be still
// valid. The idea is to correctly import TSKs generated with different
// validity period settings. This is to address scenarios when the system
// was run with long authn token validity interval and then switched to
// a shorter one.
//
// After importing keys, the TokenSigner should contain only the valid ones.
// In addition, the sequence number of the very first key generated after the
// import should be greater than any sequence number the TokenSigner has seen
// during the import.
TEST_F(TokenTest, TestTokenSignerAddKeyAfterImport) {
  static const int64_t kAuthnTokenValiditySeconds = 8;
  static const int64_t kKeyRotationSeconds = 8;
  static const int64_t kKeyValiditySeconds =
      kAuthnTokenValiditySeconds + 2 * kKeyRotationSeconds;

  TokenSigner signer(kAuthnTokenValiditySeconds, kKeyRotationSeconds);
  const TokenVerifier& verifier(signer.verifier());

  static const int64_t kExpiredKeySeqNum = 100;
  static const int64_t kKeySeqNum = kExpiredKeySeqNum - 1;
  {
    // First, try to import already expired key to check that internal key
    // sequence number advances correspondingly.
    PrivateKey private_key;
    ASSERT_OK(GeneratePrivateKey(512, &private_key));
    string private_key_str_der;
    ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(private_key_str_der);
    pb.set_key_seq_num(kExpiredKeySeqNum);
    pb.set_expire_unix_epoch_seconds(WallTime_Now() - 1);

    ASSERT_OK(signer.ImportKeys({pb}));
  }

  {
    // Check the result of importing keys: there should be no keys because
    // the only one we tried to import was already expired.
    vector<TokenSigningPublicKeyPB> public_keys(verifier.ExportKeys());
    ASSERT_TRUE(public_keys.empty());
  }

  {
    // Now import valid (not yet expired) key, but with sequence number less
    // than of the expired key.
    PrivateKey private_key;
    ASSERT_OK(GeneratePrivateKey(512, &private_key));
    string private_key_str_der;
    ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(private_key_str_der);
    pb.set_key_seq_num(kKeySeqNum);
    // Set the TSK's expiration time: make the key valid but past its activity
    // interval.
    pb.set_expire_unix_epoch_seconds(
        WallTime_Now() + (kKeyValiditySeconds - 2 * kKeyRotationSeconds - 1));

    ASSERT_OK(signer.ImportKeys({pb}));
  }

  {
    // Check the result of importing keys.
    vector<TokenSigningPublicKeyPB> public_keys(verifier.ExportKeys());
    ASSERT_EQ(1, public_keys.size());
    ASSERT_EQ(kKeySeqNum, public_keys[0].key_seq_num());
  }

  {
    // The newly imported key should be used to sign tokens.
    SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
    ASSERT_OK(signer.SignToken(&token));
    ASSERT_TRUE(token.has_signature());
    ASSERT_TRUE(token.has_signing_key_seq_num());
    EXPECT_EQ(kKeySeqNum, token.signing_key_seq_num());
  }

  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_EQ(kExpiredKeySeqNum + 1, key->key_seq_num());
    ASSERT_OK(signer.AddKey(std::move(key)));
    bool has_rotated = false;
    ASSERT_OK(signer.TryRotateKey(&has_rotated));
    ASSERT_TRUE(has_rotated);
  }
  {
    // Check the result of generating the new key: the identifier of the new key
    // should be +1 increment from the identifier of the expired imported key.
    vector<TokenSigningPublicKeyPB> public_keys(verifier.ExportKeys());
    ASSERT_EQ(2, public_keys.size());
    EXPECT_EQ(kKeySeqNum, public_keys[0].key_seq_num());
    EXPECT_EQ(kExpiredKeySeqNum + 1, public_keys[1].key_seq_num());
  }

  // At this point the new key should be used to sign tokens.
  SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
  ASSERT_OK(signer.SignToken(&token));
  ASSERT_TRUE(token.has_signature());
  ASSERT_TRUE(token.has_signing_key_seq_num());
  EXPECT_EQ(kExpiredKeySeqNum + 1, token.signing_key_seq_num());
}

// The AddKey() method should not allow to add a key with the sequence number
// less or equal to the sequence number of the most 'recent' key.
TEST_F(TokenTest, TestAddKeyConstraints) {
  {
    TokenSigner signer(1, 1);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));
  }
  {
    TokenSigner signer(1, 1);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    const int64_t key_seq_num = key->key_seq_num();
    key->key_seq_num_ = key_seq_num - 1;
    Status s = signer.AddKey(std::move(key));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        ": invalid key sequence number, should be at least ");
  }
  {
    TokenSigner signer(1, 1);
    static const int64_t kKeySeqNum = 100;
    PrivateKey private_key;
    ASSERT_OK(GeneratePrivateKey(512, &private_key));
    string private_key_str_der;
    ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(private_key_str_der);
    pb.set_key_seq_num(kKeySeqNum);
    // Make the key already expired.
    pb.set_expire_unix_epoch_seconds(WallTime_Now() - 1);
    ASSERT_OK(signer.ImportKeys({pb}));

    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    const int64_t key_seq_num = key->key_seq_num();
    ASSERT_GT(key_seq_num, kKeySeqNum);
    key->key_seq_num_ = kKeySeqNum;
    Status s = signer.AddKey(std::move(key));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        ": invalid key sequence number, should be at least ");
  }
}

TEST_F(TokenTest, TestGenerateAuthTokenNoUserName) {
  TokenSigner signer(10, 10);
  SignedTokenPB signed_token_pb;
  const Status& s = signer.GenerateAuthnToken("", &signed_token_pb);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no username provided for authn token");
}

TEST_F(TokenTest, TestIsCurrentKeyValid) {
  static const int64_t kAuthnTokenValiditySeconds = 1;
  static const int64_t kKeyRotationSeconds = 1;
  static const int64_t kKeyValiditySeconds =
      kAuthnTokenValiditySeconds + 2 * kKeyRotationSeconds;

  TokenSigner signer(kAuthnTokenValiditySeconds, kKeyRotationSeconds);
  EXPECT_FALSE(signer.IsCurrentKeyValid());
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));
  }
  EXPECT_TRUE(signer.IsCurrentKeyValid());
  SleepFor(MonoDelta::FromSeconds(kKeyValiditySeconds));
  // The key should expire after its validity interval.
  EXPECT_FALSE(signer.IsCurrentKeyValid());

  // Anyway, current implementation allows to use an expired key to sign tokens.
  SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
  EXPECT_OK(signer.SignToken(&token));
}

TEST_F(TokenTest, TestTokenSignerAddKeys) {
  {
    TokenSigner signer(10, 10);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));

    ASSERT_OK(signer.CheckNeedKey(&key));
    // It's not time to add next key yet.
    ASSERT_EQ(nullptr, key.get());
  }

  {
    // Special configuration for TokenSigner: rotation interval is zero,
    // so should be able to add two keys right away.
    TokenSigner signer(10, 0);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));

    // Should be able to add next key right away.
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));

    // Active key and next key are already in place: no need for a new key.
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_EQ(nullptr, key.get());
  }

  if (AllowSlowTests()) {
    // Special configuration for TokenSigner: short interval for key rotation.
    // It should not need next key right away, but should need next key after
    // the rotation interval.
    static const int64_t kKeyRotationIntervalSeconds = 8;
    TokenSigner signer(10, kKeyRotationIntervalSeconds);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));

    // Should not need next key right away.
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_EQ(nullptr, key.get());

    SleepFor(MonoDelta::FromSeconds(kKeyRotationIntervalSeconds));

    // Should need next key after the rotation interval.
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));

    // Active key and next key are already in place: no need for a new key.
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_EQ(nullptr, key.get());
  }
}

// Test how key rotation works.
TEST_F(TokenTest, TestTokenSignerSignVerifyExport) {
  // Key rotation interval 0 allows adding 2 keys in a row with no delay.
  TokenSigner signer(10, 0);
  const TokenVerifier& verifier(signer.verifier());

  // Should start off with no signing keys.
  ASSERT_TRUE(verifier.ExportKeys().empty());

  // Trying to sign a token when there is no TSK should give an error.
  SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
  Status s = signer.SignToken(&token);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // Generate and set a new key.
  int64_t signing_key_seq_num;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    signing_key_seq_num = key->key_seq_num();
    ASSERT_GT(signing_key_seq_num, -1);
    ASSERT_OK(signer.AddKey(std::move(key)));
  }

  // We should see the key now if we request TSKs starting at a
  // lower sequence number.
  ASSERT_EQ(1, verifier.ExportKeys().size());
  // We should not see the key if we ask for the sequence number
  // that it is assigned.
  ASSERT_EQ(0, verifier.ExportKeys(signing_key_seq_num).size());

  // We should be able to sign a token now.
  ASSERT_OK(signer.SignToken(&token));
  ASSERT_TRUE(token.has_signature());
  ASSERT_EQ(signing_key_seq_num, token.signing_key_seq_num());

  // Set next key and check that we return the right keys.
  int64_t next_signing_key_seq_num;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    next_signing_key_seq_num = key->key_seq_num();
    ASSERT_GT(next_signing_key_seq_num, signing_key_seq_num);
    ASSERT_OK(signer.AddKey(std::move(key)));
  }
  ASSERT_EQ(2, verifier.ExportKeys().size());
  ASSERT_EQ(1, verifier.ExportKeys(signing_key_seq_num).size());
  ASSERT_EQ(0, verifier.ExportKeys(next_signing_key_seq_num).size());

  // The first key should be used for signing: the next one is saved
  // for the next round.
  {
    SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
    ASSERT_OK(signer.SignToken(&token));
    ASSERT_TRUE(token.has_signature());
    ASSERT_EQ(signing_key_seq_num, token.signing_key_seq_num());
  }
}

// Test that the TokenSigner can export its public keys in protobuf form
// via bound TokenVerifier.
TEST_F(TokenTest, TestExportKeys) {
  // Test that the exported public keys don't contain private key material,
  // and have an appropriate expiration.
  const int64_t key_exp_seconds = 30;
  const int64_t key_rotation_seconds = 10;
  TokenSigner signer(key_exp_seconds - 2 * key_rotation_seconds,
                     key_rotation_seconds);
  int64_t key_seq_num;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    key_seq_num = key->key_seq_num();
    ASSERT_OK(signer.AddKey(std::move(key)));
  }
  const TokenVerifier& verifier(signer.verifier());
  auto keys = verifier.ExportKeys();
  ASSERT_EQ(1, keys.size());
  const TokenSigningPublicKeyPB& key = keys[0];
  ASSERT_TRUE(key.has_rsa_key_der());
  ASSERT_EQ(key_seq_num, key.key_seq_num());
  ASSERT_TRUE(key.has_expire_unix_epoch_seconds());
  const int64_t now = WallTime_Now();
  ASSERT_GT(key.expire_unix_epoch_seconds(), now);
  ASSERT_LE(key.expire_unix_epoch_seconds(), now + key_exp_seconds);
}

// Test that the TokenVerifier can import keys exported by the TokenSigner
// and then verify tokens signed by it.
TEST_F(TokenTest, TestEndToEnd_Valid) {
  TokenSigner signer(10, 10);
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));
  }

  // Make and sign a token.
  SignedTokenPB signed_token = MakeUnsignedToken(WallTime_Now() + 600);
  ASSERT_OK(signer.SignToken(&signed_token));

  // Try to verify it.
  TokenVerifier verifier;
  ASSERT_OK(verifier.ImportKeys(signer.verifier().ExportKeys()));
  TokenPB token;
  ASSERT_EQ(VerificationResult::VALID, verifier.VerifyTokenSignature(signed_token, &token));
}

// Test all of the possible cases covered by token verification.
// See VerificationResult.
TEST_F(TokenTest, TestEndToEnd_InvalidCases) {
  // Key rotation interval 0 allows adding 2 keys in a row with no delay.
  TokenSigner signer(10, 0);
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));
  }

  TokenVerifier verifier;
  ASSERT_OK(verifier.ImportKeys(signer.verifier().ExportKeys()));

  // Make and sign a token, but corrupt the data in it.
  {
    SignedTokenPB signed_token = MakeUnsignedToken(WallTime_Now() + 600);
    ASSERT_OK(signer.SignToken(&signed_token));
    signed_token.set_token_data("xyz");
    TokenPB token;
    ASSERT_EQ(VerificationResult::INVALID_TOKEN,
              verifier.VerifyTokenSignature(signed_token, &token));
  }

  // Make and sign a token, but corrupt the signature.
  {
    SignedTokenPB signed_token = MakeUnsignedToken(WallTime_Now() + 600);
    ASSERT_OK(signer.SignToken(&signed_token));
    signed_token.set_signature("xyz");
    TokenPB token;
    ASSERT_EQ(VerificationResult::INVALID_SIGNATURE,
              verifier.VerifyTokenSignature(signed_token, &token));
  }

  // Make and sign a token, but set it to be already expired.
  {
    SignedTokenPB signed_token = MakeUnsignedToken(WallTime_Now() - 10);
    ASSERT_OK(signer.SignToken(&signed_token));
    TokenPB token;
    ASSERT_EQ(VerificationResult::EXPIRED_TOKEN,
              verifier.VerifyTokenSignature(signed_token, &token));
  }

  // Make and sign a token which uses an incompatible feature flag.
  {
    SignedTokenPB signed_token = MakeIncompatibleToken();
    ASSERT_OK(signer.SignToken(&signed_token));
    TokenPB token;
    ASSERT_EQ(VerificationResult::INCOMPATIBLE_FEATURE,
              verifier.VerifyTokenSignature(signed_token, &token));
  }

  // Set a new signing key, but don't inform the verifier of it yet. When we
  // verify, we expect the verifier to complain the key is unknown.
  {
    {
      std::unique_ptr<TokenSigningPrivateKey> key;
      ASSERT_OK(signer.CheckNeedKey(&key));
      ASSERT_NE(nullptr, key.get());
      ASSERT_OK(signer.AddKey(std::move(key)));
      bool has_rotated = false;
      ASSERT_OK(signer.TryRotateKey(&has_rotated));
      ASSERT_TRUE(has_rotated);
    }
    SignedTokenPB signed_token = MakeUnsignedToken(WallTime_Now() + 600);
    ASSERT_OK(signer.SignToken(&signed_token));
    TokenPB token;
    ASSERT_EQ(VerificationResult::UNKNOWN_SIGNING_KEY,
              verifier.VerifyTokenSignature(signed_token, &token));
  }

  // Set a new signing key which is already expired, and inform the verifier
  // of all of the current keys. The verifier should recognize the key but
  // know that it's expired.
  {
    {
      unique_ptr<TokenSigningPrivateKey> tsk;
      ASSERT_OK(GenerateTokenSigningKey(100, WallTime_Now() - 1, &tsk));
      // This direct access is necessary because AddKey() does not allow to add
      // an expired key.
      TokenSigningPublicKeyPB tsk_public_pb;
      tsk->ExportPublicKeyPB(&tsk_public_pb);
      ASSERT_OK(verifier.ImportKeys({tsk_public_pb}));
      signer.tsk_deque_.push_front(std::move(tsk));
    }

    SignedTokenPB signed_token = MakeUnsignedToken(WallTime_Now() + 600);
    // Current implementation allows to use an expired key to sign tokens.
    ASSERT_OK(signer.SignToken(&signed_token));
    TokenPB token;
    ASSERT_EQ(VerificationResult::EXPIRED_SIGNING_KEY,
              verifier.VerifyTokenSignature(signed_token, &token));
  }
}

// Test functionality of the TokenVerifier::ImportKeys() method.
TEST_F(TokenTest, TestTokenVerifierImportKeys) {
  TokenVerifier verifier;

  // An attempt to import no keys is fine.
  ASSERT_OK(verifier.ImportKeys({}));
  ASSERT_TRUE(verifier.ExportKeys().empty());

  TokenSigningPublicKeyPB tsk_public_pb;
  const auto exp_time = WallTime_Now() + 600;
  tsk_public_pb.set_key_seq_num(100500);
  tsk_public_pb.set_expire_unix_epoch_seconds(exp_time);
  string public_key_str_der;
  ASSERT_OK(GeneratePublicKeyStrDer(&public_key_str_der));
  tsk_public_pb.set_rsa_key_der(public_key_str_der);

  ASSERT_OK(verifier.ImportKeys({ tsk_public_pb }));
  {
    const auto& exported_tsks_public_pb = verifier.ExportKeys();
    ASSERT_EQ(1, exported_tsks_public_pb.size());
    EXPECT_EQ(tsk_public_pb.SerializeAsString(),
              exported_tsks_public_pb[0].SerializeAsString());
  }

  // Re-importing the same key again is fine, and the total number
  // of exported keys should not increase.
  ASSERT_OK(verifier.ImportKeys({ tsk_public_pb }));
  {
    const auto& exported_tsks_public_pb = verifier.ExportKeys();
    ASSERT_EQ(1, exported_tsks_public_pb.size());
    EXPECT_EQ(tsk_public_pb.SerializeAsString(),
              exported_tsks_public_pb[0].SerializeAsString());
  }
}

} // namespace security
} // namespace kudu
