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
#include <deque>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(tsk_num_rsa_bits);

using kudu::pb_util::SecureDebugString;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace security {

namespace {

// Dummy variables to use when their values don't matter much.
const int kNumBits = 512;
const int64_t kTokenValiditySeconds = 10;
const char kUser[] = "user";

// Repeatedly signs tokens and attempts to rotate TSKs until the active TSK's
// sequence number passes `seq_num`, returning the last token signed by the TSK
// at `seq_num`. This token is roughly the last possible token signed in the
// TSK's activity interval.
// The TokenGenerator 'generate_token' is a lambda that fills in a
// SignedTokenPB and returns a Status.
template <class TokenGenerator>
Status SignUntilRotatePast(TokenSigner* signer, TokenGenerator generate_token,
                           const string& token_type, int64_t seq_num,
                           SignedTokenPB* last_signed_by_tsk) {
  SignedTokenPB last_signed;
  RETURN_NOT_OK_PREPEND(generate_token(&last_signed),
      Substitute("Failed to generate first $0 token", token_type));
  DCHECK_EQ(seq_num, last_signed.signing_key_seq_num())
      << Substitute("Unexpected starting seq_num for $0 token", token_type);

  auto cur_seq_num = seq_num;
  while (cur_seq_num == seq_num) {
    SleepFor(MonoDelta::FromMilliseconds(50));
    KLOG_EVERY_N_SECS(INFO, 1) <<
        Substitute("Generating $0 token for activity interval $1", token_type, seq_num);
    RETURN_NOT_OK_PREPEND(signer->TryRotateKey(), "Failed to attempt to rotate key");
    SignedTokenPB signed_token;
    RETURN_NOT_OK_PREPEND(generate_token(&signed_token),
        Substitute("Failed to generate $0 token", token_type));
    // We want to return the last token signed by the `seq_num` TSK, so only
    // update it when appropriate.
    cur_seq_num = signed_token.signing_key_seq_num();
    if (cur_seq_num == seq_num) {
      last_signed = std::move(signed_token);
    }
  }
  *last_signed_by_tsk = std::move(last_signed);
  return Status::OK();
}

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
  RETURN_NOT_OK(GeneratePrivateKey(kNumBits, &private_key));
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
    RETURN_NOT_OK(GeneratePrivateKey(kNumBits, private_key.get()));
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
  TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 10);
  const TokenVerifier& verifier(signer.verifier());

  SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
  Status s = signer.SignToken(&token);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  static const int64_t kKeySeqNum = 100;
  PrivateKey private_key;
  ASSERT_OK(GeneratePrivateKey(kNumBits, &private_key));
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
  static const int64_t kAuthzTokenValiditySeconds = 1;
  static const int64_t kKeyRotationSeconds = 1;

  TokenSigner signer(kAuthnTokenValiditySeconds, kAuthzTokenValiditySeconds, kKeyRotationSeconds);

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
  static const int64_t kKeyRotationSeconds = 8;

  TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, kKeyRotationSeconds);
  const int64_t key_validity_seconds = signer.key_validity_seconds_;
  const TokenVerifier& verifier(signer.verifier());

  static const int64_t kExpiredKeySeqNum = 100;
  static const int64_t kKeySeqNum = kExpiredKeySeqNum - 1;
  {
    // First, try to import already expired key to check that internal key
    // sequence number advances correspondingly.
    PrivateKey private_key;
    ASSERT_OK(GeneratePrivateKey(kNumBits, &private_key));
    string private_key_str_der;
    ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(private_key_str_der);
    pb.set_key_seq_num(kExpiredKeySeqNum);
    pb.set_expire_unix_epoch_seconds(WallTime_Now() - 1);

    ASSERT_OK(signer.ImportKeys({pb}));

    // Check the result of importing keys: there should be no keys because
    // the only one we tried to import was already expired.
    vector<TokenSigningPublicKeyPB> public_keys(verifier.ExportKeys());
    ASSERT_TRUE(public_keys.empty());
  }

  {
    // Now import valid (not yet expired) key, but with sequence number less
    // than of the expired key.
    PrivateKey private_key;
    ASSERT_OK(GeneratePrivateKey(kNumBits, &private_key));
    string private_key_str_der;
    ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(private_key_str_der);
    pb.set_key_seq_num(kKeySeqNum);
    // Set the TSK's expiration time: make the key valid but past its activity
    // interval.
    pb.set_expire_unix_epoch_seconds(
        WallTime_Now() + (key_validity_seconds - 2 * kKeyRotationSeconds - 1));

    ASSERT_OK(signer.ImportKeys({pb}));

    // Check the result of importing keys. The lower sequence number is
    // accepted, even though we previously imported a key with a higher
    // sequence number that was expired.
    vector<TokenSigningPublicKeyPB> public_keys(verifier.ExportKeys());
    ASSERT_EQ(1, public_keys.size());
    ASSERT_EQ(kKeySeqNum, public_keys[0].key_seq_num());

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
    // If a signer has not created a TSK yet, it will create a key, and will
    // happily accept the generated key.
    TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 1);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));
  }
  {
    // If the key sequence number added to the signer isn't monotonically
    // increasing, the signer will complain.
    TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 1);
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
    // Test importing expired keys. The signer should be OK with it.
    TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 1);
    static const int64_t kKeySeqNum = 100;
    PrivateKey private_key;
    ASSERT_OK(GeneratePrivateKey(kNumBits, &private_key));
    string private_key_str_der;
    ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(private_key_str_der);
    pb.set_key_seq_num(kKeySeqNum);
    // Make the key already expired.
    pb.set_expire_unix_epoch_seconds(WallTime_Now() - 1);
    ASSERT_OK(signer.ImportKeys({pb}));

    // Generated keys thereafter are expected to have higher sequence numbers
    // than the imported expired keys.
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

TEST_F(TokenTest, TestGenerateAuthnTokenNoUserName) {
  TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 10);
  SignedTokenPB signed_token_pb;
  const Status& s = signer.GenerateAuthnToken("", &signed_token_pb);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no username provided for authn token");
}

TEST_F(TokenTest, TestGenerateAuthzToken) {
  // We cannot generate tokens with no username associated with it.
  std::shared_ptr<TokenVerifier> verifier(new TokenVerifier());
  TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 10, verifier);
  TablePrivilegePB table_privilege;
  SignedTokenPB signed_token_pb;
  Status s = signer.GenerateAuthzToken("", table_privilege, &signed_token_pb);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no username provided for authz token");

  // Generated tokens will have the specified privileges.
  const string kAuthorized = "authzed";
  unique_ptr<TokenSigningPrivateKey> key;
  ASSERT_OK(signer.CheckNeedKey(&key));
  ASSERT_NE(nullptr, key.get());
  ASSERT_OK(signer.AddKey(std::move(key)));
  ASSERT_OK(signer.GenerateAuthzToken(kAuthorized,
                                      table_privilege,
                                      &signed_token_pb));
  ASSERT_TRUE(signed_token_pb.has_token_data());
  TokenPB token_pb;
  ASSERT_EQ(VerificationResult::VALID,
            verifier->VerifyTokenSignature(signed_token_pb, &token_pb));
  ASSERT_TRUE(token_pb.has_authz());
  ASSERT_EQ(kAuthorized, token_pb.authz().username());
  ASSERT_TRUE(token_pb.authz().has_table_privilege());
  ASSERT_EQ(SecureDebugString(table_privilege),
            SecureDebugString(token_pb.authz().table_privilege()));
}

TEST_F(TokenTest, TestIsCurrentKeyValid) {
  // This test sleeps for a key validity period, so set it up to be short.
  static const int64_t kShortTokenValiditySeconds = 1;
  static const int64_t kKeyRotationSeconds = 1;

  TokenSigner signer(kShortTokenValiditySeconds, kShortTokenValiditySeconds, kKeyRotationSeconds);
  static const int64_t key_validity_seconds = signer.key_validity_seconds_;

  EXPECT_FALSE(signer.IsCurrentKeyValid());
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.CheckNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.AddKey(std::move(key)));
  }
  EXPECT_TRUE(signer.IsCurrentKeyValid());
  SleepFor(MonoDelta::FromSeconds(key_validity_seconds));
  // The key should expire after its validity interval.
  EXPECT_FALSE(signer.IsCurrentKeyValid());

  // Anyway, current implementation allows to use an expired key to sign tokens.
  SignedTokenPB token = MakeUnsignedToken(WallTime_Now());
  EXPECT_OK(signer.SignToken(&token));
}

TEST_F(TokenTest, TestTokenSignerAddKeys) {
  {
    TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 10);
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
    TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 0);
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
    TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, kKeyRotationIntervalSeconds);
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
  TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 0);
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
  // for the next rotation.
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
  TokenSigner signer(key_exp_seconds - 2 * key_rotation_seconds, 0,
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
  TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 10);
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
  TokenSigner signer(kTokenValiditySeconds, kTokenValiditySeconds, 0);
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

// Test using different token validity intervals.
TEST_F(TokenTest, TestVaryingTokenValidityIntervals) {
  constexpr int kShortValiditySeconds = 2;
  const int kLongValiditySeconds = kShortValiditySeconds * 3;
  std::shared_ptr<TokenVerifier> verifier(new TokenVerifier());
  TokenSigner signer(kLongValiditySeconds, kShortValiditySeconds, 10, verifier);
  unique_ptr<TokenSigningPrivateKey> key;
  ASSERT_OK(signer.CheckNeedKey(&key));
  ASSERT_NE(nullptr, key.get());
  ASSERT_OK(signer.AddKey(std::move(key)));

  const TablePrivilegePB table_privilege;
  SignedTokenPB signed_authn;
  SignedTokenPB signed_authz;
  ASSERT_OK(signer.GenerateAuthnToken(kUser, &signed_authn));
  ASSERT_OK(signer.GenerateAuthzToken(kUser, table_privilege, &signed_authz));
  TokenPB authn_token;
  TokenPB authz_token;
  ASSERT_EQ(VerificationResult::VALID, verifier->VerifyTokenSignature(signed_authn, &authn_token));
  ASSERT_EQ(VerificationResult::VALID, verifier->VerifyTokenSignature(signed_authz, &authz_token));

  // Wait for the authz validity interval to pass and verify its expiration.
  SleepFor(MonoDelta::FromSeconds(1 + kShortValiditySeconds));
  EXPECT_EQ(VerificationResult::VALID, verifier->VerifyTokenSignature(signed_authn, &authn_token));
  EXPECT_EQ(VerificationResult::EXPIRED_TOKEN,
            verifier->VerifyTokenSignature(signed_authz, &authz_token));

  // Wait for the authn validity interval to pass and verify its expiration.
  SleepFor(MonoDelta::FromSeconds(kLongValiditySeconds - kShortValiditySeconds));
  EXPECT_EQ(VerificationResult::EXPIRED_TOKEN,
            verifier->VerifyTokenSignature(signed_authn, &authn_token));
  EXPECT_EQ(VerificationResult::EXPIRED_TOKEN,
            verifier->VerifyTokenSignature(signed_authz, &authz_token));
}

// Test to check the invariant that all tokens signed within a TSK's activity
// interval must be expired by the end of the TSK's validity interval.
TEST_F(TokenTest, TestKeyValidity) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }
  // Note: this test's runtime is roughly the length of a key-validity
  // interval, which is determined by the token validity intervals and the key
  // rotation interval.
  const int kShortValiditySeconds = 2;
  const int kLongValiditySeconds = 6;
  const int kKeyRotationSeconds = 5;
  std::shared_ptr<TokenVerifier> verifier(new TokenVerifier());
  TokenSigner signer(kLongValiditySeconds, kShortValiditySeconds, kKeyRotationSeconds, verifier);
  unique_ptr<TokenSigningPrivateKey> key;
  ASSERT_OK(signer.CheckNeedKey(&key));
  ASSERT_NE(nullptr, key.get());
  ASSERT_OK(signer.AddKey(std::move(key)));

  // First, start a countdown for the first TSK's validity interval. Any token
  // signed during the first TSK's activity interval must be expired once this
  // latch counts down.
  vector<thread> threads;
  CountDownLatch first_tsk_validity_latch(1);
  const double key_validity_seconds = signer.key_validity_seconds_;
  threads.emplace_back([&first_tsk_validity_latch, key_validity_seconds] {
    SleepFor(MonoDelta::FromSeconds(key_validity_seconds));
    LOG(INFO) << Substitute("First TSK's validity interval of $0 secs has finished!",
                            key_validity_seconds);
    first_tsk_validity_latch.CountDown();
  });

  // Set up a second TSK so our threads can rotate TSKs when the time comes.
  while (true) {
    KLOG_EVERY_N_SECS(INFO, 1) << "Waiting for a second key...";
    unique_ptr<TokenSigningPrivateKey> tsk;
    ASSERT_OK(signer.CheckNeedKey(&tsk));
    if (tsk) {
      LOG(INFO) << "Added second key!";
      ASSERT_OK(signer.AddKey(std::move(tsk)));
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(50));
  }

  // Utility lambda to check that the token is expired.
  const auto verify_expired = [&verifier] (const SignedTokenPB& signed_token,
                                           const string& token_type) {
    TokenPB token_pb;
    const auto result = verifier->VerifyTokenSignature(signed_token, &token_pb);
    const auto expire_secs = token_pb.expire_unix_epoch_seconds();
    ASSERT_EQ(VerificationResult::EXPIRED_TOKEN, result)
        << Substitute("$0 token expires at $1, currently: $2",
                      token_type, expire_secs, WallTime_Now());
  };

  // Create a thread that repeatedly signs new authn tokens, returning the
  // final one signed by TSK with seq_num 0. At the end of the key validity
  // period, this token will not be valid.
  vector<SignedTokenPB> tsks(2);
  vector<Status> results(2);
  threads.emplace_back([&] {
    results[0] = SignUntilRotatePast(&signer,
        [&] (SignedTokenPB* signed_token) {
          return signer.GenerateAuthnToken(kUser, signed_token);
        },
        "authn", 0, &tsks[0]);
    first_tsk_validity_latch.Wait();
  });

  // Do the same for authz tokens.
  threads.emplace_back([&] {
    SignedTokenPB last_signed_by_first_tsk;
    results[1] = SignUntilRotatePast(&signer,
        [&] (SignedTokenPB* signed_token) {
          return signer.GenerateAuthzToken(kUser, TablePrivilegePB(), signed_token);
        },
        "authz", 0, &tsks[1]);
    first_tsk_validity_latch.Wait();
  });

  for (auto& t : threads) {
    t.join();
  }
  EXPECT_OK(results[0]);
  EXPECT_OK(results[1]);
  NO_FATALS(verify_expired(tsks[0], "authn"));
  NO_FATALS(verify_expired(tsks[1], "authz"));
}

} // namespace security
} // namespace kudu
