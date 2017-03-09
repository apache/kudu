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
#pragma once

#include <memory>
#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

// Wrapper around a TokenSigningPublicKeyPB that provides useful functionality
// to verify tokens.
//
// This represents a standalone public key useful for token verification.
class TokenSigningPublicKey {
 public:
  explicit TokenSigningPublicKey(const TokenSigningPublicKeyPB& pb);
  ~TokenSigningPublicKey();

  const TokenSigningPublicKeyPB& pb() const {
    return pb_;
  }

  // Initialize the object. Should be called only once.
  Status Init() WARN_UNUSED_RESULT;

  // Verify the signature in a given token.
  // This method is thread-safe.
  // NOTE: this does _not_ verify the expiration.
  bool VerifySignature(const SignedTokenPB& token) const;

 private:
  const TokenSigningPublicKeyPB pb_;
  // The 'key_' member is a parsed version of rsa_key_der() from pb_.
  // In essence, the 'key_' is a public key for message signature verification.
  PublicKey key_;

  DISALLOW_COPY_AND_ASSIGN(TokenSigningPublicKey);
};

// Contains a private key used to sign tokens, along with its sequence
// number and expiration date.
class TokenSigningPrivateKey {
 public:
  explicit TokenSigningPrivateKey(const TokenSigningPrivateKeyPB& pb);
  TokenSigningPrivateKey(int64_t key_seq_num,
                         int64_t expire_time,
                         std::unique_ptr<PrivateKey> key);
  ~TokenSigningPrivateKey();

  // Sign a token, and store the signature and signing key's sequence number.
  Status Sign(SignedTokenPB* token) const WARN_UNUSED_RESULT;

  // Export data into corresponding PB structure.
  void ExportPB(TokenSigningPrivateKeyPB* pb) const;

  // Export the public-key portion of this signing key.
  void ExportPublicKeyPB(TokenSigningPublicKeyPB* pb) const;

  int64_t key_seq_num() const { return key_seq_num_; }
  int64_t expire_time() const { return expire_time_; }

 private:
  FRIEND_TEST(TokenTest, TestAddKeyConstraints);

  std::unique_ptr<PrivateKey> key_;
  // The 'private_key_der_' is a serialized 'key_' in DER format: just a cache.
  std::string private_key_der_;
  // The 'public_key_der_' is serialized public part of 'key_' in DER format;
  // just a cache.
  std::string public_key_der_;

  int64_t key_seq_num_;
  int64_t expire_time_;

  DISALLOW_COPY_AND_ASSIGN(TokenSigningPrivateKey);
};

} // namespace security
} // namespace kudu
