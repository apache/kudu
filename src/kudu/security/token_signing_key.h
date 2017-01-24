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

#include "kudu/gutil/macros.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

// Wrapper around a TokenSigningPublicKeyPB that provides useful functionality
// verify tokens.
//
// Like the underlying OpenSSL types, this can either represent a
// public/private keypair, or a standalone public key. Attempts to
// call Sign() without a private key present will result in an error.
//
// This class is thread-safe.
class TokenSigningPublicKey {
 public:
  explicit TokenSigningPublicKey(const TokenSigningPublicKeyPB& pb);
  ~TokenSigningPublicKey();

  const TokenSigningPublicKeyPB& pb() const {
    return pb_;
  }

  // Verify the signature in a given token.
  // NOTE: this does _not_ verify the expiration.
  bool VerifySignature(const SignedTokenPB& token) const;
 private:
  TokenSigningPublicKeyPB pb_;

  // TODO(PKI): parse the underlying PB DER data into an EVP_PKEY
  // and store that instead.
  DISALLOW_COPY_AND_ASSIGN(TokenSigningPublicKey);
};

// Contains a private key used to sign tokens, along with its sequence
// number and expiration date.
class TokenSigningPrivateKey {
 public:
  TokenSigningPrivateKey(int64_t key_seq_num,
                         int64_t expire_time,
                         std::unique_ptr<PrivateKey> key);
  ~TokenSigningPrivateKey();

  // Sign a token, and store the signature and signing key's sequence number.
  Status Sign(SignedTokenPB* token) const;

  // Export the public-key portion of this signing key.
  void ExportPublicKeyPB(TokenSigningPublicKeyPB* pb);
 private:
  std::unique_ptr<PrivateKey> key_;
  int64_t key_seq_num_;
  int64_t expire_time_;

  DISALLOW_COPY_AND_ASSIGN(TokenSigningPrivateKey);
};


} // namespace security
} // namespace kudu
