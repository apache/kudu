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

#include <map>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/rw_mutex.h"

namespace kudu {
namespace security {

class TokenSigningPublicKey;
class TokenSigningPublicKeyPB;
class SignedTokenPB;
enum class VerificationResult;

// Class responsible for verifying tokens provided to a server.
//
// This class manages a set of public keys, each identified by a sequence
// number. It exposes the latest known sequence number, which can be sent
// to a 'TokenSigner' running on another node. That node can then
// export public keys, which are transferred back to this node and imported
// into the 'TokenVerifier'.
//
// Each signed token also includes the key sequence number that signed it,
// so this class can look up the correct key and verify the token's
// validity and expiration.
//
// Note that this class does not perform any "business logic" around the
// content of a token. It only verifies that the token has a valid signature
// and is not yet expired. Any business rules around authorization or
// authentication are left up to callers.
//
// This class is thread-safe.
class TokenVerifier {
 public:
  TokenVerifier();
  ~TokenVerifier();

  // Return the highest key sequence number known by this instance.
  //
  // If no keys are known, returns -1.
  int64_t GetMaxKnownKeySequenceNumber() const;

  // Import a set of public keys provided by a TokenSigner instance (which might
  // be running on a remote node). If any public keys already exist with matching key
  // sequence numbers, they are replaced by the new keys.
  Status ImportPublicKeys(const std::vector<TokenSigningPublicKeyPB>& public_keys)
    WARN_UNUSED_RESULT;

  // Verify the signature on the given token.
  VerificationResult VerifyTokenSignature(const SignedTokenPB& signed_token) const;

  // TODO(PKI): should expire out old key versions at some point. eg only
  // keep old key versions until their expiration is an hour or two in the past?
  // Not sure where we'll call this from, and likely the "slow leak" isn't
  // critical for first implementation.
  // void ExpireOldKeys();

 private:
  // Lock protecting keys_by_seq_
  mutable RWMutex lock_;
  std::map<int64_t, std::unique_ptr<TokenSigningPublicKey>> keys_by_seq_;

  DISALLOW_COPY_AND_ASSIGN(TokenVerifier);
};

// Result of a token verification.
enum class VerificationResult {
  // The signature is valid and the token is not expired.
  VALID,
  // The token itself is invalid (e.g. missing its signature or data,
  // can't be deserialized, etc).
  INVALID_TOKEN,
  // The signature is invalid (i.e. cryptographically incorrect).
  INVALID_SIGNATURE,
  // The signature is valid, but the token has already expired.
  EXPIRED_TOKEN,
  // The signature is valid, but the signing key is no longer valid.
  EXPIRED_SIGNING_KEY,
  // The signing key used to sign this token is not available.
  UNKNOWN_SIGNING_KEY,
  // The token uses an incompatible feature which isn't supported by this
  // version of the server. We reject the token to give a "default deny"
  // policy.
  INCOMPATIBLE_FEATURE
};


} // namespace security
} // namespace kudu
