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

#include "kudu/security/token_verifier.h"

#include <mutex>
#include <string>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/util/locks.h"

using std::lock_guard;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace security {

TokenVerifier::TokenVerifier() {
}

TokenVerifier::~TokenVerifier() {
}

int64_t TokenVerifier::GetMaxKnownKeySequenceNumber() const {
  shared_lock<RWMutex> l(lock_);
  if (keys_by_seq_.empty()) {
    return -1;
  }

  return keys_by_seq_.rbegin()->first;
}

// Import a set of public keys provided by the token signer (typically
// running on another node).
void TokenVerifier::ImportPublicKeys(const vector<TokenSigningPublicKeyPB>& public_keys) {
  // Do the copy construction outside of the lock, to avoid holding the
  // lock while doing lots of allocation.
  vector<unique_ptr<TokenSigningPublicKey>> tsks;
  for (const auto& pb : public_keys) {
    // Sanity check the key.
    CHECK(pb.has_rsa_key_der());
    CHECK(pb.has_key_seq_num());
    CHECK(pb.has_expire_unix_epoch_seconds());
    tsks.emplace_back(new TokenSigningPublicKey { pb });
  }

  lock_guard<RWMutex> l(lock_);
  for (auto&& tsk_ptr : tsks) {
    keys_by_seq_.emplace(tsk_ptr->pb().key_seq_num(), std::move(tsk_ptr));
  }
}

// Verify the signature on the given token.
VerificationResult TokenVerifier::VerifyTokenSignature(
    const SignedTokenPB& signed_token) const {
  if (!signed_token.has_signature() ||
      !signed_token.has_signing_key_seq_num() ||
      !signed_token.has_token_data()) {
    return VerificationResult::INVALID_TOKEN;
  }

  // TODO(perf): should we return the deserialized TokenPB here
  // since callers are probably going to need it, anyway?
  TokenPB token;
  if (!token.ParseFromString(signed_token.token_data()) ||
      !token.has_expire_unix_epoch_seconds()) {
    return VerificationResult::INVALID_TOKEN;
  }

  int64_t now = WallTime_Now();
  if (token.expire_unix_epoch_seconds() < now) {
    return VerificationResult::EXPIRED_TOKEN;
  }

  for (auto flag : token.incompatible_features()) {
    if (!TokenPB::Feature_IsValid(flag)) {
      return VerificationResult::INCOMPATIBLE_FEATURE;
    }
  }

  {
    shared_lock<RWMutex> l(lock_);
    auto* tsk = FindPointeeOrNull(keys_by_seq_, signed_token.signing_key_seq_num());
    if (!tsk) {
      return VerificationResult::UNKNOWN_SIGNING_KEY;
    }
    if (tsk->pb().expire_unix_epoch_seconds() < now) {
      return VerificationResult::EXPIRED_SIGNING_KEY;
    }
    if (!tsk->VerifySignature(signed_token)) {
      return VerificationResult::INVALID_SIGNATURE;
    }
  }

  return VerificationResult::VALID;
}

} // namespace security
} // namespace kudu

