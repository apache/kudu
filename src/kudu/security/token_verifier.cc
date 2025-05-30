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

#include <algorithm>
#include <iterator>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"

using std::lock_guard;
using std::shared_lock;
using std::string;
using std::transform;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace security {

TokenVerifier::TokenVerifier() {
}

TokenVerifier::~TokenVerifier() {
}

int64_t TokenVerifier::GetMaxKnownKeySequenceNumber() const {
  shared_lock l(lock_);
  if (keys_by_seq_.empty()) {
    return -1;
  }

  return keys_by_seq_.rbegin()->first;
}

// Import a set of public keys provided by the token signer (typically
// running on another node).
Status TokenVerifier::ImportKeys(const vector<TokenSigningPublicKeyPB>& keys) {
  // Do the construction outside of the lock, to avoid holding the
  // lock while doing lots of allocation.
  vector<unique_ptr<TokenSigningPublicKey>> tsks;
  for (const auto& pb : keys) {
    // Sanity check the key.
    if (!pb.has_rsa_key_der()) {
      return Status::RuntimeError(
          "token-signing public key message must include the signing key");
    }
    if (!pb.has_key_seq_num()) {
      return Status::RuntimeError(
          "token-signing public key message must include the signing key sequence number");
    }
    if (!pb.has_expire_unix_epoch_seconds()) {
      return Status::RuntimeError(
          "token-signing public key message must include an expiration time");
    }
    tsks.emplace_back(new TokenSigningPublicKey { pb });
    RETURN_NOT_OK(tsks.back()->Init());
  }

  lock_guard l(lock_);
  for (auto&& tsk_ptr : tsks) {
    keys_by_seq_.emplace(tsk_ptr->pb().key_seq_num(), std::move(tsk_ptr));
  }
  return Status::OK();
}

std::vector<TokenSigningPublicKeyPB> TokenVerifier::ExportKeys(
    int64_t after_sequence_number) const {
  vector<TokenSigningPublicKeyPB> ret;
  shared_lock l(lock_);
  ret.reserve(keys_by_seq_.size());
  transform(keys_by_seq_.upper_bound(after_sequence_number),
            keys_by_seq_.end(),
            back_inserter(ret),
            [](const KeysMap::value_type& e) { return e.second->pb(); });
  return ret;
}

// Verify the signature on the given token.
TokenVerificationResult TokenVerifier::VerifyTokenSignature(
    const SignedTokenPB& signed_token, TokenPB* token) const {
  if (!signed_token.has_signature() ||
      !signed_token.has_signing_key_seq_num() ||
      !signed_token.has_token_data()) {
    return TokenVerificationResult::INVALID_TOKEN;
  }

  if (!token->ParseFromString(signed_token.token_data()) ||
      !token->has_expire_unix_epoch_seconds()) {
    return TokenVerificationResult::INVALID_TOKEN;
  }

  int64_t now = WallTime_Now();
  if (token->expire_unix_epoch_seconds() < now) {
    return TokenVerificationResult::EXPIRED_TOKEN;
  }

  for (auto flag : token->incompatible_features()) {
    if (!TokenPB::Feature_IsValid(flag)) {
      constexpr const char* const kFormat =
          "received token with unknown feature $0; consider updating server";
      KLOG_EVERY_N_SECS(WARNING, 60) << Substitute(
          kFormat, static_cast<uint32_t>(flag)) << THROTTLE_MSG;
      return TokenVerificationResult::INCOMPATIBLE_FEATURE;
    }
  }

  {
    shared_lock l(lock_);
    auto* tsk = FindPointeeOrNull(keys_by_seq_, signed_token.signing_key_seq_num());
    if (!tsk) {
      return TokenVerificationResult::UNKNOWN_SIGNING_KEY;
    }
    if (tsk->pb().expire_unix_epoch_seconds() < now) {
      return TokenVerificationResult::EXPIRED_SIGNING_KEY;
    }
    if (!tsk->VerifySignature(signed_token)) {
      return TokenVerificationResult::INVALID_SIGNATURE;
    }
  }

  return TokenVerificationResult::VALID;
}

const char* TokenVerificationResultToString(TokenVerificationResult r) {
  switch (r) {
    case security::TokenVerificationResult::VALID:
      return "valid";
    case security::TokenVerificationResult::INVALID_TOKEN:
      return "invalid token";
    case security::TokenVerificationResult::INVALID_SIGNATURE:
      return "invalid token signature";
    case security::TokenVerificationResult::EXPIRED_TOKEN:
      return "token expired";
    case security::TokenVerificationResult::EXPIRED_SIGNING_KEY:
      return "token signing key expired";
    case security::TokenVerificationResult::UNKNOWN_SIGNING_KEY:
      return "token signed with unknown key";
    case security::TokenVerificationResult::INCOMPATIBLE_FEATURE:
      return "token uses incompatible feature";
    default:
      LOG(FATAL) << "unexpected VerificationResult value: "
                 << static_cast<int>(r);
  }
}

} // namespace security
} // namespace kudu
