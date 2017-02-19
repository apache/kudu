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

#include "kudu/security/token_signer.h"

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/gutil/walltime.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

DEFINE_int64(authn_token_validity_seconds, 120,
             "Period of time for which an issued authentication token is valid.");
// TODO(PKI): docs for what actual effect this has, given we don't support
// token renewal.
// TODO(PKI): this is set extremely low, so that we don't forget to come back to
// this and add rolling and refetching code.
TAG_FLAG(authn_token_validity_seconds, experimental);

DEFINE_int32(tsk_num_rsa_bits, 2048,
             "Number of bits used for token signing keys");
// TODO(PKI) is 1024 enough for TSKs since they rotate frequently?
// maybe it would verify faster?

using std::lock_guard;
using std::map;
using std::shared_ptr;
using std::string;
using std::unique_lock;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace security {

TokenSigner::TokenSigner(int64_t key_validity_seconds,
                         int64_t key_rotation_seconds,
                         shared_ptr<TokenVerifier> verifier)
    : verifier_(std::move(verifier)),
      key_validity_seconds_(key_validity_seconds),
      key_rotation_seconds_(key_rotation_seconds),
      next_key_seq_num_(0) {
  CHECK(verifier_);
}

TokenSigner::~TokenSigner() {
}

Status TokenSigner::ImportKeys(const vector<TokenSigningPrivateKeyPB>& keys) {
  lock_guard<RWMutex> l(lock_);

  const int64_t now = WallTime_Now();
  map<int64_t, unique_ptr<TokenSigningPrivateKey>> tsk_by_seq;
  vector<TokenSigningPublicKeyPB> public_keys_pb;
  public_keys_pb.reserve(keys.size());
  for (const auto& key : keys) {
    // Check the input for consistency.
    CHECK(key.has_key_seq_num());
    CHECK(key.has_expire_unix_epoch_seconds());
    CHECK(key.has_rsa_key_der());

    const int64_t key_seq_num = key.key_seq_num();
    unique_ptr<TokenSigningPrivateKey> tsk(new TokenSigningPrivateKey(key));

    // Advance the key sequence number, if needed.
    next_key_seq_num_ = std::max(next_key_seq_num_, key_seq_num + 1);
    const int64_t key_expire_time = tsk->expire_time();
    if (key_expire_time <= now) {
      // Do nothing else with an expired TSK.
      continue;
    }

    // Need the public part of the key for the TokenVerifier.
    {
      TokenSigningPublicKeyPB public_key_pb;
      tsk->ExportPublicKeyPB(&public_key_pb);
      public_keys_pb.emplace_back(std::move(public_key_pb));
    }

    tsk_by_seq[key_seq_num] = std::move(tsk);
    if (tsk_by_seq.size() > 2) {
      tsk_by_seq.erase(tsk_by_seq.begin());
    }
  }
  // Register the public parts of the imported keys with the TokenVerifier.
  RETURN_NOT_OK(verifier_->ImportKeys(public_keys_pb));

  // Use two most recent keys known so far (in terms of sequence numbers)
  // for token signing.
  for (auto& e : tsk_deque_) {
    const int64_t seq_num = e->key_seq_num();
    tsk_by_seq[seq_num] = std::move(e);
  }
  tsk_deque_.clear();
  for (auto& e : tsk_by_seq) {
    tsk_deque_.emplace_back(std::move(e.second));
  }
  while (tsk_deque_.size() > 2) {
    tsk_deque_.pop_front();
  }

  return Status::OK();
}

Status TokenSigner::GenerateAuthnToken(string username,
                                       SignedTokenPB* signed_token) const {
  TokenPB token;
  token.set_expire_unix_epoch_seconds(
      WallTime_Now() + FLAGS_authn_token_validity_seconds);
  AuthnTokenPB* authn = token.mutable_authn();
  authn->mutable_username()->assign(std::move(username));

  SignedTokenPB ret;
  if (!token.SerializeToString(ret.mutable_token_data())) {
    return Status::RuntimeError("could not serialize authn token");
  }

  RETURN_NOT_OK(SignToken(&ret));
  signed_token->Swap(&ret);
  return Status::OK();
}

Status TokenSigner::SignToken(SignedTokenPB* token) const {
  CHECK(token);
  shared_lock<RWMutex> l(lock_);
  if (tsk_deque_.empty()) {
    return Status::IllegalState("no token signing key");
  }
  TokenSigningPrivateKey* key = tsk_deque_.front().get();
  RETURN_NOT_OK_PREPEND(key->Sign(token), "could not sign authn token");
  return Status::OK();
}

Status TokenSigner::CheckNeedKey(unique_ptr<TokenSigningPrivateKey>* tsk) const {
  CHECK(tsk);
  const int64_t now = WallTime_Now();

  unique_lock<RWMutex> l(lock_);
  if (tsk_deque_.empty()) {
    // No active key: need a new one.
    const int64 key_seq_num = next_key_seq_num_++;
    const int64 key_expiration = now + key_validity_seconds_;
    // Generation of cryptographically strong key takes many CPU cycles;
    // do not want to block other parallel activity.
    l.unlock();
    return GenerateSigningKey(key_seq_num, key_expiration, tsk);
  }

  if (tsk_deque_.size() >= 2) {
    // It does not make much sense to keep more than two keys in the queue.
    // It's enough to have just one active key and next key ready to be
    // activated when it's time to do so.  However, it does not mean the
    // process of key refreshement is about to stop once there are two keys
    // in the queue: the TryRotate() method (which should be called periodically
    // along with CheckNeedKey()/AddKey() pair) will eventually pop the
    // current key out of the keys queue once the key enters its inactive phase.
    tsk->reset();
    return Status::OK();
  }

  // The currently active key is in the front of the queue.
  const auto* key = tsk_deque_.front().get();

  // Check if it's time to generate a new token signing key.
  //
  //   <-----AAAAA===========>
  //         ^
  //        now
  //
  const auto key_creation_time = key->expire_time() - key_validity_seconds_;
  if (key_creation_time + key_rotation_seconds_ <= now) {
    // It's time to create and start propagating next key.
    const int64 key_seq_num = next_key_seq_num_++;
    const int64 key_expiration = now + key_validity_seconds_;
    // Generation of cryptographically strong key takes many CPU cycles:
    // do not want to block other parallel activity.
    l.unlock();
    return GenerateSigningKey(key_seq_num, key_expiration, tsk);
  }

  // It's not yet time to generate a new key.
  tsk->reset();
  return Status::OK();
}

Status TokenSigner::AddKey(unique_ptr<TokenSigningPrivateKey> tsk) {
  CHECK(tsk);
  const int64_t seq_num = tsk->key_seq_num();
  if (seq_num < 0) {
    return Status::InvalidArgument("invalid key sequence number");
  }
  if (tsk->expire_time() <= WallTime_Now()) {
    return Status::InvalidArgument("key has already expired");
  }

  lock_guard<RWMutex> l(lock_);
  next_key_seq_num_ = std::max(next_key_seq_num_, seq_num + 1);
  // Register the public part of the key in TokenVerifier first.
  TokenSigningPublicKeyPB public_key_pb;
  tsk->ExportPublicKeyPB(&public_key_pb);
  RETURN_NOT_OK(verifier_->ImportKeys({public_key_pb}));

  tsk_deque_.emplace_back(std::move(tsk));

  return Status::OK();
}

Status TokenSigner::TryRotateKey(bool* has_rotated) {
  lock_guard<RWMutex> l(lock_);
  if (has_rotated) {
    *has_rotated = false;
  }
  if (tsk_deque_.size() < 2) {
    // No next key to rotate to.
    return Status::OK();
  }

  const auto* key = tsk_deque_.front().get();
  // Check if it's time to switch to next key. The key propagation interval
  // is equal to the key rotation interval.
  //
  // current active key   <-----AAAAA===========>
  //           next key        <-----AAAAA===========>
  //                                 ^
  //                                now
  //
  const auto key_creation_time = key->expire_time() - key_validity_seconds_;
  if (key_creation_time + 2 * key_rotation_seconds_ <= WallTime_Now()) {
    tsk_deque_.pop_front();
    if (has_rotated) {
      *has_rotated = true;
    }
  }
  return Status::OK();
}

Status TokenSigner::GenerateSigningKey(int64_t key_seq_num,
                                       int64_t key_expiration,
                                       unique_ptr<TokenSigningPrivateKey>* tsk) {
  unique_ptr<PrivateKey> key(new PrivateKey());
  RETURN_NOT_OK_PREPEND(
      GeneratePrivateKey(FLAGS_tsk_num_rsa_bits, key.get()),
      "could not generate new RSA token-signing key");
  tsk->reset(new TokenSigningPrivateKey(key_seq_num,
                                        key_expiration,
                                        std::move(key)));
  return Status::OK();
}

} // namespace security
} // namespace kudu
