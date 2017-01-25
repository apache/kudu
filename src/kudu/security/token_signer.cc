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

#include <map>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "kudu/gutil/walltime.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"

DEFINE_int32(token_signing_key_num_rsa_bits, 2048,
             "number of bits used for token signing keys");
// TODO(PKI) is 1024 enough for TSKs since they rotate frequently?
// maybe it would verify faster?
DEFINE_int64(token_signing_key_validity_seconds, 60 * 60 * 24 * 7,
             "number of seconds that a token signing key is valid for");
// TODO(PKI): add flag tags

using std::lock_guard;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace security {

TokenSigner::TokenSigner(int64_t next_seq_num)
    : next_seq_num_(next_seq_num) {
}

TokenSigner::~TokenSigner() {
}

Status TokenSigner::RotateSigningKey() {
  Key key;
  RETURN_NOT_OK_PREPEND(GeneratePrivateKey(FLAGS_token_signing_key_num_rsa_bits, &key),
                        "could not generate new RSA token-signing key");
  int64_t expire = WallTime_Now() + FLAGS_token_signing_key_validity_seconds;
  lock_guard<RWMutex> l(lock_);
  int64_t seq = next_seq_num_++;
  unique_ptr<TokenSigningPrivateKey> new_tsk(
      new TokenSigningPrivateKey(seq, expire, std::move(key)));
  keys_by_seq_[seq] = std::move(new_tsk);
  return Status::OK();
}

Status TokenSigner::SignToken(SignedTokenPB* token) const {
  CHECK(token);
  shared_lock<RWMutex> l(lock_);
  if (keys_by_seq_.empty()) {
    return Status::IllegalState("must generate a key before signing");
  }
  // If there is more than one key available, we use the second-latest key,
  // since the latest one may not have yet propagated to other servers, etc.
  auto it = keys_by_seq_.end();
  --it;
  if (it != keys_by_seq_.begin()) {
    --it;
  }
  const auto& tsk = it->second;
  return tsk->Sign(token);
}

std::vector<TokenSigningPublicKeyPB> TokenSigner::GetTokenSigningPublicKeys(
    int64_t after_sequence_number) const {
  vector<TokenSigningPublicKeyPB> ret;
  shared_lock<RWMutex> l(lock_);
  for (auto it = keys_by_seq_.upper_bound(after_sequence_number);
       it != keys_by_seq_.end();
       ++it) {
    ret.emplace_back();
    it->second->ExportPublicKeyPB(&ret.back());
  }
  return ret;
}


} // namespace security
} // namespace kudu
