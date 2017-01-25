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

#include "kudu/gutil/macros.h"

#include <map>
#include <memory>
#include <vector>

#include "kudu/util/rw_mutex.h"

namespace kudu {
namespace security {

class SignedTokenPB;
class TokenSigningPrivateKey;
class TokenSigningPublicKeyPB;

// Class responsible for managing Token Signing Keys (TSKs) and signing tokens.
//
// This class manages a set of private TSKs, each identified by a sequence number.
// Callers can export their public TSK counterparts, optionally transfer them
// to another node, and then import them into a TokenVerifier.
//
// The class provides the ability to rotate the current TSK. This generates a new
// key pair and assigns it a sequence number. Note that, when signing tokens,
// the most recent key is not used. Rather, the second-most-recent key is used.
// This ensures that there is plenty of time to transmit the public key for the
// new TSK to all TokenVerifiers (eg on other servers, via heartbeats), before
// the new key enters usage.
//
// On a fresh instance, with only one key, there is no "second most recent"
// key. Thus, we fall back to signing tokens with the one available key.
//
//
// Key rotation schedules and validity periods
// ===========================================
// The TokenSigner does not automatically handle the rotation of keys.
// Rotation must be performed by the caller using the 'RotateSigningKey()'
// method. Typically, key rotation is performed much more frequently than
// the validity period of the key, so that at any given point in time
// there are several valid keys.
//
// For example, consider a validity period of 4 days and a rotation interval of
// 1 day:
//
// Day      1    2    3    4    5    6    7    8
// ------------------------------------------------
// Key 1:   <AAAAAAAAA==========>
// Key 2:        <====AAAAA=========>
// Key 3:             <====AAAAA========>
// Key 4:                  <====AAAAA==========>
//                               .............
// 'A' indicates the 'Originator Usage Period' (the period in which the key
// is being used to sign tokens).
//
// '<...>' indicates the 'Recipient Usage Period' (the period in which the
// verifier will consider the key valid).
//
// When configuring the rotation and validity, consider the following constraint:
//
//   max_token_validity < tsk_validity_period - 2 * tsk_rotation_interval
//
// In the example above, this means that no token may be issued with a validity
// longer than 2 days, without risking that the signing key would expire before
// the token.
//
// TODO(PKI): should we try to enforce this constraint in code?
//
// NOTE: one other result of the above is that the first key (Key 1) is actually
// active for longer than the rest. This has some potential security implications,
// so it's worth considering rolling twice at startup.
//
// This class is thread-safe.
class TokenSigner {
 public:
  // Create a new TokenSigner. It will start assigning key sequence numbers
  // at 'next_seq_num'.
  //
  // NOTE: this does not initialize an initial key. Call 'RotateSigningKey()'
  // to initialize the first key.
  explicit TokenSigner(int64_t next_seq_num);
  ~TokenSigner();

  // Sign the given token using the current TSK.
  Status SignToken(SignedTokenPB* token) const;

  // Returns the set of valid public keys with sequence numbers greater
  // than 'after_sequence_number'.
  std::vector<TokenSigningPublicKeyPB> GetTokenSigningPublicKeys(
      int64_t after_sequence_number) const;

  // Rotate to a new token-signing key.
  //
  // See class documentation for more information.
  Status RotateSigningKey();

 private:
  // Protects following fields.
  mutable RWMutex lock_;
  std::map<int64_t, std::unique_ptr<TokenSigningPrivateKey>> keys_by_seq_;

  int64_t next_seq_num_;

  DISALLOW_COPY_AND_ASSIGN(TokenSigner);
};

} // namespace security
} // namespace kudu
