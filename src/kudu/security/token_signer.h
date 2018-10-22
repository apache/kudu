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

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/rw_mutex.h"

namespace kudu {
class Status;

namespace security {
class SignedTokenPB;
class TablePrivilegePB;
class TokenSigningPrivateKey;
class TokenSigningPrivateKeyPB;
class TokenVerifier;

// Class responsible for managing Token Signing Keys (TSKs) and signing tokens.
//
// This class manages a set of private TSKs, each identified by a sequence
// number. Callers can export their public TSK counterparts via the included
// TokenVerifier, optionally transfer them to another node, and then import
// them into a TokenVerifier.
//
// The class provides the ability to check whether it's time go generate and
// activate a new key. Every generated private/public key pair is assigned a
// sequence number. Note that, when signing tokens, the most recent key
// (a.k.a. next key) is not used. Rather, the second-most-recent key, if exists,
// is used. This ensures that there is plenty of time to transmit the public
// part of the new TSK to all TokenVerifiers (e.g. on other servers via
// heartbeats or by other means), before the new key enters usage.
//
// On a fresh instance, with only one key, there is no "second most recent"
// key. Thus, we fall back to signing tokens with the only available key.
//
// Key rotation schedules and validity periods
// ===========================================
// The TokenSigner does not automatically handle the rotation of keys.
// Rotation must be performed by an external caller using the combination of
// 'CheckNeedKey()/AddKey()' and 'TryRotateKey()' methods. Typically,
// key rotation is performed more frequently than the validity period
// of the key, so that at any given point in time there are several valid keys.
//
// Below is the life cycle of a TSK (token signing key):
//
//      <---AAAAA===============>
//      ^                       ^
// creation time          expiration time
//
// Prior to the creation time the TSK does not exist in the system.
//
// '-' propagation interval
//       The TSK is already created but not yet used to sign tokens. However,
//       its public part is already being sent to the components which
//       may be involved in validation of tokens signed by the key. This is
//       exactly 'tsk_propagation_interval' below.
//
// 'A' activity interval
//       The TSK is used to sign tokens. It's assumed that the components which
//       are involved in token verification have already received the
//       corresponding public part of the TSK.
//
// '=' inactivity interval
//       The TSK is no longer used to sign tokens. However, its public part is
//       still sent to other components and can be used to validate token
//       signatures.
//
// Shortly after the TSK's expiration the token signing components stop
// propagating its public part.
//
// The TSK is considered valid from its creation time until its expiration time.
//
// NOTE: The very first key created on the system bootstrap does not have
//       propagation interval -- it turns active immediately.
//
// NOTE: One other result of the above is that the first key (Key 1) is actually
//       active for longer than the rest. This has some potential security
//       implications, so it's worth considering rolling twice at startup.
//
// For example, consider the following configuration for token signing keys:
//   key validity period:  4 days
//   rotation interval:    1 day
//   propagation interval: 1 day
//
// Day      1    2    3    4    5    6    7    8
// ------------------------------------------------
// Key 1:   <AAAAAAAAA==========>
// Key 2:        <----AAAAA==========>
// Key 3:             <----AAAAA==========>
// Key 4:                  <----AAAAA==========>
//                              ...............
// authn token:                     <**********>
//
// 'A' indicates 'Activity Interval', i.e. the period during which the key is
// being used to sign tokens. In cryptographic terms, this is the 'Originator
// Usage Period'. Note that the Activity Interval is identically the rotation
// interval -- a key is active for some amount of time, after which, we rotate.
//
// '<...>' in cryptographic terms, indicates the 'Recipient Usage Period': the
// period during which the public part of a key is used to sign tokens, and
// verifiers will consider tokens signed by the given key as valid. At the end
// of this period, a verifier should consider tokens signed by the given TSK
// invalid and stop accepting them, even if the token signature is correct. The
// start of the period is not crucial to the validity of a token, so we don't
// perform verification for it.
//
// '<***>' indicates the validity interval for an authn token.
//
// When configuring key rotation and token validity interval durations,
// consider the following constraint:
//
// Eq 1.
//   max_token_validity = tsk_validity_period -
//       (tsk_propagation_interval + tsk_rotation_interval)
//
// Note how if the validity period for a token created at the end of the
// Activity Interval were to extend any farther than the above
// 'max_token_validity', it would be considered valid beyond the end of the
// 'tsk_validity_period', which would break the constraint that the token only
// be valid within the TSK's validity period.
//
// The idea is that the token validity interval should be contained in the
// corresponding TSK's validity interval. If the TSK is already expired at the
// time of token verification, the token is considered invalid and the
// verification of the token fails. This means that no token may be issued with
// a validity period longer than or equal to TSK inactivity interval, without
// risking that the signing/verification key would expire before the token
// itself. The edge case is demonstrated by the following scenario:
//
// * A TSK is issued at 00:00:00 on day 4.
// * An authn token generated and signed by current/active TSK at 23:59:59 on
//   day 5. That's at the very end of the TSK's activity interval.
// * From the diagram above it's clear that if the authn token validity
//   interval were set to something longer than TSK inactivity interval
//   (which is 2 days with for the specified parameters), an attempt to verify
//   the token at 00:00:00 on day 8 or later would fail due to the expiration
//   the corresponding TSK.
//
// NOTE: Current implementation of TokenSigner assumes the propagation
//       interval is equal to the rotation interval.
//
// Typical usage pattern:
//
//    TokenSigner ts(...);
//    // Load existing TSKs from the system table.
//    ...
//    RETURN_NOT_OK(ts.ImportKeys(...));
//
//    // Check that there is a valid TSK to sign keys.
//    {
//      unique_ptr<TokenSigningPrivateKey> key;
//      RETURN_NOT_OK(ts.CheckNeedKey(&key));
//      if (key) {
//        // Store the newly generated key into the system table.
//        ...
//
//        // Add the key into the queue of the TokenSigner.
//        RETURN_NOT_OK(ts.AddKey(std::move(key)));
//      }
//    }
//    // Check and switch to the next key, if it's time.
//    RETURN_NOT_OK(ts.TryRotateKey());
//
//    ...
//    // Time to time (but much more often than TSK validity/rotation interval)
//    // call the 'CheckNeedKey()/AddKey() followed by TryRotateKey()' sequence.
//    // It's a good idea to dedicate a separate periodic task for that.
//    ...
//
class TokenSigner {
 public:
  // The token validity and 'key_rotation_seconds' parameters define the
  // schedule of TSK rotation. See the class comment above for details.
  //
  // Any newly imported or generated keys are automatically imported into the
  // passed 'verifier'. If no verifier passed as a parameter, TokenSigner
  // creates one on its own. In either case, it's possible to access
  // the embedded TokenVerifier instance using the verifier() accessor.
  //
  // The 'authn_token_validity_seconds' and 'authz_token_validity_seconds'
  // parameters are used to specify validity intervals for the generated tokens
  // and with 'key_rotation_seconds' it defines validity interval of the newly
  // generated TSK:
  //
  // Eq 2.
  //   key_validity =
  //      2 * key_rotation + max(authn_token_validity, authz_token_validity)
  //
  // This selects the 'max_token_validity' in Eq 1 as the higher of the authn
  // and authz token validity intervals, and based on that, calculates the
  // effective TSK validity period based on the provided rotation interval.
  // See the above class comment for details.
  TokenSigner(int64_t authn_token_validity_seconds,
              int64_t authz_token_validity_seconds,
              int64_t key_rotation_seconds,
              std::shared_ptr<TokenVerifier> verifier = nullptr);
  ~TokenSigner();

  // Import token signing keys in PB format, notifying TokenVerifier
  // and updating internal key sequence number. This method can be called
  // multiple times. Depending on the input keys and current time, the instance
  // might not be ready to sign keys right after calling ImportKeys(),
  // so additional cycle of CheckNeedKey/AddKey might be needed.
  //
  // See the class comment above for more information about the intended usage.
  Status ImportKeys(const std::vector<TokenSigningPrivateKeyPB>& keys)
      WARN_UNUSED_RESULT;

  // Check whether it's time to generate and add a new key. If so, the new key
  // is generated and output into the 'tsk' parameter so it's possible to
  // examine and process the key as needed (e.g. store it). After that, use the
  // AddKey() method to actually add the key into the TokenSigner's key queue.
  //
  // Every non-null key returned by this method has key sequence number.
  // It's not a problem to call this method multiple times but call the AddKey()
  // method only once, effectively discarding all the generated keys except for
  // the key passed to the AddKey() call as a parameter. The key sequence number
  // always increments with every newly added key (i.e. every successful call of
  // the AddKey() method). The result key number sequence would not contain
  // any 'holes'.
  //
  // In other words, sequence of calls like
  //
  //   CheckNeedKey(k);
  //   CheckNeedKey(k);
  //   ...
  //   CheckNeedKey(k);
  //   AddKey(k);
  //
  // would increase the key sequence number just by 1. Due to that fact, the
  // following sequence of calls to CheckNeedKey()/AddKey() would work fine:
  //
  //   CheckNeedKey(k0);
  //   AddKey(k0);
  //   CheckNeedKey(k1);
  //   AddKey(k1);
  //
  // but the sequence below would fail at AddKey(k1):
  //
  //   CheckNeedKey(k0);
  //   CheckNeedKey(k1);
  //   AddKey(k0);
  //   AddKey(k1);
  //
  // See the class comment above for more information about the intended usage.
  Status CheckNeedKey(std::unique_ptr<TokenSigningPrivateKey>* tsk) const
      WARN_UNUSED_RESULT;

  // Add the new key into the token signing keys queue. Call TryRotateKey()
  // to make the newly added key active when it's time.
  //
  // See the class comment above for more information about the intended usage.
  Status AddKey(std::unique_ptr<TokenSigningPrivateKey> tsk) WARN_UNUSED_RESULT;

  // Check whether it's possible and it's time to switch to next signing key
  // from the token signing keys queue. A key can be added using the
  // CheckNeedKey()/AddKey() method pair. If there is next key to switch to
  // and it's time to do so, the methods switches to the next key and reports
  // on that via the 'has_rotated' parameter.
  // The intended use case is to call TryRotateKey() periodically.
  //
  // See the class comment above for more information about the intended usage.
  Status TryRotateKey(bool* has_rotated = nullptr) WARN_UNUSED_RESULT;

  // Populates 'signed_token' with a signed authorization token with the given
  // 'username' and table privilege. Returns an error if 'username' is empty,
  // or if the created authn token could not be serialized for some reason.
  Status GenerateAuthzToken(std::string username,
                            TablePrivilegePB privilege,
                            SignedTokenPB* signed_token) const WARN_UNUSED_RESULT;

  // Populates 'signed_token' with a signed authentication token with the given
  // 'username'. Returns an error if 'username' is empty, or if the created
  // authz token could not be serialized for some reason.
  Status GenerateAuthnToken(std::string username,
                            SignedTokenPB* signed_token) const WARN_UNUSED_RESULT;

  Status SignToken(SignedTokenPB* token) const WARN_UNUSED_RESULT;

  const TokenVerifier& verifier() const { return *verifier_; }

  // Check if the current TSK is valid: return 'true' if current key is present
  // and it's not yet expired, return 'false' otherwise.
  bool IsCurrentKeyValid() const;

 private:
  FRIEND_TEST(TokenTest, TestEndToEnd_InvalidCases);
  FRIEND_TEST(TokenTest, TestIsCurrentKeyValid);
  FRIEND_TEST(TokenTest, TestTokenSignerAddKeyAfterImport);
  FRIEND_TEST(TokenTest, TestKeyValidity);

  static Status GenerateSigningKey(int64_t key_seq_num,
                                   int64_t key_expiration,
                                   std::unique_ptr<TokenSigningPrivateKey>* tsk) WARN_UNUSED_RESULT;

  std::shared_ptr<TokenVerifier> verifier_;

  // Validity intervals for the generated tokens.
  const int64_t authn_token_validity_seconds_;
  const int64_t authz_token_validity_seconds_;

  // TSK rotation interval: number of seconds between consecutive activations
  // of new token signing keys. Note that in current implementation it defines
  // the propagation interval as well, i.e. the TSK propagation interval is
  // equal to the TSK rotation interval.
  const int64_t key_rotation_seconds_;

  // Period of validity for newly created token signing keys. In other words,
  // the expiration time for a new key is set to (now + key_validity_seconds_).
  const int64_t key_validity_seconds_;

  // Protects next_seq_num_ and tsk_deque_ members.
  mutable RWMutex lock_;

  // The sequence number of the last generated/imported key.
  int64_t last_key_seq_num_;

  // The currently active key is in the front of the queue,
  // the newly added ones are pushed into back of the queue.
  std::deque<std::unique_ptr<TokenSigningPrivateKey>> tsk_deque_;

  DISALLOW_COPY_AND_ASSIGN(TokenSigner);
};

} // namespace security
} // namespace kudu
