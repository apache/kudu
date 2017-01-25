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

#include "kudu/security/token_signing_key.h"

#include <glog/logging.h>

#include "kudu/security/token.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

TokenSigningPublicKey::TokenSigningPublicKey(const TokenSigningPublicKeyPB& pb)
    : pb_(pb) {
}

TokenSigningPublicKey::~TokenSigningPublicKey() {
}

bool TokenSigningPublicKey::VerifySignature(const SignedTokenPB& token) const {
  CHECK(pb_.has_rsa_key_der());
  // TODO(PKI): add real signatures!
  return token.signature() == "signed:" + token.token_data();
}

TokenSigningPrivateKey::TokenSigningPrivateKey(
    int64_t key_seq_num, int64_t expire_time, Key key)
    : key_(std::move(key)),
      key_seq_num_(key_seq_num),
      expire_time_(expire_time) {
}

TokenSigningPrivateKey::~TokenSigningPrivateKey() {
}

Status TokenSigningPrivateKey::Sign(SignedTokenPB* token) const {
  token->set_signature("signed:" + token->token_data());
  token->set_signing_key_seq_num(key_seq_num_);
  return Status::OK();
}

void TokenSigningPrivateKey::ExportPublicKeyPB(TokenSigningPublicKeyPB* pb) {
  pb->Clear();
  // TODO(PKI): implement me! depends on https://gerrit.cloudera.org/#/c/5783/
  // though we probably would want to export this once and cache it in DER
  // format.
  pb->set_key_seq_num(key_seq_num_);
  pb->set_rsa_key_der("TODO");
  pb->set_expire_unix_epoch_seconds(expire_time_);
}

} // namespace security
} // namespace kudu
