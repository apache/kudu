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

#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/status.h"

using std::unique_ptr;
using std::string;

namespace kudu {
namespace security {

TokenSigningPublicKey::TokenSigningPublicKey(TokenSigningPublicKeyPB pb)
    : pb_(std::move(pb)) {
}

TokenSigningPublicKey::~TokenSigningPublicKey() {
}

Status TokenSigningPublicKey::Init() {
  // This should be called only once.
  CHECK(!key_.GetRawData());
  if (!pb_.has_rsa_key_der()) {
    return Status::RuntimeError("no key for token signing helper");
  }
  RETURN_NOT_OK(key_.FromString(pb_.rsa_key_der(), DataFormat::DER));
  return Status::OK();
}

bool TokenSigningPublicKey::VerifySignature(const SignedTokenPB& token) const {
  return key_.VerifySignature(DigestType::SHA256,
      token.token_data(), token.signature()).ok();
}

TokenSigningPrivateKey::TokenSigningPrivateKey(
    const TokenSigningPrivateKeyPB& pb)
    : key_(new PrivateKey) {
  CHECK_OK(key_->FromString(pb.rsa_key_der(), DataFormat::DER));
  private_key_der_ = pb.rsa_key_der();
  key_seq_num_ = pb.key_seq_num();
  expire_time_ = pb.expire_unix_epoch_seconds();

  PublicKey public_key;
  CHECK_OK(key_->GetPublicKey(&public_key));
  CHECK_OK(public_key.ToString(&public_key_der_, DataFormat::DER));
}

TokenSigningPrivateKey::TokenSigningPrivateKey(
    int64_t key_seq_num, int64_t expire_time, unique_ptr<PrivateKey> key)
    : key_(std::move(key)),
      key_seq_num_(key_seq_num),
      expire_time_(expire_time) {
  CHECK_OK(key_->ToString(&private_key_der_, DataFormat::DER));
  PublicKey public_key;
  CHECK_OK(key_->GetPublicKey(&public_key));
  CHECK_OK(public_key.ToString(&public_key_der_, DataFormat::DER));
}

TokenSigningPrivateKey::~TokenSigningPrivateKey() {
}

Status TokenSigningPrivateKey::Sign(SignedTokenPB* token) const {
  string signature;
  RETURN_NOT_OK(key_->MakeSignature(DigestType::SHA256,
      token->token_data(), &signature));
  token->mutable_signature()->assign(std::move(signature));
  token->set_signing_key_seq_num(key_seq_num_);
  return Status::OK();
}

void TokenSigningPrivateKey::ExportPB(TokenSigningPrivateKeyPB* pb) const {
  pb->Clear();
  pb->set_key_seq_num(key_seq_num_);
  pb->set_rsa_key_der(private_key_der_);
  pb->set_expire_unix_epoch_seconds(expire_time_);
}

void TokenSigningPrivateKey::ExportPublicKeyPB(TokenSigningPublicKeyPB* pb) const {
  pb->Clear();
  pb->set_key_seq_num(key_seq_num_);
  pb->set_rsa_key_der(public_key_der_);
  pb->set_expire_unix_epoch_seconds(expire_time_);
}

} // namespace security
} // namespace kudu
