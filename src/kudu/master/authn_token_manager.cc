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

#include "kudu/master/authn_token_manager.h"

#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "kudu/gutil/walltime.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/status.h"

DEFINE_int64(authn_token_validity_seconds, 120,
             "Period of time for which an issued authentication token is valid.");
// TODO(PKI): docs for what actual effect this has, given we don't support
// token renewal.
// TODO(PKI): this is set extremely low, so that we don't forget to come back to
// this and add rolling and refetching code.
TAG_FLAG(authn_token_validity_seconds, experimental);


using kudu::security::AuthnTokenPB;
using kudu::security::SignedTokenPB;
using kudu::security::TokenPB;
using kudu::security::TokenSigner;
using std::unique_ptr;
using std::string;

namespace kudu {
namespace master {

AuthnTokenManager::AuthnTokenManager() {
}

AuthnTokenManager::~AuthnTokenManager() {
}

Status AuthnTokenManager::Init(int64_t next_tsk_seq_num) {
  CHECK(!signer_);
  unique_ptr<TokenSigner> signer(new TokenSigner(next_tsk_seq_num));

  // Roll twice at startup. See TokenSigner class documentation for reasoning.
  RETURN_NOT_OK(signer->RotateSigningKey());
  RETURN_NOT_OK(signer->RotateSigningKey());

  // TODO(PKI): need to persist the public keys every time we roll. There's
  // a bit of subtlety here: we shouldn't start exporting a key until it is
  // properly persisted. Perhaps need some refactor, so we can do:
  // 1) generate a new TSK
  // 2) try to write the public portion to system table (keep in mind we could lose
  //    leadership here)
  // 3) pass it back to the TokenSigner as successful?

  // TODO(PKI): manage a thread which periodically rolls the TSK. Otherwise
  // we'll die after some number of days (whatever the validity is).

  signer_ = std::move(signer);
  return Status::OK();
}

Status AuthnTokenManager::GenerateToken(string username,
                                        SignedTokenPB* signed_token) {
  TokenPB token;

  token.set_expire_unix_epoch_seconds(
      WallTime_Now() + FLAGS_authn_token_validity_seconds);
  AuthnTokenPB* authn = token.mutable_authn();
  authn->mutable_username()->assign(std::move(username));
  SignedTokenPB ret;

  if (!token.SerializeToString(ret.mutable_token_data())) {
    return Status::RuntimeError("could not serialize authn token");
  }
  RETURN_NOT_OK_PREPEND(signer_->SignToken(&ret), "could not sign authn token");
  signed_token->Swap(&ret);
  return Status::OK();
}

} // namespace master
} // namespace kudu
