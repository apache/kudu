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

#include <memory>
#include <string>

#include "kudu/gutil/macros.h"

namespace kudu {
class Status;

namespace security {
class SignedTokenPB;
class TokenSigner;
} // namespace security

namespace master {

class AuthnTokenManager {
 public:
  AuthnTokenManager();
  ~AuthnTokenManager();

  Status Init(int64_t next_tsk_seq_num);

  Status GenerateToken(std::string username,
                       security::SignedTokenPB* signed_token);

 private:
  std::unique_ptr<security::TokenSigner> signer_;
  DISALLOW_COPY_AND_ASSIGN(AuthnTokenManager);
};

} // namespace master
} // namespace kudu
