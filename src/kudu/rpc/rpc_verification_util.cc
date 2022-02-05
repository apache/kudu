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

#include "kudu/rpc/rpc_verification_util.h"

#include <ostream>

#include <glog/logging.h>

#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/security/token_verifier.h"

namespace kudu {

using security::TokenVerificationResult;

namespace rpc {

Status ParseTokenVerificationResult(const TokenVerificationResult& result,
                                    ErrorStatusPB::RpcErrorCodePB retry_error,
                                    ErrorStatusPB::RpcErrorCodePB* error) {
  DCHECK(error);
  switch (result) {
    case TokenVerificationResult::VALID: return Status::OK();

    case TokenVerificationResult::INVALID_TOKEN:
    case TokenVerificationResult::INVALID_SIGNATURE:
    case TokenVerificationResult::EXPIRED_TOKEN:
    case TokenVerificationResult::EXPIRED_SIGNING_KEY: {
      // These errors indicate the client should get a new token and try again.
      *error = retry_error;
      break;
    }
    case TokenVerificationResult::UNKNOWN_SIGNING_KEY: {
      // The server doesn't recognize the signing key. This indicates that the
      // server has not been updated with the most recent TSKs, so tell the
      // client to try again later.
      *error = ErrorStatusPB::ERROR_UNAVAILABLE;
      break;
    }
    case TokenVerificationResult::INCOMPATIBLE_FEATURE: {
      // These error types aren't recoverable by having the client get a new token.
      *error = ErrorStatusPB::FATAL_UNAUTHORIZED;
      break;
    }
    default:
      LOG(FATAL) << "Unknown verification result: " << static_cast<int>(result);
  }
  return Status::NotAuthorized(TokenVerificationResultToString(result));
}

} // namespace rpc
} // namespace kudu
