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

#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/status.h"

namespace kudu {

namespace security {
enum class VerificationResult;
} // namespace security

namespace rpc {

// Utility function to convert the result of a security token verification to
// an appropriate RPC error code. Returns OK if 'result' is VALID, and
// otherwise returns non-OK and sets 'error' appropriately.
// 'retry_error' is the error code to be returned to denote that verification
// should be retried after retrieving a new token.
Status ParseVerificationResult(const security::VerificationResult& result,
                               ErrorStatusPB::RpcErrorCodePB retry_error,
                               ErrorStatusPB::RpcErrorCodePB* error);

} // namespace rpc
} // namespace kudu
