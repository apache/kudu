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

#include "kudu/rpc/constants.h"

using std::set;

namespace kudu {
namespace rpc {

const char* const kMagicNumber = "hrpc";
const char* const kSaslAppName = "kudu";
const char* const kSaslProtoName = "kudu";

// NOTE: the TLS flag is dynamically added based on the local encryption
// configuration.
//
// NOTE: the TLS_AUTHENTICATION_ONLY flag is dynamically added on both
// sides based on the remote peer's address.
//
// NOTE: the REQUEST_FOOTERS is always set on the client side. The server side
// which supports parsing footer sets REQUEST_FOOTERS if client side has it set.
set<RpcFeatureFlag> kSupportedServerRpcFeatureFlags = { APPLICATION_FEATURE_FLAGS };
set<RpcFeatureFlag> kSupportedClientRpcFeatureFlags = { APPLICATION_FEATURE_FLAGS,
                                                        REQUEST_FOOTERS };

} // namespace rpc
} // namespace kudu
