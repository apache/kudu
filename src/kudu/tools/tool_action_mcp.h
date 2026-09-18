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

#include <iosfwd>
#include <string>

#include "kudu/util/status.h"

namespace kudu {
namespace tools {

// JSON-RPC 2.0 standard error codes (see the JSON-RPC 2.0 spec, section 5.1).
// The one standard code we do not emit is -32603 (Internal error): every
// handler returns a well-formed response and nothing throws, so there is no
// internal-failure path to surface. Add kJsonRpcInternalError = -32603
// together with such a path if one is ever introduced.
constexpr int kJsonRpcParseError = -32700;
constexpr int kJsonRpcInvalidRequest = -32600;
constexpr int kJsonRpcMethodNotFound = -32601;
constexpr int kJsonRpcInvalidParams = -32602;

// Runs the JSON-RPC serve loop: reads newline-delimited requests from 'in' and
// writes one response line per request to 'out', until 'in' reaches EOF.
// 'kudu_binary_path' is the absolute path to this server's own 'kudu' binary,
// a fresh child of which executes each tools/call; the caller resolves it (and
// should fail fast if it cannot). An empty path is accepted for tests that
// never execute a tool -- HandleToolsCall then returns an isError result.
Status RunMcpServeLoop(std::istream& in, std::ostream& out,
                       const std::string& kudu_binary_path);

} // namespace tools
} // namespace kudu
