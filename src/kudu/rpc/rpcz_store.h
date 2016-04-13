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

namespace kudu {
namespace rpc {

class InboundCall;

// Responsible for storing sampled traces associated with completed calls.
// Before each call is responded to, it is added to this store.
//
// The current implementation just logs traces for calls which are slow.
class RpczStore {
 public:
  RpczStore();
  ~RpczStore();

  // Process a single call, potentially sampling it for later analysis.
  //
  // If the call is sampled, it might be mutated. For example, the request
  // and response might be taken from the call and stored as part of the
  // sample. This should be called just before a call response is sent
  // to the client.
  void AddCall(InboundCall* c);

 private:
  // Log a WARNING message if the RPC response was slow enough that the
  // client likely timed out. This is based on the client-provided timeout
  // value.
  // Also can be configured to log _all_ RPC traces for help debugging.
  void LogTrace(InboundCall* call);

  DISALLOW_COPY_AND_ASSIGN(RpczStore);
};

} // namespace rpc
} // namespace kudu
