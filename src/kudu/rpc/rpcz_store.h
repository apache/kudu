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

#include <memory>
#include <unordered_map>

#include "kudu/util/locks.h"

namespace kudu {
namespace rpc {

class DumpRpczStoreRequestPB;
class DumpRpczStoreResponsePB;
class InboundCall;
class MethodSampler;
struct RpcMethodInfo;

// Responsible for storing sampled traces associated with completed calls.
// Before each call is responded to, it is added to this store.
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

  // Dump all of the collected RPC samples in response to a user query.
  void DumpPB(const DumpRpczStoreRequestPB& req,
              DumpRpczStoreResponsePB* resp);

 private:
  // Look up or create the particular MethodSampler instance which should
  // store samples for this call.
  MethodSampler* SamplerForCall(InboundCall* call);

  // Log a WARNING message if the RPC response was slow enough that the
  // client likely timed out. This is based on the client-provided timeout
  // value.
  // Also can be configured to log _all_ RPC traces for help debugging.
  void LogTrace(InboundCall* call);

  percpu_rwlock samplers_lock_;

  // Protected by samplers_lock_.
  std::unordered_map<RpcMethodInfo*, std::unique_ptr<MethodSampler>> method_samplers_;

  DISALLOW_COPY_AND_ASSIGN(RpczStore);
};

} // namespace rpc
} // namespace kudu
