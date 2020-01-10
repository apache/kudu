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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <google/protobuf/message.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"

namespace kudu {
namespace rpc {

class InboundCall;
class RemoteMethod;
class ResultTracker;
class RpcContext;

// Generated services define an instance of this class for each
// method that they implement. The generic server code implemented
// by GeneratedServiceIf look up the RpcMethodInfo in order to handle
// each RPC.
struct RpcMethodInfo : public RefCountedThreadSafe<RpcMethodInfo> {
  // Prototype protobufs for requests and responses.
  // These are empty protobufs which are cloned in order to provide an
  // instance for each request.
  std::unique_ptr<google::protobuf::Message> req_prototype;
  std::unique_ptr<google::protobuf::Message> resp_prototype;

  scoped_refptr<Histogram> handler_latency_histogram;

  // Whether we should track this method's result, using ResultTracker.
  bool track_result;

  // The authorization function for this RPC. If this function
  // returns false, the RPC has already been handled (i.e. rejected)
  // by the authorization function.
  std::function<bool(const google::protobuf::Message* req,
                     google::protobuf::Message* resp,
                     RpcContext* ctx)> authz_method;

  // The actual function to be called.
  std::function<void(const google::protobuf::Message* req,
                     google::protobuf::Message* resp,
                     RpcContext* ctx)> func;
};

// Handles incoming messages that initiate an RPC.
class ServiceIf {
 public:
  virtual ~ServiceIf();
  virtual void Handle(InboundCall* incoming) = 0;
  virtual void Shutdown();
  virtual std::string service_name() const = 0;

  // The service should return true if it supports the provided application
  // specific feature flag.
  virtual bool SupportsFeature(uint32_t feature) const;

  // Look up the method being requested by the remote call.
  //
  // If this returns nullptr, then certain functionality like
  // metrics collection will not be performed for this call.
  virtual RpcMethodInfo* LookupMethod(const RemoteMethod& method) {
    return nullptr;
  }

  // Default authorization method, which just allows all RPCs.
  //
  // See docs/design-docs/rpc.md for details on how to add custom
  // authorization checks to a service.
  bool AuthorizeAllowAll(const google::protobuf::Message* /*req*/,
                         google::protobuf::Message* /*resp*/,
                         RpcContext* /*ctx*/) {
    return true;
  }

 protected:
  bool ParseParam(InboundCall* call, google::protobuf::Message* message);
  void RespondBadMethod(InboundCall* call);
};


// Base class for code-generated service classes.
class GeneratedServiceIf : public ServiceIf {
 public:
  virtual ~GeneratedServiceIf();

  // Looks up the appropriate method in 'methods_by_name_' and executes
  // it on the current thread.
  //
  // If no such method is found, responds with an error.
  void Handle(InboundCall* incoming) override;

  RpcMethodInfo* LookupMethod(const RemoteMethod& method) override;

  // Returns the mapping from method names to method infos.
  typedef std::unordered_map<std::string, scoped_refptr<RpcMethodInfo>> MethodInfoMap;
  const MethodInfoMap& methods_by_name() const { return methods_by_name_; }

 protected:
  // For each method, stores the relevant information about how to handle the
  // call. Methods are inserted by the constructor of the generated subclass.
  // After construction, this map is accessed by multiple threads and therefore
  // must not be modified.
  MethodInfoMap methods_by_name_;

  // The result tracker for this service's methods.
  scoped_refptr<ResultTracker> result_tracker_;
};

} // namespace rpc
} // namespace kudu
