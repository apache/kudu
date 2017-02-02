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
#ifndef KUDU_RPC_RPC_CONTROLLER_H
#define KUDU_RPC_RPC_CONTROLLER_H

#include <functional>
#include <glog/logging.h>
#include <memory>
#include <unordered_set>

#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
namespace kudu {

namespace rpc {

class ErrorStatusPB;
class OutboundCall;
class RequestIdPB;

// Controller for managing properties of a single RPC call, on the client side.
//
// An RpcController maps to exactly one call and is not thread-safe. The client
// may use this class prior to sending an RPC in order to set properties such
// as the call's timeout.
//
// After the call has been sent (e.g using Proxy::AsyncRequest()) the user
// may invoke methods on the RpcController object in order to probe the status
// of the call.
class RpcController {
 public:
  RpcController();
  ~RpcController();

  // Swap the state of the controller (including ownership of sidecars, buffers,
  // etc) with another one.
  void Swap(RpcController* other);

  // Reset this controller so it may be used with another call.
  // Note that this resets the required server features.
  void Reset();

  // Return true if the call has finished.
  // A call is finished if the server has responded, or if the call
  // has timed out.
  bool finished() const;

  // Return the current status of a call.
  //
  // A call is "OK" status until it finishes, at which point it may
  // either remain in "OK" status (if the call was successful), or
  // change to an error status. Error status indicates that there was
  // some RPC-layer issue with making the call, for example, one of:
  //
  // * failed to establish a connection to the server
  // * the server was too busy to handle the request
  // * the server was unable to interpret the request (eg due to a version
  //   mismatch)
  // * a network error occurred which caused the connection to be torn
  //   down
  // * the call timed out
  Status status() const;

  // If status() returns a RemoteError object, then this function returns
  // the error response provided by the server. Service implementors may
  // use protobuf Extensions to add application-specific data to this PB.
  //
  // If Status was not a RemoteError, this returns NULL.
  // The returned pointer is only valid as long as the controller object.
  const ErrorStatusPB* error_response() const;

  // Set the timeout for the call to be made with this RPC controller.
  //
  // The configured timeout applies to the entire time period between
  // the AsyncRequest() method call and getting a response. For example,
  // if it takes too long to establish a connection to the remote host,
  // or to DNS-resolve the remote host, those will be accounted as part
  // of the timeout period.
  //
  // Timeouts must be set prior to making the request -- the timeout may
  // not currently be adjusted for an already-sent call.
  //
  // Using an uninitialized timeout will result in a call which never
  // times out (not recommended!)
  void set_timeout(const MonoDelta& timeout);

  // Like a timeout, but based on a fixed point in time instead of a delta.
  //
  // Using an uninitialized deadline means the call won't time out.
  void set_deadline(const MonoTime& deadline);

  // Allows settting the request id for the next request sent to the server.
  // A request id allows the server to identify each request sent by the client uniquely,
  // in some cases even when sent to multiple servers, enabling exactly once semantics.
  void SetRequestIdPB(std::unique_ptr<RequestIdPB> request_id);

  // Returns whether a request id has been set on RPC header.
  bool has_request_id() const;

  // Returns the currently set request id.
  // When the request is sent to the server, it gets "moved" from RpcController
  // so an absence of a request after send doesn't mean one wasn't sent.
  // REQUIRES: the controller has a request ID set.
  const RequestIdPB& request_id() const;

  // Add a requirement that the server side must support a feature with the
  // given identifier. The set of required features is sent to the server
  // with the RPC call, and if any required feature is not supported, the
  // call will fail with a NotSupported() status.
  //
  // This can be used when an RPC call changes in a way that is protobuf-compatible,
  // but for which it would not be appropriate for the server to simply ignore
  // an added field. For example, consider an API call like:
  //
  //   message DeleteAccount {
  //     optional string username = 1;
  //     optional bool dry_run = 2; // ADDED LATER!
  //   }
  //
  // In this case, if a new client which supports the 'dry_run' flag sends the RPC
  // to an old server, the old server will simply ignore the unrecognized parameter,
  // with highly problematic results. To solve this problem, the new version can
  // add a feature flag:
  //
  //   In .proto file
  //   ----------------
  //   enum MyFeatureFlags {
  //     UNKNOWN = 0;
  //     DELETE_ACCOUNT_SUPPORTS_DRY_RUN = 1;
  //   }
  //
  //   In client code:
  //   ---------------
  //   if (dry_run) {
  //     rpc.RequireServerFeature(DELETE_ACCOUNT_SUPPORTS_DRY_RUN);
  //     req.set_dry_run(true);
  //   }
  //
  // This has the effect of (a) maintaining compatibility when dry_run is not specified
  // and (b) rejecting the RPC with a "NotSupported" error when it is.
  //
  // NOTE: 'feature' is an int rather than an enum type because each service
  // must define its own enum of supported features, and protobuf doesn't support
  // any ability to 'extend' enum types. Implementers should define an enum in the
  // service's protobuf definition as shown above.
  void RequireServerFeature(uint32_t feature);

  // Executes the provided function with a reference to the required server
  // features.
  const std::unordered_set<uint32_t>& required_server_features() const {
    return required_server_features_;
  }

  // Return the configured timeout.
  MonoDelta timeout() const;

  // Fills the 'sidecar' parameter with the slice pointing to the i-th
  // sidecar upon success.
  //
  // Should only be called if the call's finished, but the controller has not
  // been Reset().
  //
  // May fail if index is invalid.
  Status GetSidecar(int idx, Slice* sidecar) const;

 private:
  friend class OutboundCall;
  friend class Proxy;

  MonoDelta timeout_;
  std::unordered_set<uint32_t> required_server_features_;

  mutable simple_spinlock lock_;

  // The id of this request.
  // Ownership is transfered to OutboundCall once the call is sent.
  std::unique_ptr<RequestIdPB> request_id_;

  // Once the call is sent, it is tracked here.
  std::shared_ptr<OutboundCall> call_;

  DISALLOW_COPY_AND_ASSIGN(RpcController);
};

} // namespace rpc
} // namespace kudu
#endif
