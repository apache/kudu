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
#include <string>
#include <vector>

#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/status_callback.h"

namespace kudu {

class MonoTime;
class Status;

namespace master {
class MasterServiceProxy;
} // namespace master

namespace client {

class KuduClient;

namespace internal {

// Encapsulates RPCs that target the leader master, handling retries and
// reconnection to the master(s).
template <class ReqClass, class RespClass>
class AsyncLeaderMasterRpc : public rpc::Rpc {
 public:
  // The input 'client' will be used to call the asynchonous master proxy
  // function 'func' on the currently-known leader master, sending over 'req',
  // and expecting the features specified by 'required_feature_flags' to be
  // present on the master. Upon successful completion of the RPC, 'resp' is
  // populated with the RPC response. Various errors (e.g. from the RPC layer
  // or from the application layer) will direct the RPC to be retried until
  // 'deadline' is reached. If the final result is an error, 'resp' may not be
  // set, or may have an application error set.
  //
  // 'user_cb' will be called on the final result of the RPC (either OK,
  // TimedOut, or some other non-retriable error).
  //
  // 'rpc_name' is a descriptor for the RPC used to add more context to logs
  // and error messages.
  //
  // Retries will be done according to the backoff type specified by 'backoff'.
  AsyncLeaderMasterRpc(const MonoTime& deadline,
                       KuduClient* client,
                       rpc::BackoffType backoff,
                       const ReqClass& req,
                       RespClass* resp,
                       const std::function<void(master::MasterServiceProxy*,
                                                const ReqClass&, RespClass*,
                                                rpc::RpcController*,
                                                const rpc::ResponseCallback&)>& func,
                       std::string rpc_name,
                       StatusCallback user_cb,
                       std::vector<uint32_t> required_feature_flags);

  // Sends the RPC using the master proxy's asynchonous API, ensuring that
  // neither the per-RPC deadline nor the overall deadline has passed.
  //
  // Resets the RPC controller before sending out a new RPC.
  void SendRpc() override;

  std::string ToString() const override;

 protected:
  // Handles 'status', retrying if necessary, and calling the user-provided
  // callback as appropriate.
  void SendRpcCb(const Status& status) override;

  // Uses 'status' and the contents of the RPC controller and RPC response to
  // determine whether reconnections or retries should be performed, and if so,
  // performs them. Additionally, updates 'status' to include more information
  // based on the state of the RPC.
  //
  // Retries take the following kinds of errors into account:
  // - TimedOut errors that indicate the operation retrier has passed its
  //   deadline (distinct from TimedOut errors that surface in the RPC layer)
  // - RPC errors that come from a failed connection, in which case the
  //   controller status will be non-OK
  // - generic RPC errors, in which case the controller status will be a
  //   RemoteError and the controller will have an error response
  // - generic Master application errors, in which case the controller status
  //   will be OK, and the response will have an ErrorStatusPB
  //
  // Returns true if a reconnection and/or retry was required and has been
  // scheduled, in which case callers should ensure that this object remains
  // alive.
  bool RetryOrReconnectIfNecessary(Status* status);

  // Attempts to reconnect with the masters and find the leader master, and
  // attempts to retry the RPC.
  virtual void ResetMasterLeaderAndRetry(rpc::CredentialsPolicy creds_policy);

  // With a new leader found, resends the RPC. 'creds_policy' is the policy
  // with which the reconnection was attempted.
  void NewLeaderMasterDeterminedCb(rpc::CredentialsPolicy creds_policy,
                                   const Status& status);

  KuduClient* client_;
  const ReqClass* req_;
  RespClass* resp_;

  // Asynchronous function that sends an RPC to the master.
  const std::function<void(master::MasterServiceProxy*,
                           const ReqClass&, RespClass*,
                           rpc::RpcController*,
                           const rpc::ResponseCallback&)> func_;

  // Name of the RPC being sent. Since multiple template instantiations may
  // exist for the same proxy function, this need not be exactly the proxy
  // function name.
  const std::string rpc_name_;

  // Callback to call upon completion of the operation (whether the RPC itself
  // was successful or not).
  const StatusCallback user_cb_;

  // List of master-side feature flags required to send this RPC. If the
  // master(s) is missing any of these flags, the RPC will yield an error.
  const std::vector<uint32_t> required_feature_flags_;
};

} // namespace internal
} // namespace client
} // namespace kudu
