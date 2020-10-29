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

#include <functional>
#include <string>

#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/util/status_callback.h"

namespace kudu {

class MonoTime;
class Status;
namespace rpc {
class RpcController;
}  // namespace rpc

namespace transactions {
class TxnManagerServiceProxy;
} // namespace transactions

namespace client {

class KuduClient;

namespace internal {

// Encapsulates RPCs that target a randomly chosen TxnManager, handling retries
// and reconnections.
//
// TODO(aserbin): implement the re-connection to TxnManagers
template <class ReqClass, class RespClass>
class AsyncRandomTxnManagerRpc : public rpc::Rpc {
 public:
  // The input 'client' will be used to call the asynchonous TxnManager's proxy
  // function 'func' on the currently used random TxnManager, sending over
  // 'req'. Upon successful completion of the RPC, 'resp' is populated with the
  // RPC response. Various errors (e.g. from the RPC layer or from the
  // application layer) will direct the RPC to be retried until 'deadline'
  // is reached. If the final result is an error, 'resp' may not be set,
  // or may have an application error set.
  //
  // 'user_cb' will be called on the final result of the RPC (either OK,
  // TimedOut, or some other non-retriable error).
  //
  // 'rpc_name' is a descriptor for the RPC used to add more context to logs
  // and error messages.
  //
  // Retries will be done according to the backoff type specified by 'backoff'.
  AsyncRandomTxnManagerRpc(
      const MonoTime& deadline,
      KuduClient* client,
      rpc::BackoffType backoff,
      const ReqClass& req,
      RespClass* resp,
      const std::function<void(transactions::TxnManagerServiceProxy*,
                               const ReqClass&, RespClass*,
                               rpc::RpcController*,
                               const rpc::ResponseCallback&)>& func,
      std::string rpc_name,
      StatusCallback user_cb);

  // Send the RPC using the TxnManagerService proxy's asynchonous API, ensuring
  // that neither the per-RPC deadline nor the overall deadline has passed.
  void SendRpc() override;

  std::string ToString() const override;

 protected:
  // Handles 'status', retrying if necessary, and calling the user-provided
  // callback as appropriate.
  void SendRpcCb(const Status& status) override;

  void ResetTxnManagerAndRetry(const Status& status);

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
  //
  // Returns true if a reconnection and/or retry was required and has been
  // scheduled, in which case callers should ensure that this object remains
  // alive.
  bool RetryIfNecessary(Status* status);

  KuduClient* client_;
  const ReqClass* req_;
  RespClass* resp_;

  // Asynchronous function that sends an RPC to current TxnManager.
  const std::function<void(transactions::TxnManagerServiceProxy*,
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
};

} // namespace internal
} // namespace client
} // namespace kudu
