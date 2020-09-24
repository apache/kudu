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
#include <memory>
#include <string>

#include "kudu/client/meta_cache.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/retriable_rpc.h"
#include "kudu/rpc/rpc.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/status_callback.h"

namespace kudu {
class MonoTime;
class Status;

namespace client {
class KuduClient;
class KuduTable;
} // namespace client

namespace transactions {

// Context to be used when sending RPCs to specific tablets.
struct TxnStatusTabletContext {
  client::sp::shared_ptr<client::KuduTable> table;
  tserver::CoordinatorOpPB coordinate_txn_op;

  // NOTE: this gets set after the tablet lookup completes.
  scoped_refptr<client::internal::RemoteTablet> tablet;
};

// Encapsulates an RPC being sent to a partition of the transaction status
// table.
class CoordinatorRpc final : public rpc::RetriableRpc<client::internal::RemoteTabletServer,
                                                      tserver::CoordinateTransactionRequestPB,
                                                      tserver::CoordinateTransactionResponsePB> {
 public:
  // NOTE: if 'op_result' is non-null, the memory it points to should stay valid
  //       until the RPC completes (i.e. until callback 'cb' is invoked).
  static CoordinatorRpc* NewRpc(std::unique_ptr<TxnStatusTabletContext> ctx,
                                const MonoTime& deadline,
                                StatusCallback cb,
                                tserver::CoordinatorOpResultPB* op_result = nullptr);

  ~CoordinatorRpc() {}

  std::string ToString() const override;

 protected:
  void Try(client::internal::RemoteTabletServer* replica,
           const rpc::ResponseCallback& callback) override;

  rpc::RetriableRpcStatus AnalyzeResponse(const Status& rpc_cb_status) override;

  // Deletes itself upon completion.
  void Finish(const Status& status) override;

  bool GetNewAuthnTokenAndRetry() override;

 private:
  CoordinatorRpc(std::unique_ptr<TxnStatusTabletContext> ctx,
                 const scoped_refptr<client::internal::MetaCacheServerPicker>& replica_picker,
                 const MonoTime& deadline,
                 StatusCallback cb,
                 tserver::CoordinatorOpResultPB* op_result);

  client::KuduClient* client_;
  client::sp::shared_ptr<client::KuduTable> table_;
  scoped_refptr<client::internal::RemoteTablet> tablet_;
  const StatusCallback cb_;
  tserver::CoordinatorOpResultPB* op_result_;
};

} // namespace transactions
} // namespace kudu
