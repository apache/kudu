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
#include "kudu/master/txn_manager.service.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

namespace master {
class Master;
}

namespace rpc {
class RpcContext;
}

namespace transactions {
class GetTransactionStateRequestPB;
class GetTransactionStateResponsePB;
class KeepTransactionAliveRequestPB;
class KeepTransactionAliveResponsePB;

// Implementation of the TxnManager service. See txn_manager.proto for docs
// on each RPC.
class TxnManagerServiceImpl : public TxnManagerServiceIf {
 public:
  explicit TxnManagerServiceImpl(master::Master* server);

  void BeginTransaction(const class BeginTransactionRequestPB* req,
                        class BeginTransactionResponsePB* resp,
                        rpc::RpcContext* ctx) override;

  void CommitTransaction(const class CommitTransactionRequestPB* req,
                         class CommitTransactionResponsePB* resp,
                         rpc::RpcContext* ctx) override;

  void AbortTransaction(const class AbortTransactionRequestPB* req,
                        class AbortTransactionResponsePB* resp,
                        rpc::RpcContext* ctx) override;

  void GetTransactionState(const GetTransactionStateRequestPB* req,
                           GetTransactionStateResponsePB* resp,
                           rpc::RpcContext* ctx) override;

  void KeepTransactionAlive(const KeepTransactionAliveRequestPB* req,
                            KeepTransactionAliveResponsePB* resp,
                            rpc::RpcContext* ctx) override;

  // Authorize an RPC call which must be from a client.
  bool AuthorizeClient(const google::protobuf::Message* req,
                       google::protobuf::Message* resp,
                       rpc::RpcContext* ctx) override;
 private:
  master::Master* server_;

  DISALLOW_COPY_AND_ASSIGN(TxnManagerServiceImpl);
};

} // namespace transactions
} // namespace kudu
