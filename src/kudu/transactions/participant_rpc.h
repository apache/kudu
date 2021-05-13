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
class Timestamp;

namespace client {
class KuduClient;
} // namespace client

namespace tablet {
class TxnMetadataPB;
} // namespace tablet

namespace transactions {

// Context to be used when sending RPCs to specific tablets.
struct TxnParticipantContext {
  client::KuduClient* client;
  tserver::ParticipantOpPB participant_op;

  // NOTE: this gets set after the tablet lookup completes.
  scoped_refptr<client::internal::RemoteTablet> tablet;
};

class ParticipantRpc final : public rpc::RetriableRpc<client::internal::RemoteTabletServer,
                                                      tserver::ParticipantRequestPB,
                                                      tserver::ParticipantResponsePB> {
 public:
  // NOTE: if 'begin_commit_timestamp' is non-null, the memory it points to
  // should stay valid until the RPC completes (i.e. until callback 'user_cb'
  // is invoked).
  static ParticipantRpc* NewRpc(std::unique_ptr<TxnParticipantContext> ctx,
                                const MonoTime& deadline,
                                StatusCallback user_cb,
                                Timestamp* begin_commit_timestamp = nullptr,
                                tablet::TxnMetadataPB* metadata_pb = nullptr);
  ~ParticipantRpc() {}
  std::string ToString() const override;

 protected:
  void Try(client::internal::RemoteTabletServer* replica,
           const rpc::ResponseCallback& callback) override;

  rpc::RetriableRpcStatus AnalyzeResponse(const Status& rpc_cb_status) override;

  // Deletes itself upon completion.
  void Finish(const Status& status) override;

  bool GetNewAuthnTokenAndRetry() override;

 private:
  ParticipantRpc(std::unique_ptr<TxnParticipantContext> ctx,
                 scoped_refptr<client::internal::MetaCacheServerPicker> replica_picker,
                 const MonoTime& deadline,
                 StatusCallback user_cb,
                 Timestamp* begin_commit_timestamp,
                 tablet::TxnMetadataPB* metadata_pb);

  client::KuduClient* client_;
  scoped_refptr<client::internal::RemoteTablet> tablet_;
  const StatusCallback user_cb_;
  Timestamp* begin_commit_timestamp_;
  tablet::TxnMetadataPB* metadata_pb_;
};

} // namespace transactions
} // namespace kudu
