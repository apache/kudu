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

#include "kudu/client/authz_token_cache.h"

#include <cstdint>
#include <functional>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/master_proxy_rpc.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

using kudu::master::MasterFeatures;
using kudu::master::MasterServiceProxy;
using kudu::rpc::BackoffType;
using kudu::rpc::ResponseCallback;
using kudu::security::SignedTokenPB;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace client {
namespace internal {

RetrieveAuthzTokenRpc::RetrieveAuthzTokenRpc(const KuduTable* table,
                                             MonoTime deadline)
    : AsyncLeaderMasterRpc(
          deadline, table->client(), BackoffType::LINEAR, req_, &resp_,
          &MasterServiceProxy::GetTableSchemaAsync, "RetrieveAuthzToken",
          [table](const Status& s) {
            table->client()->data_->authz_token_cache_.RetrievedNewAuthzTokenCb(
                table->id(), s);
          },
          { MasterFeatures::GENERATE_AUTHZ_TOKEN }),
      table_(table) {
  req_.mutable_table()->set_table_id(table_->id());
}

string RetrieveAuthzTokenRpc::ToString() const {
  return Substitute("$0 { table: '$1' ($2), attempt: $3 }", AsyncLeaderMasterRpc::ToString(),
      req_.table().table_name(), req_.table().table_id(), num_attempts());
}

void RetrieveAuthzTokenRpc::SendRpcCb(const Status& status) {
  Status new_status = status;
  // Check for generic master RPC errors.
  if (RetryOrReconnectIfNecessary(&new_status)) {
    return;
  }
  // Unwrap and return any other application errors that may be returned by the
  // master service.
  if (new_status.ok() && resp_.has_error()) {
    new_status = StatusFromPB(resp_.error().status());
  }
  if (new_status.ok()) {
    // Note: legacy masters will be caught by the required GENERATE_AUTHZ_TOKEN
    // feature, and so we can only get here without an authz token if the
    // master didn't return one, which is a programming error.
    DCHECK(resp_.has_authz_token());
    if (PREDICT_TRUE(resp_.has_authz_token())) {
      client_->data_->authz_token_cache_.Put(table_->id(), resp_.authz_token());
    }
  }
  user_cb_(new_status);
}

void AuthzTokenCache::Put(const string& table_id, SignedTokenPB authz_token) {
  VLOG(1) << Substitute("Putting new token for table $0 into the token cache", table_id);
  std::lock_guard<simple_spinlock> l(token_lock_);
  EmplaceOrUpdate(&authz_tokens_, table_id, std::move(authz_token));
}

bool AuthzTokenCache::Fetch(const string& table_id, SignedTokenPB* authz_token) {
  DCHECK(authz_token);
  std::lock_guard<simple_spinlock> l(token_lock_);
  const auto* token = FindOrNull(authz_tokens_, table_id);
  if (token) {
    *authz_token = *token;
    return true;
  }
  return false;
}

void AuthzTokenCache::RetrieveNewAuthzToken(const KuduTable* table,
                                            StatusCallback callback,
                                            MonoTime deadline) {
  DCHECK(table);
  DCHECK(deadline.Initialized());
  const string& table_id = table->id();
  std::unique_lock<simple_spinlock> l(rpc_lock_);
  // If there already exists an RPC for this table; attach the callback.
  auto* rpc_and_cbs = FindOrNull(authz_rpcs_, table_id);
  if (rpc_and_cbs) {
    DCHECK(!rpc_and_cbs->second.empty());
    VLOG(2) << Substitute("Binding to in-flight RPC to retrieve authz token for $0", table_id);
    rpc_and_cbs->second.emplace_back(std::move(callback));
  } else {
    // Otherwise, send out a new RPC.
    VLOG(2) << Substitute("Sending new RPC to retrieve authz token for $0", table_id);
    scoped_refptr<RetrieveAuthzTokenRpc> rpc(new RetrieveAuthzTokenRpc(table, deadline));
    EmplaceOrDie(&authz_rpcs_, table_id,
                 RpcAndCallbacks(rpc, { std::move(callback) }));
    l.unlock();
    rpc->SendRpc();
  }
}

void AuthzTokenCache::RetrievedNewAuthzTokenCb(const string& table_id,
                                               const Status& status) {
  VLOG(1) << Substitute("Retrieved new authz token for table $0", table_id);
  vector<StatusCallback> cbs;
  {
    // Erase the RPC from our in-flight map.
    std::lock_guard<simple_spinlock> l(rpc_lock_);
    auto rpc_and_cbs = EraseKeyReturnValuePtr(&authz_rpcs_, table_id);
    cbs = std::move(rpc_and_cbs.second);
  }
  DCHECK(!cbs.empty());
  for (const auto& cb : cbs) {
    cb(status);
  }
}

} // namespace internal
} // namespace client
} // namespace kudu
