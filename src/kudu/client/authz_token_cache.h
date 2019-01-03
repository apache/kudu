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
//
// This module is internal to the client and not a public API.
#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/client/master_proxy_rpc.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/status_callback.h"

namespace kudu {

class MonoTime;
class Status;

namespace security {
class SignedTokenPB;
} // namespace security

namespace client {

class KuduTable;

namespace internal {

// An asynchronous RPC that retrieves a new authz token for a table and puts it
// in a token cache.
class RetrieveAuthzTokenRpc : public AsyncLeaderMasterRpc<master::GetTableSchemaRequestPB,
                                                          master::GetTableSchemaResponsePB>,
                              public RefCountedThreadSafe<RetrieveAuthzTokenRpc> {
 public:
  RetrieveAuthzTokenRpc(const KuduTable* table, MonoTime deadline);
  std::string ToString() const override;

 protected:
  // Handles retries, reconnection, and such.
  void SendRpcCb(const Status& status) override;

 private:
  // Encapsulates the client and table with which the RPC will operate.
  const KuduTable* table_;

  // Request for the authz token.
  master::GetTableSchemaRequestPB req_;

  // Response containing the authz token. This gets populated before calling
  // SendRpcCb().
  master::GetTableSchemaResponsePB resp_;
};

// Cache for authz tokens received from the master. A client will receive an
// authz token upon opening the table and put it into the cache. A subsequent
// operation that requires an authz token (e.g. writes, scans) will fetch it
// from the cache and attach it to the operation request. If the tserver
// responds with an error indicating that the client needs a new token,
// 'RetrieveNewAuthzToken' can be used to do so.
//
// This class is thread-safe.
class AuthzTokenCache {
 public:
  typedef std::pair<scoped_refptr<RetrieveAuthzTokenRpc>,
                    std::vector<StatusCallback>> RpcAndCallbacks;
  // Adds an authz token to the cache for 'table_id', replacing any that
  // previously existed.
  void Put(const std::string& table_id,
           security::SignedTokenPB authz_token);

  // Fetches an authz token from the cache for 'table_id', returning true if
  // one exists and false otherwise.
  //
  // Since clients may not have the same time-keeping guarantees that servers
  // do, nor do they have private keys with which to validate tokens, no
  // checking is done to verify the expiration or validity of the returned
  // token. Such validation is delegated to the tservers and returned to the
  // client via error to retrieve new tokens as appropriate.
  bool Fetch(const std::string& table_id, security::SignedTokenPB* authz_token);

  // Runs 'callback' asynchronously after retrieving a new authz token for
  // 'table's ID and putting it in the cache. This method handles retries,
  // leader-finding, and concurrent RPCs for the same table.
  //
  // Callers should expect 'callback' to be run with Status::OK if a token was
  // successfully retrieved from the master, and with an error otherwise.
  void RetrieveNewAuthzToken(const KuduTable* table,
                             StatusCallback callback,
                             MonoTime deadline);
 private:
  friend class RetrieveAuthzTokenRpc;

  // Callback to run upon receiving a response for a RetrieveNewAuthzTokenRpc.
  // This will handle the 'status' and call the pending callbacks for the RPC
  // as appropriate.
  void RetrievedNewAuthzTokenCb(const std::string& table_id,
                                const Status& status);

  // Protects 'authz_tokens_'.
  simple_spinlock token_lock_;

  // Protects 'authz_rpcs_'.
  simple_spinlock rpc_lock_;

  // Authorization tokens stored for each table, indexed by the table ID. Note
  // that these may be expired, and it is up to the users of the cache to
  // refresh tokens upon learning of their expiration.
  //
  // Protected by 'token_lock_'.
  std::unordered_map<std::string, security::SignedTokenPB> authz_tokens_;

  // Map from a table ID to the in-flight RPC to retrieve an authz token for
  // it and the callbacks to run upon receiving its response.
  //
  // Protected by 'rpc_lock_'.
  std::unordered_map<std::string, RpcAndCallbacks> authz_rpcs_;
};

} // namespace internal
} // namespace client
} // namespace kudu
