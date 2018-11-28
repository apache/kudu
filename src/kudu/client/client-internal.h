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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace boost {
template <typename Signature>
class function;
} // namespace boost

namespace kudu {

class DnsResolver;
class PartitionSchema;
class Sockaddr;

namespace master {
class AlterTableRequestPB;
class AlterTableResponsePB;
class ConnectToMasterResponsePB;
class CreateTableRequestPB;
class CreateTableResponsePB;
class MasterServiceProxy;
class TableIdentifierPB;
} // namespace master

namespace rpc {
class Messenger;
class RequestTracker;
} // namespace rpc

namespace client {

class KuduSchema;

namespace internal {
class ConnectToClusterRpc;
class MetaCache;
class RemoteTablet;
class RemoteTabletServer;
} // namespace internal

class KuduClient::Data {
 public:
  Data();
  ~Data();

  // Selects a TS replica from the given RemoteTablet subject
  // to liveness and the provided selection criteria and blacklist.
  //
  // If no appropriate replica can be found, a non-OK status is returned and 'ts' is untouched.
  //
  // The 'candidates' return parameter indicates tservers that are live and meet the selection
  // criteria, but are possibly filtered by the blacklist. This is useful for implementing
  // retry logic.
  Status GetTabletServer(KuduClient* client,
                         const scoped_refptr<internal::RemoteTablet>& rt,
                         ReplicaSelection selection,
                         const std::set<std::string>& blacklist,
                         std::vector<internal::RemoteTabletServer*>* candidates,
                         internal::RemoteTabletServer** ts);

  Status CreateTable(KuduClient* client,
                     const master::CreateTableRequestPB& req,
                     master::CreateTableResponsePB* resp,
                     const MonoTime& deadline,
                     bool has_range_partition_bounds);

  Status IsCreateTableInProgress(KuduClient* client,
                                 master::TableIdentifierPB table,
                                 const MonoTime& deadline,
                                 bool* create_in_progress);

  Status WaitForCreateTableToFinish(KuduClient* client,
                                    master::TableIdentifierPB table,
                                    const MonoTime& deadline);

  Status DeleteTable(KuduClient* client,
                     const std::string& table_name,
                     const MonoTime& deadline,
                     bool modify_external_catalogs = true);

  Status AlterTable(KuduClient* client,
                    const master::AlterTableRequestPB& req,
                    master::AlterTableResponsePB* resp,
                    const MonoTime& deadline,
                    bool has_add_drop_partition);

  Status IsAlterTableInProgress(KuduClient* client,
                                master::TableIdentifierPB table,
                                const MonoTime& deadline,
                                bool* alter_in_progress);

  Status WaitForAlterTableToFinish(KuduClient* client,
                                   master::TableIdentifierPB table,
                                   const MonoTime& deadline);

  Status GetTableSchema(KuduClient* client,
                        const std::string& table_name,
                        const MonoTime& deadline,
                        KuduSchema* schema,
                        PartitionSchema* partition_schema,
                        std::string* table_id,
                        int* num_replicas);

  Status InitLocalHostNames();

  bool IsLocalHostPort(const HostPort& hp) const;

  bool IsTabletServerLocal(const internal::RemoteTabletServer& rts) const;

  // Returns a non-failed replica of the specified tablet based on the provided selection criteria
  // and tablet server blacklist.
  //
  // Returns NULL if there are no valid tablet servers.
  internal::RemoteTabletServer* SelectTServer(
      const scoped_refptr<internal::RemoteTablet>& rt,
      const ReplicaSelection selection,
      const std::set<std::string>& blacklist,
      std::vector<internal::RemoteTabletServer*>* candidates) const;

  // Sets 'master_proxy_' from the address specified by 'leader_addr'.
  // Called by ConnectToClusterRpc::SendRpcCb() upon successful completion.
  //
  // See also: ConnectToClusterAsync.
  void ConnectedToClusterCb(const Status& status,
                            const std::pair<Sockaddr, std::string>& leader_addr_and_name,
                            const master::ConnectToMasterResponsePB& connect_response,
                            rpc::CredentialsPolicy cred_policy);

  // Asynchronously sets 'master_proxy_' to the leader master by
  // cycling through servers listed in 'master_server_addrs_' until
  // one responds with a Raft configuration that contains the leader
  // master or 'deadline' expires.
  //
  // Invokes 'cb' with the appropriate status when finished.
  //
  // Works with both a distributed and non-distributed configuration.
  void ConnectToClusterAsync(KuduClient* client,
                             const MonoTime& deadline,
                             const StatusCallback& cb,
                             rpc::CredentialsPolicy creds_policy);

  // Synchronous version of ConnectToClusterAsync method above.
  //
  // NOTE: since this uses a Synchronizer, this may not be invoked by
  // a method that's on a reactor thread.
  Status ConnectToCluster(
      KuduClient* client,
      const MonoTime& deadline,
      rpc::CredentialsPolicy creds_policy = rpc::CredentialsPolicy::ANY_CREDENTIALS);

  std::shared_ptr<master::MasterServiceProxy> master_proxy() const;

  HostPort leader_master_hostport() const;

  std::vector<HostPort> master_hostports() const;

  uint64_t GetLatestObservedTimestamp() const;

  void UpdateLatestObservedTimestamp(uint64_t timestamp);

  // The unique id of this client.
  std::string client_id_;

  // The location of this client. This is an empty string if a location has not
  // been assigned by the leader master. Protected by 'leader_master_lock_'.
  std::string location_;

  // The user credentials of the client. This field is constant after the client
  // is built.
  rpc::UserCredentials user_credentials_;

  // The request tracker for this client.
  scoped_refptr<rpc::RequestTracker> request_tracker_;

  std::shared_ptr<rpc::Messenger> messenger_;
  gscoped_ptr<DnsResolver> dns_resolver_;
  scoped_refptr<internal::MetaCache> meta_cache_;

  // Set of hostnames and IPs on the local host.
  // This is initialized at client startup.
  std::unordered_set<std::string> local_host_names_;

  // Options the client was built with.
  std::vector<std::string> master_server_addrs_;
  MonoDelta default_admin_operation_timeout_;
  MonoDelta default_rpc_timeout_;

  // The host port of the leader master. This is set in
  // ConnectedToClusterCb, which is invoked as a callback by
  // ConnectToClusterAsync.
  HostPort leader_master_hostport_;

  // The master RPC host ports as configured on the most recently connected to
  // leader master in ConnectedToClusterCb.
  std::vector<HostPort> master_hostports_;

  // The Hive Metastore configuration of the most recently connected to leader
  // master, or an empty string if the leader master is not configured to
  // integrate with a Hive Metastore.
  std::string hive_metastore_uris_;
  bool hive_metastore_sasl_enabled_;
  std::string hive_metastore_uuid_;

  // Proxy to the leader master.
  std::shared_ptr<master::MasterServiceProxy> master_proxy_;

  // Ref-counted RPC instance: since 'ConnectToClusterAsync' call
  // is asynchronous, we need to hold a reference in this class
  // itself, as to avoid a "use-after-free" scenario.
  scoped_refptr<internal::ConnectToClusterRpc> leader_master_rpc_any_creds_;
  std::vector<StatusCallback> leader_master_callbacks_any_creds_;
  scoped_refptr<internal::ConnectToClusterRpc> leader_master_rpc_primary_creds_;
  std::vector<StatusCallback> leader_master_callbacks_primary_creds_;

  // Protects 'leader_master_rpc_{any,primary}_creds_',
  // 'leader_master_hostport_', 'master_hostports_', 'master_proxy_', and
  // 'location_'.
  //
  // See: KuduClient::Data::ConnectToClusterAsync for a more
  // in-depth explanation of why this is needed and how it works.
  mutable simple_spinlock leader_master_lock_;

  AtomicInt<uint64_t> latest_observed_timestamp_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

// Retry helper, takes a function like: Status funcName(const MonoTime& deadline, bool *retry, ...)
// The function should set the retry flag (default true) if the function should
// be retried again. On retry == false the return status of the function will be
// returned to the caller, otherwise a Status::Timeout() will be returned.
// If the deadline is already expired, no attempt will be made.
Status RetryFunc(const MonoTime& deadline,
                 const std::string& retry_msg,
                 const std::string& timeout_msg,
                 const boost::function<Status(const MonoTime&, bool*)>& func);

// Set logging verbose level through environment variable.
void SetVerboseLevelFromEnvVar();
extern const char* kVerboseEnvVar;

} // namespace client
} // namespace kudu

