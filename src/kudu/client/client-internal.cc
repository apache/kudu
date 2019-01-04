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

#include "kudu/client/client-internal.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/function.hpp>
#include <glog/logging.h>

#include "kudu/client/master_proxy_rpc.h"
#include "kudu/client/master_rpc.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/cert.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/async_util.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/thread_restrictions.h"

using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

using master::AlterTableRequestPB;
using master::AlterTableResponsePB;
using master::CreateTableRequestPB;
using master::CreateTableResponsePB;
using master::DeleteTableRequestPB;
using master::DeleteTableResponsePB;
using master::GetTableSchemaRequestPB;
using master::GetTableSchemaResponsePB;
using master::IsAlterTableDoneRequestPB;
using master::IsAlterTableDoneResponsePB;
using master::IsCreateTableDoneRequestPB;
using master::IsCreateTableDoneResponsePB;
using master::MasterFeatures;
using master::MasterServiceProxy;
using master::TableIdentifierPB;
using rpc::BackoffType;
using rpc::CredentialsPolicy;
using strings::Substitute;

namespace client {

using internal::AsyncLeaderMasterRpc;
using internal::ConnectToClusterRpc;
using internal::RemoteTablet;
using internal::RemoteTabletServer;

Status RetryFunc(const MonoTime& deadline,
                 const string& retry_msg,
                 const string& timeout_msg,
                 const boost::function<Status(const MonoTime&, bool*)>& func) {
  DCHECK(deadline.Initialized());

  if (deadline < MonoTime::Now()) {
    return Status::TimedOut(timeout_msg);
  }

  double wait_secs = 0.001;
  const double kMaxSleepSecs = 2;
  while (1) {
    MonoTime func_stime = MonoTime::Now();
    bool retry = true;
    Status s = func(deadline, &retry);
    if (!retry) {
      return s;
    }
    MonoTime now = MonoTime::Now();
    MonoDelta func_time = now - func_stime;

    VLOG(1) << retry_msg << " status=" << s.ToString();
    double secs_remaining = std::numeric_limits<double>::max();
    if (deadline.Initialized()) {
      secs_remaining = (deadline - now).ToSeconds();
    }
    wait_secs = std::min(wait_secs * 1.25, kMaxSleepSecs);

    // We assume that the function will take the same amount of time to run
    // as it did in the previous attempt. If we don't have enough time left
    // to sleep and run it again, we don't bother sleeping and retrying.
    if (wait_secs + func_time.ToSeconds() > secs_remaining) {
      break;
    }

    VLOG(1) << "Waiting for " << HumanReadableElapsedTime::ToShortString(wait_secs)
            << " before retrying...";
    SleepFor(MonoDelta::FromSeconds(wait_secs));
  }

  return Status::TimedOut(timeout_msg);
}

KuduClient::Data::Data()
    : hive_metastore_sasl_enabled_(false),
      latest_observed_timestamp_(KuduClient::kNoTimestamp) {
}

KuduClient::Data::~Data() {
  // Workaround for KUDU-956: the user may close a KuduClient while a flush
  // is still outstanding. In that case, the flush's callback will be the last
  // holder of the client reference, causing it to shut down on the reactor
  // thread. This triggers a ThreadRestrictions crash. It's not critical to
  // fix urgently, because typically once a client is shutting down, latency
  // jitter on the reactor is not a big deal (and DNS resolutions are not in flight).
  ThreadRestrictions::ScopedAllowWait allow_wait;
  dns_resolver_.reset();
}

RemoteTabletServer* KuduClient::Data::SelectTServer(const scoped_refptr<RemoteTablet>& rt,
                                                    const ReplicaSelection selection,
                                                    const set<string>& blacklist,
                                                    vector<RemoteTabletServer*>* candidates) const {
  RemoteTabletServer* ret = nullptr;
  candidates->clear();
  switch (selection) {
    case LEADER_ONLY: {
      ret = rt->LeaderTServer();
      if (ret != nullptr) {
        candidates->push_back(ret);
        if (ContainsKey(blacklist, ret->permanent_uuid())) {
          ret = nullptr;
        }
      }
      break;
    }
    case CLOSEST_REPLICA:
    case FIRST_REPLICA: {
      rt->GetRemoteTabletServers(candidates);
      // Exclude all the blacklisted candidates.
      vector<RemoteTabletServer*> filtered;
      for (RemoteTabletServer* rts : *candidates) {
        if (!ContainsKey(blacklist, rts->permanent_uuid())) {
          filtered.push_back(rts);
        } else {
          VLOG(1) << "Excluding blacklisted tserver " << rts->permanent_uuid();
        }
      }
      if (selection == FIRST_REPLICA) {
        if (!filtered.empty()) {
          ret = filtered[0];
        }
        break;
      }
      // Choose a replica as follows:
      // 1. If there is a replica local to the client, pick it. If there are
      // multiple, pick a random one.
      // 2. Otherwise, if there is a replica in the same location, pick it. If
      // there are multiple, pick a random one.
      // 3. If there are no local replicas or replicas in the same location,
      // pick a random replica.
      // TODO(wdberkeley): Eventually, the client might use the hierarchical
      // structure of a location to determine proximity.
      const string client_location = location();
      vector<RemoteTabletServer*> local;
      vector<RemoteTabletServer*> same_location;
      local.reserve(filtered.size());
      same_location.reserve(filtered.size());
      for (RemoteTabletServer* rts : filtered) {
        if (IsTabletServerLocal(*rts)) {
          local.push_back(rts);
        }
        if (!client_location.empty()) {
          const string replica_location = rts->location();
          if (client_location == replica_location) {
            same_location.push_back(rts);
          }
        }
      }
      if (!local.empty()) {
        ret = local[rand() % local.size()];
      } else if (!same_location.empty()) {
        ret = same_location[rand() % same_location.size()];
      } else if (!filtered.empty()) {
        ret = filtered[rand() % filtered.size()];
      }
      break;
    }
    default: {
      LOG(FATAL) << "Unknown ReplicaSelection value " << selection;
      break;
    }
  }

  return ret;
}

Status KuduClient::Data::GetTabletServer(KuduClient* client,
                                         const scoped_refptr<RemoteTablet>& rt,
                                         ReplicaSelection selection,
                                         const set<string>& blacklist,
                                         vector<RemoteTabletServer*>* candidates,
                                         RemoteTabletServer** ts) {
  // TODO: write a proper async version of this for async client.
  RemoteTabletServer* ret = SelectTServer(rt, selection, blacklist, candidates);
  if (PREDICT_FALSE(ret == nullptr)) {
    // Construct a blacklist string if applicable.
    string blacklist_string = "";
    if (!blacklist.empty()) {
      blacklist_string = Substitute("(blacklist replicas $0)", JoinStrings(blacklist, ", "));
    }
    return Status::ServiceUnavailable(
        Substitute("No $0 for tablet $1 $2",
                   selection == LEADER_ONLY ? "LEADER" : "replicas",
                   rt->tablet_id(),
                   blacklist_string));
  }
  Synchronizer s;
  ret->InitProxy(client, s.AsStatusCallback());
  RETURN_NOT_OK(s.Wait());

  *ts = ret;
  return Status::OK();
}

Status KuduClient::Data::CreateTable(KuduClient* client,
                                     const CreateTableRequestPB& req,
                                     CreateTableResponsePB* resp,
                                     const MonoTime& deadline,
                                     bool has_range_partition_bounds) {
  vector<uint32_t> features;
  if (has_range_partition_bounds) {
    features.push_back(MasterFeatures::RANGE_PARTITION_BOUNDS);
  }
  Synchronizer sync;
  AsyncLeaderMasterRpc<CreateTableRequestPB, CreateTableResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, resp,
      &MasterServiceProxy::CreateTableAsync, "CreateTable", sync.AsStatusCallback(),
      std::move(features));
  rpc.SendRpc();
  return sync.Wait();
}

Status KuduClient::Data::IsCreateTableInProgress(
    KuduClient* client,
    TableIdentifierPB table,
    const MonoTime& deadline,
    bool* create_in_progress) {
  IsCreateTableDoneRequestPB req;
  IsCreateTableDoneResponsePB resp;
  *req.mutable_table() = std::move(table);

  // TODO(aserbin): Add client rpc timeout and use
  // 'default_admin_operation_timeout_' as the default timeout for all
  // admin operations.
  Synchronizer sync;
  AsyncLeaderMasterRpc<IsCreateTableDoneRequestPB, IsCreateTableDoneResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, &resp,
      &MasterServiceProxy::IsCreateTableDoneAsync,
      "IsCreateTableDone", sync.AsStatusCallback(), {});
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  *create_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::Data::WaitForCreateTableToFinish(
    KuduClient* client,
    TableIdentifierPB table,
    const MonoTime& deadline) {
  return RetryFunc(deadline,
                   "Waiting on Create Table to be completed",
                   "Timed out waiting for Table Creation",
                   boost::bind(&KuduClient::Data::IsCreateTableInProgress,
                               this, client, std::move(table), _1, _2));
}

Status KuduClient::Data::DeleteTable(KuduClient* client,
                                     const string& table_name,
                                     const MonoTime& deadline,
                                     bool modify_external_catalogs) {
  DeleteTableRequestPB req;
  DeleteTableResponsePB resp;

  req.mutable_table()->set_table_name(table_name);
  req.set_modify_external_catalogs(modify_external_catalogs);
  Synchronizer sync;
  AsyncLeaderMasterRpc<DeleteTableRequestPB, DeleteTableResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, &resp,
      &MasterServiceProxy::DeleteTableAsync, "DeleteTable", sync.AsStatusCallback(), {});
  rpc.SendRpc();
  return sync.Wait();
}

Status KuduClient::Data::AlterTable(KuduClient* client,
                                    const AlterTableRequestPB& req,
                                    AlterTableResponsePB* resp,
                                    const MonoTime& deadline,
                                    bool has_add_drop_partition) {
  vector<uint32_t> required_feature_flags;
  if (has_add_drop_partition) {
    required_feature_flags.push_back(MasterFeatures::ADD_DROP_RANGE_PARTITIONS);
  }
  Synchronizer sync;
  AsyncLeaderMasterRpc<AlterTableRequestPB, AlterTableResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, resp,
      &MasterServiceProxy::AlterTableAsync, "AlterTable", sync.AsStatusCallback(),
      std::move(required_feature_flags));
  rpc.SendRpc();
  return sync.Wait();
}

Status KuduClient::Data::IsAlterTableInProgress(
    KuduClient* client,
    TableIdentifierPB table,
    const MonoTime& deadline,
    bool* alter_in_progress) {
  IsAlterTableDoneRequestPB req;
  IsAlterTableDoneResponsePB resp;
  *req.mutable_table() = std::move(table);

  Synchronizer sync;
  AsyncLeaderMasterRpc<IsAlterTableDoneRequestPB, IsAlterTableDoneResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, &resp,
      &MasterServiceProxy::IsAlterTableDoneAsync, "IsAlterTableInProgres",
      sync.AsStatusCallback(), {});
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  *alter_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::Data::WaitForAlterTableToFinish(
    KuduClient* client,
    TableIdentifierPB table,
    const MonoTime& deadline) {
  return RetryFunc(deadline,
                   "Waiting on Alter Table to be completed",
                   "Timed out waiting for AlterTable",
                   boost::bind(&KuduClient::Data::IsAlterTableInProgress,
                               this, client, std::move(table), _1, _2));
}

Status KuduClient::Data::InitLocalHostNames() {
  // Currently, we just use our configured hostname, and resolve it to come up with
  // a list of potentially local hosts. It would be better to iterate over all of
  // the local network adapters. See KUDU-327.
  string hostname;
  RETURN_NOT_OK(GetFQDN(&hostname));

  // We don't want to consider 'localhost' to be local - otherwise if a misconfigured
  // server reports its own name as localhost, all clients will hammer it.
  if (hostname != "localhost" && hostname != "localhost.localdomain") {
    local_host_names_.insert(hostname);
    VLOG(1) << "Considering host " << hostname << " local";
  }

  vector<Sockaddr> addresses;
  RETURN_NOT_OK_PREPEND(HostPort(hostname, 0).ResolveAddresses(&addresses),
                        Substitute("Could not resolve local host name '$0'", hostname));

  for (const Sockaddr& addr : addresses) {
    // Similar to above, ignore local or wildcard addresses.
    if (addr.IsWildcard()) continue;
    if (addr.IsAnyLocalAddress()) continue;

    VLOG(1) << "Considering host " << addr.host() << " local";
    local_host_names_.insert(addr.host());
  }

  return Status::OK();
}

bool KuduClient::Data::IsLocalHostPort(const HostPort& hp) const {
  return ContainsKey(local_host_names_, hp.host());
}

bool KuduClient::Data::IsTabletServerLocal(const RemoteTabletServer& rts) const {
  vector<HostPort> host_ports;
  rts.GetHostPorts(&host_ports);
  for (const HostPort& hp : host_ports) {
    if (IsLocalHostPort(hp)) return true;
  }
  return false;
}

Status KuduClient::Data::GetTableSchema(KuduClient* client,
                                        const string& table_name,
                                        const MonoTime& deadline,
                                        KuduSchema* schema,
                                        PartitionSchema* partition_schema,
                                        string* table_id,
                                        int* num_replicas) {
  GetTableSchemaRequestPB req;
  GetTableSchemaResponsePB resp;

  req.mutable_table()->set_table_name(table_name);
  Synchronizer sync;
  AsyncLeaderMasterRpc<GetTableSchemaRequestPB, GetTableSchemaResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, &resp,
      &MasterServiceProxy::GetTableSchemaAsync, "GetTableSchema", sync.AsStatusCallback(), {});
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  // Parse the server schema out of the response.
  unique_ptr<Schema> new_schema(new Schema());
  RETURN_NOT_OK(SchemaFromPB(resp.schema(), new_schema.get()));

  // Parse the server partition schema out of the response.
  PartitionSchema new_partition_schema;
  RETURN_NOT_OK(PartitionSchema::FromPB(resp.partition_schema(),
                                        *new_schema,
                                        &new_partition_schema));
  if (schema) {
    delete schema->schema_;
    schema->schema_ = new_schema.release();
  }
  if (partition_schema) {
    *partition_schema = std::move(new_partition_schema);
  }
  if (table_id) {
    *table_id = resp.table_id();
  }
  if (num_replicas) {
    *num_replicas = resp.num_replicas();
  }
  return Status::OK();
}

void KuduClient::Data::ConnectedToClusterCb(
    const Status& status,
    const pair<Sockaddr, string>& leader_addr_and_name,
    const master::ConnectToMasterResponsePB& connect_response,
    CredentialsPolicy cred_policy) {

  const auto& leader_addr = leader_addr_and_name.first;
  const auto& leader_hostname = leader_addr_and_name.second;

  // Ensure that all of the CAs reported by the master are trusted
  // in our local TLS configuration.
  if (status.ok()) {
    for (const string& cert_der : connect_response.ca_cert_der()) {
      security::Cert cert;
      Status s = cert.FromString(cert_der, security::DataFormat::DER);
      if (!s.ok()) {
        KLOG_EVERY_N_SECS(WARNING, 5) << "Master " << leader_addr.ToString()
                                     << " provided an unparseable CA cert: "
                                     << s.ToString();
        continue;
      }
      s = messenger_->mutable_tls_context()->AddTrustedCertificate(cert);
      if (!s.ok()) {
        KLOG_EVERY_N_SECS(WARNING, 5) << "Master " << leader_addr.ToString()
                                     << " provided a cert that could not be trusted: "
                                     << s.ToString();
        continue;
      }
    }
  }

  // Adopt the authentication token from the response, if it's been set.
  if (connect_response.has_authn_token()) {
    messenger_->set_authn_token(connect_response.authn_token());
    VLOG(2) << "Received and adopted authn token";
  }

  vector<StatusCallback> cbs;
  {
    std::lock_guard<simple_spinlock> l(leader_master_lock_);
    if (cred_policy == CredentialsPolicy::PRIMARY_CREDENTIALS) {
      leader_master_rpc_primary_creds_.reset();
      cbs.swap(leader_master_callbacks_primary_creds_);
    } else {
      leader_master_rpc_any_creds_.reset();
      cbs.swap(leader_master_callbacks_any_creds_);
    }

    if (status.ok()) {
      leader_master_hostport_ = HostPort(leader_hostname, leader_addr.port());
      master_hostports_.clear();
      for (const auto& hostport : connect_response.master_addrs()) {
        master_hostports_.emplace_back(HostPort(hostport.host(), hostport.port()));
      }

      const auto& hive_config = connect_response.hms_config();
      hive_metastore_uris_ = hive_config.hms_uris();
      hive_metastore_sasl_enabled_ = hive_config.hms_sasl_enabled();
      hive_metastore_uuid_ = hive_config.hms_uuid();

      location_ = connect_response.client_location();

      master_proxy_.reset(new MasterServiceProxy(messenger_, leader_addr, leader_hostname));
      master_proxy_->set_user_credentials(user_credentials_);
    }
  }

  for (const StatusCallback& cb : cbs) {
    cb.Run(status);
  }
}

Status KuduClient::Data::ConnectToCluster(KuduClient* client,
                                          const MonoTime& deadline,
                                          CredentialsPolicy creds_policy) {
  Synchronizer sync;
  ConnectToClusterAsync(client, deadline, sync.AsStatusCallback(), creds_policy);
  return sync.Wait();
}

void KuduClient::Data::ConnectToClusterAsync(KuduClient* client,
                                             const MonoTime& deadline,
                                             const StatusCallback& cb,
                                             CredentialsPolicy creds_policy) {
  DCHECK(deadline.Initialized());

  vector<pair<Sockaddr, string>> master_addrs_with_names;
  for (const string& master_server_addr : master_server_addrs_) {
    vector<Sockaddr> addrs;
    HostPort hp;
    Status s = hp.ParseString(master_server_addr, master::Master::kDefaultPort);
    if (s.ok()) {
      // TODO(todd): Do address resolution asynchronously as well.
      s = hp.ResolveAddresses(&addrs);
    }
    if (!s.ok()) {
      cb.Run(s);
      return;
    }
    if (addrs.empty()) {
      cb.Run(Status::InvalidArgument(Substitute("No master address specified by '$0'",
                                                master_server_addr)));
      return;
    }
    if (addrs.size() > 1) {
      KLOG_EVERY_N_SECS(WARNING, 1)
          << "Specified master server address '" << master_server_addr << "' "
          << "resolved to multiple IPs. Using " << addrs[0].ToString();
    }
    master_addrs_with_names.emplace_back(addrs[0], hp.host());
  }

  // This ensures that no more than one ConnectToClusterRpc of each credentials
  // policy is in flight at any time -- there isn't much sense in requesting
  // this information in parallel, since the requests should end up with the
  // same result. Instead, simply piggy-back onto the existing request by adding
  // our the callback to leader_master_callbacks_{any_creds,primary_creds}_.
  std::unique_lock<simple_spinlock> l(leader_master_lock_);

  // Optimize sending out a new request in the presence of already existing
  // requests to the leader master. Depending on the credentials policy for the
  // request, we select different strategies:
  //   * If the new request is of ANY_CREDENTIALS policy, piggy-back it on any
  //     leader master request which is progress.
  //   * If the new request is of PRIMARY_CREDENTIALS, it can by piggy-backed
  //     only on the request of the same type.
  //
  // If this is a request to re-acquire authn token, we allow it to run in
  // parallel with other requests of ANY_CREDENTIALS_TYPE policy, if any.
  // Otherwise it's hard to guarantee that the token re-acquisition request
  // will be sent: there might be other concurrent requests to leader master,
  // they might have different timeout settings and they are retried
  // independently of each other.
  DCHECK(creds_policy == CredentialsPolicy::ANY_CREDENTIALS ||
         creds_policy == CredentialsPolicy::PRIMARY_CREDENTIALS);

  // Decide on which call to piggy-back the callback: if a call with primary
  // credentials is available, piggy-back on that. Otherwise, piggy-back on
  // any-credentials-policy request.
  if (leader_master_rpc_primary_creds_) {
    // Piggy-back on an existing connection with primary credentials.
    leader_master_callbacks_primary_creds_.push_back(cb);
  } else if (leader_master_rpc_any_creds_ &&
             creds_policy == CredentialsPolicy::ANY_CREDENTIALS) {
    // Piggy-back on an existing connection with any credentials.
    leader_master_callbacks_any_creds_.push_back(cb);
  } else {
    // It's time to create a new request which would satisfy the credentials
    // policy.
    scoped_refptr<internal::ConnectToClusterRpc> rpc(
        new internal::ConnectToClusterRpc(
            std::bind(&KuduClient::Data::ConnectedToClusterCb, this,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3,
            creds_policy),
        std::move(master_addrs_with_names),
        deadline,
        client->default_rpc_timeout(),
        messenger_,
        user_credentials_,
        creds_policy));

    if (creds_policy == CredentialsPolicy::PRIMARY_CREDENTIALS) {
      DCHECK(!leader_master_rpc_primary_creds_);
      leader_master_rpc_primary_creds_ = rpc;
      leader_master_callbacks_primary_creds_.push_back(cb);
    } else {
      DCHECK(!leader_master_rpc_any_creds_);
      leader_master_rpc_any_creds_ = rpc;
      leader_master_callbacks_any_creds_.push_back(cb);
    }
    l.unlock();
    rpc->SendRpc();
  }
}

HostPort KuduClient::Data::leader_master_hostport() const {
  std::lock_guard<simple_spinlock> l(leader_master_lock_);
  return leader_master_hostport_;
}

vector<HostPort> KuduClient::Data::master_hostports() const {
  std::lock_guard<simple_spinlock> l(leader_master_lock_);
  return master_hostports_;
}

string KuduClient::Data::location() const {
  std::lock_guard<simple_spinlock> l(leader_master_lock_);
  return location_;
}

shared_ptr<master::MasterServiceProxy> KuduClient::Data::master_proxy() const {
  std::lock_guard<simple_spinlock> l(leader_master_lock_);
  return master_proxy_;
}

uint64_t KuduClient::Data::GetLatestObservedTimestamp() const {
  return latest_observed_timestamp_.Load();
}

void KuduClient::Data::UpdateLatestObservedTimestamp(uint64_t timestamp) {
  latest_observed_timestamp_.StoreMax(timestamp);
}

} // namespace client
} // namespace kudu
