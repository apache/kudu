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
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/container/vector.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>

#include "kudu/client/authz_token_cache.h"
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

DECLARE_int32(dns_resolver_max_threads_num);
DECLARE_uint32(dns_resolver_cache_capacity_mb);
DECLARE_uint32(dns_resolver_cache_ttl_sec);

using boost::container::small_vector;
using std::map;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

namespace security {
class SignedTokenPB;
} // namespace security

using master::AlterTableRequestPB;
using master::AlterTableResponsePB;
using master::ConnectToMasterResponsePB;
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
using master::ListTabletServersResponsePB;
using master::ListTabletServersRequestPB;
using master::MasterFeatures;
using master::MasterServiceProxy;
using master::TableIdentifierPB;
using rpc::BackoffType;
using rpc::CredentialsPolicy;
using security::SignedTokenPB;
using strings::Substitute;

namespace client {

using internal::AsyncLeaderMasterRpc;
using internal::ConnectToClusterRpc;
using internal::RemoteTablet;
using internal::RemoteTabletServer;

Status RetryFunc(const MonoTime& deadline,
                 const string& retry_msg,
                 const string& timeout_msg,
                 const std::function<Status(const MonoTime&, bool*)>& func) {
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
    : dns_resolver_(new DnsResolver(
          FLAGS_dns_resolver_max_threads_num,
          FLAGS_dns_resolver_cache_capacity_mb * 1024 * 1024,
          MonoDelta::FromSeconds(FLAGS_dns_resolver_cache_ttl_sec))),
      hive_metastore_sasl_enabled_(false),
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

RemoteTabletServer* KuduClient::Data::SelectTServer(
    const scoped_refptr<RemoteTablet>& rt,
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
      // 1. If there is a replica local to the client according to its IP and
      //    assigned location, pick it. If there are multiple, pick a random one.
      // 2. Otherwise, if there is a replica in the same assigned location,
      //    pick it. If there are multiple, pick a random one.
      // 3. If there are no local replicas or replicas in the same location,
      //    pick a random replica.
      // TODO(wdberkeley): Eventually, the client might use the hierarchical
      // structure of a location to determine proximity.
      // NOTE: this is the same logic implemented in RemoteTablet.java.
      const string client_location = location();
      small_vector<RemoteTabletServer*, 1> local;
      small_vector<RemoteTabletServer*, 3> same_location;
      local.reserve(filtered.size());
      same_location.reserve(filtered.size());
      for (RemoteTabletServer* rts : filtered) {
        bool ts_same_location = !client_location.empty() && client_location == rts->location();
        // Only consider a server "local" if the client is in the same
        // location, or if there is missing location info.
        if (client_location.empty() || rts->location().empty() ||
            ts_same_location) {
          if (IsTabletServerLocal(*rts)) {
            local.push_back(rts);
          }
        }
        if (ts_same_location) {
          same_location.push_back(rts);
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
  return RetryFunc(
      deadline,
      "Waiting on Create Table to be completed",
      "Timed out waiting for Table Creation",
      [&](const MonoTime& deadline, bool* retry) {
        return IsCreateTableInProgress(client, table, deadline, retry);
      });
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
  return RetryFunc(
      deadline,
      "Waiting on Alter Table to be completed",
      "Timed out waiting for AlterTable",
      [&](const MonoTime& deadline, bool* retry) {
        return IsAlterTableInProgress(client, table, deadline, retry);
      });
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
  RETURN_NOT_OK_PREPEND(dns_resolver_->ResolveAddresses(HostPort(hostname, 0),
                                                        &addresses),
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
  if (ContainsKey(local_host_names_, hp.host())) {
    return true;
  }

  // It may be that HostPort is a numeric form (non-reversable) address like
  // 127.0.1.1, etc. In that case we can still consider it local.
  Sockaddr addr;
  return addr.ParseFromNumericHostPort(hp).ok() && addr.IsAnyLocalAddress();
}

bool KuduClient::Data::IsTabletServerLocal(const RemoteTabletServer& rts) const {
  vector<HostPort> host_ports;
  rts.GetHostPorts(&host_ports);
  for (const HostPort& hp : host_ports) {
    if (IsLocalHostPort(hp)) return true;
  }
  return false;
}

Status KuduClient::Data::ListTabletServers(KuduClient* client,
                                           const MonoTime& deadline,
                                           const ListTabletServersRequestPB& req,
                                           ListTabletServersResponsePB* resp) const {
  Synchronizer sync;
  AsyncLeaderMasterRpc<ListTabletServersRequestPB, ListTabletServersResponsePB> rpc(
      deadline, client, BackoffType::EXPONENTIAL, req, resp,
      &MasterServiceProxy::ListTabletServersAsync, "ListTabletServers",
      sync.AsStatusCallback(), {});
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  if (resp->has_error()) {
    return StatusFromPB(resp->error().status());
  }
  return Status::OK();
}

void KuduClient::Data::StoreAuthzToken(const string& table_id,
                                       const SignedTokenPB& token) {
  authz_token_cache_.Put(table_id, token);
}

bool KuduClient::Data::FetchCachedAuthzToken(const string& table_id, SignedTokenPB* token) {
  return authz_token_cache_.Fetch(table_id, token);
}

Status KuduClient::Data::OpenTable(KuduClient* client,
                                   const TableIdentifierPB& table_identifier,
                                   client::sp::shared_ptr<KuduTable>* table) {
  KuduSchema schema;
  string table_id;
  string table_name;
  int num_replicas;
  PartitionSchema partition_schema;
  map<string, string> extra_configs;
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout_;
  RETURN_NOT_OK(GetTableSchema(client,
                               deadline,
                               table_identifier,
                               &schema,
                               &partition_schema,
                               &table_id,
                               &table_name,
                               &num_replicas,
                               &extra_configs));

  // When the table name is specified, use the caller-provided table name.
  // This reduces surprises, e.g., when the HMS integration is on and table
  // names are case-insensitive.
  string effective_table_name = table_identifier.has_table_name() ?
      table_identifier.table_name() :
      table_name;

  // TODO(wdberkeley): In the future, probably will look up the table in some
  //                   map to reuse KuduTable instances.
  table->reset(new KuduTable(client->shared_from_this(),
                             effective_table_name, table_id, num_replicas,
                             schema, partition_schema, extra_configs));

  // When opening a table, clear the existing cached non-covered range entries.
  // This avoids surprises where a new table instance won't be able to see the
  // current range partitions of a table for up to the ttl.
  meta_cache_->ClearNonCoveredRangeEntries(table_id);

  return Status::OK();
}

Status KuduClient::Data::GetTableSchema(KuduClient* client,
                                        const MonoTime& deadline,
                                        const TableIdentifierPB& table,
                                        KuduSchema* schema,
                                        PartitionSchema* partition_schema,
                                        std::string* table_id,
                                        std::string* table_name,
                                        int* num_replicas,
                                        map<string, string>* extra_configs) {
  GetTableSchemaRequestPB req;
  GetTableSchemaResponsePB resp;

  if (table.has_table_id()) {
    req.mutable_table()->set_table_id(table.table_id());
  } else {
    req.mutable_table()->set_table_name(table.table_name());
  }
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
  if (table_name) {
    *table_name = resp.table_name();
  }
  if (table_id) {
    *table_id = resp.table_id();
  }
  if (num_replicas) {
    *num_replicas = resp.num_replicas();
  }
  if (extra_configs) {
    map<string, string> result(resp.extra_configs().begin(), resp.extra_configs().end());
    *extra_configs = std::move(result);
  }
  // Cache the authz token if the response came with one. It might not have one
  // if running against an older master that does not support authz tokens.
  if (resp.has_authz_token()) {
    StoreAuthzToken(resp.table_id(), resp.authz_token());
  }
  return Status::OK();
}

void KuduClient::Data::ConnectedToClusterCb(
    const Status& status,
    const pair<Sockaddr, string>& leader_addr_and_name,
    const ConnectToMasterResponsePB& connect_response,
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
    cb(status);
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
      // TODO(todd): Until address resolution is done asynchronously, we need
      // to allow waiting as some callers may be reactor threads.
      ThreadRestrictions::ScopedAllowWait allow_wait;
      s = dns_resolver_->ResolveAddresses(hp, &addrs);
    }
    if (!s.ok()) {
      cb(s);
      return;
    }
    if (addrs.empty()) {
      cb(Status::InvalidArgument(Substitute("No master address specified by '$0'",
                                            master_server_addr)));
      return;
    }
    if (addrs.size() > 1) {
      KLOG_EVERY_N_SECS(INFO, 1)
          << Substitute("Specified master server address '$0' resolved to multiple IPs $1. "
                        "Connecting to each one of them.",
                        master_server_addr, Sockaddr::ToCommaSeparatedString(addrs));
    }
    for (const Sockaddr& addr : addrs) {
      master_addrs_with_names.emplace_back(addr, hp.host());
    }
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
            [this, creds_policy](const Status& status,
                                 const std::pair<Sockaddr, string>& leader_master,
                                 const ConnectToMasterResponsePB& connect_response) {
              this->ConnectedToClusterCb(status, leader_master, connect_response, creds_policy);
            },
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

Status KuduClient::Data::RetrieveAuthzToken(const KuduTable* table,
                                            const MonoTime& deadline) {
  Synchronizer sync;
  RetrieveAuthzTokenAsync(table, sync.AsStatusCallback(), deadline);
  return sync.Wait();
}

void KuduClient::Data::RetrieveAuthzTokenAsync(const KuduTable* table,
                                               const StatusCallback& cb,
                                               const MonoTime& deadline) {
  DCHECK(deadline.Initialized());
  authz_token_cache_.RetrieveNewAuthzToken(table, cb, deadline);
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
