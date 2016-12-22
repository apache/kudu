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
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kudu/client/meta_cache.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/master/master.h"
#include "kudu/master/master_rpc.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/thread_restrictions.h"

using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

using consensus::RaftPeerPB;
using master::AlterTableRequestPB;
using master::AlterTableResponsePB;
using master::CreateTableRequestPB;
using master::CreateTableResponsePB;
using master::DeleteTableRequestPB;
using master::DeleteTableResponsePB;
using master::GetLeaderMasterRpc;
using master::GetTableSchemaRequestPB;
using master::GetTableSchemaResponsePB;
using master::GetTabletLocationsRequestPB;
using master::GetTabletLocationsResponsePB;
using master::IsAlterTableDoneRequestPB;
using master::IsAlterTableDoneResponsePB;
using master::IsCreateTableDoneRequestPB;
using master::IsCreateTableDoneResponsePB;
using master::ListTablesRequestPB;
using master::ListTablesResponsePB;
using master::ListTabletServersRequestPB;
using master::ListTabletServersResponsePB;
using master::MasterErrorPB;
using master::MasterFeatures;
using master::MasterServiceProxy;
using rpc::ErrorStatusPB;
using rpc::Rpc;
using rpc::RpcController;
using strings::Substitute;

namespace client {

using internal::GetTableSchemaRpc;
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

template<class ReqClass, class RespClass>
Status KuduClient::Data::SyncLeaderMasterRpc(
    const MonoTime& deadline,
    KuduClient* client,
    const ReqClass& req,
    RespClass* resp,
    const char* func_name,
    const boost::function<Status(MasterServiceProxy*,
                                 const ReqClass&,
                                 RespClass*,
                                 RpcController*)>& func,
    vector<uint32_t> required_feature_flags) {
  DCHECK(deadline.Initialized());

  for (int num_attempts = 0;; num_attempts++) {
    RpcController rpc;

    // Sleep if necessary.
    if (num_attempts > 0) {
      SleepFor(ComputeExponentialBackoff(num_attempts));
    }

    // Have we already exceeded our deadline?
    MonoTime now = MonoTime::Now();
    if (deadline < now) {
      return Status::TimedOut(Substitute("$0 timed out after deadline expired",
                                         func_name));
    }

    // The RPC's deadline is intentionally earlier than the overall
    // deadline so that we reserve some time with which to find a new
    // leader master and retry before the overall deadline expires.
    //
    // TODO: KUDU-683 tracks cleanup for this.
    MonoTime rpc_deadline = now + client->default_rpc_timeout();
    rpc.set_deadline(MonoTime::Earliest(rpc_deadline, deadline));

    for (uint32_t required_feature_flag : required_feature_flags) {
      rpc.RequireServerFeature(required_feature_flag);
    }

    // Take a ref to the proxy in case it disappears from underneath us.
    shared_ptr<MasterServiceProxy> proxy(master_proxy());

    Status s = func(proxy.get(), req, resp, &rpc);
    if (s.IsRemoteError()) {
      const ErrorStatusPB* err = rpc.error_response();
      if (err &&
          err->has_code() &&
          err->code() == ErrorStatusPB::ERROR_SERVER_TOO_BUSY) {
        continue;
      }
    }

    if (s.IsNetworkError()) {
      KLOG_EVERY_N_SECS(WARNING, 1)
          << "Unable to send the request (" << SecureShortDebugString(req)
          << ") to leader Master (" << leader_master_hostport().ToString() << "): "
          << s.ToString();
      if (client->IsMultiMaster()) {
        LOG(INFO) << "Determining the new leader Master and retrying...";
        WARN_NOT_OK(SetMasterServerProxy(client, deadline),
                    "Unable to determine the new leader Master");
        continue;
      }
    }

    if (s.IsTimedOut()) {
      if (MonoTime::Now() < deadline) {
        KLOG_EVERY_N_SECS(WARNING, 1)
            << "Unable to send the request (" << SecureShortDebugString(req)
            << ") to leader Master (" << leader_master_hostport().ToString()
            << "): " << s.ToString();
        if (client->IsMultiMaster()) {
          LOG(INFO) << "Determining the new leader Master and retrying...";
          WARN_NOT_OK(SetMasterServerProxy(client, deadline),
                      "Unable to determine the new leader Master");
        }
        continue;
      } else {
        // Operation deadline expired during this latest RPC.
        s = s.CloneAndPrepend(Substitute("$0 timed out after deadline expired",
                                         func_name));
      }
    }

    if (s.ok() && resp->has_error()) {
      if (resp->error().code() == MasterErrorPB::NOT_THE_LEADER ||
          resp->error().code() == MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED) {
        if (client->IsMultiMaster()) {
          KLOG_EVERY_N_SECS(INFO, 1) << "Determining the new leader Master and retrying...";
          WARN_NOT_OK(SetMasterServerProxy(client, deadline),
                      "Unable to determine the new leader Master");
          continue;
        }
      }
    }
    return s;
  }
}

// Explicit specialization for callers outside this compilation unit.
template
Status KuduClient::Data::SyncLeaderMasterRpc(
    const MonoTime& deadline,
    KuduClient* client,
    const GetTabletLocationsRequestPB& req,
    GetTabletLocationsResponsePB* resp,
    const char* func_name,
    const boost::function<Status(MasterServiceProxy*,
                                 const GetTabletLocationsRequestPB&,
                                 GetTabletLocationsResponsePB*,
                                 RpcController*)>& func,
    vector<uint32_t> required_feature_flags);
template
Status KuduClient::Data::SyncLeaderMasterRpc(
    const MonoTime& deadline,
    KuduClient* client,
    const ListTablesRequestPB& req,
    ListTablesResponsePB* resp,
    const char* func_name,
    const boost::function<Status(MasterServiceProxy*,
                                 const ListTablesRequestPB&,
                                 ListTablesResponsePB*,
                                 RpcController*)>& func,
    vector<uint32_t> required_feature_flags);
template
Status KuduClient::Data::SyncLeaderMasterRpc(
    const MonoTime& deadline,
    KuduClient* client,
    const ListTabletServersRequestPB& req,
    ListTabletServersResponsePB* resp,
    const char* func_name,
    const boost::function<Status(MasterServiceProxy*,
                                 const ListTabletServersRequestPB&,
                                 ListTabletServersResponsePB*,
                                 RpcController*)>& func,
    vector<uint32_t> required_feature_flags);

KuduClient::Data::Data()
    : latest_observed_timestamp_(KuduClient::kNoTimestamp) {
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
      // Filter out all the blacklisted candidates.
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
      } else if (selection == CLOSEST_REPLICA) {
        // Choose a local replica.
        for (RemoteTabletServer* rts : filtered) {
          if (IsTabletServerLocal(*rts)) {
            ret = rts;
            break;
          }
        }
        // Fallback to a random replica if none are local.
        if (ret == nullptr && !filtered.empty()) {
          ret = filtered[rand() % filtered.size()];
        }
      }
      break;
    }
    default: {
      LOG(FATAL) << "Unknown ProxySelection value " << selection;
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
                                     const KuduSchema& schema,
                                     const MonoTime& deadline,
                                     bool has_range_partition_bounds) {
  CreateTableResponsePB resp;

  vector<uint32_t> features;
  if (has_range_partition_bounds) {
    features.push_back(MasterFeatures::RANGE_PARTITION_BOUNDS);
  }
  Status s = SyncLeaderMasterRpc<CreateTableRequestPB, CreateTableResponsePB>(
      deadline, client, req, &resp, "CreateTable", &MasterServiceProxy::CreateTable,
      features);
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status KuduClient::Data::IsCreateTableInProgress(KuduClient* client,
                                                 const string& table_name,
                                                 const MonoTime& deadline,
                                                 bool *create_in_progress) {
  IsCreateTableDoneRequestPB req;
  IsCreateTableDoneResponsePB resp;
  req.mutable_table()->set_table_name(table_name);

  // TODO: Add client rpc timeout and use 'default_admin_operation_timeout_' as
  // the default timeout for all admin operations.
  Status s =
      SyncLeaderMasterRpc<IsCreateTableDoneRequestPB, IsCreateTableDoneResponsePB>(
          deadline,
          client,
          req,
          &resp,
          "IsCreateTableDone",
          &MasterServiceProxy::IsCreateTableDone,
          {});
  // RETURN_NOT_OK macro can't take templated function call as param,
  // and SyncLeaderMasterRpc must be explicitly instantiated, else the
  // compiler complains.
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *create_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::Data::WaitForCreateTableToFinish(KuduClient* client,
                                                    const string& table_name,
                                                    const MonoTime& deadline) {
  return RetryFunc(deadline,
                   "Waiting on Create Table to be completed",
                   "Timed out waiting for Table Creation",
                   boost::bind(&KuduClient::Data::IsCreateTableInProgress,
                               this, client, table_name, _1, _2));
}

Status KuduClient::Data::DeleteTable(KuduClient* client,
                                     const string& table_name,
                                     const MonoTime& deadline) {
  DeleteTableRequestPB req;
  DeleteTableResponsePB resp;

  req.mutable_table()->set_table_name(table_name);
  Status s = SyncLeaderMasterRpc<DeleteTableRequestPB, DeleteTableResponsePB>(
      deadline, client, req, &resp,
      "DeleteTable", &MasterServiceProxy::DeleteTable, {});
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status KuduClient::Data::AlterTable(KuduClient* client,
                                    const AlterTableRequestPB& req,
                                    const MonoTime& deadline,
                                    bool has_add_drop_partition) {
  vector<uint32_t> required_feature_flags;
  if (has_add_drop_partition) {
    required_feature_flags.push_back(MasterFeatures::ADD_DROP_RANGE_PARTITIONS);
  }
  AlterTableResponsePB resp;
  Status s =
      SyncLeaderMasterRpc<AlterTableRequestPB, AlterTableResponsePB>(
          deadline,
          client,
          req,
          &resp,
          "AlterTable",
          &MasterServiceProxy::AlterTable,
          std::move(required_feature_flags));
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status KuduClient::Data::IsAlterTableInProgress(KuduClient* client,
                                                const string& table_name,
                                                const MonoTime& deadline,
                                                bool *alter_in_progress) {
  IsAlterTableDoneRequestPB req;
  IsAlterTableDoneResponsePB resp;

  req.mutable_table()->set_table_name(table_name);
  Status s =
      SyncLeaderMasterRpc<IsAlterTableDoneRequestPB, IsAlterTableDoneResponsePB>(
          deadline,
          client,
          req,
          &resp,
          "IsAlterTableDone",
          &MasterServiceProxy::IsAlterTableDone,
          {});
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *alter_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::Data::WaitForAlterTableToFinish(KuduClient* client,
                                                   const string& alter_name,
                                                   const MonoTime& deadline) {
  return RetryFunc(deadline,
                   "Waiting on Alter Table to be completed",
                   "Timed out waiting for AlterTable",
                   boost::bind(&KuduClient::Data::IsAlterTableInProgress,
                               this,
                               client, alter_name, _1, _2));
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
  Status s = SyncLeaderMasterRpc<GetTableSchemaRequestPB, GetTableSchemaResponsePB>(
      deadline, client, req, &resp,
      "GetTableSchema", &MasterServiceProxy::GetTableSchema, {});
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  // Parse the server schema out of the response.
  unique_ptr<Schema> new_schema(new Schema());
  RETURN_NOT_OK(SchemaFromPB(resp.schema(), new_schema.get()));

  // Parse the server partition schema out of the response.
  PartitionSchema new_partition_schema;
  RETURN_NOT_OK(PartitionSchema::FromPB(resp.partition_schema(),
                                        *new_schema.get(),
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

void KuduClient::Data::LeaderMasterDetermined(const Status& status,
                                              const HostPort& host_port) {
  Sockaddr leader_sock_addr;
  Status new_status = status;
  if (new_status.ok()) {
    new_status = SockaddrFromHostPort(host_port, &leader_sock_addr);
  }

  vector<StatusCallback> cbs;
  {
    std::lock_guard<simple_spinlock> l(leader_master_lock_);
    cbs.swap(leader_master_callbacks_);
    leader_master_rpc_.reset();

    if (new_status.ok()) {
      leader_master_hostport_ = host_port;
      master_proxy_.reset(new MasterServiceProxy(messenger_, leader_sock_addr));
    }
  }

  for (const StatusCallback& cb : cbs) {
    cb.Run(new_status);
  }
}

Status KuduClient::Data::SetMasterServerProxy(KuduClient* client,
                                              const MonoTime& deadline) {
  Synchronizer sync;
  SetMasterServerProxyAsync(client, deadline, sync.AsStatusCallback());
  return sync.Wait();
}

void KuduClient::Data::SetMasterServerProxyAsync(KuduClient* client,
                                                 const MonoTime& deadline,
                                                 const StatusCallback& cb) {
  DCHECK(deadline.Initialized());

  vector<Sockaddr> master_sockaddrs;
  for (const string& master_server_addr : master_server_addrs_) {
    vector<Sockaddr> addrs;
    Status s;
    // TODO: Do address resolution asynchronously as well.
    s = ParseAddressList(master_server_addr, master::Master::kDefaultPort, &addrs);
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
    master_sockaddrs.push_back(addrs[0]);
  }

  // This ensures that no more than one GetLeaderMasterRpc is in
  // flight at a time -- there isn't much sense in requesting this information
  // in parallel, since the requests should end up with the same result.
  // Instead, we simply piggy-back onto the existing request by adding our own
  // callback to leader_master_callbacks_.
  std::unique_lock<simple_spinlock> l(leader_master_lock_);
  leader_master_callbacks_.push_back(cb);
  if (!leader_master_rpc_) {
    // No one is sending a request yet - we need to be the one to do it.
    leader_master_rpc_.reset(new GetLeaderMasterRpc(
                               Bind(&KuduClient::Data::LeaderMasterDetermined,
                                    Unretained(this)),
                               std::move(master_sockaddrs),
                               deadline,
                               client->default_rpc_timeout(),
                               messenger_));
    l.unlock();
    leader_master_rpc_->SendRpc();
  }


}

HostPort KuduClient::Data::leader_master_hostport() const {
  std::lock_guard<simple_spinlock> l(leader_master_lock_);
  return leader_master_hostport_;
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
