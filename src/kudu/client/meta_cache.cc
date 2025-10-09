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

#include "kudu/client/meta_cache.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/repeated_field.h> // IWYU pragma: keep

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/master_proxy_rpc.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/key_range.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/token.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/async_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"

using kudu::consensus::RaftPeerPB;
using kudu::master::ANY_REPLICA;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::GetTabletLocationsRequestPB;
using kudu::master::GetTabletLocationsResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::TabletLocationsPB;
using kudu::master::TSInfoPB;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::BackoffType;
using kudu::rpc::CredentialsPolicy;
using kudu::rpc::RpcController;
using kudu::security::SignedTokenPB;
using kudu::tserver::TabletServerAdminServiceProxy;
using kudu::tserver::TabletServerServiceProxy;
using std::set;
using std::shared_lock;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

// TODO(todd) before enabling by default, need to think about how this works with
// docker/k8s -- I think the abstract namespace is scoped to a given k8s pod. We
// probably need to have the client blacklist the socket if it attempts to use it
// and can't connect.
DEFINE_bool(client_use_unix_domain_sockets, false,
            "Whether to try to connect to tablet servers using unix domain sockets. "
            "This will only be attempted if the server has indicated that it is listening "
            "on such a socket and the client is running on the same host.");
TAG_FLAG(client_use_unix_domain_sockets, experimental);

DEFINE_int32(client_tablet_locations_by_id_ttl_ms, 60 * 60 * 1000, // 60 minutes
             "Maximum time in milliseconds that clients will cache tablet "
             "locations keyed by tablet ID.");
TAG_FLAG(client_tablet_locations_by_id_ttl_ms, advanced);
TAG_FLAG(client_tablet_locations_by_id_ttl_ms, runtime);

DEFINE_bool(prevent_kudu_3461_infinite_recursion, true,
            "Whether or not to prevent infinite recursion caused due to stale "
            "client metacache as described in KUDU-3461. Used for testing only!");
TAG_FLAG(prevent_kudu_3461_infinite_recursion, unsafe);

namespace kudu {
namespace client {
namespace internal {

RemoteTabletServer::RemoteTabletServer(const master::TSInfoPB& pb)
  : uuid_(pb.permanent_uuid()) {
  Update(pb);
}

void RemoteTabletServer::DnsResolutionFinished(const HostPort& hp,
                                               vector<Sockaddr>* addrs,
                                               KuduClient* client,
                                               const StatusCallback& user_callback,
                                               const Status &result_status) {
  SCOPED_CLEANUP({ delete addrs; });
  Status s = result_status;

  if (s.ok() && addrs->empty()) {
    s = Status::NetworkError("No addresses for " + hp.ToString());
  }

  if (!s.ok()) {
    s = s.CloneAndPrepend("Failed to resolve address for TS " + uuid_);
    user_callback(s);
    return;
  }

  VLOG(1) << "Successfully resolved " << hp.ToString() << ": "
          << (*addrs)[0].ToString();
  auto proxy = std::make_shared<TabletServerServiceProxy>(
        client->data_->messenger_, (*addrs)[0], hp.host());
  proxy->set_user_credentials(client->data_->user_credentials_);
  auto admin_proxy = std::make_shared<TabletServerAdminServiceProxy>(
        client->data_->messenger_, (*addrs)[0], hp.host());

  {
    std::lock_guard l(lock_);
    proxy_ = std::move(proxy);
    admin_proxy_ = std::move(admin_proxy);
    proxy_->set_user_credentials(client->data_->user_credentials_);
  }
  user_callback(s);
}

void RemoteTabletServer::InitProxy(KuduClient* client, const StatusCallback& cb) {
  HostPort hp;
  {
    std::unique_lock<simple_spinlock> l(lock_);

    if (proxy_) {
      // Already have a proxy created.
      l.unlock();
      cb(Status::OK());
      return;
    }

    CHECK(!rpc_hostports_.empty());
    // TODO: if the TS advertises multiple host/ports, pick the right one
    // based on some kind of policy. For now just use the first always.
    hp = rpc_hostports_[0];
  }

  auto addrs = new vector<Sockaddr>;

  if (FLAGS_client_use_unix_domain_sockets && unix_domain_socket_path_ &&
      client->data_->IsLocalHostPort(hp)) {
    Sockaddr unix_socket;
    Status parse_status = unix_socket.ParseUnixDomainPath(*unix_domain_socket_path_);
    if (!parse_status.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 60)
          << Substitute("Tablet server $0 ($1) reported an invalid UNIX domain socket path '$2'",
                        hp.ToString(), uuid_, *unix_domain_socket_path_);
      // Fall through to normal TCP path.
    } else {
      VLOG(1) << Substitute("Will try to connect to UNIX socket $0 for local tablet server $1 ($2)",
                            unix_socket.ToString(), hp.ToString(), uuid_);
      addrs->emplace_back(unix_socket);
      this->DnsResolutionFinished(hp, addrs, client, cb, Status::OK());
      return;
    }
  }

  client->data_->dns_resolver_->ResolveAddressesAsync(
      hp, addrs, [=](const Status& s) {
        this->DnsResolutionFinished(hp, addrs, client, cb, s);
      });
}

void RemoteTabletServer::Update(const master::TSInfoPB& pb) {
  CHECK_EQ(pb.permanent_uuid(), uuid_);

  std::lock_guard l(lock_);

  rpc_hostports_.clear();
  for (const HostPortPB& hostport_pb : pb.rpc_addresses()) {
    rpc_hostports_.emplace_back(hostport_pb.host(), hostport_pb.port());
  }
  location_ = pb.location();
  if (pb.has_unix_domain_socket_path()) {
    unix_domain_socket_path_.emplace(pb.unix_domain_socket_path());
  } else {
    unix_domain_socket_path_.reset();
  }
}

const string& RemoteTabletServer::permanent_uuid() const {
  return uuid_;
}

string RemoteTabletServer::location() const {
  std::lock_guard l(lock_);
  return location_;
}

shared_ptr<TabletServerServiceProxy> RemoteTabletServer::proxy() const {
  std::lock_guard l(lock_);
  CHECK(proxy_);
  return proxy_;
}

shared_ptr<TabletServerAdminServiceProxy> RemoteTabletServer::admin_proxy() {
  std::lock_guard l(lock_);
  DCHECK(admin_proxy_);
  return admin_proxy_;
}

string RemoteTabletServer::ToString() const {
  string ret = uuid_;
  std::lock_guard l(lock_);
  if (!rpc_hostports_.empty()) {
    strings::SubstituteAndAppend(&ret, " ($0)", rpc_hostports_[0].ToString());
  }
  return ret;
}

void RemoteTabletServer::GetHostPorts(vector<HostPort>* host_ports) const {
  std::lock_guard l(lock_);
  *host_ports = rpc_hostports_;
}

////////////////////////////////////////////////////////////


Status RemoteTablet::Refresh(
    const TabletServerMap& tservers,
    const TabletLocationsPB& locs_pb,
    const google::protobuf::RepeatedPtrField<TSInfoPB>& ts_info_dict) {

  vector<RemoteReplica> replicas;

  // Handle "old-style" non-interned replicas. It's used for backward compatibility.
  for (const auto& r : locs_pb.deprecated_replicas()) {
    RemoteReplica replica = { FindOrDie(tservers, r.ts_info().permanent_uuid()),
                              r.role(), /*failed=*/false };
    replicas.push_back(replica);
  }
  // Handle interned replicas.
  for (const auto& r : locs_pb.interned_replicas()) {
    if (r.ts_info_idx() >= ts_info_dict.size()) {
      return Status::Corruption(Substitute(
          "invalid response from master: referenced tablet idx $0 but only $1 present",
          r.ts_info_idx(), ts_info_dict.size()));
    }
    const TSInfoPB& ts_info = ts_info_dict.Get(r.ts_info_idx());
    RemoteReplica replica = { FindOrDie(tservers, ts_info.permanent_uuid()),
                              r.role(), /*failed=*/false };
    replicas.push_back(replica);
  }

  // Adopt the data from the successful response.
  std::lock_guard l(lock_);
  replicas_ = std::move(replicas);

  stale_ = false;
  return Status::OK();
}

void RemoteTablet::MarkStale() {
  VLOG(2) << Substitute("Marking tablet stale, tablet id $0", tablet_id_);
  stale_ = true;
}

bool RemoteTablet::stale() const {
  return stale_;
}

void RemoteTablet::MarkReplicaFailed(RemoteTabletServer *ts,
                                     const Status& status) {
  std::lock_guard l(lock_);
  VLOG(2) << "Tablet " << tablet_id_ << ": Current remote replicas in meta cache: "
          << ReplicasAsStringUnlocked();
  KLOG_EVERY_N_SECS(WARNING, 1) << Substitute(
      "tablet $0: replica $1 has failed: $2",
                                     tablet_id_, ts->ToString(), status.ToString());
  for (RemoteReplica& rep : replicas_) {
    if (rep.ts == ts) {
      rep.failed = true;
    }
  }
}

int RemoteTablet::GetNumFailedReplicas() const {
  int failed = 0;
  std::lock_guard l(lock_);
  for (const RemoteReplica& rep : replicas_) {
    if (rep.failed) {
      failed++;
    }
  }
  return failed;
}

RemoteTabletServer* RemoteTablet::LeaderTServer() const {
  std::lock_guard l(lock_);
  for (const RemoteReplica& replica : replicas_) {
    if (!replica.failed && replica.role == RaftPeerPB::LEADER) {
      return replica.ts;
    }
  }
  return nullptr;
}

bool RemoteTablet::HasLeader() const {
  return LeaderTServer() != nullptr;
}

void RemoteTablet::GetRemoteTabletServers(vector<RemoteTabletServer*>* servers) const {
  servers->clear();
  std::lock_guard l(lock_);
  for (const RemoteReplica& replica : replicas_) {
    if (replica.failed) {
      continue;
    }
    servers->push_back(replica.ts);
  }
}

void RemoteTablet::GetRemoteReplicas(vector<RemoteReplica>* replicas) const {
  replicas->clear();
  std::lock_guard l(lock_);
  for (const auto& r : replicas_) {
    if (r.failed) {
      continue;
    }
    replicas->push_back(r);
  }
}

void RemoteTablet::MarkTServerAsLeader(const RemoteTabletServer* server) {
  std::lock_guard l(lock_);
  for (RemoteReplica& replica : replicas_) {
    if (replica.ts == server) {
      replica.role = RaftPeerPB::LEADER;
    } else if (replica.role == RaftPeerPB::LEADER) {
      replica.role = RaftPeerPB::FOLLOWER;
    }
  }
  VLOG(3) << "Latest replicas: " << ReplicasAsStringUnlocked();
}

void RemoteTablet::MarkTServerAsFollower(const RemoteTabletServer* server) {
  std::lock_guard l(lock_);
  for (RemoteReplica& replica : replicas_) {
    if (replica.ts == server) {
      replica.role = RaftPeerPB::FOLLOWER;
    }
  }
  VLOG(3) << "Latest replicas: " << ReplicasAsStringUnlocked();
}

string RemoteTablet::ReplicasAsString() const {
  std::lock_guard l(lock_);
  return ReplicasAsStringUnlocked();
}

string RemoteTablet::ReplicasAsStringUnlocked() const {
  DCHECK(lock_.is_locked());
  string replicas_str;
  for (const RemoteReplica& rep : replicas_) {
    if (!replicas_str.empty()) replicas_str += ", ";
    strings::SubstituteAndAppend(&replicas_str, "$0 ($1, $2)",
                                rep.ts->permanent_uuid(),
                                RaftPeerPB::Role_Name(rep.role),
                                rep.failed ? "FAILED" : "OK");
  }
  return replicas_str;
}

bool MetaCacheEntry::Contains(const PartitionKey& partition_key) const {
  DCHECK(Initialized());
  return lower_bound_partition_key() <= partition_key &&
      (upper_bound_partition_key().empty() ||
       upper_bound_partition_key() > partition_key);
}

bool MetaCacheEntry::stale() const {
  DCHECK(Initialized());
  return expiration_time_ < MonoTime::Now() ||
         (!is_non_covered_range() && tablet_->stale());
}

string MetaCacheEntry::DebugString(const KuduTable* table) const {
  DCHECK(Initialized());
  const auto& lower_bound = lower_bound_partition_key();
  const auto& upper_bound = upper_bound_partition_key();

  string lower_bound_string = MetaCache::DebugLowerBoundPartitionKey(table, lower_bound);

  // The upper bound is exclusive, so it's necessary to get proper hash schema
  // for the key.
  string upper_bound_string = upper_bound.empty()
      ? "<end>"
      : table->partition_schema().PartitionKeyDebugString(upper_bound, *table->schema().schema_);

  MonoDelta ttl = expiration_time_ - MonoTime::Now();

  if (is_non_covered_range()) {
    return strings::Substitute(
        "NonCoveredRange { lower_bound: ($0), upper_bound: ($1), ttl: $2ms }",
        lower_bound_string, upper_bound_string, ttl.ToMilliseconds());
  } else {
    return strings::Substitute(
        "Tablet { id: $0, lower_bound: ($1), upper_bound: ($2), ttl: $3ms }",
        tablet()->tablet_id(), lower_bound_string, upper_bound_string, ttl.ToMilliseconds());
  }
}

MetaCacheServerPicker::MetaCacheServerPicker(KuduClient* client,
                                             scoped_refptr<MetaCache> meta_cache,
                                             const KuduTable* table,
                                             RemoteTablet* const tablet)
    : client_(client),
      meta_cache_(std::move(meta_cache)),
      table_(table),
      tablet_(tablet) {}

void MetaCacheServerPicker::PickLeader(const ServerPickedCallback& callback,
                                       const MonoTime& deadline) {
  // Choose a destination TS according to the following algorithm:
  // 1. If the tablet metadata is stale, refresh it (goto step 5).
  // 2. Select the leader, provided:
  //    a. The current leader is known,
  //    b. It hasn't failed, and
  //    c. It isn't currently marked as a follower.
  // 3. If there's no good leader select another replica, provided:
  //    a. It hasn't failed, and
  //    b. It hasn't rejected our write due to being a follower.
  // 4. Preemptively mark the replica we selected in step 3 as "leader" in the
  //    meta cache, so that our selection remains sticky until the next Master
  //    metadata refresh.
  // 5. If we're out of appropriate replicas, force a lookup to the master
  //    to fetch new consensus configuration information.
  // 6. When the lookup finishes, forget which replicas were followers and
  //    retry the write (i.e. goto 2).
  // 7. If we issue the write and it fails because the destination was a
  //    follower, remember that fact and retry the write (i.e. goto 2).
  // 8. Repeat steps 1-7 until the write succeeds, fails for other reasons,
  //    or the write's deadline expires.
  RemoteTabletServer* leader = nullptr;
  if (!tablet_->stale()) {
    leader = tablet_->LeaderTServer();
    if (VLOG_IS_ON(2)) {
      if (leader) {
        VLOG(2) << Substitute("Client entry of tablet $0 is fresh, leader uuid $1",
                              tablet_->tablet_id(), leader->permanent_uuid());
      } else {
        VLOG(2) << Substitute("Client entry of tablet $0 is fresh (no leader).",
                              tablet_->tablet_id());
      }
    }

    bool marked_as_follower = false;
    {
      std::lock_guard lock(lock_);
      marked_as_follower = ContainsKey(followers_, leader);
    }
    if (leader && marked_as_follower) {
      VLOG(2) << "Tablet " << tablet_->tablet_id() << ": We have a follower for a leader: "
          << leader->ToString();

      // Mark the node as a follower in the cache so that on the next go-round,
      // LeaderTServer() will not return it as a leader unless a full metadata
      // refresh has occurred. This also avoids LookupTabletByKey() going into
      // "fast path" mode and not actually performing a metadata refresh from the
      // Master when it needs to.
      tablet_->MarkTServerAsFollower(leader);
      leader = nullptr;
    }
    if (!leader) {
      // Try to "guess" the next leader.
      vector<RemoteTabletServer*> replicas;
      tablet_->GetRemoteTabletServers(&replicas);
      set<RemoteTabletServer*> followers_copy;
      {
        std::lock_guard lock(lock_);
        followers_copy = followers_;

      }
      for (RemoteTabletServer* ts : replicas) {
        if (!ContainsKey(followers_copy, ts)) {
          leader = ts;
          break;
        }
      }
      if (leader) {
        // Mark this next replica "preemptively" as the leader in the meta cache,
        // so we go to it first on the next write if writing was successful.
        VLOG(1) << "Tablet " << tablet_->tablet_id() << ": Previous leader failed. "
            << "Preemptively marking tserver " << leader->ToString()
            << " as leader in the meta cache.";
        tablet_->MarkTServerAsLeader(leader);
      } else {
        VLOG(1) << "Tablet " << tablet_->tablet_id() << ": No valid leader.";
      }
    }
  }

  // If we've tried all replicas, force a lookup to the master to find the
  // new leader. This relies on some properties of LookupTabletByKey():
  // 1. The fast path only works when there's a non-failed leader (which we
  //    know is untrue here).
  // 2. The slow path always fetches consensus configuration information and updates the
  //    looked-up tablet.
  // Put another way, we don't care about the lookup results at all; we're
  // just using it to fetch the latest consensus configuration information.
  //
  // TODO(dralves): When we support tablet splits, we should let the lookup
  // shift the write to another tablet (i.e. if it's since been split).
  if (!leader) {
    if (table_) {
      VLOG(2) << Substitute("Table $0: No valid leader, lookup tablet by key.",
                            table_->name());
      if (PREDICT_TRUE(FLAGS_prevent_kudu_3461_infinite_recursion)) {
        // First check metacache for the tablet
        scoped_refptr<RemoteTablet> remote_tablet;
        Status fastpath_status = meta_cache_->FastLookupTabletByKey(table_,
                                                                    tablet_->partition().begin(),
                                                                    MetaCache::LookupType::kPoint,
                                                                    &remote_tablet);
        if (!fastpath_status.IsIncomplete()) {
          VLOG(2) << Substitute("Explicit fastpath lookup succeeded(maybe), "
                                "proceed with callback, table: $0",
                                table_->name());
          const auto& known_tablet_id = tablet_->tablet_id();
          if (remote_tablet && remote_tablet->tablet_id() != known_tablet_id) {
            // Skip further processing if tablet in question has turned invalid
            LOG(INFO) << Substitute(
                "tablet seems to be replaced: former ID $0, new ID $1 (table $2)",
                known_tablet_id, remote_tablet->tablet_id(), table_->name());
            callback(Status::InvalidArgument(
                         Substitute("tablet ID $0 is not valid", known_tablet_id)),
                     nullptr);
            return;
          }
        }
      }
      meta_cache_->LookupTabletByKey(
          table_,
          tablet_->partition().begin(),
          deadline,
          MetaCache::LookupType::kPoint,
          /*remote_tablet*/nullptr,
          [this, callback, deadline](const Status& s) {
            this->LookUpTabletCb(callback, deadline, s);
          });
    } else {
      VLOG(1) << "No valid table or leader: lookup tablet by ID " << tablet_->tablet_id();
      meta_cache_->LookupTabletById(
          client_,
          tablet_->tablet_id(),
          deadline,
          /*remote_tablet*/nullptr,
          [this, callback, deadline](const Status& s) {
            this->LookUpTabletCb(callback, deadline, s);
          });
    }
    return;
  }

  // If we have a current TS initialize the proxy.
  // Make sure we have a working proxy before sending out the RPC.
  leader->InitProxy(client_, [this, callback, leader](const Status& s) {
    this->InitProxyCb(callback, leader, s);
  });
}

void MetaCacheServerPicker::MarkServerFailed(RemoteTabletServer* replica, const Status& status) {
  tablet_->MarkReplicaFailed(CHECK_NOTNULL(replica), status);
}

void MetaCacheServerPicker::MarkReplicaNotLeader(RemoteTabletServer* replica) {
  {
    std::lock_guard lock(lock_);
    followers_.insert(CHECK_NOTNULL(replica));
  }
}

void MetaCacheServerPicker::MarkResourceNotFound(RemoteTabletServer* /*replica*/) {
  tablet_->MarkStale();
}

// Called whenever a tablet lookup in the metacache completes.
void MetaCacheServerPicker::LookUpTabletCb(const ServerPickedCallback& callback,
                                           const MonoTime& deadline,
                                           const Status& status) {
  // Whenever we lookup the tablet, clear the set of followers.
  {
    std::lock_guard lock(lock_);
    followers_.clear();
  }

  // If we couldn't lookup the tablet call the user callback immediately.
  if (!status.ok()) {
    VLOG(2) << Substitute("Tablet lookup failed, tablet id: $0, status: $1",
                          tablet_->tablet_id(), status.ToString());
    callback(status, nullptr);
    return;
  }

  // If we could lookup the tablet run the picking method again.
  //
  // TODO if we add new Pick* methods the method to (re-)call needs to be passed as
  // a callback, for now we just have PickLeader so we can call it directly.
  PickLeader(callback, deadline);
}

void MetaCacheServerPicker::InitProxyCb(const ServerPickedCallback& callback,
                                        RemoteTabletServer* replica,
                                        const Status& status) {
  callback(status, replica);
}


////////////////////////////////////////////////////////////

MetaCache::MetaCache(KuduClient* client,
                     ReplicaController::Visibility replica_visibility)
    : client_(client),
      master_lookup_sem_(50),
      replica_visibility_(replica_visibility) {
}

void MetaCache::UpdateTabletServerUnlocked(const TSInfoPB& pb) {
  DCHECK(lock_.is_write_locked());
  const auto& ts_uuid = pb.permanent_uuid();
  auto* ts = FindPtrOrNull(ts_cache_, ts_uuid);
  if (ts) {
    ts->Update(pb);
    return;
  }

  // First check whether the information about the tablet server is already
  // present in the registry.
  ts = FindPointeeOrNull(ts_registry_, ts_uuid);
  if (ts) {
    // If the tablet server is already registered, update the existing entry.
    ts->Update(pb);
  } else {
    // If the tablet server isn't registered, add a new entry.
    unique_ptr<RemoteTabletServer> entry(new RemoteTabletServer(pb));
    ts = entry.get();
    EmplaceOrDie(&ts_registry_, ts_uuid, std::move(entry));
  }
  // Now add the entry into the cache.
  VLOG(1) << Substitute("client caching new TabletServer $0", ts_uuid);
  InsertOrDie(&ts_cache_, ts_uuid, ts);
}

// A (tablet id) --> tablet lookup. May be in-flight to a master, or may be
// handled locally.
//
// Keeps a reference on the owning meta cache while alive.
class LookupRpcById : public AsyncLeaderMasterRpc<GetTabletLocationsRequestPB,
                                                  GetTabletLocationsResponsePB> {
 public:
  LookupRpcById(scoped_refptr<MetaCache> meta_cache,
                KuduClient* client,
                StatusCallback user_cb,
                const string& tablet_id,
                scoped_refptr<RemoteTablet>* remote_tablet,
                const MonoTime& deadline);
  virtual ~LookupRpcById() = default;

  void SendRpc() override;
  void SendRpcSlowPath();
  string ToString() const override {
    return Substitute("$0 { tablet: '$1', attempt: $2 }",
                      rpc_name_, tablet_id_, num_attempts());
  }
 private:
  void SendRpcCb(const Status& status) override;

  const string tablet_id_;
  scoped_refptr<MetaCache> meta_cache_;

  GetTabletLocationsRequestPB req_;
  GetTabletLocationsResponsePB resp_;

  scoped_refptr<RemoteTablet>* remote_tablet_;
};

LookupRpcById::LookupRpcById(scoped_refptr<MetaCache> meta_cache,
                             KuduClient* client,
                             StatusCallback user_cb,
                             const string& tablet_id,
                             scoped_refptr<RemoteTablet>* remote_tablet,
                             const MonoTime& deadline)
    : AsyncLeaderMasterRpc(deadline, client, BackoffType::LINEAR, req_, &resp_,
          &MasterServiceProxy::GetTabletLocationsAsync, "LookupRpcById", std::move(user_cb), {}),
      tablet_id_(tablet_id),
      meta_cache_(std::move(meta_cache)),
      remote_tablet_(remote_tablet) {
  req_.add_tablet_ids(tablet_id_);
  req_.set_intern_ts_infos_in_response(true);
}

void LookupRpcById::SendRpc() {
  Status fastpath_status = meta_cache_->DoFastPathLookupById(
      tablet_id_, remote_tablet_);
  if (!fastpath_status.IsIncomplete()) {
    user_cb_(fastpath_status);
    delete this;
    return;
  }
  SendRpcSlowPath();
}

void LookupRpcById::SendRpcSlowPath() {
  AsyncLeaderMasterRpc::SendRpc();
}

namespace {
// Handles master-related errors and transient lookup-related errors,
// scheduling retries and returning 'true' if rescheduled, in which case,
// callers should ensure this object remains alive. Updates 'status' to include
// more information based on the response.
template <class LookupRpcClass, class RespClass>
bool RetryLookupIfNecessary(Status* status,
                            const RespClass& resp,
                            rpc::RpcRetrier* retrier,
                            LookupRpcClass* rpc) {
  if (rpc->RetryOrReconnectIfNecessary(status)) {
    return true;
  }
  // Handle ServiceUnavailable codes from BuildLocationsForTablet().
  if (status->ok() && resp.has_error()) {
    *status = StatusFromPB(resp.error().status());
    if (status->IsServiceUnavailable()) {
      retrier->DelayedRetry(rpc, *status);
      return true;
    }
  }
  return false;
}
} // anonymous namespace

void LookupRpcById::SendRpcCb(const Status& status) {
  unique_ptr<LookupRpcById> delete_me(this);

  // Check for generic lookup errors.
  Status new_status = status;
  if (RetryLookupIfNecessary(&new_status, resp_, mutable_retrier(), this)) {
    ignore_result(delete_me.release());
    return;
  }

  // If there were no errors, process the response.
  if (new_status.ok()) {
    MetaCacheEntry entry;
    new_status = meta_cache_->ProcessGetTabletLocationsResponse(tablet_id_, resp_, &entry);
    if (new_status.ok() && remote_tablet_) {
      *remote_tablet_ = entry.tablet();
    }
  }
  if (!new_status.ok()) {
    // Otherwise, prep the final error.
    new_status = new_status.CloneAndPrepend(Substitute("$0 failed", ToString()));
    KLOG_EVERY_N_SECS(WARNING, 1) << new_status.ToString();
  }
  user_cb_(new_status);
}

// A (table, partition_key) --> tablet lookup. May be in-flight to a master, or
// may be handled locally.
//
// Keeps a reference on the owning meta cache while alive.
class LookupRpc
    : public AsyncLeaderMasterRpc<GetTableLocationsRequestPB,
                                  GetTableLocationsResponsePB> {
 public:
  LookupRpc(scoped_refptr<MetaCache> meta_cache,
            StatusCallback user_cb,
            const KuduTable* table,
            PartitionKey partition_key,
            scoped_refptr<RemoteTablet>* remote_tablet,
            const MonoTime& deadline,
            MetaCache::LookupType lookup_type,
            ReplicaController::Visibility replica_visibility);
  virtual ~LookupRpc();

  // Looks up the tablet location in the meta cache, and if it isn't there,
  // sends an RPC to perform the lookup.
  //
  // The abstraction is a bit muddied since this may not actually send an RPC
  // if the location exists in the meta cache. It's written in this way to
  // avoid extraneous RPC calls and to leverage common retry logic.
  //
  // Upon completion, either the user callback will be called and this object
  // should delete itself, or a retry has been rescheduled and the object
  // should remain alive.
  void SendRpc() override;

  // Send an RPC to perform the lookup without consulting the meta cache.
  void SendRpcSlowPath();

  string ToString() const override;

  const GetTableLocationsRequestPB& req() const { return req_; }
  const GetTableLocationsResponsePB& resp() const { return resp_; }
  const string& table_name() const { return table_->name(); }
  const string& table_id() const { return table_->id(); }
  const PartitionKey& partition_key() const { return partition_key_; }
  bool is_exact_lookup() const {
    return lookup_type_ == MetaCache::LookupType::kPoint;
  }
  int locations_to_fetch() const {
    switch (lookup_type_) {
      case MetaCache::LookupType::kLowerBound:
        return kFetchTabletsPerRangeLookup;
      case MetaCache::LookupType::kPoint:
        return kFetchTabletsPerPointLookup;
    }
    __builtin_unreachable();
  }
  const KuduTable* table() const { return table_; }

 protected:
  void ResetMasterLeaderAndRetry(CredentialsPolicy creds_policy) override;

 private:
  // Handles retry logic and processes the response, sticking locations into
  // the meta cache.
  void SendRpcCb(const Status& status) override;

  std::shared_ptr<MasterServiceProxy> master_proxy() const {
    return table_->client()->data_->master_proxy();
  }

  // Pointer back to the tablet cache. Populated with location information
  // if the lookup finishes successfully.
  //
  // When the RPC is destroyed, a master lookup permit is returned to the
  // cache if one was acquired in the first place.
  scoped_refptr<MetaCache> meta_cache_;

  // Request body.
  GetTableLocationsRequestPB req_;

  // Response body.
  GetTableLocationsResponsePB resp_;

  // Table to lookup.
  const KuduTable* table_;

  // Encoded partition key to lookup.
  PartitionKey partition_key_;

  // When lookup finishes successfully, the selected tablet is written here
  // prior to invoking the user-provided callback.
  scoped_refptr<RemoteTablet>* remote_tablet_;

  // Whether this lookup has acquired a master lookup permit.
  bool has_permit_;

  // Whether this lookup is for a range or a point.
  const MetaCache::LookupType lookup_type_;

  // Controlling which replicas to look up. If set to Visibility::ALL,
  // non-voter tablet replicas, if any, appear in the lookup result in addition
  // to 'regular' voter replicas.
  const ReplicaController::Visibility replica_visibility_;
};

LookupRpc::LookupRpc(scoped_refptr<MetaCache> meta_cache,
                     StatusCallback user_cb, const KuduTable* table,
                     PartitionKey partition_key,
                     scoped_refptr<RemoteTablet>* remote_tablet,
                     const MonoTime& deadline,
                     MetaCache::LookupType lookup_type,
                     ReplicaController::Visibility replica_visibility)
    : AsyncLeaderMasterRpc(deadline, meta_cache->client_, BackoffType::LINEAR, req_, &resp_,
          &MasterServiceProxy::GetTableLocationsAsync,
          "LookupRpc", std::move(user_cb), {}),
      meta_cache_(std::move(meta_cache)),
      table_(table),
      partition_key_(std::move(partition_key)),
      remote_tablet_(remote_tablet),
      has_permit_(false),
      lookup_type_(lookup_type),
      replica_visibility_(replica_visibility) {
  DCHECK(deadline.Initialized());
}

LookupRpc::~LookupRpc() {
  if (has_permit_) {
    meta_cache_->ReleaseMasterLookupPermit();
  }
}

void LookupRpc::SendRpc() {
  Status fastpath_status = meta_cache_->DoFastPathLookup(
      table_, &partition_key_, lookup_type_, remote_tablet_);
  if (!fastpath_status.IsIncomplete()) {
    user_cb_(fastpath_status);
    delete this;
    return;
  }
  SendRpcSlowPath();
}

void LookupRpc::SendRpcSlowPath() {
  // Slow path: must lookup the tablet in the master.
  VLOG(4) << "Fast lookup: no cache entry for " << ToString()
          << ": refreshing our metadata from the Master";

  if (!has_permit_) {
    has_permit_ = meta_cache_->AcquireMasterLookupPermit();
  }
  if (!has_permit_) {
    // Couldn't get a permit, try again in a little while.
    mutable_retrier()->DelayedRetry(this, Status::TimedOut(
        "client has too many outstanding requests to the master"));
    return;
  }

  // The end partition key is left unset intentionally to prefetch information
  // on additional tablets.
  req_.mutable_table()->set_table_id(table_->id());

  // The range information is set using both the legacy and the contemporary
  // notation, i.e. using 'partition_key_start' and 'partition_key_range' fields
  // correspondingly. That's to allow a client of newer versions to work with
  // servers of prior versions when sending request for a table without custom
  // hash schemas per range, while also being able to query about table
  // locations if a table has custom hash schemas per range when it served
  // by a tablet server of newer versions.
  auto* key_start = req_.mutable_key_start();
  key_start->set_hash_key(partition_key_.hash_key());
  key_start->set_range_key(partition_key_.range_key());
  req_.set_partition_key_start(partition_key_.ToString());

  req_.set_max_returned_locations(locations_to_fetch());
  req_.set_intern_ts_infos_in_response(true);
  if (replica_visibility_ == ReplicaController::Visibility::ALL) {
    req_.set_replica_type_filter(master::ANY_REPLICA);
  }

  VLOG(2) << Substitute("Slowpathing RPC $0: refreshing our metadata from the Master",
                        ToString());
  // Actually send the request.
  AsyncLeaderMasterRpc::SendRpc();
}

string LookupRpc::ToString() const {
  return Substitute("$0 { table: '$1', partition-key: ($2), attempt: $3 }",
                    rpc_name_,
                    table_->name(),
                    MetaCache::DebugLowerBoundPartitionKey(table_, partition_key_),
                    num_attempts());
}

void LookupRpc::ResetMasterLeaderAndRetry(CredentialsPolicy creds_policy) {
  table_->client()->data_->ConnectToClusterAsync(
      table_->client(),
      retrier().deadline(),
      [=](const Status& s) { this->NewLeaderMasterDeterminedCb(creds_policy, s); },
      creds_policy);
}

void LookupRpc::SendRpcCb(const Status& status) {
  // If we exit and haven't scheduled a retry, this object should delete
  // itself.
  unique_ptr<LookupRpc> delete_me(this);

  // Check for generic lookup errors.
  Status new_status = status;
  if (RetryLookupIfNecessary(&new_status, resp_, mutable_retrier(), this)) {
    ignore_result(delete_me.release());
    return;
  }

  // If there were no errors, process the response.
  if (new_status.ok()) {
    MetaCacheEntry entry;
    new_status = meta_cache_->ProcessLookupResponse(*this, &entry, locations_to_fetch());
    if (entry.is_non_covered_range()) {
      new_status = Status::NotFound("No tablet covering the requested range partition",
                                    entry.DebugString(table_));
    } else if (remote_tablet_) {
      *remote_tablet_ = entry.tablet();
    }
  } else {
    // Otherwise, prep the final error.
    new_status = new_status.CloneAndPrepend(Substitute("$0 failed", ToString()));
    KLOG_EVERY_N_SECS(WARNING, 1) << new_status.ToString();
  }
  user_cb_(new_status);
}

Status MetaCache::ProcessLookupResponse(const LookupRpc& rpc,
                                        MetaCacheEntry* cache_entry,
                                        int max_returned_locations) {
  VLOG(2) << "Processing master response for " << rpc.ToString()
          << ". Response: " << pb_util::SecureShortDebugString(rpc.resp());

  // If there are no tablets in the response, then the table is empty. If
  // there were any tablets in the table they would have been returned, since
  // the master guarantees that if the partition key falls in a non-covered
  // range, the previous tablet will be returned, and we did not set an upper
  // bound partition key on the request.
  DCHECK(!rpc.resp().tablet_locations().empty() ||
         !rpc.req().has_partition_key_end());

  return ProcessGetTableLocationsResponse(rpc.table(), rpc.partition_key(), rpc.is_exact_lookup(),
      rpc.resp(), cache_entry, max_returned_locations);
}

Status MetaCache::ProcessGetTabletLocationsResponse(const string& tablet_id,
                                                    const GetTabletLocationsResponsePB& resp,
                                                    MetaCacheEntry* cache_entry) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 50, "processing tablet locations response");
  MonoTime expiration_time = MonoTime::Now() +
      MonoDelta::FromMilliseconds(FLAGS_client_tablet_locations_by_id_ttl_ms);
  const auto& tablet_locations = resp.tablet_locations();
  if (tablet_locations.empty()) {
    return Status::NotFound("");
  }
  const auto& ts_infos = resp.ts_infos();
  DCHECK_EQ(1, tablet_locations.size());
  const auto& tablet = tablet_locations[0];
  DCHECK_EQ(tablet_id, tablet.tablet_id());
  const auto& tablet_lower_bound = tablet.partition().partition_key_start();
  const auto& tablet_upper_bound = tablet.partition().partition_key_end();

  std::lock_guard l(lock_);
  for (const auto& ts_info : ts_infos) {
    UpdateTabletServerUnlocked(ts_info);
  }
  scoped_refptr<RemoteTablet> remote = FindPtrOrNull(tablets_by_id_, tablet_id);
  if (remote) {
    // Partition should not have changed.
    DCHECK_EQ(tablet_lower_bound, remote->partition().begin().ToString());
    DCHECK_EQ(tablet_upper_bound, remote->partition().end().ToString());

    VLOG(3) << "Refreshing tablet " << tablet_id << ": " << SecureShortDebugString(tablet);
    RETURN_NOT_OK_PREPEND(remote->Refresh(ts_cache_, tablet, ts_infos),
                          Substitute("failed to refresh locations for tablet $0",
                                      tablet_id));
    MetaCacheEntry entry(expiration_time, remote);
    auto& mapped_entry = LookupOrEmplace(&entry_by_tablet_id_, tablet_id, std::move(entry));
    // NOTE: it's harmless to call refresh_expiration_time() if we just
    // constructed the entry with the same time.
    mapped_entry.refresh_expiration_time(expiration_time);
  } else {
    Partition partition;
    Partition::FromPB(tablet.partition(), &partition);
    remote = new RemoteTablet(tablet_id, partition);
    RETURN_NOT_OK_PREPEND(remote->Refresh(ts_cache_, tablet, ts_infos),
                          Substitute("failed to refresh locations for tablet $0",
                                     tablet_id));
    MetaCacheEntry entry(expiration_time, remote);
    VLOG(3) << Substitute("Caching '$0' entry", tablet_id);

    EmplaceOrDie(&tablets_by_id_, tablet_id, std::move(remote));
    EmplaceOrDie(&entry_by_tablet_id_, tablet_id, std::move(entry));
  }
  *cache_entry = FindOrDie(entry_by_tablet_id_, tablet_id);
  return Status::OK();
}

Status MetaCache::ProcessGetTableLocationsResponse(const KuduTable* table,
                                                   const PartitionKey& partition_key,
                                                   bool is_exact_lookup,
                                                   const GetTableLocationsResponsePB& resp,
                                                   MetaCacheEntry* cache_entry,
                                                   int max_returned_locations) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 50, "processing table locations response");
  MonoTime expiration_time = MonoTime::Now() +
      MonoDelta::FromMilliseconds(resp.ttl_millis());

  std::lock_guard l(lock_);
  TabletMap& tablets_by_key = LookupOrInsert(&tablets_by_table_and_key_,
                                             table->id(), TabletMap());

  const auto& tablet_locations = resp.tablet_locations();

  if (tablet_locations.empty()) {
    tablets_by_key.clear();
    MetaCacheEntry entry(expiration_time, {}, {});
    VLOG(3) << "Caching '" << table->name() << "' entry " << entry.DebugString(table);
    InsertOrDie(&tablets_by_key, {}, entry);
  } else {
    // First, update the tserver cache, needed for the Refresh calls below.
    // It's used for backward compatibility.
    for (const TabletLocationsPB& tablet : tablet_locations) {
      for (const auto& replicas : tablet.deprecated_replicas()) {
        UpdateTabletServerUnlocked(replicas.ts_info());
      }
    }
    // In the case of "interned" replicas, the above 'deprecated_replicas' lists will be empty
    // and instead we'll need to update from the top-level list of tservers.
    const auto& ts_infos = resp.ts_infos();
    for (const TSInfoPB& ts_info : ts_infos) {
      UpdateTabletServerUnlocked(ts_info);
    }

    // The comments below will reference the following diagram:
    //
    //   +---+   +---+---+
    //   |   |   |   |   |
    // A | B | C | D | E | F
    //   |   |   |   |   |
    //   +---+   +---+---+
    //
    // It depicts a tablet locations response from the master containing three
    // tablets: B, D and E. Three non-covered ranges are present: A, C, and F.
    // An RPC response containing B, D and E could occur if the lookup partition
    // key falls in A, B, or C, although the existence of A as an initial
    // non-covered range can only be inferred if the lookup partition key falls
    // in A.

    const auto& partition_pb = tablet_locations.Get(0).partition();
    const PartitionKey first_lower_bound(
        Partition::StringToPartitionKey(partition_pb.partition_key_start(),
                                        partition_pb.hash_buckets_size()));
    if (partition_key < first_lower_bound) {
      // If the first tablet is past the requested partition key, then the
      // partition key falls in an initial non-covered range, such as A.

      // Clear any existing entries which overlap with the discovered non-covered range.
      tablets_by_key.erase(tablets_by_key.begin(), tablets_by_key.lower_bound(first_lower_bound));
      MetaCacheEntry entry(expiration_time, {}, first_lower_bound);
      VLOG(3) << "Caching '" << table->name() << "' entry " << entry.DebugString(table);
      InsertOrDie(&tablets_by_key, {}, entry);
    }

    // last_upper_bound tracks the upper bound of the previously processed
    // entry, so that we can determine when we have found a non-covered range.
    auto last_upper_bound = first_lower_bound;
    for (const TabletLocationsPB& tablet : tablet_locations) {
      const auto& p = tablet.partition();
      const PartitionKey tablet_lower_bound(Partition::StringToPartitionKey(
          p.partition_key_start(), p.hash_buckets_size()));
      const PartitionKey tablet_upper_bound(Partition::StringToPartitionKey(
          p.partition_key_end(), p.hash_buckets_size()));

      if (last_upper_bound < tablet_lower_bound) {
        // There is a non-covered range between the previous tablet and this tablet.
        // This will discover C while processing the tablet location for D.

        // Clear any existing entries which overlap with the discovered non-covered range.
        tablets_by_key.erase(tablets_by_key.lower_bound(last_upper_bound),
                             tablets_by_key.lower_bound(tablet_lower_bound));

        MetaCacheEntry entry(expiration_time, last_upper_bound, tablet_lower_bound);
        VLOG(3) << "Caching '" << table->name() << "' entry " << entry.DebugString(table);
        InsertOrDie(&tablets_by_key, last_upper_bound, entry);
      }
      last_upper_bound = tablet_upper_bound;

      // Now process the tablet itself (such as B, D, or E). If we already know
      // about the tablet, then we only need to refresh it's replica locations
      // and the entry TTL. If the tablet is unknown, then we need to create a
      // new RemoteTablet for it.
      const string& tablet_id = tablet.tablet_id();
      scoped_refptr<RemoteTablet> remote = FindPtrOrNull(tablets_by_id_, tablet_id);
      if (remote) {
        // Partition should not have changed.
        DCHECK_EQ(tablet_lower_bound, remote->partition().begin());
        DCHECK_EQ(tablet_upper_bound, remote->partition().end());

        VLOG(3) << Substitute("Refreshing tablet $0: $1",
                              tablet_id, SecureShortDebugString(tablet));
        RETURN_NOT_OK_PREPEND(remote->Refresh(ts_cache_, tablet, ts_infos),
                              Substitute("failed to refresh locations for tablet $0",
                                         tablet_id));
        // Update the entry TTL.
        auto* entry = FindOrNull(tablets_by_key, tablet_lower_bound);
        if (entry) {
          DCHECK(!entry->is_non_covered_range() &&
                 entry->upper_bound_partition_key() == tablet_upper_bound);
          entry->refresh_expiration_time(expiration_time);
        } else {
          // A remote tablet exists, but isn't indexed for key-based lookups.
          // This might happen if the entry was removed after tablet range
          // was dropped, but then a scan token with stale information on tablet
          // locations was provided to start a scan. Let's index it now.
          MetaCacheEntry entry(expiration_time, remote);
          VLOG(3) << Substitute("Caching '$0' entry $1",
              table->name(), entry.DebugString(table));
          EmplaceOrDie(&tablets_by_key, tablet_lower_bound, std::move(entry));
        }
        continue;
      }
      // Clear any existing entries which overlap with the discovered tablet.
      tablets_by_key.erase(tablets_by_key.lower_bound(tablet_lower_bound),
                           tablet_upper_bound.empty() ? tablets_by_key.end() :
                           tablets_by_key.lower_bound(tablet_upper_bound));

      Partition partition;
      Partition::FromPB(tablet.partition(), &partition);
      remote = new RemoteTablet(tablet_id, partition);
      RETURN_NOT_OK_PREPEND(remote->Refresh(ts_cache_, tablet, ts_infos),
                            Substitute("failed to refresh locations for tablet $0",
                                       tablet_id));

      MetaCacheEntry entry(expiration_time, remote);
      VLOG(3) << "Caching '" << table->name() << "' entry " << entry.DebugString(table);

      EmplaceOrDie(&tablets_by_id_, tablet_id, std::move(remote));
      EmplaceOrDie(&tablets_by_key, tablet_lower_bound, std::move(entry));
    }

    if (!last_upper_bound.empty() && tablet_locations.size() < max_returned_locations) {
      // There is a non-covered range between the last tablet and the end of the
      // partition key space, such as F.

      // Clear existing entries which overlap with the discovered non-covered range.
      tablets_by_key.erase(tablets_by_key.lower_bound(last_upper_bound),
                           tablets_by_key.end());

      MetaCacheEntry entry(expiration_time, last_upper_bound, {});
      VLOG(3) << "Caching '" << table->name() << "' entry " << entry.DebugString(table);
      InsertOrDie(&tablets_by_key, last_upper_bound, entry);
    }
  }

  // Finally, lookup the discovered entry and return it to the requestor.
  *cache_entry = FindFloorOrDie(tablets_by_key, partition_key);
  if (!is_exact_lookup && cache_entry->is_non_covered_range() &&
      !cache_entry->upper_bound_partition_key().empty()) {
    *cache_entry = FindFloorOrDie(tablets_by_key, cache_entry->upper_bound_partition_key());
    DCHECK(!cache_entry->is_non_covered_range());
  }
  return Status::OK();
}

bool MetaCache::LookupEntryByKeyFastPath(const KuduTable* table,
                                         const PartitionKey& partition_key,
                                         MetaCacheEntry* entry) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 50, "looking up entry by key");
  shared_lock<rw_spinlock> l(lock_.get_lock());
  const TabletMap* tablets = FindOrNull(tablets_by_table_and_key_, table->id());
  if (PREDICT_FALSE(!tablets)) {
    // No cache available for this table.
    VLOG(2) << Substitute("No cache available for table $0", table->name());
    return false;
  }

  const MetaCacheEntry* e = FindFloorOrNull(*tablets, partition_key);
  if (PREDICT_FALSE(!e)) {
    // No tablets with a start partition key lower than 'partition_key'.
    VLOG(2) << Substitute("Table $0: No tablets found with a start key lower than input key.",
                          table->name());
    return false;
  }

  // Stale entries must be re-fetched.
  if (e->stale()) {
    VLOG(2) << Substitute("Table $0: stale entry must be re-fetched: $1",
                          table->name(), e->DebugString(table));
    return false;
  }

  if (e->Contains(partition_key)) {
    *entry = *e;
    return true;
  }

  VLOG(2) << Substitute("Table $0: Fastpath lookup by key failed!", table->name());
  return false;
}

Status MetaCache::FastLookupTabletByKey(const KuduTable* table,
                                        PartitionKey partition_key,
                                        MetaCache::LookupType lookup_type,
                                        scoped_refptr<RemoteTablet>* remote_tablet) {
  return DoFastPathLookup(table, &partition_key, lookup_type, remote_tablet);
}

Status MetaCache::DoFastPathLookup(const KuduTable* table,
                                   PartitionKey* partition_key,
                                   MetaCache::LookupType lookup_type,
                                   scoped_refptr<RemoteTablet>* remote_tablet) {
  static const string err_str = "No tablet covering the requested range partition";
  MetaCacheEntry entry;
  while (PREDICT_TRUE(LookupEntryByKeyFastPath(table, *partition_key, &entry))
         && (entry.is_non_covered_range() || entry.tablet()->HasLeader())) {
    VLOG(4) << Substitute("Fast lookup: found $0 for $1",
                          entry.DebugString(table),
                          DebugLowerBoundPartitionKey(table, *partition_key));
    if (!entry.is_non_covered_range()) {
      if (remote_tablet) {
        *remote_tablet = entry.tablet();
      }
      return Status::OK();
    }
    if (lookup_type == LookupType::kPoint || entry.upper_bound_partition_key().empty()) {
      VLOG(2) << Substitute(err_str);
      return Status::NotFound(err_str, entry.DebugString(table));
    }
    *partition_key = entry.upper_bound_partition_key();
  }
  VLOG(2) << Substitute("Fastpath lookup failed with incomplete status");
  return Status::Incomplete("");
}

bool MetaCache::LookupEntryByIdFastPath(const string& tablet_id,
                                        MetaCacheEntry* entry) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 50, "looking up entry by ID");
  shared_lock<rw_spinlock> l(lock_.get_lock());
  const auto* cache_entry = FindOrNull(entry_by_tablet_id_, tablet_id);
  if (PREDICT_FALSE(!cache_entry)) {
    return false;
  }
  if (cache_entry->stale()) {
    return false;
  }
  *entry = *cache_entry;
  return true;
}

Status MetaCache::DoFastPathLookupById(const string& tablet_id,
                                       scoped_refptr<RemoteTablet>* remote_tablet) {
  MetaCacheEntry entry;
  if (PREDICT_TRUE(LookupEntryByIdFastPath(tablet_id, &entry) &&
                   entry.tablet()->HasLeader())) {
    DCHECK(!entry.is_non_covered_range());
    if (remote_tablet) {
      *remote_tablet = entry.tablet();
    }
    return Status::OK();
  }
  // If we have no cached entry, or the cached entries don't have a leader, we
  // must do another lookup.
  return Status::Incomplete("");
}

void MetaCache::ClearNonCoveredRangeEntries(const std::string& table_id) {
  VLOG(3) << "Clearing non-covered range entries of table " << table_id;
  std::lock_guard l(lock_);

  TabletMap* tablets = FindOrNull(tablets_by_table_and_key_, table_id);
  if (PREDICT_FALSE(!tablets)) {
    // No cache available for this table.
    return;
  }

  for (auto it = tablets->begin(); it != tablets->end();) {
    if (it->second.is_non_covered_range()) {
      it = tablets->erase(it);
    } else {
      it++;
    }
  }
}

void MetaCache::ClearCache() {
  VLOG(3) << "Clearing cache";
  std::lock_guard l(lock_);
  ts_cache_.clear();
  tablets_by_id_.clear();
  tablets_by_table_and_key_.clear();
  entry_by_tablet_id_.clear();
}

Status MetaCache::GetTableKeyRanges(const KuduTable* table,
                                    const PartitionKey& partition_key,
                                    LookupType lookup_type,
                                    uint64_t split_size_bytes,
                                    const MonoDelta& timeout,
                                    vector<RangeWithRemoteTablet>* range_tablets) {
  scoped_refptr<internal::RemoteTablet> tablet;
  Synchronizer sync;
  MonoTime deadline = MonoTime::Now() + timeout;
  LookupTabletByKey(table,
                    partition_key,
                    deadline,
                    lookup_type,
                    &tablet,
                    sync.AsStatusCallback());
  RETURN_NOT_OK(sync.Wait());

  if (split_size_bytes == 0) {
    KeyRange key_range(
        tablet->partition().begin().ToString(),
        tablet->partition().end().ToString(),
        split_size_bytes);
    range_tablets->emplace_back(key_range, tablet);
    return Status::OK();
  }
  DCHECK_GT(split_size_bytes, 0);

  RemoteTabletServer *ts;
  vector<RemoteTabletServer*> candidates;
  set<string> blacklist;
  RETURN_NOT_OK(table->client()->data_->GetTabletServer(table->client(),
                                                        tablet,
                                                        KuduClient::LEADER_ONLY,
                                                        blacklist,
                                                        &candidates,
                                                        &ts));
  CHECK(ts);
  CHECK(ts->proxy());
  auto proxy = ts->proxy();

  tserver::SplitKeyRangeRequestPB req;
  tserver::SplitKeyRangeResponsePB resp;
  req.set_tablet_id(tablet->tablet_id());
  if (!tablet->partition().begin().ToString().empty()) {
    req.set_start_primary_key(tablet->partition().begin().ToString());
  }
  if (!tablet->partition().end().ToString().empty()) {
    req.set_stop_primary_key(tablet->partition().end().ToString());
  }
  req.set_target_chunk_size_bytes(split_size_bytes);
  SignedTokenPB authz_token;
  if (table->client()->data_->FetchCachedAuthzToken(table->id(), &authz_token)) {
    *req.mutable_authz_token() = std::move(authz_token);
  } else {
    // Note: this is expected if attempting to connect to a cluster that does
    // not support fine-grained access control.
    VLOG(1) << "no authz token for table " << table->id();
  }

  RpcController rpc;
  rpc.set_timeout(timeout);
  RETURN_NOT_OK(proxy->SplitKeyRange(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  for (const auto& range : resp.ranges()) {
    KeyRange key_range(
        range.has_start_primary_key() ? range.start_primary_key() : "",
        range.has_stop_primary_key() ? range.stop_primary_key() : "",
        range.size_bytes_estimates());
    range_tablets->emplace_back(key_range, tablet);
  }

  return Status::OK();
}

void MetaCache::LookupTabletByKey(const KuduTable* table,
                                  PartitionKey partition_key,
                                  const MonoTime& deadline,
                                  MetaCache::LookupType lookup_type,
                                  scoped_refptr<RemoteTablet>* remote_tablet,
                                  const StatusCallback& callback) {
  // Try a fast path without allocating a LookupRpc.
  // This avoids the allocation and also reference count increment/decrements.
  Status fastpath_status = DoFastPathLookup(
      table, &partition_key, lookup_type, remote_tablet);
  if (!fastpath_status.IsIncomplete()) {
    callback(fastpath_status);
    return;
  }

  VLOG(2) << Substitute("Fastpath lookup failed with $0. Proceed with RPC lookup,"
                        " table: $1",
                        fastpath_status.ToString(), table->name());
  LookupRpc* rpc = new LookupRpc(this,
                                 callback,
                                 table,
                                 std::move(partition_key),
                                 remote_tablet,
                                 deadline,
                                 lookup_type,
                                 replica_visibility_);
  rpc->SendRpcSlowPath();
}

void MetaCache::LookupTabletById(KuduClient* client,
                                 const string& tablet_id,
                                 const MonoTime& deadline,
                                 scoped_refptr<RemoteTablet>* remote_tablet,
                                 const StatusCallback& lookup_complete_cb) {
  Status fastpath_status = DoFastPathLookupById(tablet_id, remote_tablet);
  if (!fastpath_status.IsIncomplete()) {
    lookup_complete_cb(fastpath_status);
    return;
  }
  LookupRpcById* rpc = new LookupRpcById(this, client, lookup_complete_cb, tablet_id,
                                         remote_tablet, deadline);
  rpc->SendRpcSlowPath();
}

void MetaCache::MarkTSFailed(RemoteTabletServer* ts,
                             const Status& status) {
  LOG(INFO) << Substitute("marking tablet server $0 as failed", ts->ToString());
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 50, "marking tablet server as failed");
  const auto ts_status = status.CloneAndPrepend("TS failed");

  shared_lock<rw_spinlock> l(lock_.get_lock());
  // TODO(adar): replace with a ts->tablet multimap for faster lookup?
  for (const auto& tablet : tablets_by_id_) {
    // We just loop on all tablets; if a tablet does not have a replica on this
    // TS, MarkReplicaFailed() returns false and we ignore the return value.
    tablet.second->MarkReplicaFailed(ts, ts_status);
  }
}

bool MetaCache::AcquireMasterLookupPermit() {
  return master_lookup_sem_.TryAcquire();
}

void MetaCache::ReleaseMasterLookupPermit() {
  master_lookup_sem_.Release();
}

string MetaCache::DebugLowerBoundPartitionKey(const KuduTable* table,
                                              const PartitionKey& partition_key) {
  return partition_key.empty() ? "<start>" :
      table->partition_schema().PartitionKeyDebugString(partition_key, *table->schema().schema_);
}

} // namespace internal
} // namespace client
} // namespace kudu
