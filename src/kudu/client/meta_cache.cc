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

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/client-internal.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"

using std::map;
using std::set;
using std::shared_ptr;
using std::string;
using strings::Substitute;

namespace kudu {

using consensus::RaftPeerPB;
using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using master::MasterServiceProxy;
using master::TabletLocationsPB;
using master::TabletLocationsPB_ReplicaPB;
using master::TSInfoPB;
using rpc::CredentialsPolicy;
using rpc::Messenger;
using rpc::Rpc;
using rpc::ErrorStatusPB;
using tserver::TabletServerServiceProxy;

namespace client {

namespace internal {

namespace {
const int MAX_RETURNED_TABLE_LOCATIONS = 10;
} // anonymous namespace

////////////////////////////////////////////////////////////

RemoteTabletServer::RemoteTabletServer(const master::TSInfoPB& pb)
  : uuid_(pb.permanent_uuid()) {

  Update(pb);
}

void RemoteTabletServer::DnsResolutionFinished(const HostPort& hp,
                                               vector<Sockaddr>* addrs,
                                               KuduClient* client,
                                               const StatusCallback& user_callback,
                                               const Status &result_status) {
  gscoped_ptr<vector<Sockaddr> > scoped_addrs(addrs);

  Status s = result_status;

  if (s.ok() && addrs->empty()) {
    s = Status::NetworkError("No addresses for " + hp.ToString());
  }

  if (!s.ok()) {
    s = s.CloneAndPrepend("Failed to resolve address for TS " + uuid_);
    user_callback.Run(s);
    return;
  }

  VLOG(1) << "Successfully resolved " << hp.ToString() << ": "
          << (*addrs)[0].ToString();

  {
    std::lock_guard<simple_spinlock> l(lock_);
    proxy_.reset(new TabletServerServiceProxy(client->data_->messenger_, (*addrs)[0], hp.host()));
  }
  user_callback.Run(s);
}

void RemoteTabletServer::InitProxy(KuduClient* client, const StatusCallback& cb) {
  HostPort hp;
  {
    std::unique_lock<simple_spinlock> l(lock_);

    if (proxy_) {
      // Already have a proxy created.
      l.unlock();
      cb.Run(Status::OK());
      return;
    }

    CHECK(!rpc_hostports_.empty());
    // TODO: if the TS advertises multiple host/ports, pick the right one
    // based on some kind of policy. For now just use the first always.
    hp = rpc_hostports_[0];
  }

  auto addrs = new vector<Sockaddr>();
  client->data_->dns_resolver_->ResolveAddresses(
    hp, addrs, Bind(&RemoteTabletServer::DnsResolutionFinished,
                    Unretained(this), hp, addrs, client, cb));
}

void RemoteTabletServer::Update(const master::TSInfoPB& pb) {
  CHECK_EQ(pb.permanent_uuid(), uuid_);

  std::lock_guard<simple_spinlock> l(lock_);

  rpc_hostports_.clear();
  for (const HostPortPB& hostport_pb : pb.rpc_addresses()) {
    rpc_hostports_.push_back(HostPort(hostport_pb.host(), hostport_pb.port()));
  }
}

const string& RemoteTabletServer::permanent_uuid() const {
  return uuid_;
}

shared_ptr<TabletServerServiceProxy> RemoteTabletServer::proxy() const {
  std::lock_guard<simple_spinlock> l(lock_);
  CHECK(proxy_);
  return proxy_;
}

string RemoteTabletServer::ToString() const {
  string ret = uuid_;
  std::lock_guard<simple_spinlock> l(lock_);
  if (!rpc_hostports_.empty()) {
    strings::SubstituteAndAppend(&ret, " ($0)", rpc_hostports_[0].ToString());
  }
  return ret;
}

void RemoteTabletServer::GetHostPorts(vector<HostPort>* host_ports) const {
  std::lock_guard<simple_spinlock> l(lock_);
  *host_ports = rpc_hostports_;
}

////////////////////////////////////////////////////////////


void RemoteTablet::Refresh(const TabletServerMap& tservers,
                           const google::protobuf::RepeatedPtrField
                             <TabletLocationsPB_ReplicaPB>& replicas) {
  // Adopt the data from the successful response.
  std::lock_guard<simple_spinlock> l(lock_);
  replicas_.clear();
  for (const TabletLocationsPB_ReplicaPB& r : replicas) {
    RemoteReplica rep;
    rep.ts = FindOrDie(tservers, r.ts_info().permanent_uuid());
    rep.role = r.role();
    rep.failed = false;
    replicas_.push_back(rep);
  }
  stale_ = false;
}

void RemoteTablet::MarkStale() {
  std::lock_guard<simple_spinlock> l(lock_);
  stale_ = true;
}

bool RemoteTablet::stale() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return stale_;
}

void RemoteTablet::MarkReplicaFailed(RemoteTabletServer *ts,
                                     const Status& status) {
  std::lock_guard<simple_spinlock> l(lock_);
  VLOG(2) << "Tablet " << tablet_id_ << ": Current remote replicas in meta cache: "
          << ReplicasAsStringUnlocked();
  KLOG_EVERY_N_SECS(WARNING, 1) << "Tablet " << tablet_id_ << ": Replica " << ts->ToString()
               << " has failed: " << status.ToString();
  for (RemoteReplica& rep : replicas_) {
    if (rep.ts == ts) {
      rep.failed = true;
    }
  }
}

int RemoteTablet::GetNumFailedReplicas() const {
  int failed = 0;
  std::lock_guard<simple_spinlock> l(lock_);
  for (const RemoteReplica& rep : replicas_) {
    if (rep.failed) {
      failed++;
    }
  }
  return failed;
}

RemoteTabletServer* RemoteTablet::LeaderTServer() const {
  std::lock_guard<simple_spinlock> l(lock_);
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
  std::lock_guard<simple_spinlock> l(lock_);
  for (const RemoteReplica& replica : replicas_) {
    if (replica.failed) {
      continue;
    }
    servers->push_back(replica.ts);
  }
}

void RemoteTablet::GetRemoteReplicas(vector<RemoteReplica>* replicas) const {
  replicas->clear();
  std::lock_guard<simple_spinlock> l(lock_);
  for (const auto& r : replicas_) {
    if (r.failed) {
      continue;
    }
    replicas->push_back(r);
  }
}

void RemoteTablet::MarkTServerAsLeader(const RemoteTabletServer* server) {
  std::lock_guard<simple_spinlock> l(lock_);
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
  std::lock_guard<simple_spinlock> l(lock_);
  for (RemoteReplica& replica : replicas_) {
    if (replica.ts == server) {
      replica.role = RaftPeerPB::FOLLOWER;
    }
  }
  VLOG(3) << "Latest replicas: " << ReplicasAsStringUnlocked();
}

string RemoteTablet::ReplicasAsString() const {
  std::lock_guard<simple_spinlock> l(lock_);
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

bool MetaCacheEntry::Contains(const string& partition_key) const {
  DCHECK(Initialized());
  return lower_bound_partition_key() <= partition_key &&
         (upper_bound_partition_key().empty() || upper_bound_partition_key() > partition_key);
}

bool MetaCacheEntry::stale() const {
  DCHECK(Initialized());
  return expiration_time_ < MonoTime::Now() ||
         (!is_non_covered_range() && tablet_->stale());
}

string MetaCacheEntry::DebugString(const KuduTable* table) const {
  DCHECK(Initialized());
  const string& lower_bound = lower_bound_partition_key();
  const string& upper_bound = upper_bound_partition_key();

  string lower_bound_string = lower_bound.empty() ? "<start>" :
    table->partition_schema().PartitionKeyDebugString(lower_bound, *table->schema().schema_);

  string upper_bound_string = upper_bound.empty() ? "<end>" :
    table->partition_schema().PartitionKeyDebugString(upper_bound, *table->schema().schema_);

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
                                             const scoped_refptr<MetaCache>& meta_cache,
                                             const KuduTable* table,
                                             RemoteTablet* const tablet)
    : client_(client),
      meta_cache_(meta_cache),
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
    bool marked_as_follower = false;
    {
      std::lock_guard<simple_spinlock> lock(lock_);
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
        std::lock_guard<simple_spinlock> lock(lock_);
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
  // TODO: When we support tablet splits, we should let the lookup shift
  // the write to another tablet (i.e. if it's since been split).
  if (!leader) {
    meta_cache_->LookupTabletByKey(
        table_,
        tablet_->partition().partition_key_start(),
        deadline,
        NULL,
        Bind(&MetaCacheServerPicker::LookUpTabletCb, Unretained(this), callback, deadline));
    return;
  }

  // If we have a current TS initialize the proxy.
  // Make sure we have a working proxy before sending out the RPC.
  leader->InitProxy(client_,
                    Bind(&MetaCacheServerPicker::InitProxyCb,
                         Unretained(this),
                         callback,
                         leader));
}

void MetaCacheServerPicker::MarkServerFailed(RemoteTabletServer* replica, const Status& status) {
  tablet_->MarkReplicaFailed(CHECK_NOTNULL(replica), status);
}

void MetaCacheServerPicker::MarkReplicaNotLeader(RemoteTabletServer* replica) {
  {
    std::lock_guard<simple_spinlock> lock(lock_);
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
    std::lock_guard<simple_spinlock> lock(lock_);
    followers_.clear();
  }

  // If we couldn't lookup the tablet call the user callback immediately.
  if (!status.ok()) {
    callback.Run(status, nullptr);
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
  callback.Run(status, replica);
}


////////////////////////////////////////////////////////////

MetaCache::MetaCache(KuduClient* client)
  : client_(client),
    master_lookup_sem_(50) {
}

MetaCache::~MetaCache() {
  STLDeleteValues(&ts_cache_);
}

void MetaCache::UpdateTabletServer(const TSInfoPB& pb) {
  DCHECK(lock_.is_write_locked());
  RemoteTabletServer* ts = FindPtrOrNull(ts_cache_, pb.permanent_uuid());
  if (ts) {
    ts->Update(pb);
    return;
  }

  VLOG(1) << "Client caching new TabletServer " << pb.permanent_uuid();
  InsertOrDie(&ts_cache_, pb.permanent_uuid(), new RemoteTabletServer(pb));
}

// A (table, partition_key) --> tablet lookup. May be in-flight to a master, or
// may be handled locally.
//
// Keeps a reference on the owning metacache while alive.
class LookupRpc : public Rpc {
 public:
  LookupRpc(const scoped_refptr<MetaCache>& meta_cache,
            StatusCallback user_cb,
            const KuduTable* table,
            string partition_key,
            scoped_refptr<RemoteTablet>* remote_tablet,
            const MonoTime& deadline,
            shared_ptr<Messenger> messenger,
            bool is_exact_lookup);
  virtual ~LookupRpc();
  virtual void SendRpc() OVERRIDE;
  virtual string ToString() const OVERRIDE;

  const GetTableLocationsRequestPB& req() const { return req_; }
  const GetTableLocationsResponsePB& resp() const { return resp_; }
  const string& table_name() const { return table_->name(); }
  const string& table_id() const { return table_->id(); }
  const string& partition_key() const { return partition_key_; }
  bool is_exact_lookup() const { return is_exact_lookup_; }
  const KuduTable* table() const { return table_; }

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  std::shared_ptr<MasterServiceProxy> master_proxy() const {
    return table_->client()->data_->master_proxy();
  }

  void ResetMasterLeaderAndRetry(CredentialsPolicy creds_policy);

  void NewLeaderMasterDeterminedCb(const Status& status);

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

  // User-specified callback to invoke when the lookup finishes.
  //
  // Always invoked, regardless of success or failure.
  StatusCallback user_cb_;

  // Table to lookup.
  const KuduTable* table_;

  // Encoded partition key to lookup.
  string partition_key_;

  // When lookup finishes successfully, the selected tablet is
  // written here prior to invoking 'user_cb_'.
  scoped_refptr<RemoteTablet>* remote_tablet_;

  // Whether this lookup has acquired a master lookup permit.
  bool has_permit_;

  // If true, this lookup is for an exact tablet match with the requested
  // partition key. If false, the next tablet after the partition key should be
  // returned if the partition key falls in a non-covered partition range.
  bool is_exact_lookup_;
};

LookupRpc::LookupRpc(const scoped_refptr<MetaCache>& meta_cache,
                     StatusCallback user_cb, const KuduTable* table,
                     string partition_key,
                     scoped_refptr<RemoteTablet>* remote_tablet,
                     const MonoTime& deadline,
                     shared_ptr<Messenger> messenger,
                     bool is_exact_lookup)
    : Rpc(deadline, std::move(messenger)),
      meta_cache_(meta_cache),
      user_cb_(std::move(user_cb)),
      table_(table),
      partition_key_(std::move(partition_key)),
      remote_tablet_(remote_tablet),
      has_permit_(false),
      is_exact_lookup_(is_exact_lookup) {
  DCHECK(deadline.Initialized());
}

LookupRpc::~LookupRpc() {
  if (has_permit_) {
    meta_cache_->ReleaseMasterLookupPermit();
  }
}

void LookupRpc::SendRpc() {
  // Fast path: lookup in the cache.
  MetaCacheEntry entry;
  while (PREDICT_TRUE(meta_cache_->LookupTabletByKeyFastPath(table_, partition_key_, &entry))
         && (entry.is_non_covered_range() || entry.tablet()->HasLeader())) {
    VLOG(4) << "Fast lookup: found " << entry.DebugString(table_) << " for " << ToString();
    if (!entry.is_non_covered_range()) {
      if (remote_tablet_) {
        *remote_tablet_ = entry.tablet();
      }
      user_cb_.Run(Status::OK());
      delete this;
      return;
    }
    if (is_exact_lookup_ || entry.upper_bound_partition_key().empty()) {
      user_cb_.Run(Status::NotFound("No tablet covering the requested range partition",
                                    entry.DebugString(table_)));
      delete this;
      return;
    }
    partition_key_ = entry.upper_bound_partition_key();
  }

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

  // Fill out the request.
  req_.mutable_table()->set_table_id(table_->id());
  req_.set_partition_key_start(partition_key_);
  req_.set_max_returned_locations(MAX_RETURNED_TABLE_LOCATIONS);

  // The end partition key is left unset intentionally so that we'll prefetch
  // some additional tablets.

  // See KuduClient::Data::SyncLeaderMasterRpc().
  MonoTime now = MonoTime::Now();
  if (retrier().deadline() < now) {
    SendRpcCb(Status::TimedOut("timed out after deadline expired"));
    return;
  }
  MonoTime rpc_deadline = now + meta_cache_->client_->default_rpc_timeout();
  mutable_retrier()->mutable_controller()->set_deadline(
      MonoTime::Earliest(rpc_deadline, retrier().deadline()));

  master_proxy()->GetTableLocationsAsync(req_, &resp_,
                                         mutable_retrier()->mutable_controller(),
                                         boost::bind(&LookupRpc::SendRpcCb, this, Status::OK()));
}

string LookupRpc::ToString() const {
  return Substitute("GetTableLocations { table: '$0', partition-key: ($1), attempt: $2 }",
                    table_->name(),
                    (partition_key_.empty() ? "<start>" :
                     table_->partition_schema()
                            .PartitionKeyDebugString(partition_key_, *table_->schema().schema_)),
                    num_attempts());
}

void LookupRpc::ResetMasterLeaderAndRetry(CredentialsPolicy creds_policy) {
  table_->client()->data_->ConnectToClusterAsync(
      table_->client(),
      retrier().deadline(),
      Bind(&LookupRpc::NewLeaderMasterDeterminedCb, Unretained(this)),
      creds_policy);
}

void LookupRpc::NewLeaderMasterDeterminedCb(const Status& status) {
  if (status.ok()) {
    mutable_retrier()->mutable_controller()->Reset();
    SendRpc();
  } else {
    KLOG_EVERY_N_SECS(WARNING, 1) << "Failed to determine new Master: " << status.ToString();
    mutable_retrier()->DelayedRetry(this, status);
  }
}

void LookupRpc::SendRpcCb(const Status& status) {
  gscoped_ptr<LookupRpc> delete_me(this); // delete on scope exit

  if (!status.ok()) {
    // Non-RPC failure. We only support TimedOut for LookupRpc.
    CHECK(status.IsTimedOut()) << status.ToString();
  }

  Status new_status = status;
  // Check for generic RPC errors.
  if (new_status.ok() && mutable_retrier()->HandleResponse(this, &new_status)) {
    ignore_result(delete_me.release());
    return;
  }

  // Check for specific application response errors.
  if (new_status.ok() && resp_.has_error()) {
    if (resp_.error().code() == master::MasterErrorPB::NOT_THE_LEADER ||
        resp_.error().code() == master::MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED) {
      if (meta_cache_->client_->IsMultiMaster()) {
        KLOG_EVERY_N_SECS(WARNING, 1) << "Leader Master has changed, re-trying...";
        ResetMasterLeaderAndRetry(CredentialsPolicy::ANY_CREDENTIALS);
        ignore_result(delete_me.release());
        return;
      }
    }
    new_status = StatusFromPB(resp_.error().status());
  }

  // Check for more generic errors (TimedOut can come from multiple places).
  if (new_status.IsTimedOut()) {
    if (MonoTime::Now() < retrier().deadline()) {
      if (meta_cache_->client_->IsMultiMaster()) {
        KLOG_EVERY_N_SECS(WARNING, 1) << "Leader Master timed out, re-trying...";
        ResetMasterLeaderAndRetry(CredentialsPolicy::ANY_CREDENTIALS);
        ignore_result(delete_me.release());
        return;
      }
    } else {
      // Operation deadline expired during this latest RPC.
      new_status = new_status.CloneAndPrepend(
          "timed out after deadline expired");
    }
  }

  if (new_status.IsNetworkError()) {
    if (meta_cache_->client_->IsMultiMaster()) {
      KLOG_EVERY_N_SECS(WARNING, 1) << "Encountered a network error from the Master: "
                                    << new_status.ToString() << ", retrying...";
      ResetMasterLeaderAndRetry(CredentialsPolicy::ANY_CREDENTIALS);
      ignore_result(delete_me.release());
      return;
    }
  }

  if (new_status.IsNotAuthorized()) {
    const rpc::RpcController& controller(retrier().controller());
    const ErrorStatusPB* err = controller.error_response();
    if (err && err->has_code() &&
        err->code() == ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN) {
      KLOG_EVERY_N_SECS(INFO, 1)
          << "Re-connecting to cluster in attempt to get new authn token";
      ResetMasterLeaderAndRetry(CredentialsPolicy::PRIMARY_CREDENTIALS);
      ignore_result(delete_me.release());
      return;
    }
  }

  if (new_status.IsServiceUnavailable()) {
    // One or more of the tablets is not running; retry after a backoff period.
    mutable_retrier()->DelayedRetry(this, new_status);
    ignore_result(delete_me.release());
    return;
  }

  if (new_status.ok()) {
    MetaCacheEntry entry;
    new_status = meta_cache_->ProcessLookupResponse(*this, &entry);
    if (entry.is_non_covered_range()) {
      new_status = Status::NotFound("No tablet covering the requested range partition",
                                    entry.DebugString(table_));
    } else if (remote_tablet_) {
      *remote_tablet_ = entry.tablet();
    }
  } else {
    new_status = new_status.CloneAndPrepend(Substitute("$0 failed", ToString()));
    KLOG_EVERY_N_SECS(WARNING, 1) << new_status.ToString();
  }
  user_cb_.Run(new_status);
}

Status MetaCache::ProcessLookupResponse(const LookupRpc& rpc,
                                        MetaCacheEntry* cache_entry) {
  VLOG(2) << "Processing master response for " << rpc.ToString()
          << ". Response: " << SecureShortDebugString(rpc.resp());

  MonoTime expiration_time = MonoTime::Now() +
      MonoDelta::FromMilliseconds(rpc.resp().ttl_millis());

  std::lock_guard<rw_spinlock> l(lock_);
  TabletMap& tablets_by_key = LookupOrInsert(&tablets_by_table_and_key_,
                                             rpc.table_id(), TabletMap());

  const auto& tablet_locations = rpc.resp().tablet_locations();

  if (tablet_locations.empty()) {
    // If there are no tablets in the response, then the table is empty. If
    // there were any tablets in the table they would have been returned, since
    // the master guarantees that if the partition key falls in a non-covered
    // range, the previous tablet will be returned, and we did not set an upper
    // bound partition key on the request.
    DCHECK(!rpc.req().has_partition_key_end());

    tablets_by_key.clear();
    MetaCacheEntry entry(expiration_time, "", "");
    VLOG(3) << "Caching '" << rpc.table_name() << "' entry " << entry.DebugString(rpc.table());
    InsertOrDie(&tablets_by_key, "", std::move(entry));
  } else {

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

    const auto& first_lower_bound = tablet_locations.Get(0).partition().partition_key_start();
    if (rpc.partition_key() < first_lower_bound) {
      // If the first tablet is past the requested partition key, then the
      // partition key falls in an initial non-covered range, such as A.

      // Clear any existing entries which overlap with the discovered non-covered range.
      tablets_by_key.erase(tablets_by_key.begin(), tablets_by_key.lower_bound(first_lower_bound));
      MetaCacheEntry entry(expiration_time, "", first_lower_bound);
      VLOG(3) << "Caching '" << rpc.table_name() << "' entry " << entry.DebugString(rpc.table());
      InsertOrDie(&tablets_by_key, "", std::move(entry));
    }

    // last_upper_bound tracks the upper bound of the previously processed
    // entry, so that we can determine when we have found a non-covered range.
    string last_upper_bound = first_lower_bound;
    for (const TabletLocationsPB& tablet : tablet_locations) {
      const auto& tablet_lower_bound = tablet.partition().partition_key_start();
      const auto& tablet_upper_bound = tablet.partition().partition_key_end();

      if (last_upper_bound < tablet_lower_bound) {
        // There is a non-covered range between the previous tablet and this tablet.
        // This will discover C while processing the tablet location for D.

        // Clear any existing entries which overlap with the discovered non-covered range.
        tablets_by_key.erase(tablets_by_key.lower_bound(last_upper_bound),
                             tablets_by_key.lower_bound(tablet_lower_bound));

        MetaCacheEntry entry(expiration_time, last_upper_bound, tablet_lower_bound);
        VLOG(3) << "Caching '" << rpc.table_name() << "' entry " << entry.DebugString(rpc.table());
        InsertOrDie(&tablets_by_key, last_upper_bound, std::move(entry));
      }
      last_upper_bound = tablet_upper_bound;

      // Now process the tablet itself (such as B, D, or E). If we already know
      // about the tablet, then we only need to refresh it's replica locations
      // and the entry TTL. If the tablet is unknown, then we need to create a
      // new RemoteTablet for it.

      // First, update the tserver cache, needed for the Refresh calls below.
      for (const TabletLocationsPB_ReplicaPB& replicas : tablet.replicas()) {
        UpdateTabletServer(replicas.ts_info());
      }

      string tablet_id = tablet.tablet_id();
      scoped_refptr<RemoteTablet> remote = FindPtrOrNull(tablets_by_id_, tablet_id);
      if (remote.get() != nullptr) {
        // Partition should not have changed.
        DCHECK_EQ(tablet_lower_bound, remote->partition().partition_key_start());
        DCHECK_EQ(tablet_upper_bound, remote->partition().partition_key_end());

        VLOG(3) << "Refreshing tablet " << tablet_id << ": " << SecureShortDebugString(tablet);
        remote->Refresh(ts_cache_, tablet.replicas());

        // Update the entry TTL.
        auto& entry = FindOrDie(tablets_by_key, tablet_lower_bound);
        DCHECK(!entry.is_non_covered_range() &&
               entry.upper_bound_partition_key() == tablet_upper_bound);
        entry.refresh_expiration_time(expiration_time);
        continue;
      }

      // Clear any existing entries which overlap with the discovered tablet.
      tablets_by_key.erase(tablets_by_key.lower_bound(tablet_lower_bound),
                           tablet_upper_bound.empty() ? tablets_by_key.end() :
                             tablets_by_key.lower_bound(tablet_upper_bound));

      Partition partition;
      Partition::FromPB(tablet.partition(), &partition);
      remote = new RemoteTablet(tablet_id, partition);
      remote->Refresh(ts_cache_, tablet.replicas());

      MetaCacheEntry entry(expiration_time, remote);
      VLOG(3) << "Caching '" << rpc.table_name() << "' entry " << entry.DebugString(rpc.table());

      InsertOrDie(&tablets_by_id_, tablet_id, remote);
      InsertOrDie(&tablets_by_key, tablet_lower_bound, std::move(entry));
    }

    if (!last_upper_bound.empty() && tablet_locations.size() < MAX_RETURNED_TABLE_LOCATIONS) {
      // There is a non-covered range between the last tablet and the end of the
      // partition key space, such as F.

      // Clear existing entries which overlap with the discovered non-covered range.
      tablets_by_key.erase(tablets_by_key.lower_bound(last_upper_bound),
                           tablets_by_key.end());

      MetaCacheEntry entry(expiration_time, last_upper_bound, "");
      VLOG(3) << "Caching '" << rpc.table_name() << "' entry " << entry.DebugString(rpc.table());
      InsertOrDie(&tablets_by_key, last_upper_bound, std::move(entry));
    }
  }

  // Finally, lookup the discovered entry and return it to the requestor.
  *cache_entry = FindFloorOrDie(tablets_by_key, rpc.partition_key());
  if (!rpc.is_exact_lookup() && cache_entry->is_non_covered_range() &&
      !cache_entry->upper_bound_partition_key().empty()) {
    *cache_entry = FindFloorOrDie(tablets_by_key, cache_entry->upper_bound_partition_key());
    DCHECK(!cache_entry->is_non_covered_range());
  }
  return Status::OK();
}

bool MetaCache::LookupTabletByKeyFastPath(const KuduTable* table,
                                          const string& partition_key,
                                          MetaCacheEntry* entry) {
  shared_lock<rw_spinlock> l(lock_);
  const TabletMap* tablets = FindOrNull(tablets_by_table_and_key_, table->id());
  if (PREDICT_FALSE(!tablets)) {
    // No cache available for this table.
    return false;
  }

  const MetaCacheEntry* e = FindFloorOrNull(*tablets, partition_key);
  if (PREDICT_FALSE(!e)) {
    // No tablets with a start partition key lower than 'partition_key'.
    return false;
  }

  // Stale entries must be re-fetched.
  if (e->stale()) {
    return false;
  }

  if (e->Contains(partition_key)) {
    *entry = *e;
    return true;
  }

  return false;
}

void MetaCache::ClearNonCoveredRangeEntries(const std::string& table_id) {
  VLOG(3) << "Clearing non-covered range entries of table " << table_id;
  std::lock_guard<rw_spinlock> l(lock_);

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
  std::lock_guard<rw_spinlock> l(lock_);
  STLDeleteValues(&ts_cache_);
  tablets_by_id_.clear();
  tablets_by_table_and_key_.clear();
}

void MetaCache::LookupTabletByKey(const KuduTable* table,
                                  string partition_key,
                                  const MonoTime& deadline,
                                  scoped_refptr<RemoteTablet>* remote_tablet,
                                  const StatusCallback& callback) {
  LookupRpc* rpc = new LookupRpc(this,
                                 callback,
                                 table,
                                 std::move(partition_key),
                                 remote_tablet,
                                 deadline,
                                 client_->data_->messenger_,
                                 true);
  rpc->SendRpc();
}

void MetaCache::LookupTabletByKeyOrNext(const KuduTable* table,
                                        string partition_key,
                                        const MonoTime& deadline,
                                        scoped_refptr<RemoteTablet>* remote_tablet,
                                        const StatusCallback& callback) {
  LookupRpc* rpc = new LookupRpc(this,
                                 callback,
                                 table,
                                 std::move(partition_key),
                                 remote_tablet,
                                 deadline,
                                 client_->data_->messenger_,
                                 false);
  rpc->SendRpc();
}

void MetaCache::MarkTSFailed(RemoteTabletServer* ts,
                             const Status& status) {
  LOG(INFO) << "Marking tablet server " << ts->ToString() << " as failed.";
  shared_lock<rw_spinlock> l(lock_);

  Status ts_status = status.CloneAndPrepend("TS failed");

  // TODO: replace with a ts->tablet multimap for faster lookup?
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

} // namespace internal
} // namespace client
} // namespace kudu
