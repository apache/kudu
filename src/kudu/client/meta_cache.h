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
#ifndef KUDU_CLIENT_META_CACHE_H
#define KUDU_CLIENT_META_CACHE_H

#include <boost/function.hpp>
#include <map>
#include <set>
#include <string>
#include <memory>
#include <unordered_map>
#include <vector>

#include "kudu/common/partition.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc.h"
#include "kudu/util/async_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/status.h"

namespace kudu {

class KuduPartialRow;

namespace tserver {
class TabletServerServiceProxy;
} // namespace tserver

namespace master {
class MasterServiceProxy;
class TabletLocationsPB_ReplicaPB;
class TSInfoPB;
} // namespace master

namespace client {

class ClientTest_TestMasterLookupPermits_Test;
class ClientTest_TestMetaCacheExpiry_Test;
class KuduClient;
class KuduTable;

namespace internal {

class LookupRpc;
class MetaCache;
class RemoteTablet;

// The information cached about a given tablet server in the cluster.
//
// This class is thread-safe.
class RemoteTabletServer {
 public:
  explicit RemoteTabletServer(const master::TSInfoPB& pb);

  // Initialize the RPC proxy to this tablet server, if it is not already set up.
  // This will involve a DNS lookup if there is not already an active proxy.
  // If there is an active proxy, does nothing.
  void InitProxy(KuduClient* client, const StatusCallback& cb);

  // Update information from the given pb.
  // Requires that 'pb''s UUID matches this server.
  void Update(const master::TSInfoPB& pb);

  // Return the current proxy to this tablet server. Requires that InitProxy()
  // be called prior to this.
  std::shared_ptr<tserver::TabletServerServiceProxy> proxy() const;

  std::string ToString() const;

  void GetHostPorts(std::vector<HostPort>* host_ports) const;

  // Returns the remote server's uuid.
  const std::string& permanent_uuid() const;

 private:
  // Internal callback for DNS resolution.
  void DnsResolutionFinished(const HostPort& hp,
                             std::vector<Sockaddr>* addrs,
                             KuduClient* client,
                             const StatusCallback& user_callback,
                             const Status &result_status);

  mutable simple_spinlock lock_;
  const std::string uuid_;

  std::vector<HostPort> rpc_hostports_;
  std::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  DISALLOW_COPY_AND_ASSIGN(RemoteTabletServer);
};

struct RemoteReplica {
  RemoteTabletServer* ts;
  consensus::RaftPeerPB::Role role;
  bool failed;
};

typedef std::unordered_map<std::string, RemoteTabletServer*> TabletServerMap;

// A ServerPicker for tablets servers, backed by the MetaCache.
// Replicas are returned fully initialized and ready to be used.
class MetaCacheServerPicker : public rpc::ServerPicker<RemoteTabletServer> {
 public:
  MetaCacheServerPicker(KuduClient* client,
                        const scoped_refptr<MetaCache>& meta_cache,
                        const KuduTable* table,
                        RemoteTablet* const tablet);

  virtual ~MetaCacheServerPicker() {}
  void PickLeader(const ServerPickedCallback& callback, const MonoTime& deadline) override;
  void MarkServerFailed(RemoteTabletServer* replica, const Status& status) override;
  void MarkReplicaNotLeader(RemoteTabletServer* replica) override;
  void MarkResourceNotFound(RemoteTabletServer* replica) override;
 private:

  // Called whenever a tablet lookup in the metacache completes.
  void LookUpTabletCb(const ServerPickedCallback& callback,
                      const MonoTime& deadline,
                      const Status& status);

  // Called when the proxy is initialized.
  void InitProxyCb(const ServerPickedCallback& callback,
                   RemoteTabletServer* replica,
                   const Status& status);

  // Lock protecting accesses/updates to 'followers_'.
  mutable simple_spinlock lock_;

  // Reference to the client so that we can initialize a replica proxy, when we find it.
  KuduClient* client_;

  // A ref to the meta cache.
  scoped_refptr<MetaCache> meta_cache_;

  // The table we're writing to.
  const KuduTable* table_;

  // The tablet we're picking replicas for.
  RemoteTablet* const tablet_;

  // TSs that refused writes and that were marked as followers as a consequence.
  std::set<RemoteTabletServer*> followers_;
};

// The client's view of a given tablet. This object manages lookups of
// the tablet's locations, status, etc.
//
// This class is thread-safe.
class RemoteTablet : public RefCountedThreadSafe<RemoteTablet> {
 public:
  RemoteTablet(std::string tablet_id,
               Partition partition)
      : tablet_id_(std::move(tablet_id)),
        partition_(std::move(partition)),
        stale_(false) {
  }

  // Updates this tablet's replica locations.
  void Refresh(const TabletServerMap& tservers,
               const google::protobuf::RepeatedPtrField
                 <master::TabletLocationsPB_ReplicaPB>& replicas);

  // Mark this tablet as stale, indicating that the cached tablet metadata is
  // out of date. Staleness is checked by the MetaCache when
  // LookupTabletByKey() is called to determine whether the fast (non-network)
  // path can be used or whether the metadata must be refreshed from the Master.
  void MarkStale();

  // Whether the tablet has been marked as stale.
  bool stale() const;

  // Mark any replicas of this tablet hosted by 'ts' as failed. They will
  // not be returned in future cache lookups.
  //
  // The provided status is used for logging.
  void MarkReplicaFailed(RemoteTabletServer *ts, const Status& status);

  // Return the number of failed replicas for this tablet.
  int GetNumFailedReplicas() const;

  // Return the tablet server which is acting as the current LEADER for
  // this tablet, provided it hasn't failed.
  //
  // Returns NULL if there is currently no leader, or if the leader has
  // failed. Given that the replica list may change at any time,
  // callers should always check the result against NULL.
  RemoteTabletServer* LeaderTServer() const;

  // Writes this tablet's TSes (across all replicas) to 'servers'. Skips
  // failed replicas.
  void GetRemoteTabletServers(std::vector<RemoteTabletServer*>* servers) const;

  // Writes this tablet's replicas to 'replicas'. Skips failed replicas.
  void GetRemoteReplicas(std::vector<RemoteReplica>* replicas) const;

  // Return true if the tablet currently has a known LEADER replica
  // (i.e the next call to LeaderTServer() is likely to return non-NULL)
  bool HasLeader() const;

  const std::string& tablet_id() const { return tablet_id_; }

  const Partition& partition() const {
    return partition_;
  }

  // Mark the specified tablet server as the leader of the consensus configuration in the cache.
  void MarkTServerAsLeader(const RemoteTabletServer* server);

  // Mark the specified tablet server as a follower in the cache.
  void MarkTServerAsFollower(const RemoteTabletServer* server);

  // Return stringified representation of the list of replicas for this tablet.
  std::string ReplicasAsString() const;

 private:
  // Same as ReplicasAsString(), except that the caller must hold lock_.
  std::string ReplicasAsStringUnlocked() const;

  const std::string tablet_id_;
  const Partition partition_;

  // All non-const members are protected by 'lock_'.
  mutable simple_spinlock lock_;
  bool stale_;
  std::vector<RemoteReplica> replicas_;

  DISALLOW_COPY_AND_ASSIGN(RemoteTablet);
};

// MetaCacheEntry holds either a tablet and its associated `RemoteTablet`
// instance, or a non-covered partition range.
class MetaCacheEntry {
 public:

  MetaCacheEntry() { }

  // Construct a MetaCacheEntry representing a tablet.
  MetaCacheEntry(MonoTime expiration_time, scoped_refptr<RemoteTablet> tablet)
      : expiration_time_(expiration_time),
        tablet_(std::move(tablet)) {
  }

  // Construct a MetaCacheEntry representing a non-covered range with the
  // provided range partition bounds.
  MetaCacheEntry(MonoTime expiration_time,
                 std::string lower_bound_partition_key,
                 std::string upper_bound_partition_key)
      : expiration_time_(expiration_time),
        lower_bound_partition_key_(std::move(lower_bound_partition_key)),
        upper_bound_partition_key_(std::move(upper_bound_partition_key)) {
  }

  // Returns `true` if this is a non-covered partition range.
  bool is_non_covered_range() const {
    DCHECK(Initialized());
    return tablet_.get() == nullptr;
  }

  // Returns the remote tablet, should only be called if this entry contains a
  // tablet.
  const scoped_refptr<RemoteTablet>& tablet() const {
    DCHECK(tablet_);
    DCHECK(Initialized());
    return tablet_;
  }

  // Returns the inclusive lower bound partition key for the entry.
  const std::string& lower_bound_partition_key() const {
    DCHECK(Initialized());
    if (is_non_covered_range()) {
      return lower_bound_partition_key_;
    } else {
      return tablet_->partition().partition_key_start();
    }
  }

  // Returns the exclusive upper bound partition key for the entry.
  const std::string& upper_bound_partition_key() const {
    DCHECK(Initialized());
    if (is_non_covered_range()) {
      return upper_bound_partition_key_;
    } else {
      return tablet_->partition().partition_key_end();
    }
  }

  void refresh_expiration_time(MonoTime expiration_time) {
    DCHECK(Initialized());
    DCHECK(expiration_time.Initialized());
    // Do not check that the new expiration time comes after the existing expiration
    // time, because that may not hold if the master changes it's configured ttl.
    expiration_time_ = expiration_time;
  }

  // Returns true if the partition key is contained in this meta cache entry.
  bool Contains(const std::string& partition_key) const;

  // Returns true if this meta cache entry is stale.
  bool stale() const;

  // Returns a formatted string representation of the metacache suitable for
  // debug printing.
  //
  // This string will not be redacted, since table partitions are considered
  // metadata.
  std::string DebugString(const KuduTable* table) const;

 private:

  // Returns true if the entry is initialized.
  bool Initialized() const {
    return expiration_time_.Initialized();
  }

  // The expiration time of this cached entry.
  MonoTime expiration_time_;

  // The tablet. If this is a non-covered range then the tablet will be a nullptr.
  scoped_refptr<RemoteTablet> tablet_;

  // The lower bound partition key, if this is a non-covered range.
  std::string lower_bound_partition_key_;

  // The upper bound partition key, if this is a non-covered range.
  std::string upper_bound_partition_key_;
};

// Manager of RemoteTablets and RemoteTabletServers. The client consults
// this class to look up a given tablet or server.
//
// This class will also be responsible for cache eviction policies, etc.
class MetaCache : public RefCountedThreadSafe<MetaCache> {
 public:
  // The passed 'client' object must remain valid as long as MetaCache is alive.
  explicit MetaCache(KuduClient* client);
  ~MetaCache();

  // Look up which tablet hosts the given partition key for a table. When it is
  // available, the tablet is stored in 'remote_tablet' (if not NULL) and the
  // callback is fired. Only tablets with non-failed LEADERs are considered.
  //
  // NOTE: the callback may be called from an IO thread or inline with this
  // call if the cached data is already available.
  //
  // NOTE: the memory referenced by 'table' must remain valid until 'callback'
  // is invoked.
  void LookupTabletByKey(const KuduTable* table,
                         std::string partition_key,
                         const MonoTime& deadline,
                         scoped_refptr<RemoteTablet>* remote_tablet,
                         const StatusCallback& callback);

  // Look up which tablet hosts the given partition key, or the next tablet if
  // the key falls in a non-covered range partition.
  void LookupTabletByKeyOrNext(const KuduTable* table,
                               std::string partition_key,
                               const MonoTime& deadline,
                               scoped_refptr<RemoteTablet>* remote_tablet,
                               const StatusCallback& callback);

  // Clears the meta cache.
  void ClearCache();

  // Mark any replicas of any tablets hosted by 'ts' as failed. They will
  // not be returned in future cache lookups.
  void MarkTSFailed(RemoteTabletServer* ts, const Status& status);

  // Acquire or release a permit to perform a (slow) master lookup.
  //
  // If acquisition fails, caller may still do the lookup, but is first
  // blocked for a short time to prevent lookup storms.
  bool AcquireMasterLookupPermit();
  void ReleaseMasterLookupPermit();

 private:
  friend class LookupRpc;

  FRIEND_TEST(client::ClientTest, TestMasterLookupPermits);
  FRIEND_TEST(client::ClientTest, TestMetaCacheExpiry);

  // Called on the slow LookupTablet path when the master responds. Populates
  // the tablet caches and returns a reference to the first one.
  Status ProcessLookupResponse(const LookupRpc& rpc, MetaCacheEntry* entry);

  // Lookup the given tablet by key, only consulting local information.
  // Returns true and sets *remote_tablet if successful.
  bool LookupTabletByKeyFastPath(const KuduTable* table,
                                 const std::string& partition_key,
                                 MetaCacheEntry* entry);

  // Update our information about the given tablet server.
  //
  // This is called when we get some response from the master which contains
  // the latest host/port info for a server.
  //
  // NOTE: Must be called with lock_ held.
  void UpdateTabletServer(const master::TSInfoPB& pb);

  KuduClient* client_;

  rw_spinlock lock_;

  // Cache of Tablet Server locations: TS UUID -> RemoteTabletServer*.
  //
  // Given that the set of tablet servers is bounded by physical machines, we never
  // evict entries from this map until the MetaCache is destructed. So, no need to use
  // shared_ptr, etc.
  //
  // Protected by lock_.
  TabletServerMap ts_cache_;

  // Cache of tablets, keyed by partition key.
  //
  // Protected by lock_.
  typedef std::map<std::string, MetaCacheEntry> TabletMap;

  // Cache of tablets and non-covered ranges, keyed by table id.
  //
  // Protected by lock_.
  std::unordered_map<std::string, TabletMap> tablets_by_table_and_key_;

  // Cache of tablets, keyed by tablet ID.
  //
  // Protected by lock_
  std::unordered_map<std::string, scoped_refptr<RemoteTablet>> tablets_by_id_;

  // Prevents master lookup "storms" by delaying master lookups when all
  // permits have been acquired.
  Semaphore master_lookup_sem_;

  DISALLOW_COPY_AND_ASSIGN(MetaCache);
};

} // namespace internal
} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_META_CACHE_H */
