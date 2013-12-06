// Copyright (c) 2013, Cloudera, inc.
//
// This module is internal to the client and not a public API.
#ifndef KUDU_CLIENT_META_CACHE_H
#define KUDU_CLIENT_META_CACHE_H

#include <boost/function.hpp>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <vector>

#include "gutil/macros.h"
#include "util/locks.h"
#include "util/monotime.h"
#include "util/net/net_util.h"
#include "util/status.h"

namespace kudu {

namespace rpc {
class Messenger;
} // namespace rpc

namespace tserver {
class TabletServerServiceProxy;
} // namespace tserver

namespace master {
class MasterServiceProxy;
class TSInfoPB;
} // namespace master

namespace client {

class KuduClient;
class KuduTable;
class PartialRow;

//typedef boost::function<void(const Status& status, const std::vector<Sockaddr>& addr)>
//        ResolveAddressCallback;


typedef boost::function<void(const Status& status)> StatusCallback;

// The information cached about a given tablet server in the cluster.
//
// This class is thread-safe.
class RemoteTabletServer {
 public:
  explicit RemoteTabletServer(const master::TSInfoPB& pb);

  // Refresh the RPC proxy to this tablet server. This may involve a DNS
  // lookup if there is not already an active proxy.
  void RefreshProxy(KuduClient* client, const StatusCallback& cb,
                    bool force);

  // Update information from the given pb.
  // Requires that 'pb''s UUID matches this server.
  void Update(const master::TSInfoPB& pb);

  // Return the current proxy to this tablet server. Requires that RefreshProxy
  // be called prior to this.
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy() const;

  std::string ToString() const;

 private:
  // Internal callback for DNS resolution.
  void DnsResolutionFinished(const Status &result_status,
                             const HostPort& hp,
                             vector<Sockaddr>* addrs,
                             KuduClient* client,
                             const StatusCallback& user_callback);

  mutable simple_spinlock lock_;
  const std::string uuid_;

  std::vector<HostPort> rpc_hostports_;
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  DISALLOW_COPY_AND_ASSIGN(RemoteTabletServer);
};

struct RemoteReplica {
  RemoteTabletServer* ts;
};

struct InFlightRefresh;

// The client's view of a given tablet. This object manages lookups of
// the tablet's locations, status, etc.
//
// This class is thread-safe.
class RemoteTablet {
 public:
  explicit RemoteTablet(const std::string& tablet_id)
    : tablet_id_(tablet_id),
      state_(kInvalid) {
  }

  // Refresh the replica information about this tablet from the master
  // if necessary.
  //
  // When the replicas are available, StatusCallback is called. If the information
  // is already cached, 'cb' may be called immediately from the same thread.
  //
  // The provided callback may run on an IO thread, so should never block or
  // otherwise be expensive to run.
  void Refresh(KuduClient* cient, const StatusCallback& cb,
               bool force);

  // Return the tablet server hosting the Nth replica.
  //
  // Returns NULL if 'idx' is out of bounds. Since the list of
  // replicas may be updated by other threads concurrenctly, callers
  // should always check the result against NULL.
  RemoteTabletServer* replica_tserver(int idx);

  const std::string& tablet_id() const { return tablet_id_; }

 private:
  enum State {
    // The current data in this structure is known to be incorrect.
    // (eg the structure has just been created).
    kInvalid,

    // TODO: add a 'kLookingUp' state which would be used when one thread
    // is already in the process of resolving this tablet. Then other threads
    // can piggy-back on the same response, rather than potentially duplicating
    // work.

    // As best we know, the current information about this tablet is
    // up-to-date and can be used without any round-trip to the master.
    kValid
  };

  // RPC Callback (see impl for details).
  void GetTabletLocationsCB(KuduClient* client, InFlightRefresh* ifr);

  const std::string tablet_id_;

  mutable simple_spinlock lock_;

  // All non-const members are protected by 'lock_'.
  State state_;
  std::vector<RemoteReplica> replicas_;

  DISALLOW_COPY_AND_ASSIGN(RemoteTablet);
};

// Manager of RemoteTablets and RemoteTabletServers. The client consults
// this class to look up a given tablet or server.
//
// This class will also be responsible for cache eviction policies, etc.
class MetaCache {
 public:
  // The passed 'client' object must remain valid as long as MetaCache is alive.
  explicit MetaCache(KuduClient* client);
  ~MetaCache();

  // Look up which tablet hosts the given row of the given table.
  // When it is available, the tablet is stored in *remote_tablet
  // and the callback is fired.
  // NOTE: the callback may be called from an IO thread or inline with
  // this call if the cached data is already available.
  //
  // The returned RemoteTablet has not been Refresh()ed upon the callback
  // firing.
  //
  // TODO: we probably need some kind of struct here for things
  // like timeout/trace/etc.
  void LookupTabletByRow(const KuduTable* table,
                         const PartialRow& row,
                         std::tr1::shared_ptr<RemoteTablet>* remote_tablet,
                         const StatusCallback& callback);

  // Look up or create the RemoteTablet object for the given tablet ID.
  //
  // This is always a local operation (no network round trips or DNS resolution, etc).
  void LookupTabletByID(const std::string& tablet_id,
                        std::tr1::shared_ptr<RemoteTablet>* remote_tablet);


  // TODO: make private

 private:
  friend class RemoteTablet;

  // Update our information about the given tablet server.
  //
  // This is called when we get some response from the master which contains
  // the latest host/port info for a server.
  //
  // Returns the updated record.
  RemoteTabletServer* UpdateTabletServer(const master::TSInfoPB& pb);

  KuduClient* client_;

  rw_spinlock lock_;

  // Cache of tablet servers, by UUID.
  //
  // Given that the set of tablet servers is bounded by physical machines, we never
  // evict entries from this map until the MetaCache is destructed. So, no need to use
  // shared_ptr, etc.
  //
  // Protected by lock_
  std::tr1::unordered_map<std::string, RemoteTabletServer*> ts_cache_;

  // Cache of tablets which we are maintaining information about.
  std::tr1::unordered_map<std::string, std::tr1::shared_ptr<RemoteTablet> > tablet_cache_;

  DISALLOW_COPY_AND_ASSIGN(MetaCache);
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_META_CACHE_H */
