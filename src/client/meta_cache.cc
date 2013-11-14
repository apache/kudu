// Copyright (c) 2013, Cloudera, inc.

#include "client/client.h"
#include "client/meta_cache.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "master/master.proxy.h"
#include "util/net/net_util.h"

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>

using kudu::master::GetTabletLocationsRequestPB;
using kudu::master::GetTabletLocationsResponsePB;;
using kudu::master::HostPortPB;
using kudu::master::MasterServiceProxy;
using kudu::tserver::TabletServerServiceProxy;
using kudu::rpc::RpcController;
using std::string;
using std::tr1::shared_ptr;

namespace kudu {
namespace client {

////////////////////////////////////////////////////////////

RemoteTabletServer::RemoteTabletServer(const master::TSInfoPB& pb)
  : uuid_(pb.permanent_uuid()) {

  Update(pb);
}

void RemoteTabletServer::RefreshProxy(KuduClient* client,
                                      const StatusCallback& cb,
                                      bool force) {
  HostPort hp;
  {
    boost::lock_guard<simple_spinlock> l(lock_);

    if (proxy_ && !force) {
      // Already have a proxy created.
      cb(Status::OK());
      return;
    }

    CHECK(!rpc_hostports_.empty());
    // TODO: if the TS advertises multiple host/ports, pick the right one
    // based on some kind of policy. For now just use the first always.
    hp = rpc_hostports_[0];
  }

  // TODO: the DNS resolution should be done asynchronously in a thread-pool.
  // For now, we'll just do it inline, but still provide an async API so that
  // we can async-ify this in the future without changing callers.
  vector<Sockaddr> addrs;
  Status s = hp.ResolveAddresses(&addrs);
  if (s.ok() && addrs.empty()) {
    s = Status::NotFound("No addresses for " + hp.ToString());
  }

  if (!s.ok()) {
    s = s.CloneAndPrepend("Failed to resolve address for TS " + uuid_);
    cb(s);
    return;
  }

  VLOG(1) << "Successfully resolved " << hp.ToString() << ": "
          << addrs[0].ToString();

  {
    boost::lock_guard<simple_spinlock> l(lock_);
    proxy_.reset(new TabletServerServiceProxy(client->messenger(), addrs[0]));
  }
  cb(s);
}

void RemoteTabletServer::Update(const master::TSInfoPB& pb) {
  CHECK_EQ(pb.permanent_uuid(), uuid_);

  boost::lock_guard<simple_spinlock> l(lock_);

  rpc_hostports_.clear();
  BOOST_FOREACH(const HostPortPB& hostport_pb, pb.rpc_addresses()) {
    rpc_hostports_.push_back(HostPort(hostport_pb.host(), hostport_pb.port()));
  }
}

shared_ptr<TabletServerServiceProxy> RemoteTabletServer::proxy() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK(proxy_);
  return proxy_;
}

////////////////////////////////////////////////////////////

// State information for RemoteTablet::Refresh's async path.
struct InFlightRefresh {
  RpcController rpc;
  GetTabletLocationsResponsePB resp;
  StatusCallback user_callback;
};

void RemoteTablet::Refresh(KuduClient* client, const StatusCallback& cb,
                           bool force) {
  {
    boost::unique_lock<simple_spinlock> l(lock_);
    if (state_ == kValid) {
      cb(Status::OK());
      return;
    }
  }
  // Need to actually refresh.

  // TODO: add some kind of flag in here that a refresh is in progress so
  // multiple refresh calls piggy-back.

  // TODO: add RPC timeout
  InFlightRefresh *ref = new InFlightRefresh;
  ref->user_callback = cb;

  GetTabletLocationsRequestPB req;
  req.add_tablet_ids(tablet_id_);

  shared_ptr<MasterServiceProxy> master = client->master_proxy();
  master->GetTabletLocationsAsync(req, &ref->resp, &ref->rpc,
                                  boost::bind(&RemoteTablet::GetTabletLocationsCB, this,
                                              client, ref));
}

// Callback for GetTabletLocationsAsync (see above).
// Takes care of parsing the RPC result, adopting the new information,
// and calling the user-provided callback.
void RemoteTablet::GetTabletLocationsCB(KuduClient* client, InFlightRefresh* ifr) {
  gscoped_ptr<InFlightRefresh> ifr_deleter(ifr); // delete on scope exit

  Status s = ifr->rpc.status();

  if (ifr->resp.tablet_locations().size() != 1 ||
      ifr->resp.tablet_locations(0).tablet_id() != tablet_id_) {
    // TODO: need better error handling here.
    s = Status::NotFound("RPC response invalid", ifr->resp.DebugString());
  }

  if (!s.ok()) {
    LOG(WARNING) << "Failed to fetch tablet locations for " << tablet_id_
                 << ": " << s.ToString();
    ifr->user_callback(s);
    return;
  }

  // Adopt the data from the successful response.
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    BOOST_FOREACH(const master::TabletLocationsPB_ReplicaPB& replica,
                  ifr->resp.tablet_locations(0).replicas()) {
      RemoteReplica rep;
      rep.ts = client->meta_cache_->UpdateTabletServer(replica.ts_info());
      replicas_.push_back(rep);
    }

    state_ = kValid;
  }
  ifr->user_callback(s);
}

RemoteTabletServer* RemoteTablet::replica_tserver(int idx) {
  CHECK_GE(idx, 0);
  boost::lock_guard<simple_spinlock> l(lock_);
  if (idx >= replicas_.size()) return NULL;
  return replicas_[idx].ts;
}

////////////////////////////////////////////////////////////

MetaCache::MetaCache(KuduClient* client)
  : client_(client) {
}

MetaCache::~MetaCache() {
  STLDeleteValues(&ts_cache_);
}

RemoteTabletServer* MetaCache::UpdateTabletServer(const master::TSInfoPB& pb) {
  for (int i = 0; i < 2; i++) {
    // Try two times: on the first time, just take the read-lock, since it's likely
    // that we don't need to modify the map. If we fail to find the TS, then we'll
    // go through the loop again with the write lock.
    boost::shared_lock<rw_spinlock> l_shared(lock_, boost::defer_lock);
    boost::unique_lock<rw_spinlock> l_exclusive(lock_, boost::defer_lock);

    if (i == 0) {
      l_shared.lock();
    } else {
      l_exclusive.lock();
    }

    RemoteTabletServer* ret = FindPtrOrNull(ts_cache_, pb.permanent_uuid());
    if (ret) {
      ret->Update(pb);
      return ret;
    }

    // If we only took the shared lock, we need to try again with the exclusive
    // one before we actually insert the new TS.
    if (l_shared.owns_lock()) continue;
    DCHECK(l_exclusive.owns_lock());

    VLOG(1) << "Client caching new TabletServer " << pb.permanent_uuid();

    ret = new RemoteTabletServer(pb);
    InsertOrDie(&ts_cache_, pb.permanent_uuid(), ret);
    return ret;
  }
  LOG(FATAL) << "Cannot reach here";
  return NULL;
}

void MetaCache::LookupTablet(const string& tablet_id,
                             shared_ptr<RemoteTablet>* remote_tablet) {
  // Most of the time, we'll already have an object for this tablet in the
  // cache, so we can just use a read-lock.
  {
    boost::shared_lock<rw_spinlock> l(lock_);
    if (FindCopy(tablet_cache_, tablet_id, remote_tablet)) {
      return;
    }
  }

  // We didn't have an object for this tablet. So, we need to insert a new one,
  // requiring the write-lock.
  {
    boost::unique_lock<rw_spinlock> l(lock_);
    *remote_tablet = LookupOrInsertNewSharedPtr(&tablet_cache_, tablet_id, tablet_id);
    return;
  }
}

} // namespace client
} // namespace kudu
