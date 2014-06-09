// Copyright (c) 2013, Cloudera, inc.

#include "client/client.h"
#include "client/meta_cache.h"
#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "master/master.proxy.h"
#include "util/net/dns_resolver.h"
#include "util/net/net_util.h"

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>

using kudu::HostPortPB;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::GetTabletLocationsRequestPB;
using kudu::master::GetTabletLocationsResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::TabletLocationsPB;
using kudu::master::TabletLocationsPB_ReplicaPB;
using kudu::master::TSInfoPB;
using kudu::metadata::QuorumPeerPB;
using kudu::tserver::TabletServerServiceProxy;
using kudu::rpc::RpcController;
using std::string;
using std::map;
using std::tr1::shared_ptr;

namespace kudu {
namespace client {

////////////////////////////////////////////////////////////

RemoteTabletServer::RemoteTabletServer(const master::TSInfoPB& pb)
  : uuid_(pb.permanent_uuid()) {

  Update(pb);
}

void RemoteTabletServer::DnsResolutionFinished(const Status &result_status,
                                               const HostPort& hp,
                                               vector<Sockaddr>* addrs,
                                               KuduClient* client,
                                               const StatusCallback& user_callback) {
  gscoped_ptr<vector<Sockaddr> > scoped_addrs(addrs);

  Status s = result_status;

  if (s.ok() && addrs->empty()) {
    s = Status::NotFound("No addresses for " + hp.ToString());
  }

  if (!s.ok()) {
    s = s.CloneAndPrepend("Failed to resolve address for TS " + uuid_);
    user_callback(s);
    return;
  }

  VLOG(1) << "Successfully resolved " << hp.ToString() << ": "
          << (*addrs)[0].ToString();

  {
    boost::lock_guard<simple_spinlock> l(lock_);
    proxy_.reset(new TabletServerServiceProxy(client->messenger(), (*addrs)[0]));
  }
  user_callback(s);
}

void RemoteTabletServer::RefreshProxy(KuduClient* client,
                                      const StatusCallback& cb,
                                      bool force) {
  HostPort hp;
  {
    boost::unique_lock<simple_spinlock> l(lock_);

    if (proxy_ && !force) {
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

  vector<Sockaddr>* addrs = new vector<Sockaddr>();
  client->dns_resolver()->ResolveAddresses(
    hp, addrs, boost::bind(&RemoteTabletServer::DnsResolutionFinished,
                           this, _1, hp, addrs, client, cb));
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

string RemoteTabletServer::ToString() const {
  return uuid_;
}

void RemoteTabletServer::GetHostPorts(std::vector<HostPort>* host_ports) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  *host_ports = rpc_hostports_;
}

////////////////////////////////////////////////////////////


void RemoteTablet::Refresh(const TabletServerMap& tservers,
                           const google::protobuf::RepeatedPtrField
                             <TabletLocationsPB_ReplicaPB>& replicas) {
  // Adopt the data from the successful response.
  boost::lock_guard<simple_spinlock> l(lock_);
  replicas_.clear();
  BOOST_FOREACH(const TabletLocationsPB_ReplicaPB& r, replicas) {
    RemoteReplica rep;
    rep.ts = FindOrDie(tservers, r.ts_info().permanent_uuid());
    rep.role = r.role();
    rep.failed = false;
    replicas_.push_back(rep);
  }
}

void RemoteTablet::MarkReplicaFailed(RemoteTabletServer *ts) {
  LOG(WARNING) << "Replica " << tablet_id_ << " on ts " << ts->ToString() << " has failed";

  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(RemoteReplica& rep, replicas_) {
    if (rep.ts == ts) {
      rep.failed = true;
    }
  }
}

int RemoteTablet::GetNumFailedReplicas() const {
  int failed = 0;
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const RemoteReplica& rep, replicas_) {
    if (rep.failed) {
      failed++;
    }
  }
  return failed;
}

RemoteTabletServer* RemoteTablet::FirstTServer() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const RemoteReplica& rep, replicas_) {
    if (!rep.failed) {
      return rep.ts;
    }
  }
  return NULL;
}

RemoteTabletServer* RemoteTablet::LeaderTServer() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const RemoteReplica& replica, replicas_) {
    if (!replica.failed && replica.role == QuorumPeerPB::LEADER) {
      return replica.ts;
    }
  }
  return NULL;
}

bool RemoteTablet::HasLeader() const {
  return LeaderTServer() != NULL;
}

void RemoteTablet::GetRemoteTabletServers(std::vector<RemoteTabletServer*>* servers) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const RemoteReplica& replica, replicas_) {
    if (replica.failed) {
      continue;
    }
    servers->push_back(replica.ts);
  }
}

////////////////////////////////////////////////////////////

MetaCache::MetaCache(KuduClient* client)
  : client_(client),
    slice_data_arena_(16 * 1024, 128 * 1024), // arbitrarily chosen
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

struct InFlightLookup {
  InFlightLookup()
    : mc(NULL),
      permit_held(false),
      remote_tablet(NULL) {
  }

  ~InFlightLookup() {
    if (permit_held) {
      mc->ReleaseMasterLookupPermit();
    }
  }

  scoped_refptr<MetaCache> mc;
  bool permit_held;
  RpcController rpc;
  GetTableLocationsResponsePB resp;
  StatusCallback user_callback;
  string table_name;
  Slice key;
  scoped_refptr<RemoteTablet> *remote_tablet;
};

void MetaCache::GetTableLocationsCB(InFlightLookup* ifl) {
  gscoped_ptr<InFlightLookup> ifl_deleter(ifl); // delete on scope exit

  // The logging below refers to tablet locations even though the RPC was
  // GetTableLocations. That's because we're using said RPC to look up
  // the location of a particular tablet.
  Status s = ifl->rpc.status();
  if (!s.ok()) {
    LOG(WARNING) << "Failed to fetch tablet with start key " << ifl->key << ": "
        << s.ToString();
    ifl->user_callback(s);
    return;
  }

  s = StatusFromPB(ifl->resp.error().status());
  if (!s.ok()) {
    LOG(WARNING) << "Failed to fetch tablet with start key " << ifl->key << ": "
        << s.ToString();
    ifl->user_callback(s);
    return;
  }

  if (ifl->resp.tablet_locations_size() == 0) {
    LOG(WARNING) << "Unable to find tablet with start key " << ifl->key;
    ifl->user_callback(Status::NotFound("No tablet found"));
    return;
  }

  boost::unique_lock<rw_spinlock> l(lock_);
  SliceTabletMap& tablets_by_key = LookupOrInsert(&tablets_by_table_and_key_,
                                                  ifl->table_name, SliceTabletMap());
  BOOST_FOREACH(const TabletLocationsPB& loc, ifl->resp.tablet_locations()) {
    // First, update the tserver cache, needed for the Refresh calls below.
    BOOST_FOREACH(const TabletLocationsPB_ReplicaPB& r, loc.replicas()) {
      UpdateTabletServer(r.ts_info());
    }

    // Next, update the tablet caches.
    string tablet_id = loc.tablet_id();
    scoped_refptr<RemoteTablet> remote = FindPtrOrNull(tablets_by_id_, tablet_id);
    if (remote.get() != NULL) {
      // Start/end keys should not have changed.
      DCHECK_EQ(loc.start_key(), remote->start_key().ToString());
      DCHECK_EQ(loc.end_key(), remote->end_key().ToString());

      VLOG(3) << "Refreshing tablet " << tablet_id << ": "
              << loc.ShortDebugString();
      remote->Refresh(ts_cache_, loc.replicas());
      continue;
    }

    VLOG(3) << "Caching tablet " << tablet_id << " for (" << ifl->table_name
            << "," << loc.start_key() << "," << loc.end_key() << ")"
            << ": " << loc.ShortDebugString();

    // The keys will outlive the pbs, so we relocate their data into our arena.
    Slice relocated_start_key;
    Slice relocated_end_key;
    CHECK(slice_data_arena_.RelocateSlice(Slice(loc.start_key()), &relocated_start_key));
    CHECK(slice_data_arena_.RelocateSlice(Slice(loc.end_key()), &relocated_end_key));

    remote = new RemoteTablet(tablet_id, relocated_start_key, relocated_end_key);
    remote->Refresh(ts_cache_, loc.replicas());

    InsertOrDie(&tablets_by_id_, tablet_id, remote);
    InsertOrDie(&tablets_by_key, remote->start_key(), remote);
  }

  // Always return the first tablet.
  *ifl->remote_tablet = FindOrDie(tablets_by_id_,
                                  ifl->resp.tablet_locations(0).tablet_id());
  l.unlock();
  ifl->user_callback(Status::OK());
}

bool MetaCache::LookupTabletByKeyFastPath(const KuduTable* table,
                                          const Slice& key,
                                          scoped_refptr<RemoteTablet>* remote_tablet) {
  boost::shared_lock<rw_spinlock> l(lock_);
  const SliceTabletMap* tablets = FindOrNull(tablets_by_table_and_key_, table->name());
  if (PREDICT_FALSE(!tablets)) {
    // No cache available for this table.
    return false;
  }

  const scoped_refptr<RemoteTablet>* r = FindFloorOrNull(*tablets, key);
  if (PREDICT_FALSE(!r)) {
    // No tablets with a start key lower than 'key'.
    return false;
  }

  if ((*r)->end_key().compare(key) > 0 ||  // key < tablet.end
      (*r)->end_key().empty()) {           // tablet doesn't end
    *remote_tablet = *r;
    return true;
  }

  return false;
}

void MetaCache::LookupTabletByKey(const KuduTable* table,
                                  const Slice& key,
                                  scoped_refptr<RemoteTablet>* remote_tablet,
                                  const StatusCallback& callback) {
  const Schema& schema = table->schema();
  bool permit_acquired;

  // We make up to two fast lookup calls.
  // - The first is unconditional.
  // - The second is made if the first misses and if we failed to acquire a
  //   master (slow) lookup permit. Failed permit acquisition means we
  //   slept for a bit, and it's possible that the cache was populated
  //   while we were asleep.
  for (int i = 0; i < 2; i++) {
    if (PREDICT_TRUE(LookupTabletByKeyFastPath(table, key, remote_tablet)) &&
        (*remote_tablet)->HasLeader()) {
      VLOG(3) << "Fast lookup: found tablet " << (*remote_tablet)->tablet_id()
                << " for " << schema.DebugEncodedRowKey(key.ToString())
                << " of " << table->name() << " in "
                << (i == 0 ? "first" : "second") << " pass";

      callback(Status::OK());
      return;
    }

    if (i == 0) {
      permit_acquired = AcquireMasterLookupPermit();
      VLOG(3) << (permit_acquired ? "Acquired" : "Did not acquire")
              << " permit for master lookup";
      if (permit_acquired) {
        break;
      }
    }
  }

  // Slow path: must lookup the tablet in the master.
  VLOG(3) << "Fast lookup: no tablet "
          << " for " << schema.DebugEncodedRowKey(key.ToString())
          << " of " << table->name();
  GetTableLocationsRequestPB req;
  req.mutable_table()->set_table_name(table->name());
  req.set_start_key(key.data(), key.size());

  // The end key is left unset intentionally so that we'll prefetch some
  // additional tablets.

  InFlightLookup *ref = new InFlightLookup;
  ref->mc = this;
  ref->permit_held = permit_acquired;
  ref->rpc.set_timeout(MonoDelta::FromMilliseconds(5000));
  ref->user_callback = callback;
  ref->table_name = table->name();
  ref->key = key;
  ref->remote_tablet = remote_tablet;

  shared_ptr<MasterServiceProxy> master = table->client()->master_proxy();
  master->GetTableLocationsAsync(req, &ref->resp, &ref->rpc,
                                 boost::bind(&MetaCache::GetTableLocationsCB, this, ref));
}

void MetaCache::LookupTabletByID(const std::string& tablet_id,
                                 scoped_refptr<RemoteTablet>* remote_tablet) {
  *remote_tablet = FindOrDie(tablets_by_id_, tablet_id);
}

void MetaCache::MarkTSFailed(RemoteTabletServer* ts) {
  boost::shared_lock<rw_spinlock> l(lock_);

  // TODO: replace with a ts->tablet multimap for faster lookup?
  BOOST_FOREACH(const TabletMap::value_type& tablet, tablets_by_id_) {
    tablet.second->MarkReplicaFailed(ts);
  }
}

bool MetaCache::AcquireMasterLookupPermit() {
  return master_lookup_sem_.TimedAcquire(MonoDelta::FromMilliseconds(5));
}

void MetaCache::ReleaseMasterLookupPermit() {
  master_lookup_sem_.Release();
}

} // namespace client
} // namespace kudu
