// Copyright (c) 2013, Cloudera, inc.

#include "kudu/client/meta_cache.h"

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/rpc.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"

using std::string;
using std::map;
using std::tr1::shared_ptr;
using strings::Substitute;

namespace kudu {

using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using master::MasterServiceProxy;
using master::TabletLocationsPB;
using master::TabletLocationsPB_ReplicaPB;
using master::TSInfoPB;
using metadata::QuorumPeerPB;
using rpc::Messenger;
using tserver::TabletServerServiceProxy;

namespace client {

namespace internal {

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
    s = Status::NotFound("No addresses for " + hp.ToString());
  }

  if (!s.ok()) {
    s = s.CloneAndPrepend("Failed to resolve address for TS " + uuid_);
    user_callback.Run(s);
    return;
  }

  VLOG(1) << "Successfully resolved " << hp.ToString() << ": "
          << (*addrs)[0].ToString();

  {
    lock_guard<simple_spinlock> l(&lock_);
    proxy_.reset(new TabletServerServiceProxy(client->data_->messenger_, (*addrs)[0]));
  }
  user_callback.Run(s);
}

void RemoteTabletServer::RefreshProxy(KuduClient* client,
                                      const StatusCallback& cb,
                                      bool force) {
  HostPort hp;
  {
    unique_lock<simple_spinlock> l(&lock_);

    if (proxy_ && !force) {
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

  vector<Sockaddr>* addrs = new vector<Sockaddr>();
  client->data_->dns_resolver_->ResolveAddresses(
    hp, addrs, Bind(&RemoteTabletServer::DnsResolutionFinished,
                    Unretained(this), hp, addrs, client, cb));
}

void RemoteTabletServer::Update(const master::TSInfoPB& pb) {
  CHECK_EQ(pb.permanent_uuid(), uuid_);

  lock_guard<simple_spinlock> l(&lock_);

  rpc_hostports_.clear();
  BOOST_FOREACH(const HostPortPB& hostport_pb, pb.rpc_addresses()) {
    rpc_hostports_.push_back(HostPort(hostport_pb.host(), hostport_pb.port()));
  }
}

shared_ptr<TabletServerServiceProxy> RemoteTabletServer::proxy() const {
  lock_guard<simple_spinlock> l(&lock_);
  CHECK(proxy_);
  return proxy_;
}

string RemoteTabletServer::ToString() const {
  return uuid_;
}

void RemoteTabletServer::GetHostPorts(vector<HostPort>* host_ports) const {
  lock_guard<simple_spinlock> l(&lock_);
  *host_ports = rpc_hostports_;
}

////////////////////////////////////////////////////////////


void RemoteTablet::Refresh(const TabletServerMap& tservers,
                           const google::protobuf::RepeatedPtrField
                             <TabletLocationsPB_ReplicaPB>& replicas) {
  // Adopt the data from the successful response.
  lock_guard<simple_spinlock> l(&lock_);
  replicas_.clear();
  BOOST_FOREACH(const TabletLocationsPB_ReplicaPB& r, replicas) {
    RemoteReplica rep;
    rep.ts = FindOrDie(tservers, r.ts_info().permanent_uuid());
    rep.role = r.role();
    rep.failed = false;
    replicas_.push_back(rep);
  }
}

void RemoteTablet::MarkReplicaFailed(RemoteTabletServer *ts,
                                     const Status& status) {
  LOG(WARNING) << "Replica " << tablet_id_ << " on ts " << ts->ToString() << " has failed: "
               << status.ToString();

  lock_guard<simple_spinlock> l(&lock_);
  BOOST_FOREACH(RemoteReplica& rep, replicas_) {
    if (rep.ts == ts) {
      rep.failed = true;
    }
  }
}

int RemoteTablet::GetNumFailedReplicas() const {
  int failed = 0;
  lock_guard<simple_spinlock> l(&lock_);
  BOOST_FOREACH(const RemoteReplica& rep, replicas_) {
    if (rep.failed) {
      failed++;
    }
  }
  return failed;
}

RemoteTabletServer* RemoteTablet::FirstTServer() const {
  lock_guard<simple_spinlock> l(&lock_);
  BOOST_FOREACH(const RemoteReplica& rep, replicas_) {
    if (!rep.failed) {
      return rep.ts;
    }
  }
  return NULL;
}

RemoteTabletServer* RemoteTablet::LeaderTServer() const {
  lock_guard<simple_spinlock> l(&lock_);
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

void RemoteTablet::GetRemoteTabletServers(vector<RemoteTabletServer*>* servers) const {
  lock_guard<simple_spinlock> l(&lock_);
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
    master_lookup_sem_(50),
    timeout_(MonoDelta::FromMilliseconds(5000)) { // TODO: make this configurable
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

// A GetTableLocations RPC which is in-flight to a master.
//
// Keeps a reference on the owning metacache while alive.
class LookupRpc : public Rpc {
 public:
  LookupRpc(const scoped_refptr<MetaCache>& meta_cache,
            const shared_ptr<MasterServiceProxy>& master_proxy,
            const StatusCallback& user_cb,
            const string& table_name,
            const Slice& key,
            scoped_refptr<RemoteTablet> *remote_tablet,
            const MonoTime& deadline,
            const shared_ptr<Messenger>& messenger);
  virtual ~LookupRpc();
  virtual void SendRpc() OVERRIDE;
  virtual string ToString() const OVERRIDE;

  const GetTableLocationsResponsePB& resp() const { return resp_; }
  const string& table_name() const { return table_name_; }

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  // Pointer back to the tablet cache. Populated with location information
  // when the lookup finishes successfully. When the RPC is destroyed, a
  // master lookup permit is returned to the cache.
  scoped_refptr<MetaCache> meta_cache_;

  // Proxy to the master server servicing this lookup.
  shared_ptr<MasterServiceProxy> master_proxy_;

  // Request body.
  GetTableLocationsRequestPB req_;

  // Response body.
  GetTableLocationsResponsePB resp_;

  // User-specified callback to invoke when the lookup finishes.
  //
  // Always invoked, regardless of success or failure.
  StatusCallback user_cb_;

  // Table to lookup.
  string table_name_;

  // Encoded key to lookup.
  Slice key_;

  // When lookup finishes successfully, the selected tablet is
  // written here prior to invoking 'user_cb_'.
  scoped_refptr<RemoteTablet> *remote_tablet_;
};

LookupRpc::LookupRpc(const scoped_refptr<MetaCache>& meta_cache,
                     const shared_ptr<MasterServiceProxy>& master_proxy,
                     const StatusCallback& user_cb,
                     const string& table_name,
                     const Slice& key,
                     scoped_refptr<RemoteTablet> *remote_tablet,
                     const MonoTime& deadline,
                     const shared_ptr<Messenger>& messenger)
  : Rpc(deadline, messenger),
    meta_cache_(meta_cache),
    master_proxy_(master_proxy),
    user_cb_(user_cb),
    table_name_(table_name),
    key_(key),
    remote_tablet_(remote_tablet) {

  req_.mutable_table()->set_table_name(table_name_);
  req_.set_start_key(key_.data(), key_.size());

  // The end key is left unset intentionally so that we'll prefetch some
  // additional tablets.
}

LookupRpc::~LookupRpc() {
  meta_cache_->ReleaseMasterLookupPermit();
}

void LookupRpc::SendRpc() {
  master_proxy_->GetTableLocationsAsync(req_, &resp_, &retrier().controller(),
                                        boost::bind(&LookupRpc::SendRpcCb, this, Status::OK()));
}

string LookupRpc::ToString() const {
  return Substitute("GetTableLocations($0, $1)", table_name_, key_.ToString());
}

void LookupRpc::SendRpcCb(const Status& status) {
  gscoped_ptr<LookupRpc> delete_me(this); // delete on scope exit

  // Prefer early failures over controller failures.
  Status new_status = status;
  if (new_status.ok() && retrier().HandleResponse(this, &new_status)) {
    ignore_result(delete_me.release());
    return;
  }

  // Prefer controller failures over response failures.
  if (new_status.ok() && resp_.has_error()) {
    new_status = StatusFromPB(resp_.error().status());
  }

  // Prefer response failures over no tablets found.
  if (new_status.ok() && resp_.tablet_locations_size() == 0) {
    new_status = Status::NotFound("No such tablet found");
  }

  if (new_status.ok()) {
    *remote_tablet_ = meta_cache_->ProcessLookupResponse(*this);
  } else {
    LOG(WARNING) << ToString() << " failed: " << new_status.ToString();
  }
  user_cb_.Run(new_status);
}

const scoped_refptr<RemoteTablet>& MetaCache::ProcessLookupResponse(const LookupRpc& rpc) {
  lock_guard<rw_spinlock> l(&lock_);
  SliceTabletMap& tablets_by_key = LookupOrInsert(&tablets_by_table_and_key_,
                                                  rpc.table_name(), SliceTabletMap());
  BOOST_FOREACH(const TabletLocationsPB& loc, rpc.resp().tablet_locations()) {
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

    VLOG(3) << "Caching tablet " << tablet_id << " for (" << rpc.table_name()
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
  return FindOrDie(tablets_by_id_, rpc.resp().tablet_locations(0).tablet_id());
}

bool MetaCache::LookupTabletByKeyFastPath(const KuduTable* table,
                                          const Slice& key,
                                          scoped_refptr<RemoteTablet>* remote_tablet) {
  shared_lock<rw_spinlock> l(&lock_);
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

void MetaCache::LookupTabletByKeyCb(const Status& abort_status,
                                    const KuduTable* table,
                                    const Slice& key,
                                    scoped_refptr<RemoteTablet>* remote_tablet,
                                    const StatusCallback& callback) {
  if (!abort_status.ok()) {
    LOG(WARNING) << "Rescheduled lookup failed: "
                 << abort_status.ToString();
    callback.Run(abort_status);
  } else {
    LookupTabletByKey(table, key, remote_tablet, callback);
  }
}

void MetaCache::LookupTabletByKey(const KuduTable* table,
                                  const Slice& key,
                                  scoped_refptr<RemoteTablet>* remote_tablet,
                                  const StatusCallback& callback) {
  const Schema* schema = table->schema().schema_.get();

  // Fast path: lookup in the cache.
  if (PREDICT_TRUE(LookupTabletByKeyFastPath(table, key, remote_tablet)) &&
      (*remote_tablet)->HasLeader()) {
    VLOG(3) << "Fast lookup: found tablet " << (*remote_tablet)->tablet_id()
                    << " for " << schema->DebugEncodedRowKey(key.ToString())
                    << " of " << table->name();
    callback.Run(Status::OK());
    return;
  }

  // Slow path: must lookup the tablet in the master.
  VLOG(3) << "Fast lookup: no tablet"
          << " for " << schema->DebugEncodedRowKey(key.ToString())
          << " of " << table->name();

  if (!AcquireMasterLookupPermit()) {
    // Couldn't get a permit, try again in 1+rand(1..5) ms.
    int num_ms = 1 + ((rand() % 5) + 1);
    VLOG(3) << "All master lookup permits are held, will retry in "
            << num_ms << " ms";
    client_->data_->messenger_->ScheduleOnReactor(
        boost::bind(&MetaCache::LookupTabletByKeyCb, this, _1, table, key,
                    remote_tablet, callback),
        MonoDelta::FromMilliseconds(num_ms));
    return;
  }

  // Got a permit, construct and issue the lookup RPC.
  MonoTime deadline;
  if (timeout_.Initialized()) {
    deadline = MonoTime::Now(MonoTime::FINE);
    deadline.AddDelta(timeout_);
  }
  LookupRpc* rpc = new LookupRpc(this,
                                 client_->data_->master_proxy_,
                                 callback,
                                 table->name(),
                                 key,
                                 remote_tablet,
                                 deadline,
                                 client_->data_->messenger_);
  rpc->SendRpc();
}

void MetaCache::LookupTabletByID(const string& tablet_id,
                                 scoped_refptr<RemoteTablet>* remote_tablet) {
  *remote_tablet = FindOrDie(tablets_by_id_, tablet_id);
}

void MetaCache::MarkTSFailed(RemoteTabletServer* ts,
                             const Status& status) {
  shared_lock<rw_spinlock> l(&lock_);

  Status ts_status = status.CloneAndPrepend("TS failed");

  // TODO: replace with a ts->tablet multimap for faster lookup?
  BOOST_FOREACH(const TabletMap::value_type& tablet, tablets_by_id_) {
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
