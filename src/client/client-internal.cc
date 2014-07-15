// Copyright (c) 2014, Cloudera,inc.

#include "client/client-internal.h"

#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "client/meta_cache.h"
#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/strings/substitute.h"
#include "master/master.h"
#include "master/master.pb.h"
#include "master/master.proxy.h"
#include "rpc/rpc_controller.h"
#include "util/net/dns_resolver.h"

namespace kudu {

using master::IsAlterTableDoneRequestPB;
using master::IsAlterTableDoneResponsePB;
using master::IsCreateTableDoneRequestPB;
using master::IsCreateTableDoneResponsePB;
using rpc::RpcController;
using strings::Substitute;

namespace client {

KuduClient::Data::Data() {
}

KuduClient::Data::~Data() {
}

Status KuduClient::Data::GetTabletServer(KuduClient* client,
                                         const string& tablet_id,
                                         ReplicaSelection selection,
                                         RemoteTabletServer** ts) {
  // TODO: write a proper async version of this for async client.
  scoped_refptr<RemoteTablet> remote_tablet;
  meta_cache_->LookupTabletByID(tablet_id, &remote_tablet);

  RemoteTabletServer* ret = NULL;
  switch (selection) {
    case LEADER_ONLY:
      ret = remote_tablet->LeaderTServer();
      break;
    case CLOSEST_REPLICA:
      ret = PickClosestReplica(remote_tablet);
      break;
    case FIRST_REPLICA:
      ret = remote_tablet->FirstTServer();
      break;
    default:
      LOG(FATAL) << "Unknown ProxySelection value " << selection;
  }
  if (PREDICT_FALSE(ret == NULL)) {
    return Status::ServiceUnavailable(
        Substitute("No $0 for tablet $1",
                   selection == LEADER_ONLY ? "LEADER" : "replicas", tablet_id));
  }
  Synchronizer s;
  ret->RefreshProxy(client, s.AsStatusCallback(), false);
  RETURN_NOT_OK(s.Wait());

  *ts = ret;
  return Status::OK();
}

Status KuduClient::Data::IsCreateTableInProgress(const string& table_name,
                                                 const MonoTime& deadline,
                                                 bool *create_in_progress) {
  IsCreateTableDoneRequestPB req;
  IsCreateTableDoneResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE)));
  RETURN_NOT_OK(master_proxy_->IsCreateTableDone(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *create_in_progress = !resp.done();
  return Status::OK();
}
Status KuduClient::Data::IsAlterTableInProgress(const string& table_name,
                                                const MonoTime& deadline,
                                                bool *alter_in_progress) {
  IsAlterTableDoneRequestPB req;
  IsAlterTableDoneResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  rpc.set_timeout(deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE)));
  RETURN_NOT_OK(master_proxy_->IsAlterTableDone(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  *alter_in_progress = !resp.done();
  return Status::OK();
}

Status KuduClient::Data::InitLocalHostNames() {
  // Currently, we just use our configured hostname, and resolve it to come up with
  // a list of potentially local hosts. It would be better to iterate over all of
  // the local network adapters. See KUDU-327.
  string hostname;
  RETURN_NOT_OK(GetHostname(&hostname));

  // We don't want to consider 'localhost' to be local - otherwise if a misconfigured
  // server reports its own name as localhost, all clients will hammer it.
  if (hostname != "localhost" && hostname != "localhost.localdomain") {
    local_host_names_.insert(hostname);
    VLOG(1) << "Considering host " << hostname << " local";
  }

  vector<Sockaddr> addresses;
  RETURN_NOT_OK_PREPEND(HostPort(hostname, 0).ResolveAddresses(&addresses),
                        Substitute("Could not resolve local host name '$0'", hostname));

  BOOST_FOREACH(const Sockaddr& addr, addresses) {
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
  BOOST_FOREACH(const HostPort& hp, host_ports) {
    if (IsLocalHostPort(hp)) return true;
  }
  return false;
}

RemoteTabletServer* KuduClient::Data::PickClosestReplica(
  const scoped_refptr<RemoteTablet>& rt) const {

  vector<RemoteTabletServer*> candidates;
  rt->GetRemoteTabletServers(&candidates);

  BOOST_FOREACH(RemoteTabletServer* rts, candidates) {
    if (IsTabletServerLocal(*rts)) {
      return rts;
    }
  }

  // No local one found. Pick a random one
  return !candidates.empty() ? candidates[rand() % candidates.size()] : NULL;
}

} // namespace client
} // namespace kudu
