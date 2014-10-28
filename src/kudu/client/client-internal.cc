// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/client-internal.h"

#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "kudu/client/meta_cache.h"
#include "kudu/client/rpc.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"

namespace kudu {

using master::IsAlterTableDoneRequestPB;
using master::IsAlterTableDoneResponsePB;
using master::IsCreateTableDoneRequestPB;
using master::IsCreateTableDoneResponsePB;
using master::ListMastersRequestPB;
using master::ListMastersResponsePB;
using master::MasterServiceProxy;
using metadata::QuorumPeerPB;

using rpc::RpcController;
using strings::Substitute;

namespace client {

using internal::RemoteTablet;
using internal::RemoteTabletServer;
using internal::Rpc;

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
  rpc.set_deadline(deadline);
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

namespace internal {

// NOTE: This RPC may only be called synchronously: it is the callers
// responsibility to de-allocate the RPC object when finished.
class GetLeaderMasterRpc : public Rpc {
 public:
  GetLeaderMasterRpc(const StatusCallback& cb,
                     const vector<Sockaddr>& master_addrs,
                     const MonoTime& deadline,
                     const shared_ptr<rpc::Messenger>& messenger);

  virtual void SendRpc() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

  virtual ~GetLeaderMasterRpc();

  const HostPort& leader_host_port() const {
    return leader_host_port_;
  }

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  // Sets 'leader_host_port_' to the host/port of the leader master if
  // there is enough information in 'resp_' to do so.  Returns
  // 'Status::NotFound' if no leader master is found.
  Status LeaderMasterHostPortFromResponse();

  StatusCallback cb_;
  int node_idx_;
  vector<Sockaddr> master_addrs_;
  ListMastersResponsePB resp_;
  HostPort leader_host_port_;
};

GetLeaderMasterRpc::GetLeaderMasterRpc(const StatusCallback& cb,
                                       const vector<Sockaddr>& master_addrs,
                                       const MonoTime& deadline,
                                       const shared_ptr<rpc::Messenger>& messenger)
    : Rpc(deadline, messenger),
      cb_(cb),
      node_idx_(0),
      master_addrs_(master_addrs) {
}

void GetLeaderMasterRpc::SendRpc() {
  ListMastersRequestPB req;
  MasterServiceProxy proxy_for_idx(retrier().messenger(), master_addrs_[node_idx_]);
  proxy_for_idx.ListMastersAsync(
      req, &resp_, &retrier().controller(),
      boost::bind(&GetLeaderMasterRpc::SendRpcCb, this, Status::OK()));
}

std::string GetLeaderMasterRpc::ToString() const {
  vector<string> master_addrs_str;
  // TODO add a generic method to gutil/strings/join.h to be able to
  // join string representation of elements of a container by
  // iterating over the container while calling an arbitrary method
  // on those elements.
  BOOST_FOREACH(const Sockaddr& master_addr, master_addrs_) {
    master_addrs_str.push_back(master_addr.ToString());
  }
  return Substitute("GetLeaderMasterRpc: idx = $0, master_addrs = $1",
                    node_idx_, JoinStrings(master_addrs_str, ","));
}

GetLeaderMasterRpc::~GetLeaderMasterRpc() {
}

Status GetLeaderMasterRpc::LeaderMasterHostPortFromResponse() {
  if (resp_.has_error()) {
    return StatusFromPB(resp_.error());
  }
  BOOST_FOREACH(const ListMastersResponsePB::Entry& entry, resp_.masters()) {
    if (entry.has_error()) {
      LOG(WARNING) << "Error encountered for master entry " << entry.ShortDebugString()
                   << ": " << StatusFromPB(entry.error()).ToString();
      continue;
    }
    if (!entry.has_role()) {
      if (entry.local() && resp_.masters().size() == 1) {
        // Non-distributed master configuration: there is only one
        // entry in the list and it's a local entry.
        return HostPortFromPB(entry.registration().rpc_addresses(0), &leader_host_port_);
      } else {
        return Status::IllegalState(
            Substitute("Every master in a distributed configuration must have a role,"
                       "but entry ($0) has no role. Rest of ListMastersResponse: $1",
                       entry.ShortDebugString(), resp_.ShortDebugString()));
      }
    } else if (entry.role() == QuorumPeerPB::LEADER) {
      // This is the leader of a distributed quorum.
      return HostPortFromPB(entry.registration().rpc_addresses(0), &leader_host_port_);
    }
  }
  return Status::NotFound("No leader found. ListMastersResponse: " +
                          resp_.ShortDebugString());
}

void GetLeaderMasterRpc::SendRpcCb(const Status& status) {
  Status new_status = status;
  if (new_status.ok() && retrier().HandleResponse(this, &new_status)) {
    return;
  }

  if (new_status.ok()) {
    new_status = LeaderMasterHostPortFromResponse();
  }

  // If there was a network error talking to the master at 'node_idx'
  // or if 'resp_' doesn't contain the leader master, try again from
  // the next node.  If we've exhausted 'proxy_by_idx_', then delay
  // before cycling through 'proxy_by_idx_' again.
  if (new_status.IsNetworkError() || new_status.IsNotFound()) {
    if (++node_idx_ == master_addrs_.size()) {
      node_idx_ = 0;
      retrier().DelayedRetry(this);
    } else {
      // Don't delay if we haven't exhausted the masters.
      retrier().controller().Reset();
      SendRpc();
    }
    return;
  }
  cb_.Run(new_status);
}

} // namespace internal

Status KuduClient::Data::SetMasterServerProxy() {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_select_master_timeout_);
  vector<Sockaddr> master_sockaddrs;
  BOOST_FOREACH(const string& master_server_addr, master_server_addrs_) {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(ParseAddressList(master_server_addr, master::Master::kDefaultPort, &addrs));
    if (addrs.empty()) {
      return Status::InvalidArgument(Substitute("No master address specified by '$0'",
                                                 master_server_addr));
    }
    if (addrs.size() > 1) {
      LOG(WARNING) << "Specified master server address '" << master_server_addr << "' "
                   << "resolved to multiple IPs. Using " << addrs[0].ToString();
    }
    master_sockaddrs.push_back(addrs[0]);
  }

  // See 'GetLeaderMasterRpc' above.
  Synchronizer sync;
  internal::GetLeaderMasterRpc rpc(sync.AsStatusCallback(),
                                   master_sockaddrs,
                                   deadline,
                                   messenger_);
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());

  // Instantiate 'master_proxy_'.
  Sockaddr leader_sock_addr;
  RETURN_NOT_OK(SockaddrFromHostPort(rpc.leader_host_port(), &leader_sock_addr));
  master_proxy_.reset(new MasterServiceProxy(messenger_, leader_sock_addr));
  return Status::OK();
}

} // namespace client
} // namespace kudu
