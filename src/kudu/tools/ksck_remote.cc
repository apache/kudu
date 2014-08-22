// Copyright (c) 2014, Cloudera, inc.

#include "kudu/tools/ksck_remote.h"

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"

DEFINE_int64(timeout_ms, 1000 * 60, "RPC timeout in milliseconds");
DEFINE_int64(tablets_batch_size_max, 100, "How many tablets to get from the Master per RPC");

namespace kudu {
namespace tools {

static const std::string kMessengerName = "ksck";

using rpc::Messenger;
using rpc::MessengerBuilder;
using rpc::RpcController;
using std::tr1::shared_ptr;
using std::vector;
using std::string;
using strings::Substitute;

MonoDelta GetDefaultTimeout() {
  return MonoDelta::FromMilliseconds(FLAGS_timeout_ms);
}

Status RemoteKsckTabletServer::Connect() {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(address_, tserver::TabletServer::kDefaultPort, &addrs));

  MessengerBuilder builder(kMessengerName);
  RETURN_NOT_OK(builder.Build(&messenger_));
  proxy_.reset(new tserver::TabletServerServiceProxy(messenger_, addrs[0]));

  tserver::PingRequestPB req;
  tserver::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  return proxy_->Ping(req, &resp, &rpc);
}


Status RemoteKsckMaster::Connect() {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(address_, master::Master::kDefaultPort, &addrs));

  MessengerBuilder builder(kMessengerName);
  RETURN_NOT_OK(builder.Build(&messenger_));
  proxy_.reset(new master::MasterServiceProxy(messenger_, addrs[0]));

  master::PingRequestPB req;
  master::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  return proxy_->Ping(req, &resp, &rpc);
}

Status RemoteKsckMaster::RetrieveTabletServersList(
    vector<shared_ptr<KsckTabletServer> >& tablet_servers) {
  master::ListTabletServersRequestPB req;
  master::ListTabletServersResponsePB resp;
  RpcController rpc;

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->ListTabletServers(req, &resp, &rpc));
  BOOST_FOREACH(const master::ListTabletServersResponsePB_Entry& e, resp.servers()) {
    HostPortPB addr = e.registration().rpc_addresses(0);
    HostPort hp(addr.host(), addr.port());
    tablet_servers.push_back(shared_ptr<KsckTabletServer>(
        new RemoteKsckTabletServer(e.instance_id().permanent_uuid(), hp.ToString())));
  }
  return Status::OK();
}

Status RemoteKsckMaster::RetrieveTablesList(vector<shared_ptr<KsckTable> >& tables) {
  master::ListTablesRequestPB req;
  master::ListTablesResponsePB resp;
  RpcController rpc;

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->ListTables(req, &resp, &rpc));
  // TODO check resp.error
  vector<shared_ptr<KsckTable> > tables_temp;
  BOOST_FOREACH(const master::ListTablesResponsePB_TableInfo& info, resp.tables()) {
    int num_replicas;
    RETURN_NOT_OK(GetNumReplicasForTable(info.name(), &num_replicas));
    shared_ptr<KsckTable> table(new KsckTable(info.name(), num_replicas));
    tables_temp.push_back(table);
  }
  tables.assign(tables_temp.begin(), tables_temp.end());
  return Status::OK();
}

Status RemoteKsckMaster::RetrieveTabletsList(const shared_ptr<KsckTable>& table) {
  vector<shared_ptr<KsckTablet> > tablets;
  bool more_tablets = true;
  string last_key;
  while (more_tablets) {
    GetTabletsBatch(table->name(), &last_key, tablets, &more_tablets);
  }

  table->set_tablets(tablets);
  return Status::OK();
}

Status RemoteKsckMaster::GetTabletsBatch(const string& table_name, string* last_key,
   vector<shared_ptr<KsckTablet> >& tablets, bool* more_tablets) {
  master::GetTableLocationsRequestPB req;
  master::GetTableLocationsResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);
  req.set_max_returned_locations(FLAGS_tablets_batch_size_max);
  req.set_start_key(*last_key);

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->GetTableLocations(req, &resp, &rpc));
  BOOST_FOREACH(const master::TabletLocationsPB& locations, resp.tablet_locations()) {
    shared_ptr<KsckTablet> tablet(new KsckTablet(locations.tablet_id()));
    vector<shared_ptr<KsckTabletReplica> > replicas;
    BOOST_FOREACH(const master::TabletLocationsPB_ReplicaPB& replica, locations.replicas()) {
      bool is_leader = replica.role() == metadata::QuorumPeerPB::LEADER;
      bool is_follower = replica.role() == metadata::QuorumPeerPB::FOLLOWER;
      replicas.push_back(shared_ptr<KsckTabletReplica>(
          new KsckTabletReplica(replica.ts_info().permanent_uuid(), is_leader, is_follower)));
    }
    tablet->set_replicas(replicas);
    tablets.push_back(tablet);
  }
  if (resp.tablet_locations_size() != 0) {
    *last_key = (resp.tablet_locations().end() - 1)->end_key();
  } else {
    return Status::NotFound(Substitute(
      "The Master returned 0 tablets for GetTableLocations of table $0 at start key $1",
      table_name, *(last_key)));
  }
  if (last_key->empty()) {
    *more_tablets = false;
  }
  return Status::OK();
}

Status RemoteKsckMaster::GetNumReplicasForTable(const string& table_name, int* num_replicas) {
  master::GetTableSchemaRequestPB req;
  master::GetTableSchemaResponsePB resp;
  RpcController rpc;

  req.mutable_table()->set_table_name(table_name);

  rpc.set_timeout(GetDefaultTimeout());
  RETURN_NOT_OK(proxy_->GetTableSchema(req, &resp, &rpc));

  *num_replicas = resp.num_replicas();
  return Status::OK();
}

} // namespace tools
} // namespace kudu
