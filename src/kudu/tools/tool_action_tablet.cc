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

#include "kudu/tools/tool_action.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduTablet;
using client::KuduTabletServer;
using consensus::ChangeConfigType;
using consensus::ConsensusServiceProxy;
using consensus::RaftPeerPB;
using rpc::RpcController;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace {

const char* const kReplicaTypeArg = "replica_type";
const char* const kReplicaUuidArg = "replica_uuid";

Status GetRpcAddressForTS(const client::sp::shared_ptr<KuduClient>& client,
                          const string& uuid,
                          HostPort* hp) {
  vector<KuduTabletServer*> servers;
  ElementDeleter deleter(&servers);
  RETURN_NOT_OK(client->ListTabletServers(&servers));
  for (const auto* s : servers) {
    if (s->uuid() == uuid) {
      hp->set_host(s->hostname());
      hp->set_port(s->port());
      return Status::OK();
    }
  }

  return Status::NotFound(Substitute(
      "server $0 has no RPC address registered with the Master", uuid));
}

Status GetTabletLeader(const client::sp::shared_ptr<KuduClient>& client,
                       const string& tablet_id,
                       string* leader_uuid,
                       HostPort* leader_hp) {
  KuduTablet* tablet_raw;
  unique_ptr<KuduTablet> tablet;
  RETURN_NOT_OK(client->GetTablet(tablet_id, &tablet_raw));
  tablet.reset(tablet_raw);

  for (const auto* r : tablet->replicas()) {
    if (r->is_leader()) {
      *leader_uuid = r->ts().uuid();
      leader_hp->set_host(r->ts().hostname());
      leader_hp->set_port(r->ts().port());
      return Status::OK();
    }
  }

  return Status::NotFound(Substitute(
      "No leader replica found for tablet $0", tablet_id));
}

Status ChangeConfig(const RunnerContext& context, ChangeConfigType cc_type) {
  // Parse and validate arguments.
  RaftPeerPB peer_pb;
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& replica_uuid = FindOrDie(context.required_args, kReplicaUuidArg);
  if (cc_type == consensus::ADD_SERVER || cc_type == consensus::CHANGE_ROLE) {
    const string& replica_type = FindOrDie(context.required_args, kReplicaTypeArg);
    string uppercase_peer_type;
    ToUpperCase(replica_type, &uppercase_peer_type);
    RaftPeerPB::MemberType member_type_val;
    if (!RaftPeerPB::MemberType_Parse(uppercase_peer_type, &member_type_val)) {
      return Status::InvalidArgument("Unrecognized peer type", replica_type);
    }
    peer_pb.set_member_type(member_type_val);
  }
  peer_pb.set_permanent_uuid(replica_uuid);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));

  // When adding a new server, we need to provide the server's RPC address.
  if (cc_type == consensus::ADD_SERVER) {
    HostPort hp;
    RETURN_NOT_OK(GetRpcAddressForTS(client, replica_uuid, &hp));
    RETURN_NOT_OK(HostPortToPB(hp, peer_pb.mutable_last_known_addr()));
  }

  // Find this tablet's leader replica. We need its UUID and RPC address.
  string leader_uuid;
  HostPort leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp));

  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

  consensus::ChangeConfigRequestPB req;
  consensus::ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(client->default_admin_operation_timeout());
  req.set_dest_uuid(leader_uuid);
  req.set_tablet_id(tablet_id);
  req.set_type(cc_type);
  *req.mutable_server() = peer_pb;
  RETURN_NOT_OK(proxy->ChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status AddReplica(const RunnerContext& context) {
  return ChangeConfig(context, consensus::ADD_SERVER);
}

Status ChangeReplicaType(const RunnerContext& context) {
  return ChangeConfig(context, consensus::CHANGE_ROLE);
}

Status RemoveReplica(const RunnerContext& context) {
  return ChangeConfig(context, consensus::REMOVE_SERVER);
}

Status LeaderStepDown(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));

  // If leader is not present, command can gracefully return.
  string leader_uuid;
  HostPort leader_hp;
  Status s = GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp);
  if (s.IsNotFound()) {
    cout << s.ToString() << endl;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

  consensus::LeaderStepDownRequestPB req;
  consensus::LeaderStepDownResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(client->default_admin_operation_timeout());
  req.set_dest_uuid(leader_uuid);
  req.set_tablet_id(tablet_id);

  RETURN_NOT_OK(proxy->LeaderStepDown(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildTabletMode() {
  unique_ptr<Action> add_replica =
      ActionBuilder("add_replica", &AddReplica)
      .Description("Add a new replica to a tablet's Raft configuration")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kReplicaUuidArg, "New replica's UUID" })
      .AddRequiredParameter(
          { kReplicaTypeArg, "New replica's type. Must be VOTER or NON-VOTER."
          })
      .Build();

  unique_ptr<Action> change_replica_type =
      ActionBuilder("change_replica_type", &ChangeReplicaType)
      .Description(
          "Change the type of an existing replica in a tablet's Raft configuration")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kReplicaUuidArg, "Existing replica's UUID" })
      .AddRequiredParameter(
          { kReplicaTypeArg, "Existing replica's new type. Must be VOTER or NON-VOTER."
          })
      .Build();

  unique_ptr<Action> remove_replica =
      ActionBuilder("remove_replica", &RemoveReplica)
      .Description("Remove an existing replica from a tablet's Raft configuration")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kReplicaUuidArg, "Existing replica's UUID" })
      .Build();

  unique_ptr<Action> leader_step_down =
      ActionBuilder("leader_step_down", &LeaderStepDown)
      .Description("Force the tablet's leader replica to step down")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .Build();

  unique_ptr<Mode> change_config =
      ModeBuilder("change_config")
      .Description("Change a tablet's Raft configuration")
      .AddAction(std::move(add_replica))
      .AddAction(std::move(change_replica_type))
      .AddAction(std::move(remove_replica))
      .Build();

  return ModeBuilder("tablet")
      .Description("Operate on remote Kudu tablets")
      .AddMode(std::move(change_config))
      .AddAction(std::move(leader_step_down))
      .Build();
}

} // namespace tools
} // namespace kudu

