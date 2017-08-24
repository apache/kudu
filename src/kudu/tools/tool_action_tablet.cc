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
#include <fstream>  // IWYU pragma: keep
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DEFINE_int64(move_copy_timeout_sec, 600,
             "Number of seconds to wait for tablet copy to complete when relocating a tablet");
DEFINE_int64(move_leader_timeout_sec, 30,
             "Number of seconds to wait for a leader when relocating a leader tablet");

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduTablet;
using client::KuduTabletServer;
using consensus::ChangeConfigType;
using consensus::ConsensusServiceProxy;
using consensus::ConsensusStatePB;
using consensus::GetConsensusStateRequestPB;
using consensus::GetConsensusStateResponsePB;
using consensus::GetLastOpIdRequestPB;
using consensus::GetLastOpIdResponsePB;
using consensus::OpId;
using consensus::RaftPeerPB;
using rpc::RpcController;
using std::cout;
using std::endl;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace {

const char* const kReplicaTypeArg = "replica_type";
const char* const kTsUuidArg = "ts_uuid";
const char* const kFromTsUuidArg = "from_ts_uuid";
const char* const kToTsUuidArg = "to_ts_uuid";

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

Status DoKsckForTablet(const vector<string>& master_addresses, const string& tablet_id) {
  shared_ptr<KsckMaster> master;
  RETURN_NOT_OK(RemoteKsckMaster::Build(master_addresses, &master));
  shared_ptr<KsckCluster> cluster(new KsckCluster(master));

  // Print to an unopened ofstream to discard ksck output.
  // See https://stackoverflow.com/questions/8243743.
  std::ofstream null_stream;
  Ksck ksck(cluster, &null_stream);
  ksck.set_tablet_id_filters({ tablet_id });
  RETURN_NOT_OK(ksck.CheckMasterRunning());
  RETURN_NOT_OK(ksck.FetchTableAndTabletInfo());
  RETURN_NOT_OK(ksck.FetchInfoFromTabletServers());
  return ksck.CheckTablesConsistency();
}

Status WaitForCleanKsck(const vector<string>& master_addresses,
                        const string& tablet_id,
                        const MonoDelta& timeout) {
  Status s;
  MonoTime deadline = MonoTime::Now() + timeout;
  while (MonoTime::Now() < deadline) {
    s = DoKsckForTablet(master_addresses, tablet_id);
    if (s.ok()) return s;
    SleepFor(MonoDelta::FromMilliseconds(1000));
  }
  return s.CloneAndPrepend("timed out with ksck errors remaining: last error");
}

Status DoChangeConfig(const vector<string>& master_addresses,
                      const string& tablet_id,
                      const string& replica_uuid,
                      const boost::optional<RaftPeerPB::MemberType>& member_type,
                      ChangeConfigType cc_type) {
  if (cc_type == consensus::REMOVE_SERVER && member_type) {
    return Status::InvalidArgument("cannot supply Raft member type when removing a server");
  }
  if ((cc_type == consensus::ADD_SERVER || cc_type == consensus::CHANGE_ROLE) && !member_type) {
    return Status::InvalidArgument(
        "must specify member type when adding a server or changing member type");
  }

  RaftPeerPB peer_pb;
  peer_pb.set_permanent_uuid(replica_uuid);
  if (member_type) {
    peer_pb.set_member_type(*member_type);
  }

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

Status ChangeConfig(const RunnerContext& context, ChangeConfigType cc_type) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& replica_uuid = FindOrDie(context.required_args, kTsUuidArg);
  boost::optional<RaftPeerPB::MemberType> member_type;
  if (cc_type == consensus::ADD_SERVER || cc_type == consensus::CHANGE_ROLE) {
    const string& replica_type = FindOrDie(context.required_args, kReplicaTypeArg);
    string uppercase_peer_type;
    ToUpperCase(replica_type, &uppercase_peer_type);
    RaftPeerPB::MemberType member_type_val;
    if (!RaftPeerPB::MemberType_Parse(uppercase_peer_type, &member_type_val)) {
      return Status::InvalidArgument("Unrecognized peer type", replica_type);
    }
    member_type = member_type_val;
  }

  return DoChangeConfig(master_addresses, tablet_id, replica_uuid, member_type, cc_type);
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

Status DoLeaderStepDown(const client::sp::shared_ptr<KuduClient>& client, const string& tablet_id,
                        const string& leader_uuid, const HostPort& leader_hp) {
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

  return DoLeaderStepDown(client, tablet_id, leader_uuid, leader_hp);
}

Status GetConsensusState(const unique_ptr<ConsensusServiceProxy>& proxy,
                         const string& tablet_id,
                         const string& replica_uuid,
                         const MonoDelta& timeout,
                         ConsensusStatePB* consensus_state) {
  GetConsensusStateRequestPB req;
  GetConsensusStateResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);
  req.set_dest_uuid(replica_uuid);
  req.add_tablet_ids(tablet_id);
  RETURN_NOT_OK(proxy->GetConsensusState(req, &resp, &controller));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.tablets_size() == 0) {
    return Status::NotFound("tablet not found:", tablet_id);
  }
  DCHECK_EQ(1, resp.tablets_size());
  *consensus_state = resp.tablets(0).cstate();
  return Status::OK();
}

Status GetLastCommittedOpId(const string& tablet_id, const string& replica_uuid,
                            const HostPort& replica_hp, const MonoDelta& timeout, OpId* opid) {
  GetLastOpIdRequestPB req;
  GetLastOpIdResponsePB resp;
  RpcController controller;
  controller.set_timeout(timeout);
  req.set_tablet_id(tablet_id);
  req.set_dest_uuid(replica_uuid);
  req.set_opid_type(consensus::COMMITTED_OPID);

  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(replica_hp.host(), replica_hp.port(), &proxy));
  RETURN_NOT_OK(proxy->GetLastOpId(req, &resp, &controller));
  *opid = resp.opid();
  return Status::OK();
}

Status ChangeLeader(const client::sp::shared_ptr<KuduClient>& client, const string& tablet_id,
                    const string& old_leader_uuid, const HostPort& old_leader_hp,
                    const MonoDelta& timeout) {
  unique_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(old_leader_hp.host(), old_leader_hp.port(), &proxy));
  ConsensusStatePB cstate;
  RETURN_NOT_OK(GetConsensusState(proxy, tablet_id, old_leader_uuid,
                                  client->default_admin_operation_timeout(), &cstate));
  int64 current_term = -1;
  MonoTime deadline = MonoTime::Now() + timeout;

  while (MonoTime::Now() < deadline) {
    RETURN_NOT_OK(GetConsensusState(proxy, tablet_id, old_leader_uuid,
                                    client->default_admin_operation_timeout(), &cstate));
    if (!cstate.leader_uuid().empty()) {
      if (cstate.current_term() > current_term) {
        // There is a new term with an elected leader.
        current_term = cstate.current_term();
        VLOG(1) << "Leader changed in term " << current_term << " to new uuid "
                << cstate.leader_uuid();
        // If the elected leader is still the old one, ask it to step down (potentially
        // repeatedly if it keeps getting re-elected).
        if (cstate.leader_uuid() == old_leader_uuid) {
          RETURN_NOT_OK(DoLeaderStepDown(client, tablet_id, old_leader_uuid, old_leader_hp));
        }
      }
      // If our current leader is not the old leader, check to see if it has asserted its
      // leadership by replicating an op in its term to the old leader.
      if (cstate.leader_uuid() != old_leader_uuid) {
        OpId opid;
        RETURN_NOT_OK(GetLastCommittedOpId(tablet_id, old_leader_uuid, old_leader_hp,
                                           client->default_admin_operation_timeout(), &opid));
        if (opid.term() == current_term) {
          return Status::OK();
        }
        VLOG(1) << "Term " << current_term << " leader " << cstate.leader_uuid()
                << " has not yet committed an op in its term on old leader "
                << old_leader_uuid
                << " (committed=" << opid << ")";
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(500));
  }

  return Status::TimedOut(Substitute(
      "waiting for old leader $0 ($1) to step down and new leader to be elected",
      old_leader_uuid, old_leader_hp.ToString()));
}

Status MoveReplica(const RunnerContext &context) {
  const string& master_addresses_str = FindOrDie(context.required_args, kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& from_ts_uuid = FindOrDie(context.required_args, kFromTsUuidArg);
  const string& to_ts_uuid = FindOrDie(context.required_args, kToTsUuidArg);

  // Check the tablet is in perfect health and, if so, add the new server.
  RETURN_NOT_OK_PREPEND(DoKsckForTablet(master_addresses, tablet_id),
                        "ksck pre-move health check failed");
  RETURN_NOT_OK(DoChangeConfig(master_addresses, tablet_id, to_ts_uuid,
                               RaftPeerPB::VOTER, consensus::ADD_SERVER));

  // Wait until the tablet copy completes and the tablet returns to perfect health.
  MonoDelta copy_timeout = MonoDelta::FromSeconds(FLAGS_move_copy_timeout_sec);
  RETURN_NOT_OK_PREPEND(WaitForCleanKsck(master_addresses, tablet_id, copy_timeout),
                        "failed waiting for clean ksck after add server");

  // Finally, remove the chosen replica.
  // If it is the leader, it will be asked to step down.
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder().master_server_addrs(master_addresses).Build(&client));
  string leader_uuid;
  HostPort leader_hp;
  RETURN_NOT_OK(GetTabletLeader(client, tablet_id, &leader_uuid, &leader_hp));
  if (from_ts_uuid == leader_uuid) {
    RETURN_NOT_OK_PREPEND(ChangeLeader(client, tablet_id,
                                       leader_uuid, leader_hp,
                                       MonoDelta::FromSeconds(FLAGS_move_leader_timeout_sec)),
                          "failed changing leadership from the replica to be removed");
  }
  return DoChangeConfig(master_addresses, tablet_id, from_ts_uuid,
                        boost::none, consensus::REMOVE_SERVER);
}

} // anonymous namespace

unique_ptr<Mode> BuildTabletMode() {
  unique_ptr<Action> add_replica =
      ActionBuilder("add_replica", &AddReplica)
      .Description("Add a new replica to a tablet's Raft configuration")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kTsUuidArg,
                              "UUID of the tablet server that should host the new replica" })
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
      .AddRequiredParameter({ kTsUuidArg,
                              "UUID of the tablet server hosting the existing replica" })
      .AddRequiredParameter(
          { kReplicaTypeArg, "Existing replica's new type. Must be VOTER or NON-VOTER."
          })
      .Build();

  unique_ptr<Action> remove_replica =
      ActionBuilder("remove_replica", &RemoveReplica)
      .Description("Remove an existing replica from a tablet's Raft configuration")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kTsUuidArg,
                              "UUID of the tablet server hosting the existing replica" })
      .Build();

  const string move_extra_desc = "The replica move tool effectively moves a "
      "replica from one tablet server to another by adding a replica to the "
      "new server and then removing it from the old one. It requires that "
      "ksck return no errors when run against the target tablet. If the move "
      "fails, the user should wait for any tablet copy to complete, and, if "
      "the copy succeeds, use remove_replica manually. If the copy fails, the "
      "new replica will be deleted automatically after some time, and then the "
      "move can be retried.";
  unique_ptr<Action> move_replica =
      ActionBuilder("move_replica", &MoveReplica)
      .Description("Move a tablet replica from one tablet server to another")
      .ExtraDescription(move_extra_desc)
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kFromTsUuidArg, "UUID of the tablet server to move from" })
      .AddRequiredParameter({ kToTsUuidArg, "UUID of the tablet server to move to" })
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
      .AddAction(std::move(move_replica))
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

