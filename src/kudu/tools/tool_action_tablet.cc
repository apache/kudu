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

#include <fstream>  // IWYU pragma: keep
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tools/tool_replica_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DEFINE_int64(move_copy_timeout_sec, 600,
             "Number of seconds to wait for tablet copy to complete when relocating a tablet");
DEFINE_int64(move_leader_timeout_sec, 30,
             "Number of seconds to wait for a leader when relocating a leader tablet");

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTablet;
using kudu::client::KuduTabletServer;
using kudu::consensus::ADD_PEER;
using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigType;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::GetConsensusStateRequestPB;
using kudu::consensus::GetConsensusStateResponsePB;
using kudu::consensus::GetLastOpIdRequestPB;
using kudu::consensus::GetLastOpIdResponsePB;
using kudu::consensus::MODIFY_PEER;
using kudu::consensus::RaftPeerPB;
using kudu::master::MasterServiceProxy;
using kudu::master::ReplaceTabletRequestPB;
using kudu::master::ReplaceTabletResponsePB;
using std::cerr;
using std::cout;
using std::endl;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

const char* const kReplicaTypeArg = "replica_type";
const char* const kTsUuidArg = "ts_uuid";
const char* const kFromTsUuidArg = "from_ts_uuid";
const char* const kToTsUuidArg = "to_ts_uuid";

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

Status ChangeConfig(const RunnerContext& context, ChangeConfigType cc_type) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = Split(master_addresses_str, ",");
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& replica_uuid = FindOrDie(context.required_args, kTsUuidArg);
  boost::optional<RaftPeerPB::MemberType> member_type;
  if (cc_type == consensus::ADD_PEER || cc_type == consensus::MODIFY_PEER) {
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
  return ChangeConfig(context, consensus::ADD_PEER);
}

Status ChangeReplicaType(const RunnerContext& context) {
  return ChangeConfig(context, consensus::MODIFY_PEER);
}

Status RemoveReplica(const RunnerContext& context) {
  return ChangeConfig(context, consensus::REMOVE_PEER);
}

Status LeaderStepDown(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = Split(master_addresses_str, ",");
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));

  // If leader is not present, command can gracefully return.
  string leader_uuid;
  HostPort leader_hp;
  bool is_no_leader = false;
  Status s = GetTabletLeader(client, tablet_id,
                             &leader_uuid, &leader_hp, &is_no_leader);
  if (s.IsNotFound() && is_no_leader) {
    cout << s.ToString() << endl;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  return DoLeaderStepDown(tablet_id, leader_uuid, leader_hp,
                          client->default_admin_operation_timeout());
}

Status WaitForMoveToComplete(const vector<string>& master_addresses,
                             client::sp::shared_ptr<KuduClient> client,
                             const string& tablet_id,
                             const string& from_ts_uuid,
                             const string& to_ts_uuid,
                             MonoDelta copy_timeout) {
  // Wait until the tablet copy completes and the tablet returns to perfect health.
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + copy_timeout;
  while (MonoTime::Now() < deadline) {
    bool is_completed = false;
    Status move_completion_status;
    RETURN_NOT_OK(CheckCompleteMove(master_addresses, client, tablet_id,
                                    from_ts_uuid, to_ts_uuid,
                                    &is_completed, &move_completion_status));
    if (is_completed) {
      return move_completion_status;
    }
    SleepFor(MonoDelta::FromMilliseconds(500));
  }
  return Status::TimedOut(Substitute("unable to complete tablet replica move after $0",
                                     (MonoTime::Now() - start).ToString()));
}

Status MoveReplica(const RunnerContext& context) {
  const vector<string> master_addresses = Split(
        FindOrDie(context.required_args, kMasterAddressesArg), ",");
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& from_ts_uuid = FindOrDie(context.required_args, kFromTsUuidArg);
  const string& to_ts_uuid = FindOrDie(context.required_args, kToTsUuidArg);

  // Check the tablet is in perfect health first.
  // We retry since occasionally ksck returns bad status because of transient
  // issues like leader elections.
  RETURN_NOT_OK_PREPEND(WaitForCleanKsck(master_addresses,
                                         tablet_id,
                                         MonoDelta::FromSeconds(5)),
                        "ksck pre-move health check failed");
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));
  RETURN_NOT_OK(ScheduleReplicaMove(master_addresses, client,
                                    tablet_id, from_ts_uuid, to_ts_uuid));
  const auto copy_timeout = MonoDelta::FromSeconds(FLAGS_move_copy_timeout_sec);
  return WaitForMoveToComplete(master_addresses, client, tablet_id,
                               from_ts_uuid, to_ts_uuid, copy_timeout);

}

Status ReplaceTablet(const RunnerContext& context) {
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));

  ReplaceTabletRequestPB req;
  ReplaceTabletResponsePB resp;
  req.set_tablet_id(tablet_id);
  Status s = proxy.SyncRpc<ReplaceTabletRequestPB, ReplaceTabletResponsePB>(
      req, &resp, "ReplaceTablet", &MasterServiceProxy::ReplaceTablet);
  RETURN_NOT_OK(s);

  if (resp.has_error()) {
    s = StatusFromPB(resp.error().status());
    return s.CloneAndPrepend(Substitute("unable to replace tablet $0", tablet_id));
  }
  cerr << "Replaced tablet " << tablet_id << " with tablet ";
  cout << resp.replacement_tablet_id() << endl;
  return Status::OK();
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

  unique_ptr<Action> replace_tablet =
      ActionBuilder("unsafe_replace_tablet", &ReplaceTablet)
      .Description("Replace a tablet with an empty one, deleting the previous tablet.")
      .ExtraDescription("Use this tool to repair a table when one of its tablets has permanently "
                        "lost all of its replicas. It replaces the unrecoverable tablet with a new "
                        "empty one representing the same partition. Its primary use is to jettison "
                        "an unrecoverable tablet in order to make the rest of the table "
                        "available.\n\n"
                        "NOTE: The original tablet will be deleted. Its data will be permanently "
                        "lost. Additionally, clients should be restarted before attempting to "
                        "use the repaired table (see KUDU-2376).")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .Build();

  return ModeBuilder("tablet")
      .Description("Operate on remote Kudu tablets")
      .AddMode(std::move(change_config))
      .AddAction(std::move(leader_step_down))
      .AddAction(std::move(replace_tablet))
      .Build();
}

} // namespace tools
} // namespace kudu

