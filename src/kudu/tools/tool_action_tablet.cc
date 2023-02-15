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
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/client/client.h" // IWYU pragma: keep
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
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

DEFINE_bool(abrupt, false,
            "Whether the leader should step down without attempting to "
            "transfer leadership gracefully. A graceful transfer minimizes "
            "delays in tablet operations, but will fail if the tablet cannot "
            "arrange a successor.");
DEFINE_string(new_leader_uuid, "",
              "UUID of the server that leadership should be transferred to. "
              "Leadership may only be transferred to a voting member of the "
              "leader's active config. If the designated successor cannot "
              "catch up to the leader within one election timeout, leadership "
              "transfer will not occur. If blank, the leader chooses its own "
              "successor, attempting to transfer leadership as soon as "
              "possible. This cannot be set if --abrupt is set.");
DEFINE_string(current_leader_uuid, "",
              "UUID of the server that currently hosts leader replica of the tablet.");
DEFINE_int64(move_copy_timeout_sec, 600,
             "Number of seconds to wait for tablet copy to complete when relocating a tablet");
DEFINE_int64(move_leader_timeout_sec, 30,
             "Number of seconds to wait for a leader when relocating a leader tablet");

using kudu::client::KuduClient;
using kudu::client::KuduTablet;
using kudu::consensus::ADD_PEER;
using kudu::consensus::ChangeConfigType;
using kudu::consensus::LeaderStepDownMode;
using kudu::consensus::MODIFY_PEER;
using kudu::consensus::RaftPeerPB;
using kudu::master::MasterServiceProxy;
using kudu::master::ReplaceTabletRequestPB;
using kudu::master::ReplaceTabletResponsePB;
using std::cerr;
using std::cout;
using std::endl;
using std::make_optional;
using std::nullopt;
using std::optional;
using std::ostream;
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
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& replica_uuid = FindOrDie(context.required_args, kTsUuidArg);
  optional<RaftPeerPB::MemberType> member_type;
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

  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
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
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));

  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const LeaderStepDownMode mode = FLAGS_abrupt ? LeaderStepDownMode::ABRUPT :
                                                 LeaderStepDownMode::GRACEFUL;
  const optional<string> new_leader_uuid =
    FLAGS_new_leader_uuid.empty() ? nullopt
                                  : make_optional(FLAGS_new_leader_uuid);
  if (mode == LeaderStepDownMode::ABRUPT && new_leader_uuid) {
    return Status::InvalidArgument("cannot specify both --new_leader_uuid and --abrupt");
  }

  const optional<string> current_leader_uuid = FLAGS_current_leader_uuid.empty()
      ? nullopt
      : make_optional(FLAGS_current_leader_uuid);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(master_addresses, &client));
  string leader_uuid;
  HostPort leader_hp;
  Status s;

  if (current_leader_uuid) {
    s = GetTabletReplicaHostInfo(client, tablet_id, *current_leader_uuid,
                                 &leader_hp);
    if (s.IsNotFound()) {
      return s.CloneAndPrepend(
          Substitute("unable to transfer leadership, current leader $0 not found",
                     *current_leader_uuid));
    }
  } else {
    bool no_leader = false;
    s = GetTabletLeader(client, tablet_id,
                        &leader_uuid, &leader_hp, &no_leader);
    if (s.IsNotFound() && no_leader) {
      // If leadership should be transferred to a specific node, exit with an
      // error if there's no leader since we can't orchestrate the transfer.
      if (new_leader_uuid) {
        return s.CloneAndPrepend(
            Substitute("unable to transfer leadership to $0", *new_leader_uuid));
      }
      // Otherwise, a new election should happen soon, which will achieve
      // something like what the client wanted, so we'll exit gracefully.
      cout << s.ToString() << endl;
      return Status::OK();
    }
  }
  RETURN_NOT_OK(s);

  // If the requested new leader is the leader, the command can short-circuit.
  if (new_leader_uuid && leader_uuid == *new_leader_uuid) {
    cout << Substitute("Requested new leader $0 is already the leader",
                       leader_uuid) << endl;
    return Status::OK();
  }
  return DoLeaderStepDown(tablet_id,
                          current_leader_uuid ? *current_leader_uuid : leader_uuid,
                          leader_hp,
                          mode,
                          new_leader_uuid,
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
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
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
  RETURN_NOT_OK(CreateKuduClient(master_addresses, &client));
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
      req, &resp, "ReplaceTablet", &MasterServiceProxy::ReplaceTabletAsync);
  RETURN_NOT_OK(s);

  if (resp.has_error()) {
    s = StatusFromPB(resp.error().status());
    return s.CloneAndPrepend(Substitute("unable to replace tablet $0", tablet_id));
  }
  cerr << "Replaced tablet " << tablet_id << " with tablet ";
  cout << resp.replacement_tablet_id() << endl;
  return Status::OK();
}

Status ShowInfo(const RunnerContext& context) {
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(master_addresses, &client));

  KuduTablet* tablet_raw = nullptr;
  RETURN_NOT_OK(client->GetTablet(tablet_id, &tablet_raw));
  unique_ptr<KuduTablet> tablet(tablet_raw);

  cout << Substitute("Table id: $0, Table name: $1",
                     tablet->table_id(), tablet->table_name()) << endl;
  DataTable cmatrix({ "UUID", "Host", "Port", "Is Leader"});
  for (const auto& replica : tablet->replicas()) {
    cmatrix.AddRow({replica->ts().uuid(), replica->ts().hostname(),
                   Substitute("$0", replica->ts().port()),
                   Substitute("$0", replica->is_leader())});
  }
  ostream out(std::cout.rdbuf());
  return cmatrix.PrintTo(out);
}

} // anonymous namespace

unique_ptr<Mode> BuildTabletMode() {
  unique_ptr<Action> add_replica =
      ClusterActionBuilder("add_replica", &AddReplica)
      .Description("Add a new replica to a tablet's Raft configuration")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kTsUuidArg,
                              "UUID of the tablet server that should host the new replica" })
      .AddRequiredParameter(
          { kReplicaTypeArg, "New replica's type. Must be VOTER or NON_VOTER."
          })
      .Build();

  unique_ptr<Action> change_replica_type =
      ClusterActionBuilder("change_replica_type", &ChangeReplicaType)
      .Description(
          "Change the type of an existing replica in a tablet's Raft configuration")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kTsUuidArg,
                              "UUID of the tablet server hosting the existing replica" })
      .AddRequiredParameter(
          { kReplicaTypeArg, "Existing replica's new type. Must be VOTER or NON_VOTER."
          })
      .Build();

  unique_ptr<Action> remove_replica =
      ClusterActionBuilder("remove_replica", &RemoveReplica)
      .Description("Remove an existing replica from a tablet's Raft configuration")
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
      ClusterActionBuilder("move_replica", &MoveReplica)
      .Description("Move a tablet replica from one tablet server to another")
      .ExtraDescription(move_extra_desc)
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kFromTsUuidArg, "UUID of the tablet server to move from" })
      .AddRequiredParameter({ kToTsUuidArg, "UUID of the tablet server to move to" })
      .Build();

  unique_ptr<Action> leader_step_down =
      ClusterActionBuilder("leader_step_down", &LeaderStepDown)
      .Description("Change the tablet's leader")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("abrupt")
      .AddOptionalParameter("new_leader_uuid")
      .AddOptionalParameter("current_leader_uuid")
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
      ClusterActionBuilder("unsafe_replace_tablet", &ReplaceTablet)
      .Description("Replace a tablet with an empty one, deleting the previous tablet.")
      .ExtraDescription("Use this tool to repair a table when one of its tablets has permanently "
                        "lost all of its replicas. It replaces the unrecoverable tablet with a new "
                        "empty one representing the same partition. Its primary use is to jettison "
                        "an unrecoverable tablet in order to make the rest of the table "
                        "available.\n\n"
                        "NOTE: The original tablet will be deleted. Its data will be permanently "
                        "lost. Additionally, clients should be restarted before attempting to "
                        "use the repaired table (see KUDU-2376).")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .Build();

  unique_ptr<Action> show_info =
      ClusterActionBuilder("show_info", &ShowInfo)
      .Description("Show the table and location information of a tablet.")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .Build();

  return ModeBuilder("tablet")
      .Description("Operate on remote Kudu tablets")
      .AddMode(std::move(change_config))
      .AddAction(std::move(leader_step_down))
      .AddAction(std::move(replace_tablet))
      .AddAction(std::move(show_info))
      .Build();
}

} // namespace tools
} // namespace kudu

