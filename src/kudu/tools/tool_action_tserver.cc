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

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/status.h"

DEFINE_bool(allow_missing_tserver, false, "If true, performs the action on the "
    "tserver even if it has not been registered with the master and has no "
    "existing tserver state records associated with it.");

DECLARE_string(columns);

using std::cout;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

using master::ChangeTServerStateRequestPB;
using master::ChangeTServerStateResponsePB;
using master::ListTabletServersRequestPB;
using master::ListTabletServersResponsePB;
using master::MasterServiceProxy;
using master::TServerStateChangePB;

namespace tools {
namespace {

const char* const kTServerAddressArg = "tserver_address";
const char* const kTServerAddressDesc = "Address of a Kudu Tablet Server of "
    "form 'hostname:port'. Port may be omitted if the Tablet Server is bound "
    "to the default port.";
const char* const kTServerIdArg = "tserver_uuid";
const char* const kTServerIdDesc = "UUID of a Kudu Tablet Server";
const char* const kFlagArg = "flag";
const char* const kValueArg = "value";

Status TServerGetFlags(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  return PrintServerFlags(address, tserver::TabletServer::kDefaultPort);
}

Status TServerSetFlag(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  const string& flag = FindOrDie(context.required_args, kFlagArg);
  const string& value = FindOrDie(context.required_args, kValueArg);
  return SetServerFlag(address, tserver::TabletServer::kDefaultPort,
                       flag, value);
}

Status TServerStatus(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  return PrintServerStatus(address, tserver::TabletServer::kDefaultPort);
}

Status TServerTimestamp(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  return PrintServerTimestamp(address, tserver::TabletServer::kDefaultPort);
}

Status ListTServers(const RunnerContext& context) {
  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));

  ListTabletServersRequestPB req;
  ListTabletServersResponsePB resp;

  proxy.SyncRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>(
      req, &resp, "ListTabletServers", &MasterServiceProxy::ListTabletServersAsync);

  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  DataTable table({});
  const auto& servers = resp.servers();

  auto hostport_to_string = [](const HostPortPB& hostport) {
    return strings::Substitute("$0:$1", hostport.host(), hostport.port());
  };

  for (const auto& column : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    vector<string> values;
    if (boost::iequals(column, "uuid")) {
      for (const auto& server : servers) {
        values.emplace_back(server.instance_id().permanent_uuid());
      }
    } else if (boost::iequals(column, "seqno")) {
      for (const auto& server : servers) {
        values.emplace_back(std::to_string(server.instance_id().instance_seqno()));
      }
    } else if (boost::iequals(column, "rpc-addresses") ||
               boost::iequals(column, "rpc_addresses")) {
      for (const auto& server : servers) {
        values.emplace_back(JoinMapped(server.registration().rpc_addresses(),
                                       hostport_to_string, ","));
      }
    } else if (boost::iequals(column, "http-addresses") ||
               boost::iequals(column, "http_addresses")) {
      for (const auto& server : servers) {
        values.emplace_back(JoinMapped(server.registration().http_addresses(),
                                       hostport_to_string, ","));
      }
    } else if (boost::iequals(column, "version")) {
      for (const auto& server : servers) {
        values.emplace_back(server.registration().software_version());
      }
    } else if (boost::iequals(column, "heartbeat")) {
      for (const auto& server : servers) {
        values.emplace_back(strings::Substitute("$0ms", server.millis_since_heartbeat()));
      }
    } else if (boost::iequals(column, "location")) {
      for (const auto& server : servers) {
        string loc = server.location();
        values.emplace_back(loc.empty() ? "<none>" : std::move(loc));
      }
    } else if (boost::iequals(column, "start_time")) {
      for (const auto& server : servers) {
        values.emplace_back(StartTimeToString(server.registration()));
      }
    } else {
      return Status::InvalidArgument("unknown column (--columns)", column);
    }
    table.AddColumn(column.ToString(), std::move(values));
  }

  RETURN_NOT_OK(table.PrintTo(cout));
  return Status::OK();
}

Status TserverDumpMemTrackers(const RunnerContext& context) {
  const auto& address = FindOrDie(context.required_args, kTServerAddressArg);
  return DumpMemTrackers(address, tserver::TabletServer::kDefaultPort);
}

Status TServerSetState(const RunnerContext& context, TServerStateChangePB::StateChange sc) {
  ChangeTServerStateRequestPB req;
  ChangeTServerStateResponsePB resp;
  const string& tserver_uuid = FindOrDie(context.required_args, kTServerIdArg);
  TServerStateChangePB* change = req.mutable_change();
  change->set_uuid(tserver_uuid);
  change->set_change(sc);
  if (FLAGS_allow_missing_tserver) {
    req.set_handle_missing_tserver(ChangeTServerStateRequestPB::ALLOW_MISSING_TSERVER);
  }
  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));
  RETURN_NOT_OK((proxy.SyncRpc<ChangeTServerStateRequestPB, ChangeTServerStateResponsePB>(
      req, &resp, "ChangeTServerState", &MasterServiceProxy::ChangeTServerStateAsync)));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status EnterMaintenance(const RunnerContext& context) {
  return TServerSetState(context, TServerStateChangePB::ENTER_MAINTENANCE_MODE);
}

Status ExitMaintenance(const RunnerContext& context) {
  return TServerSetState(context, TServerStateChangePB::EXIT_MAINTENANCE_MODE);
}

} // anonymous namespace

unique_ptr<Mode> BuildTServerMode() {
  unique_ptr<Action> dump_memtrackers =
      ActionBuilder("dump_memtrackers", &TserverDumpMemTrackers)
      .Description("Dump the memtrackers from a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddOptionalParameter("format")
      .AddOptionalParameter("memtracker_output")
      .AddOptionalParameter("timeout_ms")
      .Build();

  unique_ptr<Action> get_flags =
      ActionBuilder("get_flags", &TServerGetFlags)
      .Description("Get the gflags for a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddOptionalParameter("all_flags")
      .AddOptionalParameter("flag_tags")
      .Build();

  unique_ptr<Action> set_flag =
      ActionBuilder("set_flag", &TServerSetFlag)
      .Description("Change a gflag value on a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .AddRequiredParameter({ kFlagArg, "Name of the gflag" })
      .AddRequiredParameter({ kValueArg, "New value for the gflag" })
      .AddOptionalParameter("force")
      .Build();

  unique_ptr<Action> status =
      ActionBuilder("status", &TServerStatus)
      .Description("Get the status of a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .Build();

  unique_ptr<Action> timestamp =
      ActionBuilder("timestamp", &TServerTimestamp)
      .Description("Get the current timestamp of a Kudu Tablet Server")
      .AddRequiredParameter({ kTServerAddressArg, kTServerAddressDesc })
      .Build();

  unique_ptr<Action> list_tservers =
      ActionBuilder("list", &ListTServers)
      .Description("List tablet servers in a Kudu cluster")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddOptionalParameter("columns", string("uuid,rpc-addresses"),
                            string("Comma-separated list of tserver info fields to "
                                   "include in output.\nPossible values: uuid, "
                                   "rpc-addresses, http-addresses, version, seqno, "
                                   "heartbeat and start_time"))
      .AddOptionalParameter("format")
      .AddOptionalParameter("timeout_ms")
      .Build();

  unique_ptr<Action> enter_maintenance =
      ActionBuilder("enter_maintenance", &EnterMaintenance)
      .Description("Begin maintenance on the Tablet Server. While under "
                   "maintenance, downtime of the Tablet Server will not lead "
                   "to the immediate re-replication of its tablet replicas.")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTServerIdArg, kTServerIdDesc })
      .AddOptionalParameter("allow_missing_tserver")
      .Build();
  // Note: --allow_missing_tserver doesn't make sense for exit_maintenance
  // because if the tserver is missing, the non-existent tserver's state is
  // already NONE and so exit_maintenance is a no-op.
  unique_ptr<Action> exit_maintenance =
      ActionBuilder("exit_maintenance", &ExitMaintenance)
      .Description("End maintenance of the Tablet Server.")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTServerIdArg, kTServerIdDesc })
      .Build();
  unique_ptr<Mode> state = ModeBuilder("state")
      .Description("Operate on the state of a Kudu Tablet Server")
      .AddAction(std::move(enter_maintenance))
      .AddAction(std::move(exit_maintenance))
      .Build();

  return ModeBuilder("tserver")
      .Description("Operate on a Kudu Tablet Server")
      .AddAction(std::move(dump_memtrackers))
      .AddAction(std::move(get_flags))
      .AddAction(std::move(set_flag))
      .AddAction(std::move(status))
      .AddAction(std::move(timestamp))
      .AddAction(std::move(list_tservers))
      .AddMode(std::move(state))
      .Build();
}

} // namespace tools
} // namespace kudu

