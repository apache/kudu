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

#include <functional>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/mcp_disposition.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_runner.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/init.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DEFINE_bool(allow_missing_tserver, false, "If true, performs the action on the "
    "tserver even if it has not been registered with the master and has no "
    "existing tserver state records associated with it.");

DEFINE_bool(error_if_not_fully_quiesced, false, "If true, the command to start "
    "quiescing will return an error if the tserver is not fully quiesced, i.e. "
    "there are still tablet leaders or active scanners on it.");

DEFINE_bool(force_unregister_live_tserver, false,
            "If true, force the unregistration of the tserver even if it is not presumed dead "
            "by the master. Make sure the tserver has been shut down before setting this true.");
DEFINE_bool(remove_tserver_state, true,
            "If false, remove the tserver from the master's in-memory map but keep its persisted "
            "state (if any). If the same tserver re-registers on the master it will get its "
            "original state");

DEFINE_bool(dry_run, false, "If true, do not perform the operation; instead, "
    "print the tablet servers that it would be applied to and exit.");

DECLARE_int64(timeout_ms);
DECLARE_string(columns);

using std::cout;
using std::endl;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

using master::ChangeTServerStateRequestPB;
using master::ChangeTServerStateResponsePB;
using master::ListTabletServersRequestPB;
using master::ListTabletServersResponsePB;
using master::MasterServiceProxy;
using master::UnregisterTServerRequestPB;
using master::UnregisterTServerResponsePB;
using master::TServerStateChangePB;
using master::TServerStatePB;
using rpc::RpcController;
using tserver::QuiesceTabletServerRequestPB;
using tserver::QuiesceTabletServerResponsePB;
using tserver::TabletServer;
using tserver::TabletServerAdminServiceProxy;

namespace tools {
namespace {

const char* const kTServerIdArg = "tserver_uuid";
const char* const kTServerIdDesc = "UUID of a Kudu Tablet Server";
const char* const kFlagArg = "flag";
const char* const kValueArg = "value";
const char* const kLocationArg = "location";
const char* const kLocationDesc = "Location (e.g. /dc-0/rack-1) whose Tablet Servers to "
    "operate on. Must match the location assigned by the master exactly; see "
    "'kudu tserver list --columns=uuid,location' for the locations in use.";

Status TServerGetFlags(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  return PrintServerFlags(address, tserver::TabletServer::kDefaultPort);
}

Status TServerRun(const RunnerContext& context) {
  RETURN_NOT_OK(InitKudu());

  // Enable redaction by default. Unlike most tools, we don't want user data
  // printed to the console/log to be shown by default.
  CHECK_NE("", google::SetCommandLineOptionWithMode("redact",
      "all", google::FlagSettingMode::SET_FLAGS_DEFAULT));

  tserver::SetTabletServerFlagDefaults();
  return tserver::RunTabletServer();
}

Status TServerSetFlag(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kTServerAddressArg);
  const string& flag = FindOrDie(context.required_args, kFlagArg);
  const string& value = FindOrDie(context.required_args, kValueArg);
  return SetServerFlag(address, tserver::TabletServer::kDefaultPort,
                       flag, value);
}

Status TServerSetAllTServersFlag(const RunnerContext& context) {
  const string& flag = FindOrDie(context.required_args, kFlagArg);
  const string& value = FindOrDie(context.required_args, kValueArg);

  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));

  ListTabletServersRequestPB req;
  ListTabletServersResponsePB resp;

  RETURN_NOT_OK((proxy.SyncRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>(
      req, &resp, "ListTabletServers", &MasterServiceProxy::ListTabletServersAsync)));

  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  const auto hostport_to_string = [](const HostPortPB& hostport) {
    return Substitute("$0:$1", hostport.host(), hostport.port());
  };
  const auto& servers = resp.servers();
  bool set_failed_flag = false;
  for (const auto& server : servers) {
    const string& addr = JoinMapped(server.registration().rpc_addresses(), hostport_to_string, ",");
    Status s = SetServerFlag(addr, tserver::TabletServer::kDefaultPort, flag, value);
    if (!s.ok()) {
      set_failed_flag = true;
      LOG(WARNING) << Substitute("Set config {$0:$1} for $2 failed, error message: $3",
                                 flag, value, addr, s.ToString());
    }
  }
  if (set_failed_flag) {
    return Status::RuntimeError("Some Tablet Servers set flag failed!");
  }
  return Status::OK();
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

  const vector<string> cols = strings::Split(FLAGS_columns, ",", strings::SkipEmpty());
  ListTabletServersRequestPB req;
  for (const auto& col : cols) {
    if (iequals(col, "state")) {
      req.set_include_states(true);
    }
  }
  ListTabletServersResponsePB resp;

  RETURN_NOT_OK((proxy.SyncRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>(
      req, &resp, "ListTabletServers", &MasterServiceProxy::ListTabletServersAsync)));

  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  DataTable table({});
  const auto& servers = resp.servers();

  auto hostport_to_string = [](const HostPortPB& hostport) {
    return Substitute("$0:$1", hostport.host(), hostport.port());
  };

  for (const auto& column : cols) {
    vector<string> values;
    if (iequals(column, "uuid")) {
      for (const auto& server : servers) {
        values.emplace_back(server.instance_id().permanent_uuid());
      }
    } else if (iequals(column, "seqno")) {
      for (const auto& server : servers) {
        values.emplace_back(std::to_string(server.instance_id().instance_seqno()));
      }
    } else if (iequals(column, "rpc-addresses") ||
               iequals(column, "rpc_addresses")) {
      for (const auto& server : servers) {
        values.emplace_back(JoinMapped(server.registration().rpc_addresses(),
                                       hostport_to_string, ","));
      }
    } else if (iequals(column, "http-addresses") ||
               iequals(column, "http_addresses")) {
      for (const auto& server : servers) {
        values.emplace_back(JoinMapped(server.registration().http_addresses(),
                                       hostport_to_string, ","));
      }
    } else if (iequals(column, "version")) {
      for (const auto& server : servers) {
        values.emplace_back(server.registration().software_version());
      }
    } else if (iequals(column, "heartbeat")) {
      for (const auto& server : servers) {
        values.emplace_back(Substitute("$0ms", server.millis_since_heartbeat()));
      }
    } else if (iequals(column, "location")) {
      for (const auto& server : servers) {
        string loc = server.location();
        values.emplace_back(loc.empty() ? "<none>" : std::move(loc));
      }
    } else if (iequals(column, "start_time")) {
      for (const auto& server : servers) {
        values.emplace_back(StartTimeToString(server.registration()));
      }
    } else if (iequals(column, "state")) {
      for (const auto& server : servers) {
        values.emplace_back(TServerStatePB_Name(server.state()));
      }
    } else {
      return Status::InvalidArgument("unknown column (--columns)", column);
    }
    table.AddColumn(column, std::move(values));
  }

  RETURN_NOT_OK(table.PrintTo(cout));
  return Status::OK();
}

Status TserverDumpMemTrackers(const RunnerContext& context) {
  const auto& address = FindOrDie(context.required_args, kTServerAddressArg);
  return DumpMemTrackers(address, tserver::TabletServer::kDefaultPort);
}

Status SetTServerState(LeaderMasterProxy* proxy,
                       const string& uuid,
                       TServerStateChangePB::StateChange sc) {
  ChangeTServerStateRequestPB req;
  ChangeTServerStateResponsePB resp;
  TServerStateChangePB* change = req.mutable_change();
  change->set_uuid(uuid);
  change->set_change(sc);
  if (FLAGS_allow_missing_tserver) {
    req.set_handle_missing_tserver(ChangeTServerStateRequestPB::ALLOW_MISSING_TSERVER);
  }
  RETURN_NOT_OK((proxy->SyncRpc<ChangeTServerStateRequestPB, ChangeTServerStateResponsePB>(
      req, &resp, "ChangeTServerState", &MasterServiceProxy::ChangeTServerStateAsync)));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status TServerSetState(const RunnerContext& context, TServerStateChangePB::StateChange sc) {
  const string& tserver_uuid = FindOrDie(context.required_args, kTServerIdArg);
  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));
  return SetTServerState(&proxy, tserver_uuid, sc);
}

Status EnterMaintenance(const RunnerContext& context) {
  return TServerSetState(context, TServerStateChangePB::ENTER_MAINTENANCE_MODE);
}

Status ExitMaintenance(const RunnerContext& context) {
  return TServerSetState(context, TServerStateChangePB::EXIT_MAINTENANCE_MODE);
}

Status StartQuiescingTServer(const RunnerContext& context) {
  const auto& address = FindOrDie(context.required_args, kTServerAddressArg);
  unique_ptr<TabletServerAdminServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(address, tserver::TabletServer::kDefaultPort, &proxy));

  QuiesceTabletServerRequestPB req;
  req.set_quiesce(true);
  req.set_return_stats(FLAGS_error_if_not_fully_quiesced);
  RpcController rpc;
  QuiesceTabletServerResponsePB resp;
  RETURN_NOT_OK(proxy->Quiesce(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (FLAGS_error_if_not_fully_quiesced &&
      (resp.num_leaders() != 0 || resp.num_active_scanners() != 0)) {
    return Status::Incomplete(
        Substitute("Tablet server not fully quiesced: $0 tablet leaders and $1 active "
                   "scanners remain", resp.num_leaders(), resp.num_active_scanners()));
  }
  return Status::OK();
}

Status StopQuiescingTServer(const RunnerContext& context) {
  const auto& address = FindOrDie(context.required_args, kTServerAddressArg);
  unique_ptr<TabletServerAdminServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(address, tserver::TabletServer::kDefaultPort, &proxy));

  QuiesceTabletServerRequestPB req;
  req.set_quiesce(false);
  req.set_return_stats(false);
  QuiesceTabletServerResponsePB resp;
  RpcController rpc;
  RETURN_NOT_OK(proxy->Quiesce(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status QuiescingStatus(const RunnerContext& context) {
  const auto& address = FindOrDie(context.required_args, kTServerAddressArg);
  unique_ptr<TabletServerAdminServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(address, tserver::TabletServer::kDefaultPort, &proxy));

  QuiesceTabletServerRequestPB req;
  req.set_return_stats(true);
  QuiesceTabletServerResponsePB resp;
  RpcController rpc;
  rpc.RequireServerFeature(tserver::TabletServerFeatures::QUIESCING);
  RETURN_NOT_OK(proxy->Quiesce(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  DataTable table({});
  table.AddColumn("Quiescing", { resp.is_quiescing() ? "true" : "false" });
  table.AddColumn("Tablet Leaders", { IntToString(resp.num_leaders()) });
  table.AddColumn("Active Scanners", { IntToString(resp.num_active_scanners()) });
  return table.PrintTo(cout);
}

Status UnregisterTServer(const RunnerContext& context) {
  const auto& ts_uuid = FindOrDie(context.required_args, kTServerIdArg);
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  RETURN_NOT_OK(VerifyMasterAddressList(master_addresses));
  if (FLAGS_remove_tserver_state) {
    // We don't care about FLAGS_allow_missing_tserver because it doesn't
    // make sense for ExitMaintenance.
    RETURN_NOT_OK(ExitMaintenance(context));
  }

  string err_str;
  for (const auto& address : master_addresses) {
    unique_ptr<MasterServiceProxy> proxy;
    RETURN_NOT_OK(BuildProxy(address, master::Master::kDefaultPort, &proxy));
    UnregisterTServerRequestPB req;
    req.set_uuid(ts_uuid);
    req.set_force_unregister_live_tserver(FLAGS_force_unregister_live_tserver);
    UnregisterTServerResponsePB resp;
    RpcController rpc;
    Status s = proxy->UnregisterTServer(req, &resp, &rpc);
    if (!s.ok() || resp.has_error()) {
      err_str += Substitute(" Unable to unregister the tserver from master $0, status: $1",
                            address,
                            StatusFromPB(resp.error().status()).ToString());
    }
  }
  if (err_str.empty()) {
    return Status::OK();
  }
  return Status::RemoteError(err_str);
}

struct TServerInLocation {
  string uuid;
  // Comma-separated RPC addresses, as BuildProxy() accepts them.
  string address;
  TServerStatePB state = TServerStatePB::UNKNOWN_STATE;
};

// Get the tablet servers the master has registered in location. Returns
// NotFound if there are none.
Status ListTServersInLocation(LeaderMasterProxy* proxy,
                              const string& location,
                              vector<TServerInLocation>* tservers) {
  // An empty location would match every tserver without one, which on a
  // cluster without location mapping is all of them.
  if (location.empty() || location[0] != '/') {
    return Status::InvalidArgument(Substitute(
        "invalid location '$0': a location must start with '/'", location));
  }
  ListTabletServersRequestPB req;
  ListTabletServersResponsePB resp;
  req.set_include_states(true);
  RETURN_NOT_OK((proxy->SyncRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>(
      req, &resp, "ListTabletServers", &MasterServiceProxy::ListTabletServersAsync)));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  const auto hostport_to_string = [](const HostPortPB& hostport) {
    return Substitute("$0:$1", hostport.host(), hostport.port());
  };
  set<string> known_locations;
  vector<TServerInLocation> matched;
  for (const auto& server : resp.servers()) {
    known_locations.emplace(server.location().empty() ? "<none>" : server.location());
    if (server.location() != location) {
      continue;
    }
    TServerInLocation ts;
    ts.uuid = server.instance_id().permanent_uuid();
    ts.address = JoinMapped(server.registration().rpc_addresses(), hostport_to_string, ",");
    ts.state = server.state();
    matched.emplace_back(std::move(ts));
  }
  if (matched.empty()) {
    return Status::NotFound(Substitute(
        "no tablet server is registered in location '$0'; locations in use: $1",
        location, JoinStrings(vector<string>(known_locations.begin(),
                                            known_locations.end()), ", ")));
  }
  *tservers = std::move(matched);
  return Status::OK();
}

// Print the tservers an operation would affect, without performing it.
// show_maintenance_state adds a column with each tserver's maintenance state.
Status PrintAffectedTServers(const string& op_desc,
                             const string& location,
                             const vector<TServerInLocation>& tservers,
                             bool show_maintenance_state) {
  cout << Substitute("[dry run] would $0 for $1 tablet server(s) in location '$2':",
                     op_desc, tservers.size(), location) << endl;
  vector<string> uuids;
  vector<string> addresses;
  vector<string> states;
  for (const auto& ts : tservers) {
    uuids.emplace_back(ts.uuid);
    addresses.emplace_back(ts.address);
    states.emplace_back(TServerStatePB_Name(ts.state));
  }
  DataTable table({});
  table.AddColumn("uuid", std::move(uuids));
  table.AddColumn("rpc-addresses", std::move(addresses));
  if (show_maintenance_state) {
    table.AddColumn("maintenance state", std::move(states));
  }
  return table.PrintTo(cout);
}

// Run func against every tserver and print the outcome for each.
// All tservers are attempted even if some fail, so the table shows exactly which
// ones took the change. Any failure fails the command and prints caution.
// Incomplete from func isn't a failure. Only quiescing returns it, for a
// tserver that is quiescing but still has leaders or scanners.
// show_maintenance_state adds a column with each tserver's maintenance state
// from before the change.
Status FanOutToLocation(const string& op_desc,
                        const string& location,
                        const vector<TServerInLocation>& tservers,
                        const std::function<Status(const TServerInLocation&)>& func,
                        const string& caution,
                        bool show_maintenance_state) {
  vector<string> uuids;
  vector<string> addresses;
  vector<string> prior_states;
  vector<string> results;
  int failures = 0;
  int incomplete = 0;
  for (const auto& ts : tservers) {
    const Status s = func(ts);
    uuids.emplace_back(ts.uuid);
    addresses.emplace_back(ts.address);
    prior_states.emplace_back(TServerStatePB_Name(ts.state));
    if (s.ok()) {
      results.emplace_back("OK");
    } else if (s.IsIncomplete()) {
      ++incomplete;
      results.emplace_back(s.ToString());
    } else {
      ++failures;
      results.emplace_back(s.ToString());
      LOG(WARNING) << Substitute("Unable to $0 for tserver $1 ($2): $3",
                                 op_desc, ts.uuid, ts.address, s.ToString());
    }
  }
  DataTable table({});
  table.AddColumn("uuid", std::move(uuids));
  table.AddColumn("rpc-addresses", std::move(addresses));
  if (show_maintenance_state) {
    table.AddColumn("prior maintenance state", std::move(prior_states));
  }
  table.AddColumn("result", std::move(results));
  RETURN_NOT_OK(table.PrintTo(cout));
  if (failures != 0) {
    if (!caution.empty()) {
      cout << endl << caution << endl;
    }
    return Status::RuntimeError(Substitute(
        "unable to $0 for $1 of the $2 tablet server(s) in location '$3'",
        op_desc, failures, tservers.size(), location));
  }
  if (incomplete != 0) {
    return Status::Incomplete(Substitute(
        "$0 of the $1 tablet server(s) in location '$2' are not fully quiesced",
        incomplete, tservers.size(), location));
  }
  return Status::OK();
}

Status LocationSetState(const RunnerContext& context, TServerStateChangePB::StateChange sc) {
  const bool entering = (sc == TServerStateChangePB::ENTER_MAINTENANCE_MODE);
  const string op_desc = entering ? "enter maintenance mode" : "exit maintenance mode";
  const string& location = FindOrDie(context.required_args, kLocationArg);

  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));
  vector<TServerInLocation> tservers;
  RETURN_NOT_OK(ListTServersInLocation(&proxy, location, &tservers));

  if (FLAGS_dry_run) {
    return PrintAffectedTServers(op_desc, location, tservers,
                                 /*show_maintenance_state=*/true);
  }
  return FanOutToLocation(
      op_desc, location, tservers,
      [&](const TServerInLocation& ts) {
        return SetTServerState(&proxy, ts.uuid, sc);
      },
      entering ? "WARNING: maintenance mode is not in effect on every tablet server in "
                 "this location. Do not take the location offline yet: replicas on the "
                 "tablet servers above that failed would be re-replicated. Re-run this "
                 "command until it succeeds for all of them."
               : "WARNING: the tablet servers above that failed are still in maintenance "
                 "mode, so their replicas won't be re-replicated if they go down. Re-run "
                 "this command until it succeeds for all of them.",
      /*show_maintenance_state=*/true);
}

Status LocationEnterMaintenance(const RunnerContext& context) {
  return LocationSetState(context, TServerStateChangePB::ENTER_MAINTENANCE_MODE);
}

Status LocationExitMaintenance(const RunnerContext& context) {
  return LocationSetState(context, TServerStateChangePB::EXIT_MAINTENANCE_MODE);
}

Status LocationSetQuiescing(const RunnerContext& context, bool quiesce) {
  const string op_desc = quiesce ? "start quiescing" : "stop quiescing";
  const string& location = FindOrDie(context.required_args, kLocationArg);

  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));
  vector<TServerInLocation> tservers;
  RETURN_NOT_OK(ListTServersInLocation(&proxy, location, &tservers));

  if (FLAGS_dry_run) {
    return PrintAffectedTServers(op_desc, location, tservers,
                                 /*show_maintenance_state=*/false);
  }
  // The master doesn't track quiescing, so each tserver is asked directly.
  return FanOutToLocation(
      op_desc, location, tservers,
      [&](const TServerInLocation& ts) {
        unique_ptr<TabletServerAdminServiceProxy> ts_proxy;
        RETURN_NOT_OK(BuildProxy(ts.address, TabletServer::kDefaultPort, &ts_proxy));
        QuiesceTabletServerRequestPB req;
        req.set_quiesce(quiesce);
        req.set_return_stats(quiesce && FLAGS_error_if_not_fully_quiesced);
        QuiesceTabletServerResponsePB resp;
        RpcController rpc;
        rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
        RETURN_NOT_OK(ts_proxy->Quiesce(req, &resp, &rpc));
        if (resp.has_error()) {
          return StatusFromPB(resp.error().status());
        }
        if (quiesce && FLAGS_error_if_not_fully_quiesced &&
            (resp.num_leaders() != 0 || resp.num_active_scanners() != 0)) {
          return Status::Incomplete(
              Substitute("not fully quiesced: $0 tablet leaders and $1 active "
                         "scanners remain", resp.num_leaders(), resp.num_active_scanners()));
        }
        return Status::OK();
      },
      quiesce ? "WARNING: quiescing is not in effect on every tablet server in this "
                "location. Leadership may have moved onto the tablet servers above that "
                "failed. Re-run this command until it succeeds for all of them."
              : "WARNING: the tablet servers above that failed may still be quiescing, "
                "so they won't take leadership or serve new scans. Re-run this command "
                "until it succeeds for all of them.",
      /*show_maintenance_state=*/false);
}

Status LocationStartQuiescing(const RunnerContext& context) {
  return LocationSetQuiescing(context, /*quiesce=*/true);
}

Status LocationStopQuiescing(const RunnerContext& context) {
  return LocationSetQuiescing(context, /*quiesce=*/false);
}

Status LocationQuiescingStatus(const RunnerContext& context) {
  const string& location = FindOrDie(context.required_args, kLocationArg);

  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));
  vector<TServerInLocation> tservers;
  RETURN_NOT_OK(ListTServersInLocation(&proxy, location, &tservers));

  vector<string> uuids;
  vector<string> addresses;
  vector<string> quiescing;
  vector<string> leaders;
  vector<string> scanners;
  vector<string> errors;
  int failures = 0;
  int total_leaders = 0;
  int total_scanners = 0;
  for (const auto& ts : tservers) {
    QuiesceTabletServerResponsePB resp;
    unique_ptr<TabletServerAdminServiceProxy> ts_proxy;
    Status s = BuildProxy(ts.address, TabletServer::kDefaultPort, &ts_proxy);
    if (s.ok()) {
      // 'quiesce' is left unset, so this only reads the state.
      QuiesceTabletServerRequestPB req;
      req.set_return_stats(true);
      RpcController rpc;
      rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
      rpc.RequireServerFeature(tserver::TabletServerFeatures::QUIESCING);
      s = ts_proxy->Quiesce(req, &resp, &rpc);
      if (s.ok() && resp.has_error()) {
        s = StatusFromPB(resp.error().status());
      }
    }
    uuids.emplace_back(ts.uuid);
    addresses.emplace_back(ts.address);
    if (s.ok()) {
      quiescing.emplace_back(resp.is_quiescing() ? "true" : "false");
      leaders.emplace_back(IntToString(resp.num_leaders()));
      scanners.emplace_back(IntToString(resp.num_active_scanners()));
      errors.emplace_back("");
      total_leaders += resp.num_leaders();
      total_scanners += resp.num_active_scanners();
    } else {
      ++failures;
      quiescing.emplace_back("<unknown>");
      leaders.emplace_back("<unknown>");
      scanners.emplace_back("<unknown>");
      errors.emplace_back(s.ToString());
      LOG(WARNING) << Substitute("Unable to get the quiescing state of tserver $0 ($1): $2",
                                 ts.uuid, ts.address, s.ToString());
    }
  }
  DataTable table({});
  table.AddColumn("uuid", std::move(uuids));
  table.AddColumn("rpc-addresses", std::move(addresses));
  table.AddColumn("Quiescing", std::move(quiescing));
  table.AddColumn("Tablet Leaders", std::move(leaders));
  table.AddColumn("Active Scanners", std::move(scanners));
  table.AddColumn("Error", std::move(errors));
  RETURN_NOT_OK(table.PrintTo(cout));

  if (failures != 0) {
    return Status::RuntimeError(Substitute(
        "unable to get the quiescing state of $0 of the $1 tablet server(s) in "
        "location '$2'", failures, tservers.size(), location));
  }
  cout << endl << Substitute("$0 tablet leader(s) and $1 active scanner(s) remain in "
                            "location '$2'", total_leaders, total_scanners, location) << endl;
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildTServerMode() {
  unique_ptr<Action> dump_memtrackers =
      TServerActionBuilder("dump_memtrackers", &TserverDumpMemTrackers)
      .Description("Dump the memtrackers from a Kudu Tablet Server")
      .McpDisposition(Disposition::SURFACE)
      .AddOptionalParameter("format")
      .AddOptionalParameter("memtracker_output")
      .Build();

  unique_ptr<Action> get_flags =
      TServerActionBuilder("get_flags", &TServerGetFlags)
      .Description("Get the gflags for a Kudu Tablet Server")
      .McpDisposition(Disposition::SURFACE)
      .AddOptionalParameter("all_flags")
      .AddOptionalParameter("flags")
      .AddOptionalParameter("flag_tags")
      .Build();

  unique_ptr<Action> run =
      ActionBuilder("run", &TServerRun)
      .ProgramName("kudu-tserver")
      .Description("Run a Kudu Tablet Server")
      .McpDisposition(Disposition::REJECT)
      .ExtraDescription("Note: The tablet server is started in this process and "
                        "runs until interrupted.\n\n"
                        "The most common configuration flags are described below. "
                        "For all the configuration options pass --helpfull or see "
                        "https://kudu.apache.org/docs/configuration_reference.html"
                        "#kudu-tserver_supported")
      .AddOptionalParameter("tserver_master_addrs")
      // Even though fs_wal_dir is required, we don't want it to be positional argument.
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("block_cache_capacity_mb")
      .AddOptionalParameter("memory_limit_hard_bytes")
      .AddOptionalParameter("log_dir")
      // Unlike most tools we don't log to stderr by default to match the
      // kudu-tserver binary as closely as possible.
      .AddOptionalParameter("logtostderr", string("false"))
      .Build();

  unique_ptr<Action> set_flag =
      TServerActionBuilder("set_flag", &TServerSetFlag)
      .Description("Change a gflag value on a Kudu Tablet Server")
      .McpDisposition(Disposition::GATED)
      .AddRequiredParameter({ kFlagArg, "Name of the gflag" })
      .AddRequiredParameter({ kValueArg, "New value for the gflag" })
      .AddOptionalParameter("force")
      .AddOptionalParameter("run_consistency_check")
      .Build();

  unique_ptr<Action> set_flag_for_all =
      ClusterActionBuilder("set_flag_for_all", &TServerSetAllTServersFlag)
      .Description("Change a gflag value for all Kudu Tablet Servers in the cluster")
      .McpDisposition(Disposition::GATED)
      .AddRequiredParameter({ kFlagArg, "Name of the gflag" })
      .AddRequiredParameter({ kValueArg, "New value for the gflag" })
      .AddOptionalParameter("force")
      .Build();

  unique_ptr<Action> status =
      TServerActionBuilder("status", &TServerStatus)
      .Description("Get the status of a Kudu Tablet Server")
      .McpDisposition(Disposition::SURFACE)
      .Build();

  unique_ptr<Action> timestamp =
      TServerActionBuilder("timestamp", &TServerTimestamp)
      .Description("Get the current timestamp of a Kudu Tablet Server")
      .McpDisposition(Disposition::SURFACE)
      .Build();

  unique_ptr<Action> list_tservers =
      ClusterActionBuilder("list", &ListTServers)
      .Description("List tablet servers in a Kudu cluster")
      .McpDisposition(Disposition::SURFACE)
      .AddOptionalParameter("columns", string("uuid,rpc-addresses"),
                            string("Comma-separated list of tserver info fields to "
                                   "include in output.\nPossible values: uuid, "
                                   "rpc-addresses, http-addresses, version, seqno, "
                                   "heartbeat, start_time, state"))
      .AddOptionalParameter("format")
      .Build();

  unique_ptr<Action> quiescing_status =
      TServerActionBuilder("status", &QuiescingStatus)
      .Description("Output information about the quiescing state of a Tablet "
                   "Server.")
      .McpDisposition(Disposition::SURFACE)
      .Build();
  unique_ptr<Action> start_quiescing =
      TServerActionBuilder("start", &StartQuiescingTServer)
      .Description("Start quiescing the given Tablet Server. While a Tablet "
                   "Server is quiescing, Tablet replicas on it will no longer "
                   "attempt to become leader, and new scan requests will be "
                   "retried at other servers.")
      .McpDisposition(Disposition::GATED)
      .AddOptionalParameter("error_if_not_fully_quiesced")
      .Build();
  unique_ptr<Action> stop_quiescing =
      TServerActionBuilder("stop", &StopQuiescingTServer)
      .Description("Stop quiescing a Tablet Server.")
      .McpDisposition(Disposition::GATED)
      .Build();
  unique_ptr<Mode> quiesce = ModeBuilder("quiesce")
      .Description("Operate on the quiescing state of a Kudu Tablet Server.")
      .AddAction(std::move(quiescing_status))
      .AddAction(std::move(start_quiescing))
      .AddAction(std::move(stop_quiescing))
      .Build();

  unique_ptr<Action> enter_maintenance =
      ClusterActionBuilder("enter_maintenance", &EnterMaintenance)
      .Description("Begin maintenance on the Tablet Server. While under "
                   "maintenance, downtime of the Tablet Server will not lead "
                   "to the immediate re-replication of its tablet replicas.")
      .McpDisposition(Disposition::GATED)
      .AddRequiredParameter({ kTServerIdArg, kTServerIdDesc })
      .AddOptionalParameter("allow_missing_tserver")
      .Build();
  // Note: --allow_missing_tserver doesn't make sense for exit_maintenance
  // because if the tserver is missing, the non-existent tserver's state is
  // already NONE and so exit_maintenance is a no-op.
  unique_ptr<Action> exit_maintenance =
      ClusterActionBuilder("exit_maintenance", &ExitMaintenance)
      .Description("End maintenance of the Tablet Server.")
      .McpDisposition(Disposition::GATED)
      .AddRequiredParameter({ kTServerIdArg, kTServerIdDesc })
      .Build();
  unique_ptr<Mode> state = ModeBuilder("state")
      .Description("Operate on the state of a Kudu Tablet Server")
      .AddAction(std::move(enter_maintenance))
      .AddAction(std::move(exit_maintenance))
      .Build();

  unique_ptr<Action> unregister_tserver =
      ClusterActionBuilder("unregister", &UnregisterTServer)
          .Description(
              "Unregister a tablet server from the master's in-memory state and system catalog.")
          .McpDisposition(Disposition::GATED)
          .AddRequiredParameter({kTServerIdArg, kTServerIdDesc})
          .AddOptionalParameter("force_unregister_live_tserver")
          .AddOptionalParameter("remove_tserver_state")
          .Build();

  return ModeBuilder("tserver")
      .Description("Operate on a Kudu Tablet Server")
      .AddAction(std::move(dump_memtrackers))
      .AddAction(std::move(get_flags))
      .AddAction(std::move(run))
      .AddAction(std::move(set_flag))
      .AddAction(std::move(set_flag_for_all))
      .AddAction(std::move(status))
      .AddAction(std::move(timestamp))
      .AddAction(std::move(list_tservers))
      .AddAction(std::move(unregister_tserver))
      .AddMode(std::move(quiesce))
      .AddMode(std::move(state))
      .Build();
}

unique_ptr<Mode> BuildLocationQuiesceMode() {
  unique_ptr<Action> quiescing_status =
      ClusterActionBuilder("status", &LocationQuiescingStatus)
      .Description("Output information about the quiescing state of every Tablet "
                   "Server in a location.")
      .AddRequiredParameter({ kLocationArg, kLocationDesc })
      .Build();
  unique_ptr<Action> start_quiescing =
      ClusterActionBuilder("start", &LocationStartQuiescing)
      .Description("Start quiescing every Tablet Server in a location. While a "
                   "Tablet Server is quiescing, Tablet replicas on it will no "
                   "longer attempt to become leader, and new scan requests will "
                   "be retried at other servers.")
      .ExtraDescription("Quiescing is not persisted, so it is lost if a Tablet "
                        "Server restarts. Leadership only moves to replicas whose "
                        "Tablet Server is not quiescing, so it moves out of the "
                        "location; a tablet with no such replica keeps its leader "
                        "here. To wait for the location to drain, re-run with "
                        "--error_if_not_fully_quiesced until it succeeds, or watch "
                        "'kudu cluster location quiesce status'.")
      .AddRequiredParameter({ kLocationArg, kLocationDesc })
      .AddOptionalParameter("error_if_not_fully_quiesced")
      .AddOptionalParameter("dry_run")
      .Build();
  unique_ptr<Action> stop_quiescing =
      ClusterActionBuilder("stop", &LocationStopQuiescing)
      .Description("Stop quiescing every Tablet Server in a location.")
      .AddRequiredParameter({ kLocationArg, kLocationDesc })
      .AddOptionalParameter("dry_run")
      .Build();
  return ModeBuilder("quiesce")
      .Description("Operate on the quiescing state of every Kudu Tablet Server "
                   "in a location")
      .AddAction(std::move(quiescing_status))
      .AddAction(std::move(start_quiescing))
      .AddAction(std::move(stop_quiescing))
      .Build();
}

unique_ptr<Mode> BuildLocationStateMode() {
  unique_ptr<Action> enter_maintenance =
      ClusterActionBuilder("enter_maintenance", &LocationEnterMaintenance)
      .Description("Begin maintenance on every Tablet Server in a location. "
                   "While under maintenance, downtime of a Tablet Server will "
                   "not lead to the immediate re-replication of its tablet "
                   "replicas.")
      .ExtraDescription("This does not check whether the location is safe to take "
                        "offline; run 'kudu cluster location check' for that. If it "
                        "fails for any Tablet Server in the location, re-run it "
                        "until it succeeds for all of them.")
      .AddRequiredParameter({ kLocationArg, kLocationDesc })
      .AddOptionalParameter("dry_run")
      .Build();
  unique_ptr<Action> exit_maintenance =
      ClusterActionBuilder("exit_maintenance", &LocationExitMaintenance)
      .Description("End maintenance of every Tablet Server in a location.")
      .AddRequiredParameter({ kLocationArg, kLocationDesc })
      .AddOptionalParameter("dry_run")
      .Build();
  return ModeBuilder("state")
      .Description("Operate on the state of every Kudu Tablet Server in a location")
      .AddAction(std::move(enter_maintenance))
      .AddAction(std::move(exit_maintenance))
      .Build();
}

} // namespace tools
} // namespace kudu

