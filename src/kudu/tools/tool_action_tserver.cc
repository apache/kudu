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

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DECLARE_string(columns);

using std::string;
using std::unique_ptr;

namespace kudu {

using master::ListTabletServersRequestPB;
using master::ListTabletServersResponsePB;
using master::MasterServiceProxy;

namespace tools {
namespace {

const char* const kTServerAddressArg = "tserver_address";
const char* const kTServerAddressDesc = "Address of a Kudu Tablet Server of "
    "form 'hostname:port'. Port may be omitted if the Tablet Server is bound "
    "to the default port.";
const char* const kFlagArg = "flag";
const char* const kValueArg = "value";

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
      req, &resp, "ListTabletServers", &MasterServiceProxy::ListTabletServers);

  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  vector<string> headers;
  vector<vector<string>> columns;

  const auto& servers = resp.servers();

  auto hostport_to_string = [](const HostPortPB& hostport) {
    return strings::Substitute("$0:$1", hostport.host(), hostport.port());
  };

  for (const auto& column : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    headers.emplace_back(column.ToString());
    vector<string> values;
    if (boost::iequals(column, "uuid")) {
      for (const auto& server : servers) {
        values.push_back(server.instance_id().permanent_uuid());
      }
    } else if (boost::iequals(column, "seqno")) {
      for (const auto& server : servers) {
        values.push_back(std::to_string(server.instance_id().instance_seqno()));
      }
    } else if (boost::iequals(column, "rpc-addresses") ||
               boost::iequals(column, "rpc_addresses")) {
      for (const auto& server : servers) {
        values.push_back(JoinMapped(server.registration().rpc_addresses(),
                                    hostport_to_string, ","));
      }
    } else if (boost::iequals(column, "http-addresses") ||
               boost::iequals(column, "http_addresses")) {
      for (const auto& server : servers) {
        values.push_back(JoinMapped(server.registration().http_addresses(),
                                    hostport_to_string, ","));
      }
    } else if (boost::iequals(column, "version")) {
      for (const auto& server : servers) {
        values.push_back(server.registration().software_version());
      }
    } else {
      return Status::InvalidArgument("unknown column (--columns)", column);
    }

    columns.emplace_back(std::move(values));
  }

  RETURN_NOT_OK(PrintTable(headers, columns));
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildTServerMode() {
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
                                   "rpc-addresses, http-addresses, version, and seqno"))
      .AddOptionalParameter("format")
      .AddOptionalParameter("timeout_ms")
      .Build();

  return ModeBuilder("tserver")
      .Description("Operate on a Kudu Tablet Server")
      .AddAction(std::move(set_flag))
      .AddAction(std::move(status))
      .AddAction(std::move(timestamp))
      .AddAction(std::move(list_tservers))
      .Build();
}

} // namespace tools
} // namespace kudu

