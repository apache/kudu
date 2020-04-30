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

#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_runner.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/init.h"
#include "kudu/util/status.h"

DECLARE_bool(force);
DECLARE_int64(timeout_ms);
DECLARE_string(columns);

using kudu::master::ConnectToMasterRequestPB;
using kudu::master::ConnectToMasterResponsePB;
using kudu::master::ListMastersRequestPB;
using kudu::master::ListMastersResponsePB;
using kudu::master::Master;
using kudu::master::MasterServiceProxy;
using kudu::consensus::RaftPeerPB;
using std::cout;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {
namespace {

const char* const kMasterAddressArg = "master_address";
const char* const kMasterAddressDesc = "Address of a Kudu Master of form "
    "'hostname:port'. Port may be omitted if the Master is bound to the "
    "default port.";
const char* const kFlagArg = "flag";
const char* const kValueArg = "value";

Status MasterGetFlags(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerFlags(address, Master::kDefaultPort);
}

Status MasterRun(const RunnerContext& context) {
  RETURN_NOT_OK(InitKudu());

  // Enable redaction by default. Unlike most tools, we don't want user data
  // printed to the console/log to be shown by default.
  CHECK_NE("", google::SetCommandLineOptionWithMode("redact",
      "all", google::FlagSettingMode::SET_FLAGS_DEFAULT));

  master::SetMasterFlagDefaults();
  return master::RunMasterServer();
}

Status MasterSetFlag(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  const string& flag = FindOrDie(context.required_args, kFlagArg);
  const string& value = FindOrDie(context.required_args, kValueArg);
  return SetServerFlag(address, Master::kDefaultPort, flag, value);
}

Status MasterStatus(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerStatus(address, Master::kDefaultPort);
}

Status MasterTimestamp(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerTimestamp(address, Master::kDefaultPort);
}

Status ListMasters(const RunnerContext& context) {
  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));

  ListMastersRequestPB req;
  ListMastersResponsePB resp;

  RETURN_NOT_OK((proxy.SyncRpc<ListMastersRequestPB, ListMastersResponsePB>(
      req, &resp, "ListMasters", &MasterServiceProxy::ListMastersAsync)));

  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  DataTable table({});

  vector<ServerEntryPB> masters;
  std::copy_if(resp.masters().begin(), resp.masters().end(), std::back_inserter(masters),
               [](const ServerEntryPB& master) {
                 if (master.has_error()) {
                   LOG(WARNING) << "Failed to retrieve info for master: "
                                << StatusFromPB(master.error()).ToString();
                   return false;
                 }
                 return true;
               });

  auto hostport_to_string = [] (const HostPortPB& hostport) {
    return Substitute("$0:$1", hostport.host(), hostport.port());
  };

  for (const auto& column : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    vector<string> values;
    if (boost::iequals(column, "uuid")) {
      for (const auto& master : masters) {
        values.push_back(master.instance_id().permanent_uuid());
      }
    } else if (boost::iequals(column, "seqno")) {
      for (const auto& master : masters) {
        values.push_back(std::to_string(master.instance_id().instance_seqno()));
      }
    } else if (boost::iequals(column, "rpc-addresses") ||
               boost::iequals(column, "rpc_addresses")) {
      for (const auto& master : masters) {
        values.push_back(JoinMapped(master.registration().rpc_addresses(),
                         hostport_to_string, ","));
      }
    } else if (boost::iequals(column, "http-addresses") ||
               boost::iequals(column, "http_addresses")) {
      for (const auto& master : masters) {
        values.push_back(JoinMapped(master.registration().http_addresses(),
                                    hostport_to_string, ","));
      }
    } else if (boost::iequals(column, "version")) {
      for (const auto& master : masters) {
        values.push_back(master.registration().software_version());
      }
    } else if (boost::iequals(column, "start_time")) {
      for (const auto& master : masters) {
        values.emplace_back(StartTimeToString(master.registration()));
      }
    } else if (boost::iequals(column, "role")) {
      for (const auto& master : masters) {
        values.emplace_back(RaftPeerPB::Role_Name(master.role()));
      }
    } else {
      return Status::InvalidArgument("unknown column (--columns)", column);
    }
    table.AddColumn(column.ToString(), std::move(values));
  }

  RETURN_NOT_OK(table.PrintTo(cout));
  return Status::OK();
}

Status MasterDumpMemTrackers(const RunnerContext& context) {
  const auto& address = FindOrDie(context.required_args, kMasterAddressArg);
  return DumpMemTrackers(address, Master::kDefaultPort);
}

} // anonymous namespace

unique_ptr<Mode> BuildMasterMode() {
  ModeBuilder builder("master");
  builder.Description("Operate on a Kudu Master");

  {
    unique_ptr<Action> dump_memtrackers =
        ActionBuilder("dump_memtrackers", &MasterDumpMemTrackers)
        .Description("Dump the memtrackers from a Kudu Master")
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .AddOptionalParameter("format")
        .AddOptionalParameter("memtracker_output")
        .AddOptionalParameter("timeout_ms")
        .Build();
    builder.AddAction(std::move(dump_memtrackers));
  }
  {
    unique_ptr<Action> get_flags =
        ActionBuilder("get_flags", &MasterGetFlags)
        .Description("Get the gflags for a Kudu Master")
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .AddOptionalParameter("all_flags")
        .AddOptionalParameter("flags")
        .AddOptionalParameter("flag_tags")
        .Build();
    builder.AddAction(std::move(get_flags));
  }
  {
    unique_ptr<Action> run =
        ActionBuilder("run", &MasterRun)
        .ProgramName("kudu-master")
        .Description("Runs a Kudu Master")
        .ExtraDescription("Note: The master server is started in this process and "
                          "runs until interrupted.\n\n"
                          "The most common configuration flags are described below. "
                          "For all the configuration options pass --helpfull or see "
                          "https://kudu.apache.org/docs/configuration_reference.html"
                          "#kudu-master_supported")
        .AddOptionalParameter("master_addresses")
        // Even though fs_wal_dir is required, we don't want it to be positional argument.
        // This allows it to be passed as a standard flag.
        .AddOptionalParameter("fs_wal_dir")
        .AddOptionalParameter("fs_data_dirs")
        .AddOptionalParameter("fs_metadata_dir")
        .AddOptionalParameter("log_dir")
        // Unlike most tools we don't log to stderr by default to match the
        // kudu-master binary as closely as possible.
        .AddOptionalParameter("logtostderr", string("false"))
        .Build();
    builder.AddAction(std::move(run));
  }
  {
    unique_ptr<Action> set_flag =
        ActionBuilder("set_flag", &MasterSetFlag)
        .Description("Change a gflag value on a Kudu Master")
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .AddRequiredParameter({ kFlagArg, "Name of the gflag" })
        .AddRequiredParameter({ kValueArg, "New value for the gflag" })
        .AddOptionalParameter("force")
        .Build();
    builder.AddAction(std::move(set_flag));
  }
  {
    unique_ptr<Action> status =
        ActionBuilder("status", &MasterStatus)
        .Description("Get the status of a Kudu Master")
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .Build();
    builder.AddAction(std::move(status));
  }
  {
    unique_ptr<Action> timestamp =
        ActionBuilder("timestamp", &MasterTimestamp)
        .Description("Get the current timestamp of a Kudu Master")
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .Build();
    builder.AddAction(std::move(timestamp));
  }
  {
    unique_ptr<Action> list_masters =
        ActionBuilder("list", &ListMasters)
        .Description("List masters in a Kudu cluster")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter(
            "columns",
            string("uuid,rpc-addresses,role"),
            string("Comma-separated list of master info fields to "
                   "include in output.\nPossible values: uuid, "
                   "rpc-addresses, http-addresses, version, seqno, "
                   "start_time and role"))
        .AddOptionalParameter("format")
        .AddOptionalParameter("timeout_ms")
        .Build();
    builder.AddAction(std::move(list_masters));
  }

  return builder.Build();
}

} // namespace tools
} // namespace kudu

