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
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/monotime.h"
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
using kudu::master::MasterServiceProxy;
using kudu::master::ResetAuthzCacheRequestPB;
using kudu::master::ResetAuthzCacheResponsePB;
using kudu::rpc::RpcController;
using std::cout;
using std::map;
using std::set;
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

  proxy.SyncRpc<ListMastersRequestPB, ListMastersResponsePB>(
      req, &resp, "ListMasters", &MasterServiceProxy::ListMastersAsync);

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

// Make sure the list of master addresses specified in 'master_addresses'
// corresponds to the actual list of masters addresses in the cluster,
// as reported in ConnectToMasterResponsePB::master_addrs.
Status VerifyMasterAddressList(const vector<string>& master_addresses) {
  map<string, set<string>> addresses_per_master;
  for (const auto& address : master_addresses) {
    unique_ptr<MasterServiceProxy> proxy;
    RETURN_NOT_OK(BuildProxy(address, Master::kDefaultPort, &proxy));

    RpcController ctl;
    ctl.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    RETURN_NOT_OK(proxy->ConnectToMaster(req, &resp, &ctl));
    const auto& resp_master_addrs = resp.master_addrs();
    if (resp_master_addrs.size() != master_addresses.size()) {
      const auto addresses_provided = JoinStrings(master_addresses, ",");
      const auto addresses_cluster_config = JoinMapped(
          resp_master_addrs,
          [](const HostPortPB& pb) {
            return Substitute("$0:$1", pb.host(), pb.port());
          }, ",");
      return Status::InvalidArgument(Substitute(
          "list of master addresses provided ($0) "
          "does not match the actual cluster configuration ($1) ",
          addresses_provided, addresses_cluster_config));
    }
    set<string> addr_set;
    for (const auto& hp : resp_master_addrs) {
      addr_set.emplace(Substitute("$0:$1", hp.host(), hp.port()));
    }
    addresses_per_master.emplace(address, std::move(addr_set));
  }

  bool mismatch = false;
  if (addresses_per_master.size() > 1) {
    const auto it_0 = addresses_per_master.cbegin();
    auto it_1 = addresses_per_master.begin();
    ++it_1;
    for (auto it = it_1; it != addresses_per_master.end(); ++it) {
      if (it->second != it_0->second) {
        mismatch = true;
        break;
      }
    }
  }

  if (mismatch) {
    string err_msg = Substitute("specified: ($0);",
                                JoinStrings(master_addresses, ","));
    for (const auto& e : addresses_per_master) {
      err_msg += Substitute(" from master $0: ($1);",
                            e.first, JoinStrings(e.second, ","));
    }
    return Status::ConfigurationError(
        Substitute("master address lists mismatch: $0", err_msg));
  }

  return Status::OK();
}

Status ResetAuthzCacheAtMaster(const string& master_address) {
  unique_ptr<MasterServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(master_address, Master::kDefaultPort, &proxy));

  RpcController ctl;
  ctl.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));

  ResetAuthzCacheRequestPB req;
  ResetAuthzCacheResponsePB resp;
  RETURN_NOT_OK(proxy->ResetAuthzCache(req, &resp, &ctl));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status ResetAuthzCache(const RunnerContext& context) {
  const string& master_addresses_str =
      FindOrDie(context.required_args, kMasterAddressesArg);
  const vector<string>& master_addresses =
      strings::Split(master_addresses_str, ",");

  if (!FLAGS_force) {
    // Make sure the list of master addresses specified for the command
    // matches the actual list of masters in the cluster.
    RETURN_NOT_OK(VerifyMasterAddressList(master_addresses));
  }

  // It makes sense to reset privileges cache at every master in the cluster.
  // Otherwise, SentryAuthzProvider might return inconsistent results for authz
  // requests upon master leadership change.
  vector<Status> statuses;
  statuses.reserve(master_addresses.size());
  for (const auto& address : master_addresses) {
    auto status = ResetAuthzCacheAtMaster(address);
    statuses.emplace_back(std::move(status));
  }
  DCHECK_EQ(master_addresses.size(), statuses.size());
  string err_str;
  for (auto i = 0; i < statuses.size(); ++i) {
    const auto& s = statuses[i];
    if (s.ok()) {
      continue;
    }
    err_str += Substitute(" error from master at $0: $1",
                          master_addresses[i], s.ToString());
  }
  if (err_str.empty()) {
    return Status::OK();
  }
  return Status::Incomplete(err_str);
}

} // anonymous namespace

unique_ptr<Mode> BuildMasterMode() {
  ModeBuilder builder("master");
  builder.Description("Operate on a Kudu Master");

  {
    unique_ptr<Action> action_reset =
        ActionBuilder("reset", &ResetAuthzCache)
        .Description("Reset the privileges cache")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter(
            "force",
            boost::none,
            string("Ignore mismatches of the specified and the actual lists "
                   "of master addresses in the cluster"))
        .Build();

    unique_ptr<Mode> mode_authz_cache = ModeBuilder("authz_cache")
        .Description("Operate on the authz cache of a Kudu Master")
        .AddAction(std::move(action_reset))
        .Build();
    builder.AddMode(std::move(mode_authz_cache));
  }
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
        .AddOptionalParameter("flag_tags")
        .Build();
    builder.AddAction(std::move(get_flags));
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
            string("uuid,rpc-addresses"),
            string("Comma-separated list of master info fields to "
                   "include in output.\nPossible values: uuid, "
                   "rpc-addresses, http-addresses, version, seqno "
                   "and start_time"))
        .AddOptionalParameter("format")
        .AddOptionalParameter("timeout_ms")
        .Build();
    builder.AddAction(std::move(list_masters));
  }

  return builder.Build();
}

} // namespace tools
} // namespace kudu

