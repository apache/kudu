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
#include <cstdint>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
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
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/init.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DECLARE_bool(force);
DECLARE_int64(timeout_ms);
DECLARE_string(columns);

DEFINE_string(master_uuid, "", "Permanent UUID of the master. Only needed to disambiguate in case "
                               "of multiple masters with same RPC address");
DEFINE_int64(wait_secs, 8,
             "Timeout in seconds to wait for the newly added master to be promoted as VOTER.");

using kudu::master::AddMasterRequestPB;
using kudu::master::AddMasterResponsePB;
using kudu::master::ConnectToMasterRequestPB;
using kudu::master::ConnectToMasterResponsePB;
using kudu::master::ListMastersRequestPB;
using kudu::master::ListMastersResponsePB;
using kudu::master::Master;
using kudu::master::MasterServiceProxy;
using kudu::master::RefreshAuthzCacheRequestPB;
using kudu::master::RefreshAuthzCacheResponsePB;
using kudu::master::RemoveMasterRequestPB;
using kudu::master::RemoveMasterResponsePB;
using kudu::consensus::RaftPeerPB;
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

Status AddMasterChangeConfig(const RunnerContext& context) {
  const string& new_master_address = FindOrDie(context.required_args, kMasterAddressArg);

  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));

  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(new_master_address, Master::kDefaultPort));
  {
    AddMasterRequestPB req;
    AddMasterResponsePB resp;
    *req.mutable_rpc_addr() = HostPortToPB(hp);

    Status s = proxy.SyncRpc<AddMasterRequestPB, AddMasterResponsePB>(
        req, &resp, "AddMaster", &MasterServiceProxy::AddMasterAsync,
        {master::MasterFeatures::DYNAMIC_MULTI_MASTER});
    // It's possible this is a retry request in which case instead of returning
    // the master is already present in the Raft config error we make further checks
    // whether the master has been promoted to a VOTER.
    bool master_already_present =
        resp.has_error() && resp.error().code() == master::MasterErrorPB::MASTER_ALREADY_PRESENT;
    if (!s.ok() && !master_already_present) {
      return s;
    }
    if (master_already_present) {
      LOG(INFO) << "Master already present. Checking for promotion to VOTER...";
    }
  }

  // If the system catalog of the new master can be caught up from the WAL then the new master will
  // be promoted to a VOTER and become a FOLLOWER. However this can take some time, so we'll
  // try for a few seconds. It's perfectly normal for the new master to be not caught up from
  // the WAL in which case subsequent steps of system catalog tablet copy need to be carried out
  // as outlined in the documentation for adding a new master to Kudu cluster.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_wait_secs);
  RaftPeerPB::Role master_role = RaftPeerPB::UNKNOWN_ROLE;
  RaftPeerPB::MemberType master_type = RaftPeerPB::UNKNOWN_MEMBER_TYPE;
  do {
    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    RETURN_NOT_OK((proxy.SyncRpc<ListMastersRequestPB, ListMastersResponsePB>(
                   req, &resp, "ListMasters", &MasterServiceProxy::ListMastersAsync)));

    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }

    int i = 0;
    bool new_master_found = false;
    for (; i < resp.masters_size(); i++) {
      const auto& master = resp.masters(i);
      if (master.has_error()) {
        LOG(WARNING) << "Failed to retrieve info for master: "
                     << StatusFromPB(master.error()).ToString();
        continue;
      }
      for (const auto& master_hp : master.registration().rpc_addresses()) {
        if (hp == HostPortFromPB(master_hp)) {
          // Found the newly added master
          new_master_found = true;
          break;
        }
      }
      if (new_master_found) {
        break;
      }
    }
    if (!new_master_found) {
      return Status::NotFound(Substitute("New master $0 not found. Retry adding the master",
                                         hp.ToString()));
    }
    CHECK_LT(i, resp.masters_size());
    const auto& master = resp.masters(i);
    master_role = master.role();
    master_type = master.member_type();
    if (master_type == RaftPeerPB::VOTER &&
        (master_role == RaftPeerPB::FOLLOWER || master_role == RaftPeerPB::LEADER)) {
      LOG(INFO) << Substitute("Successfully added master $0 to the cluster. Please follow the "
                              "next steps which includes updating master addresses, updating "
                              "client configuration etc. from the Kudu administration "
                              "documentation on \"Migrating to Multiple Kudu Masters\".",
                              hp.ToString());
      return Status::OK();
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  } while (MonoTime::Now() < deadline);

  LOG(INFO) << Substitute("New master $0 part of the Raft configuration; role: $1, member_type: "
                          "$2. Please follow the next steps which includes system catalog tablet "
                          "copy, updating master addresses etc. from the Kudu administration "
                          "documentation on \"Migrating to Multiple Kudu Masters\".",
                          hp.ToString(), master_role, master_type);
  return Status::OK();
}

Status RemoveMasterChangeConfig(const RunnerContext& context) {
  const string& master_hp_str = FindOrDie(context.required_args, kMasterAddressArg);
  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(master_hp_str, Master::kDefaultPort));

  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));

  RemoveMasterRequestPB req;
  RemoveMasterResponsePB resp;
  *req.mutable_rpc_addr() = HostPortToPB(hp);
  if (!FLAGS_master_uuid.empty()) {
    *req.mutable_master_uuid() = FLAGS_master_uuid;
  }

  RETURN_NOT_OK((proxy.SyncRpc<RemoveMasterRequestPB, RemoveMasterResponsePB>(
                 req, &resp, "RemoveMaster", &MasterServiceProxy::RemoveMasterAsync,
                 { master::MasterFeatures::DYNAMIC_MULTI_MASTER })));

  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  LOG(INFO) << Substitute("Successfully removed master $0 from the cluster. Please follow the next "
                          "steps from the Kudu administration documentation on "
                          "\"Removing Kudu Masters from a Multi-Master Deployment\" or "
                          "\"Recovering from a dead Kudu Master in a Multi-Master Deployment\" "
                          "as appropriate.",
                          hp.ToString());
  return Status::OK();
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
    if (iequals(column.ToString(), "uuid")) {
      for (const auto& master : masters) {
        values.push_back(master.instance_id().permanent_uuid());
      }
    } else if (iequals(column.ToString(), "cluster_id")) {
      for (const auto& master : masters) {
        values.emplace_back(master.has_cluster_id() ? master.cluster_id() : "");
      }
    } else if (iequals(column.ToString(), "seqno")) {
      for (const auto& master : masters) {
        values.push_back(std::to_string(master.instance_id().instance_seqno()));
      }
    } else if (iequals(column.ToString(), "rpc-addresses") ||
               iequals(column.ToString(), "rpc_addresses")) {
      for (const auto& master : masters) {
        values.push_back(JoinMapped(master.registration().rpc_addresses(),
                         hostport_to_string, ","));
      }
    } else if (iequals(column.ToString(), "http-addresses") ||
               iequals(column.ToString(), "http_addresses")) {
      for (const auto& master : masters) {
        values.push_back(JoinMapped(master.registration().http_addresses(),
                                    hostport_to_string, ","));
      }
    } else if (iequals(column.ToString(), "version")) {
      for (const auto& master : masters) {
        values.push_back(master.registration().software_version());
      }
    } else if (iequals(column.ToString(), "start_time")) {
      for (const auto& master : masters) {
        values.emplace_back(StartTimeToString(master.registration()));
      }
    } else if (iequals(column.ToString(), "role")) {
      for (const auto& master : masters) {
        values.emplace_back(RaftPeerPB::Role_Name(master.role()));
      }
    } else if (iequals(column.ToString(), "member_type")) {
      for (const auto& master : masters) {
        values.emplace_back(RaftPeerPB::MemberType_Name(master.member_type()));
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

Status RefreshAuthzCacheAtMaster(const string& master_address) {
  unique_ptr<MasterServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(master_address, Master::kDefaultPort, &proxy));

  RpcController ctl;
  ctl.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));

  RefreshAuthzCacheRequestPB req;
  RefreshAuthzCacheResponsePB resp;
  RETURN_NOT_OK(proxy->RefreshAuthzCache(req, &resp, &ctl));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status RefreshAuthzCache(const RunnerContext& context) {
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));

  if (!FLAGS_force) {
    // Make sure the list of master addresses specified for the command
    // matches the actual list of masters in the cluster.
    RETURN_NOT_OK(VerifyMasterAddressList(master_addresses));
  }

  // It makes sense to refresh privileges cache at every master in the cluster.
  // Otherwise, the authorization provider might return inconsistent results for
  // authz requests upon master leadership change.
  vector<Status> statuses;
  statuses.reserve(master_addresses.size());
  for (const auto& address : master_addresses) {
    auto status = RefreshAuthzCacheAtMaster(address);
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
    unique_ptr<Action> action_refresh =
        ClusterActionBuilder("refresh", &RefreshAuthzCache)
        .Description("Refresh the authorization policies")
        .AddOptionalParameter(
            "force", boost::none,
            string(
                "Ignore mismatches of the specified and the actual lists "
                "of master addresses in the cluster"))
        .Build();

    unique_ptr<Mode> mode_authz_cache = ModeBuilder("authz_cache")
        .Description("Operate on the authz caches of the Kudu Masters")
        .AddAction(std::move(action_refresh))
        .Build();
    builder.AddMode(std::move(mode_authz_cache));
  }
  {
    unique_ptr<Action> dump_memtrackers =
        MasterActionBuilder("dump_memtrackers", &MasterDumpMemTrackers)
        .Description("Dump the memtrackers from a Kudu Master")
        .AddOptionalParameter("format")
        .AddOptionalParameter("memtracker_output")
        .Build();
    builder.AddAction(std::move(dump_memtrackers));
  }
  {
    unique_ptr<Action> get_flags =
        MasterActionBuilder("get_flags", &MasterGetFlags)
        .Description("Get the gflags for a Kudu Master")
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
        .Description("Run a Kudu Master")
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
        MasterActionBuilder("set_flag", &MasterSetFlag)
        .Description("Change a gflag value on a Kudu Master")
        .AddRequiredParameter({ kFlagArg, "Name of the gflag" })
        .AddRequiredParameter({ kValueArg, "New value for the gflag" })
        .AddOptionalParameter("force")
        .Build();
    builder.AddAction(std::move(set_flag));
  }
  {
    unique_ptr<Action> status =
        MasterActionBuilder("status", &MasterStatus)
        .Description("Get the status of a Kudu Master")
        .Build();
    builder.AddAction(std::move(status));
  }
  {
    unique_ptr<Action> timestamp =
        MasterActionBuilder("timestamp", &MasterTimestamp)
        .Description("Get the current timestamp of a Kudu Master")
        .Build();
    builder.AddAction(std::move(timestamp));
  }
  {
    unique_ptr<Action> list_masters =
        ClusterActionBuilder("list", &ListMasters)
        .Description("List masters in a Kudu cluster")
        .AddOptionalParameter(
            "columns",
            string("uuid,rpc-addresses,role"),
            string("Comma-separated list of master info fields to "
                   "include in output.\nPossible values: uuid, cluster_id"
                   "rpc-addresses, http-addresses, version, seqno, "
                   "start_time and role"))
        .AddOptionalParameter("format")
        .Build();
    builder.AddAction(std::move(list_masters));
  }
  {
    unique_ptr<Action> add_master =
        ActionBuilder("add", &AddMasterChangeConfig)
        .Description("Add a master to the Raft configuration of the Kudu cluster. "
                     "Please refer to the Kudu administration documentation on "
                     "\"Migrating to Multiple Kudu Masters\" for the complete steps.")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .AddOptionalParameter("wait_secs")
        .Build();
    builder.AddAction(std::move(add_master));
  }
  {
    unique_ptr<Action> remove_master =
        ActionBuilder("remove", &RemoveMasterChangeConfig)
        .Description("Remove a master from the Raft configuration of the Kudu cluster. "
                     "Please refer to the Kudu administration documentation on "
                     "\"Removing Kudu Masters from a Multi-Master Deployment\" or "
                     "\"Recovering from a dead Kudu Master in a Multi-Master Deployment\" "
                     "as appropriate.")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .AddOptionalParameter("master_uuid")
        .Build();
    builder.AddAction(std::move(remove_master));
  }

  return builder.Build();
}

} // namespace tools
} // namespace kudu

