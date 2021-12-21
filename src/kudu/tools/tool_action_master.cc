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
#include <csignal>
#include <cstdint>
#include <fstream> // IWYU pragma: keep
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
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
#include "kudu/master/sys_catalog.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/master_rebuilder.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/env.h"
#include "kudu/util/flags.h"
#include "kudu/util/init.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"
#include "kudu/util/subprocess.h"

DECLARE_bool(force);
DECLARE_int64(negotiation_timeout_ms);
DECLARE_int64(timeout_ms);
DECLARE_string(columns);
DECLARE_string(fs_wal_dir);
DECLARE_string(fs_data_dirs);

DEFINE_string(master_uuid, "", "Permanent UUID of the master. Only needed to disambiguate in case "
                               "of multiple masters with same RPC address");

// For catching up system catalog of a new master from WAL, we expect 8-10 secs.
// However bringing up a new master can take longer due to --ntp_initial_sync_wait_secs (60 secs)
// hence the wait secs is set higher.
DEFINE_int64(wait_secs, 64,
             "Timeout in seconds to wait while retrying operations like bringing up new master, "
             "running ksck, waiting for the new master to be promoted as VOTER, etc. This flag "
             "is not passed to the new master.");
DEFINE_string(kudu_abs_path, "", "Absolute file path of the 'kudu' executable used to bring up "
                                 "new master and other workflow steps. This flag is not passed "
                                 "to the new master.");

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
using std::endl;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {
namespace {

const char* const kTabletServerAddressArg = "tserver_address";
const char* const kTabletServerAddressDesc = "Address of a Kudu tablet server "
    "of form 'hostname:port'. Port may be omitted if the tablet server is "
    "bound to the default port.";
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

// Bring up new master at the specified HostPort 'hp' with user-supplied 'flags' using kudu
// executable 'kudu_abs_path'.
// 'master_addresses' is list of masters in existing cluster including the new
// master.
Status BringUpNewMaster(const string& kudu_abs_path,
                        vector<string> flags,
                        const HostPort& hp,
                        const vector<string>& master_addresses,
                        unique_ptr<Subprocess>* new_master_out) {
  // Ensure the new master is not already running at the specified RPC address.
  unique_ptr<MasterServiceProxy> proxy;
  RETURN_NOT_OK_PREPEND(BuildProxy(hp.ToString(), Master::kDefaultPort, &proxy),
                        "Failed building proxy for new master");

  auto is_catalog_mngr_running = [](MasterServiceProxy* master_proxy) {
    master::GetMasterRegistrationRequestPB req;
    master::GetMasterRegistrationResponsePB resp;
    RpcController rpc;
    Status s = master_proxy->GetMasterRegistration(req, &resp, &rpc);
    return s.ok() && !resp.has_error();
  };

  if (is_catalog_mngr_running(proxy.get())) {
    return Status::IllegalState(Substitute("Master $0 already running", hp.ToString()));
  }

  flags.emplace_back("--master_addresses=" + JoinStrings(master_addresses, ","));
  flags.emplace_back("--master_address_add_new_master=" + hp.ToString());
  // In case of an error in bringing up the master we want to ensure the last log lines
  // are emitted to help debug the issue. Hence don't use async logging.
  flags.emplace_back("--log_async=false");
  vector<string> argv = { kudu_abs_path, "master", "run" };
  argv.insert(argv.end(), std::make_move_iterator(flags.begin()),
              std::make_move_iterator(flags.end()));
  auto new_master = std::make_unique<Subprocess>(argv);
  RETURN_NOT_OK_PREPEND(new_master->Start(), "Failed starting new master");
  auto stop_master = MakeScopedCleanup([&] {
    WARN_NOT_OK(new_master->KillAndWait(SIGKILL), "Failed stopping new master");
  });
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_wait_secs);
  do {
    Status wait_status = new_master->WaitNoBlock();
    if (!wait_status.IsTimedOut()) {
      return Status::RuntimeError("Failed to bring up new master");
    }
    if (is_catalog_mngr_running(proxy.get())) {
      stop_master.cancel();
      *new_master_out = std::move(new_master);
      return Status::OK();
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  } while (MonoTime::Now() < deadline);

  return Status::TimedOut("Timed out waiting for the new master to come up");
}

// Check health and consensus status of masters in ksck.
Status CheckMastersHealthy(const vector<string>& master_addresses) {
  std::shared_ptr<KsckCluster> cluster;
  RETURN_NOT_OK(RemoteKsckCluster::Build(master_addresses, &cluster));

  // Print to an unopened ofstream to discard ksck output.
  // See https://stackoverflow.com/questions/8243743.
  std::ofstream null_stream;
  Ksck ksck(cluster, &null_stream);
  RETURN_NOT_OK(ksck.CheckMasterHealth());
  RETURN_NOT_OK(ksck.CheckMasterConsensus());
  return Status::OK();
}

// Check new master 'new_master_hp' is promoted to a VOTER and all masters are healthy.
// Returns the last Raft role and member_type in output parameters 'master_role' and 'master_type'
// respectively.
Status CheckMasterVoterAndHealthy(LeaderMasterProxy* proxy,
                                  const vector<string>& master_addresses,
                                  const HostPort& new_master_hp,
                                  RaftPeerPB::Role* master_role,
                                  RaftPeerPB::MemberType* master_type) {
  *master_role = RaftPeerPB::UNKNOWN_ROLE;
  *master_type = RaftPeerPB::UNKNOWN_MEMBER_TYPE;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_wait_secs);
  do {
    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    RETURN_NOT_OK((proxy->SyncRpc<ListMastersRequestPB, ListMastersResponsePB>(
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
        if (new_master_hp == HostPortFromPB(master_hp)) {
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
                                         new_master_hp.ToString()));
    }
    CHECK_LT(i, resp.masters_size());
    const auto& master = resp.masters(i);
    *master_role = master.role();
    *master_type = master.member_type();
    if (*master_type == RaftPeerPB::VOTER &&
        (*master_role == RaftPeerPB::FOLLOWER || *master_role == RaftPeerPB::LEADER)) {
      // Check the master ksck state as well.
      if (CheckMastersHealthy(master_addresses).ok()) {
        return Status::OK();
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  } while (MonoTime::Now() < deadline);

  return Status::TimedOut("Timed out waiting for master to catch up from WAL");
}

// Deletes local system catalog on 'dst_master' and copies system catalog from one of the masters
// in 'master_addresses' which includes the dst_master using the kudu executable 'kudu_abs_path'.
Status CopyRemoteSystemCatalog(const string& kudu_abs_path,
                               const HostPort& dst_master,
                               const vector<string>& master_addresses) {
  // Find source master to copy system catalog from.
  string src_master;
  const auto& dst_master_str = dst_master.ToString();
  for (const auto& addr : master_addresses) {
    if (addr != dst_master_str) {
      src_master = addr;
      break;
    }
  }
  if (src_master.empty()) {
    return Status::RuntimeError("Failed to find source master to copy system catalog");
  }

  LOG(INFO) << Substitute("Deleting system catalog on $0", dst_master_str);
  RETURN_NOT_OK_PREPEND(
      Subprocess::Call(
          { kudu_abs_path, "local_replica", "delete", master::SysCatalogTable::kSysCatalogTabletId,
            "--fs_wal_dir=" + FLAGS_fs_wal_dir, "--fs_data_dirs=" + FLAGS_fs_data_dirs,
            "-clean_unsafe" }),
      "Failed to delete system catalog");

  LOG(INFO) << Substitute("Copying system catalog from master $0", src_master);
  RETURN_NOT_OK_PREPEND(
      Subprocess::Call(
      { kudu_abs_path, "local_replica", "copy_from_remote",
        master::SysCatalogTable::kSysCatalogTabletId, src_master,
        "--fs_wal_dir=" + FLAGS_fs_wal_dir, "--fs_data_dirs=" + FLAGS_fs_data_dirs }),
      "Failed to copy system catalog");

  return Status::OK();
}

// Invoke the add master Raft change config RPC to add the supplied master 'hp'.
// If the master is already present then Status::OK is returned and a log message is printed.
Status AddMasterRaftConfig(const RunnerContext& context,
                           const HostPort& hp) {
  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));
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
    LOG(INFO) << "Master already present.";
  }
  LOG(INFO) << Substitute("Successfully added master $0 to the Raft configuration.",
                          hp.ToString());
  return Status::OK();
}

Status AddMaster(const RunnerContext& context) {
  static const char* const kPostSuccessMsg =
      "Please follow the next steps which includes updating master addresses, updating client "
      "configuration etc. from the Kudu administration documentation on "
      "\"Migrating to Multiple Kudu Masters\".";
  static const char* const kFailedStopMsg =
      "Failed to stop new master after successfully adding to the cluster. Stop the master before "
      "proceeding further.";

  // Parse input arguments.
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  // "master_addresses" after being successfully parsed above don't contain duplicates.
  // So "master_hps" won't contain any duplicates as well.
  vector<HostPort> master_hps;
  RETURN_NOT_OK(HostPort::ParseAddresses(master_addresses, Master::kDefaultPort, &master_hps));
  DCHECK_EQ(master_addresses.size(), master_hps.size());

  const string& new_master_address = FindOrDie(context.required_args, kMasterAddressArg);
  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(new_master_address, Master::kDefaultPort));

  // Check for the 'kudu' executable if the user has supplied one.
  if (!FLAGS_kudu_abs_path.empty() && !Env::Default()->FileExists(FLAGS_kudu_abs_path)) {
    return Status::NotFound(Substitute("kudu binary not found at $0", FLAGS_kudu_abs_path));
  }
  string kudu_abs_path = FLAGS_kudu_abs_path;
  if (kudu_abs_path.empty()) {
    RETURN_NOT_OK(GetKuduToolAbsolutePathSafe(&kudu_abs_path));
  }

  // Get the flags that'll be needed to bring up new master and for system catalog copy.
  if (FLAGS_fs_wal_dir.empty()) {
    return Status::InvalidArgument("Flag -fs_wal_dir not supplied");
  }
  if (FLAGS_fs_data_dirs.empty()) {
    return Status::InvalidArgument("Flag -fs_data_dirs not supplied");
  }
  GFlagsMap flags_map = GetNonDefaultFlagsMap();
  // Remove the optional parameters for this command.
  // Remaining optional flags need to be passed to the new master.
  flags_map.erase("wait_secs");
  flags_map.erase("kudu_abs_path");

  // Bring up the new master.
  vector<string> new_master_flags;
  new_master_flags.reserve(flags_map.size());
  for (const auto& name_flag_pair : flags_map)  {
    const auto& flag = name_flag_pair.second;
    new_master_flags.emplace_back(Substitute("--$0=$1", flag.name, flag.current_value));
  }
  new_master_flags.emplace_back("--master_auto_join_cluster=false");

  // Bring up the new master that includes master addresses of the cluster and itself.
  // It's possible this is a retry in which case the new master is already part of
  // master_addresses.
  // Using HostPort for comparing instead of strings as in case of latter, the default port
  // may not be specified.
  if (std::find(master_hps.begin(), master_hps.end(), hp) == master_hps.end()) {
    master_addresses.emplace_back(hp.ToString());
    master_hps.push_back(hp);
  }

  unique_ptr<Subprocess> new_master;
  RETURN_NOT_OK_PREPEND(BringUpNewMaster(kudu_abs_path, new_master_flags, hp, master_addresses,
                                         &new_master),
                        "Failed bringing up new master. See Kudu Master log for details.");
  auto stop_master = MakeScopedCleanup([&] {
    auto* master_ptr = new_master.get();
    if (master_ptr != nullptr) {
      WARN_NOT_OK(master_ptr->KillAndWait(SIGKILL), "Failed stopping new master");
    }
  });
  // Call the add master Raft change config RPC.
  RETURN_NOT_OK(AddMasterRaftConfig(context, hp));

  // If the system catalog of the new master can be caught up from the WAL then the new master will
  // transition to FAILED_UNRECOVERABLE state. However this can take some time, so we'll
  // try for a few seconds. It's perfectly normal for the new master to be not caught up from
  // the WAL in which case subsequent steps of system catalog tablet copy need to be carried out.
  RaftPeerPB::Role master_role;
  RaftPeerPB::MemberType master_type;
  LeaderMasterProxy proxy;
  // Since new master is now part of the Raft configuration, use updated 'master_addresses'.
  RETURN_NOT_OK(proxy.Init(
      master_addresses, MonoDelta::FromMilliseconds(FLAGS_timeout_ms),
      MonoDelta::FromMilliseconds(FLAGS_negotiation_timeout_ms)));

  LOG(INFO) << "Checking for master consensus and health status...";
  Status wal_catchup_status = CheckMasterVoterAndHealthy(&proxy, master_addresses, hp,
                                                         &master_role, &master_type);
  if (wal_catchup_status.ok()) {
    stop_master.cancel();
    RETURN_NOT_OK_PREPEND(new_master->KillAndWait(SIGTERM), kFailedStopMsg);
    LOG(INFO) << Substitute("Master $0 successfully caught up from WAL.", hp.ToString());
    LOG(INFO) << kPostSuccessMsg;
    return Status::OK();
  }
  if (!wal_catchup_status.IsTimedOut()) {
    RETURN_NOT_OK_PREPEND(wal_catchup_status,
                          "Unexpected error waiting for master to catchup from WAL.");
    return wal_catchup_status;
  }

  // New master could not be caught up from WAL, so next we'll attempt copying
  // system catalog.
  LOG(INFO) << Substitute("Master $0 status; role: $1, member_type: $2",
                          hp.ToString(), RaftPeerPB::Role_Name(master_role),
                          RaftPeerPB::MemberType_Name(master_type));
  LOG(INFO) << Substitute("Master $0 could not be caught up from WAL.", hp.ToString());

  RETURN_NOT_OK_PREPEND(new_master->KillAndWait(SIGTERM),
                        "Unable to stop master to proceed with system catalog copy");
  new_master.reset();

  RETURN_NOT_OK(CopyRemoteSystemCatalog(kudu_abs_path, hp, master_addresses));

  RETURN_NOT_OK_PREPEND(BringUpNewMaster(kudu_abs_path, new_master_flags, hp, master_addresses,
                                         &new_master),
                        "Failed bringing up new master after system catalog copy. "
                        "See Kudu Master log for details.");

  RETURN_NOT_OK_PREPEND(CheckMasterVoterAndHealthy(&proxy, master_addresses, hp, &master_role,
                                                   &master_type),
                        "Failed waiting for new master to be healthy after system catalog copy");

  stop_master.cancel();
  RETURN_NOT_OK_PREPEND(new_master->KillAndWait(SIGTERM), kFailedStopMsg);
  LOG(INFO) << "Successfully copied system catalog and new master is healthy.";
  LOG(INFO) << kPostSuccessMsg;
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

Status PrintRebuildReport(const RebuildReport& rebuild_report) {
  cout << "Rebuild Report" << endl;
  cout << "Tablet Servers" << endl;
  DataTable tserver_table({ "address", "status" });
  int bad_tservers = 0;
  for (const auto& pair : rebuild_report.tservers) {
    const Status& s = pair.second;
    tserver_table.AddRow({ pair.first, s.ToString() });
    if (!s.ok()) bad_tservers++;
  }
  RETURN_NOT_OK(tserver_table.PrintTo(cout));
  cout << Substitute("Rebuilt from $0 tablet servers, of which $1 had errors",
                     rebuild_report.tservers.size(), bad_tservers)
       << endl << endl;

  cout << "Replicas" << endl;
  DataTable replica_table({ "table", "tablet", "tablet server", "status" });
  int bad_replicas = 0;
  for (const auto& entry : rebuild_report.replicas) {
    const auto& key = entry.first;
    const Status& s = entry.second;
    replica_table.AddRow({ std::get<0>(key), std::get<1>(key), std::get<2>(key), s.ToString() });
    if (!s.ok()) bad_replicas++;
  }
  RETURN_NOT_OK(replica_table.PrintTo(cout));
  cout << Substitute("Rebuilt from $0 replicas, of which $1 had errors",
                     rebuild_report.replicas.size(), bad_replicas)
       << endl;

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

Status RebuildMaster(const RunnerContext& context) {
  MasterRebuilder master_rebuilder(context.variadic_args);
  RETURN_NOT_OK(master_rebuilder.RebuildMaster());
  PrintRebuildReport(master_rebuilder.GetRebuildReport());
  return Status::OK();
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
                   "include in output.\nPossible values: uuid, cluster_id, "
                   "rpc-addresses, http-addresses, version, seqno, "
                   "start_time, role and member_type"))
        .AddOptionalParameter("format")
        .Build();
    builder.AddAction(std::move(list_masters));
  }
  {
    unique_ptr<Action> add_master =
        ActionBuilder("add", &AddMaster)
        .Description("Add a master to the Kudu cluster")
        .ExtraDescription(
            "This is an advanced command that orchestrates the workflow to bring up and add a "
            "new master to the Kudu cluster. It must be run locally on the new master being added "
            "and not on the leader master. This tool shuts down the new master after completion "
            "of the command regardless of whether the new master addition is successful. "
            "After the command completes successfully, user is expected to explicitly "
            "start the new master using the same flags as supplied to this tool.\n\n"
            "Supply all the necessary flags to bring up the new master. "
            "The most common configuration flags used to bring up a master are described "
            "below. See \"Kudu Configuration Reference\" for all the configuration options that "
            "apply to a Kudu master.\n\n"
            "Please refer to the Kudu administration documentation on "
            "\"Migrating to Multiple Kudu Masters\" for the complete steps.")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .AddOptionalParameter("wait_secs")
        .AddOptionalParameter("kudu_abs_path")
        .AddOptionalParameter("fs_wal_dir")
        .AddOptionalParameter("fs_data_dirs")
        .AddOptionalParameter("fs_metadata_dir")
        .AddOptionalParameter("log_dir")
        // Unlike most tools we don't log to stderr by default to match the
        // kudu-master binary as closely as possible.
        .AddOptionalParameter("logtostderr", string("false"))
        .Build();
    builder.AddAction(std::move(add_master));
  }
  {
    unique_ptr<Action> remove_master =
        ActionBuilder("remove", &RemoveMasterChangeConfig)
        .Description("Remove a master from the Kudu cluster")
        .ExtraDescription(
            "Removes a master from the Raft configuration of the Kudu cluster.\n\n"
            "Please refer to the Kudu administration documentation on "
            "\"Removing Kudu Masters from a Multi-Master Deployment\" or "
            "\"Recovering from a dead Kudu Master in a Multi-Master Deployment\" as appropriate.")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
        .AddOptionalParameter("master_uuid")
        .Build();
    builder.AddAction(std::move(remove_master));
  }

  {
    const char* rebuild_extra_description = "Attempts to create on-disk metadata\n"
        "that can be used by a non-replicated master to recover a Kudu cluster\n"
        "that has permanently lost its masters. It has a number of limitations:\n"
        " - Security metadata like cryptographic keys are not rebuilt. Tablet servers\n"
        "   and clients must be restarted before starting the new master in order to\n"
        "   communicate with the new master.\n"
        " - Table IDs are known only by the masters. Reconstructed tables will have\n"
        "   new IDs.\n"
        " - If a create, delete, or alter table was in progress when the masters were lost,\n"
        "   it may not be possible to restore the table.\n"
        " - If all replicas of a tablet are missing, it may not be able to recover the\n"
        "   table fully. Moreover, the rebuild tool cannot detect that a tablet is\n"
        "   missing.\n"
        " - It's not possible to determine the replication factor of a table from tablet\n"
        "   server metadata. The rebuild tool sets the replication factor of each\n"
        "   table to --default_num_replicas instead.\n"
        " - It's not possible to determine the next column id for a table from tablet\n"
        "   server metadata. Instead, the rebuilt tool sets the next column id to\n"
        "   a very large number.\n"
        " - Table metadata like comments, owners, and configurations are not stored on\n"
        "   tablet servers and are thus not restored.\n"
        "WARNING: This tool is potentially unsafe. Only use it when there is no\n"
        "possibility of recovering the original masters, and you know what you\n"
        "are doing.";
    unique_ptr<Action> unsafe_rebuild =
        ActionBuilder("unsafe_rebuild", &RebuildMaster)
        .Description("Rebuild a Kudu master from tablet server metadata")
        .ExtraDescription(rebuild_extra_description)
        .AddRequiredVariadicParameter({ kTabletServerAddressArg, kTabletServerAddressDesc })
        .AddOptionalParameter("default_num_replicas")
        .AddOptionalParameter("default_schema_version")
        .AddOptionalParameter("fs_data_dirs")
        .AddOptionalParameter("fs_metadata_dir")
        .AddOptionalParameter("fs_wal_dir")
        .Build();
    builder.AddAction(std::move(unsafe_rebuild));
  }

  return builder.Build();
}

} // namespace tools
} // namespace kudu

