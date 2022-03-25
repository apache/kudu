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

#include "kudu/master/master_runner.h"

#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/master_proxy_rpc.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_options.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_copy_client.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/version_info.h"

using gflags::SET_FLAGS_DEFAULT;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::ConsensusMetadataManager;
using kudu::consensus::GetConsensusStateRequestPB;
using kudu::consensus::GetConsensusStateResponsePB;
using kudu::master::GetMasterRegistrationRequestPB;
using kudu::master::GetMasterRegistrationResponsePB;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcController;
using kudu::tablet::TabletDataState;
using kudu::tablet::TabletMetadata;
using kudu::tserver::RemoteTabletCopyClient;
using kudu::tserver::TSTabletManager;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using std::to_string;
using strings::Substitute;

DEFINE_bool(master_auto_join_cluster, true, "Whether Kudu Masters should automatically attempt "
            "to join existing an existing cluster, specified by the master addresses");
TAG_FLAG(master_auto_join_cluster, runtime);

DEFINE_int64(master_auto_join_rpc_timeout_secs, 30, "The amount of time, in seconds, to use as a "
             "timeout when sending RPCs to add this Master to a cluster.");
TAG_FLAG(master_auto_join_rpc_timeout_secs, runtime);

DEFINE_int64(master_auto_join_retry_interval_secs, 5, "The interval, in seconds, with which to "
             "retry checks to determine whether this Master should be added to a cluster.");
TAG_FLAG(master_auto_join_retry_interval_secs, runtime);

DECLARE_bool(evict_failed_followers);

DECLARE_bool(hive_metastore_sasl_enabled);
DECLARE_string(master_addresses);
DECLARE_string(keytab_file);

DECLARE_bool(auto_rebalancing_enabled);
DECLARE_bool(raft_prepare_replacement_before_eviction);

namespace kudu {
namespace master {

namespace {

// Validates that if the HMS is configured with SASL enabled, the server has a
// keytab available. This doesn't use a GROUP_FLAG_VALIDATOR because this check
// only needs to be run on a server. E.g. tools that run with the HMS don't need
// to pass in a keytab.
Status ValidateHiveMetastoreSaslEnabled() {
    if (FLAGS_hive_metastore_sasl_enabled &&
        FLAGS_keytab_file.empty()) {
        return Status::ConfigurationError("When the Hive Metastore has SASL enabled "
                                          "(--hive_metastore_sasl_enabled), Kudu must be "
                                          "configured with a keytab (--keytab_file).");
    }
    return Status::OK();
}

// Verifies that:
// - all masters are reachable,
// - all masters agree on the term,
// - there are no pending Raft configs,
// - all masters agree on the Raft config index,
// - there is a leader, and
// - if we need to add 'local_uuid' to the Raft config, it is the only change
//   required to get the Raft config to match the UUIDs corresponding to
//   'master_addrs'.
//
// Returns an error if there is a non-retriable error (e.g. there might be a
// misconfiguration) and we should not attempt to retry this method.
//
// If OK is returned, sets 'needs_retry' to true if there is a potentially
// transient error, in which case calling the method again may help. If no
// retriable error was encountered, 'needs_retry' is set to false.
//
// Sets 'leader_hp' and 'local_hp' to the current master leader's hostport and
// the local hostport respectively. Sets 'needs_add' to whether 'local_uuid'
// should be added to the Raft config. These out-parameters should only be
// used if OK is returned and 'needs_retry' is set to false.
Status VerifyMastersGetHostPorts(const vector<HostPort>& master_addrs,
                                 const string& local_uuid,
                                 const std::shared_ptr<rpc::Messenger>& messenger,
                                 HostPort* leader_hp,
                                 HostPort* local_hp,
                                 bool* needs_retry,
                                 bool* needs_add) {
  vector<set<string>> each_remote_masters_master_uuids;
  set<string> fetched_uuids;
  int64_t current_term = -1;
  int64_t committed_config_index = -1;
  for (const auto& hp : master_addrs) {
    Sockaddr master_addr;
    Status s = SockaddrFromHostPort(hp, &master_addr);
    if (!s.ok()) {
      LOG(INFO) << Substitute("Error resolving master address for $0: $1",
                              hp.ToString(), s.ToString());
      *needs_retry = true;
      return Status::OK();
    }

    // First, get the UUID of the remote master.
    GetMasterRegistrationRequestPB reg_req;
    GetMasterRegistrationResponsePB reg_resp;
    RpcController reg_rpc;
    MasterServiceProxy proxy(messenger, master_addr, master_addr.host());
    s = proxy.GetMasterRegistration(reg_req, &reg_resp, &reg_rpc);
    if (!s.ok() || reg_resp.has_error()) {
      LOG(INFO) << Substitute("Error getting master registration for $0: $1, $2",
                              master_addr.ToString(), s.ToString(),
                              SecureShortDebugString(reg_resp));
      *needs_retry = true;
      return Status::OK();
    }
    const bool is_leader = reg_resp.role() == consensus::RaftPeerPB::LEADER;
    if (is_leader) {
      *leader_hp = hp;
    }
    // Skip the local master -- we only care about what the other masters
    // think, in case we should be trying to join their quorum.
    const auto& uuid = reg_resp.instance_id().permanent_uuid();
    EmplaceIfNotPresent(&fetched_uuids, uuid);
    if (local_uuid == uuid) {
      *local_hp = hp;
      continue;
    }

    // Get the Raft config from the remote master to get their quorum's
    // UUIDs.
    RpcController rpc;
    GetConsensusStateRequestPB req;
    req.add_tablet_ids(SysCatalogTable::kSysCatalogTabletId);
    req.set_dest_uuid(uuid);
    req.set_report_health(consensus::INCLUDE_HEALTH_REPORT);
    GetConsensusStateResponsePB resp;
    ConsensusServiceProxy consensus_proxy(messenger, master_addr, master_addr.host());
    s = consensus_proxy.GetConsensusState(req, &resp, &rpc);
    if (!s.ok() || resp.has_error()) {
      LOG(INFO) << Substitute("Error getting master consensus for $0: $1",
                              master_addr.ToString(), s.ToString());
      *needs_retry = true;
      return Status::OK();
    }
    if (resp.tablets_size() != 1) {
      return Status::Corruption(
          Substitute("Error getting master consensus, expected one tablet but got $0: $1",
                     resp.tablets_size(), SecureShortDebugString(resp)));
    }
    // Retry if the the masters don't agree on the current term.
    const auto& cstate = resp.tablets(0).cstate();
    if (current_term == -1) {
      current_term = cstate.current_term();
    }
    if (cstate.current_term() != current_term) {
      LOG(INFO) << Substitute("Existing masters have differing terms: $0 vs $1",
                              current_term, cstate.current_term());
      *needs_retry = true;
      return Status::OK();
    }
    // Retry if there's a pending config -- presumably pending means it's
    // transient.
    if (cstate.has_pending_config()) {
      LOG(INFO) << Substitute("Existing masters have pending config: $0",
                              SecureShortDebugString(cstate.pending_config()));
      *needs_retry = true;
      return Status::OK();
    }
    // Retry if the masters don't agree on the current Raft config's index.
    if (committed_config_index == -1) {
      committed_config_index = cstate.committed_config().opid_index();
    }
    if (cstate.committed_config().opid_index() != committed_config_index) {
      LOG(INFO) << Substitute("Existing masters have differing Raft config indexes: $0 vs $1",
                              committed_config_index, cstate.committed_config().opid_index());
      *needs_retry = true;
      return Status::OK();
    }
    const auto& config = cstate.committed_config();
    set<string> uuids;
    for (const auto& p : config.peers()) {
      EmplaceIfNotPresent(&uuids, p.permanent_uuid());
    }
    each_remote_masters_master_uuids.emplace_back(std::move(uuids));
  }
  if (!leader_hp->Initialized()) {
    LOG(INFO) << Substitute("No leader master found from master $0", local_uuid);
    *needs_retry = true;
    return Status::OK();
  }
  // Ensure the Raft configs from each master match. If not, presumably it's
  // transient and should be retried.
  auto& raft_config_uuids = each_remote_masters_master_uuids[0];
  for (int i = 1; i < each_remote_masters_master_uuids.size(); i++) {
    const auto& cur_uuids = each_remote_masters_master_uuids[i];
    if (cur_uuids != raft_config_uuids) {
      set<string> set_diff;
      STLSetDifference(cur_uuids, raft_config_uuids, &set_diff);
      LOG(INFO) << Substitute("Remote masters have differing Raft configurations:"
                              "[$0] vs [$1] (diff: [$2])", JoinStrings(cur_uuids, ","),
                              JoinStrings(raft_config_uuids, ","), JoinStrings(set_diff, ","));
      *needs_retry = true;
      return Status::OK();
    }
  }
  // Ensure that if we need to add this master to the Raft config, it's the
  // only one we need to add.
  if (!ContainsKey(raft_config_uuids, local_uuid)) {
    EmplaceIfNotPresent(&raft_config_uuids, local_uuid);
    if (raft_config_uuids != fetched_uuids) {
      set<string> set_diff;
      STLSetDifference(fetched_uuids, raft_config_uuids, &set_diff);
      return Status::NotSupported(Substitute("Kudu only supports adding one master at a time; "
          "tentative Raft config doesn't match the UUIDs fetched from --master_addresses. "
          "Raft config + local UUID: [$0] vs fetched UUIDs: [$1], diff: [$2]",
          JoinStrings(raft_config_uuids, ","), JoinStrings(fetched_uuids, ","),
          JoinStrings(set_diff, ",")));
    }
    *needs_add = true;
  } else {
    *needs_add = false;
  }
  *needs_retry = false;
  return Status::OK();
}

// Deletes the local system catalog tablet and performs a copy from 'src_hp'.
Status ClearLocalSystemCatalogAndCopy(const HostPort& src_hp) {
  LOG(INFO) << "Clearing existing system tablet";
  FsManager fs_manager(Env::Default(), FsManagerOpts());
  RETURN_NOT_OK(fs_manager.Open());
  scoped_refptr<ConsensusMetadataManager> cmeta_manager(
      new ConsensusMetadataManager(&fs_manager));
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(TabletMetadata::Load(&fs_manager, SysCatalogTable::kSysCatalogTabletId, &meta));
  RETURN_NOT_OK(TSTabletManager::DeleteTabletData(
      meta, cmeta_manager, TabletDataState::TABLET_DATA_DELETED,
      /*last_logged_opid*/boost::none));
  LOG(INFO) << "Copying system tablet from " << src_hp.ToString();
  std::shared_ptr<rpc::Messenger> messenger;
  RETURN_NOT_OK(rpc::MessengerBuilder("tablet_copy_client").Build(&messenger));
  RemoteTabletCopyClient copy_client(SysCatalogTable::kSysCatalogTabletId,
                                     &fs_manager, cmeta_manager,
                                     messenger, nullptr /* no metrics */);
  RETURN_NOT_OK(copy_client.Start(src_hp, /*meta*/nullptr));
  RETURN_NOT_OK(copy_client.FetchAll(/*tablet_replica*/nullptr));
  return copy_client.Finish();
}

} // anonymous namespace

void SetMasterFlagDefaults() {
  constexpr int32_t kDefaultRpcServiceQueueLength = 100;

  // Reset some default values before parsing gflags.
  CHECK_NE("", SetCommandLineOptionWithMode(
      "rpc_bind_addresses",
      strings::Substitute("0.0.0.0:$0", Master::kDefaultPort).c_str(),
      SET_FLAGS_DEFAULT));
  CHECK_NE("", SetCommandLineOptionWithMode(
      "webserver_port",
      to_string(Master::kDefaultWebPort).c_str(),
      SET_FLAGS_DEFAULT));
  // Even in a small Kudu cluster, masters might be flooded with requests coming
  // from many clients (those like GetTableSchema are rather small and can be
  // processed fast, but it might be a bunch of them coming at once).
  // In addition, TSHeartbeatRequestPB from tablet servers are put into the same
  // RPC queue (see KUDU-2955). So, it makes sense to increase the default
  // setting for the RPC service queue length.
  CHECK_NE("", SetCommandLineOptionWithMode(
      "rpc_service_queue_length",
      to_string(kDefaultRpcServiceQueueLength).c_str(),
      SET_FLAGS_DEFAULT));
  // Setting the default value of the 'force_block_cache_capacity' flag to
  // 'false' makes the corresponding group validator enforce proper settings
  // for the memory limit and the cfile cache capacity.
  CHECK_NE("", SetCommandLineOptionWithMode("force_block_cache_capacity",
                                            "false",
                                            SET_FLAGS_DEFAULT));
  // A multi-node Master leader should not evict failed Master followers
  // because there is no-one to assign replacement servers in order to maintain
  // the desired replication factor. (It's not turtles all the way down!)
  CHECK_NE("", SetCommandLineOptionWithMode("evict_failed_followers",
                                            "false",
                                            SET_FLAGS_DEFAULT));
  // SET_FLAGS_DEFAULT won't reset the flag value if it has previously been
  // set, instead it will only change the default. Because we want to ensure
  // evict_failed_followers is always false, we explicitly set the flag.
  FLAGS_evict_failed_followers = false;
}

Status RunMasterServer() {
  string nondefault_flags = GetNonDefaultFlags();
  LOG(INFO) << "Master server non-default flags:\n"
            << nondefault_flags << '\n'
            << "Master server version:\n"
            << VersionInfo::GetAllVersionInfo();

  RETURN_NOT_OK(ValidateHiveMetastoreSaslEnabled());

  MasterOptions opts;
  unique_ptr<Master> server(new Master(opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->Start());

  const auto local_uuid = server->fs_manager()->uuid();
  while (FLAGS_master_auto_join_cluster) {
    if (!opts.IsDistributed()) {
      // We definitely don't have to add a master if there's only one master
      // specified.
      break;
    }
    // Keep checking to see if the other masters think we are a part of their
    // Raft group. If so, we don't need to do anything.
    bool try_again = false;
    bool needs_add = false;
    HostPort leader_hp;
    HostPort local_hp;
    RETURN_NOT_OK(VerifyMastersGetHostPorts(opts.master_addresses(),
                                            local_uuid, server->messenger(),
                                            &leader_hp, &local_hp,
                                            &try_again, &needs_add));

    // Something went wrong. Sleep for a bit and try later.
    if (try_again) {
      LOG(INFO) << "Couldn't verify the masters in the cluster. Trying again...";
      SleepFor(MonoDelta::FromSeconds(FLAGS_master_auto_join_retry_interval_secs));
      continue;
    }
    if (!needs_add) {
      // All the other masters think this master is a part of the quorum.
      // There's nothing left to do but run!
      break;
    }
    // The other masters don't see this master in their quorum. Add it now.
    LOG(INFO) << Substitute(
        "Detected that this master $0 is joining an existing cluster", local_uuid);

    // Send an add master RPC to the leader master.
    LOG(INFO) << Substitute("Initiating AddMaster RPC to add $0", local_hp.ToString());
    vector<string> master_addrs;
    for (const auto& hp : opts.master_addresses()) {
      master_addrs.emplace_back(hp.ToString());
    }
    client::sp::shared_ptr<KuduClient> client;
    RETURN_NOT_OK(KuduClientBuilder()
        .master_server_addrs(master_addrs)
        .Build(&client));
    AddMasterRequestPB add_req;
    *add_req.mutable_rpc_addr() = HostPortToPB(local_hp);
    AddMasterResponsePB add_resp;;
    Synchronizer sync;
    client::internal::AsyncLeaderMasterRpc<AddMasterRequestPB, AddMasterResponsePB> add_rpc(
        MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_master_auto_join_rpc_timeout_secs),
        client.get(), rpc::BackoffType::LINEAR,
        add_req, &add_resp, &MasterServiceProxy::AddMasterAsync,
        "AddMaster", sync.AsStatusCallback(), {MasterFeatures::DYNAMIC_MULTI_MASTER});
    add_rpc.SendRpc();
    Status s = sync.Wait();
    bool master_already_present =
        add_resp.has_error() &&
        add_resp.error().code() == master::MasterErrorPB::MASTER_ALREADY_PRESENT;
    if (!s.ok() && !master_already_present) {
      RETURN_NOT_OK_PREPEND(s, "Failed to perform AddMaster RPC");
    }
    server->Shutdown();

    // If we succeeded, wipe the system catalog on this node and initiate a
    // copy from another node.
    RETURN_NOT_OK(ClearLocalSystemCatalogAndCopy(leader_hp));
    server.reset(new Master(opts));
    RETURN_NOT_OK(server->Init());
    RETURN_NOT_OK(server->Start());
    break;
  }

  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }
}

} // namespace master
} // namespace kudu
