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

#include "kudu/tserver/simple_tablet_manager.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/clock/clock.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"
#include "kudu/util/pb_util.h"


using std::set;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataCreateMode;
using consensus::ConsensusMetadataManager;
using consensus::ConsensusStatePB;
using consensus::ConsensusOptions;
using consensus::PeerProxyFactory;
using consensus::ConsensusRound;
using consensus::RpcPeerProxyFactory;
using consensus::EXCLUDE_HEALTH_REPORT;
using consensus::INCLUDE_HEALTH_REPORT;
using consensus::TimeManager;
using consensus::OpId;
using consensus::OpIdToString;
using consensus::RECEIVED_OPID;
using consensus::RaftConfigPB;
using consensus::RaftPeerPB;
using consensus::RaftConsensus;
using consensus::kMinimumTerm;
using fs::DataDirManager;
using log::Log;
using log::LogOptions;
using pb_util::SecureDebugString;
using pb_util::SecureShortDebugString;


namespace tserver {

const std::string TSTabletManager::kSysCatalogTabletId("00000000000000000000000000000000");

TSTabletManager::TSTabletManager(TabletServer* server)
  : fs_manager_(server->fs_manager()),
    cmeta_manager_(new ConsensusMetadataManager(fs_manager_)),
    server_(server),
    metric_registry_(server->metric_registry()),
    state_(MANAGER_INITIALIZING),
    mark_dirty_clbk_(Bind(&TSTabletManager::MarkTabletDirty, Unretained(this))) {
}

TSTabletManager::~TSTabletManager() {
}

Status TSTabletManager::Load(FsManager *fs_manager) {
  if (server_->opts().IsDistributed()) {
    LOG(INFO) << "Verifying existing consensus state";
    scoped_refptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK_PREPEND(cmeta_manager_->Load(kSysCatalogTabletId, &cmeta),
                          "Unable to load consensus metadata for tablet " + kSysCatalogTabletId);
    ConsensusStatePB cstate = cmeta->ToConsensusStatePB();
    RETURN_NOT_OK(consensus::VerifyRaftConfig(cstate.committed_config()));
    CHECK(!cstate.has_pending_config());

    // Make sure the set of masters passed in at start time matches the set in
    // the on-disk cmeta.
    set<string> peer_addrs_from_opts;
    for (const auto& hp : server_->opts().tserver_addresses) {
      peer_addrs_from_opts.insert(hp.ToString());
    }
    if (peer_addrs_from_opts.size() < server_->opts().tserver_addresses.size()) {
      LOG(WARNING) << Substitute("Found duplicates in --tserver_addresses: "
                                 "the unique set of addresses is $0",
                                 JoinStrings(peer_addrs_from_opts, ", "));
    }
    set<string> peer_addrs_from_disk;
    for (const auto& p : cstate.committed_config().peers()) {
      HostPort hp;
      RETURN_NOT_OK(HostPortFromPB(p.last_known_addr(), &hp));
      peer_addrs_from_disk.insert(hp.ToString());
    }
    vector<string> symm_diff;
    std::set_symmetric_difference(peer_addrs_from_opts.begin(),
                                  peer_addrs_from_opts.end(),
                                  peer_addrs_from_disk.begin(),
                                  peer_addrs_from_disk.end(),
                                  std::back_inserter(symm_diff));
    if (!symm_diff.empty()) {
      string msg = Substitute(
          "on-disk master list ($0) and provided master list ($1) differ. "
          "Their symmetric difference is: $2",
          JoinStrings(peer_addrs_from_disk, ", "),
          JoinStrings(peer_addrs_from_opts, ", "),
          JoinStrings(symm_diff, ", "));
      return Status::InvalidArgument(msg);
    }
  }

  return SetupRaft();
}

Status TSTabletManager::CreateNew(FsManager *fs_manager) {
  RaftConfigPB config;
  if (server_->opts().IsDistributed()) {
    RETURN_NOT_OK_PREPEND(CreateDistributedConfig(server_->opts(), &config),
                          "Failed to create new distributed Raft config");
  } else {
    config.set_obsolete_local(true);
    config.set_opid_index(consensus::kInvalidOpIdIndex);
    RaftPeerPB* peer = config.add_peers();
    peer->set_permanent_uuid(fs_manager->uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

  RETURN_NOT_OK_PREPEND(cmeta_manager_->Create(kSysCatalogTabletId, config, consensus::kMinimumTerm),
                        "Unable to persist consensus metadata for tablet " + kSysCatalogTabletId);

  return SetupRaft();
}

Status TSTabletManager::CreateDistributedConfig(const TabletServerOptions& options,
                                                RaftConfigPB* committed_config) {
  DCHECK(options.IsDistributed());

  RaftConfigPB new_config;
  new_config.set_obsolete_local(false);
  new_config.set_opid_index(consensus::kInvalidOpIdIndex);

  // Build the set of followers from our server options.
  for (const HostPort& host_port : options.tserver_addresses) {
    RaftPeerPB peer;
    HostPortPB peer_host_port_pb;
    RETURN_NOT_OK(HostPortToPB(host_port, &peer_host_port_pb));
    peer.mutable_last_known_addr()->CopyFrom(peer_host_port_pb);
    peer.set_member_type(RaftPeerPB::VOTER);
    new_config.add_peers()->CopyFrom(peer);
  }

  // Now resolve UUIDs.
  // By the time a SysCatalogTable is created and initted, the masters should be
  // starting up, so this should be fine to do.
  DCHECK(server_->messenger());
  RaftConfigPB resolved_config = new_config;
  resolved_config.clear_peers();
  for (const RaftPeerPB& peer : new_config.peers()) {
    if (peer.has_permanent_uuid()) {
      resolved_config.add_peers()->CopyFrom(peer);
    } else {
      LOG(INFO) << SecureShortDebugString(peer)
                << " has no permanent_uuid. Determining permanent_uuid...";
      RaftPeerPB new_peer = peer;
      RETURN_NOT_OK_PREPEND(consensus::SetPermanentUuidForRemotePeer(server_->messenger(),
                                                                     &new_peer),
                            Substitute("Unable to resolve UUID for peer $0",
                                       SecureShortDebugString(peer)));
      resolved_config.add_peers()->CopyFrom(new_peer);
    }
  }

  RETURN_NOT_OK(consensus::VerifyRaftConfig(resolved_config));
  VLOG(1) << "Distributed Raft configuration: " << SecureShortDebugString(resolved_config);

  *committed_config = resolved_config;
  return Status::OK();
}

Status TSTabletManager::WaitUntilConsensusRunning(const MonoDelta& timeout) {
  MonoTime start(MonoTime::Now());

  int backoff_exp = 0;
  const int kMaxBackoffExp = 8;
  while (true) {
    if (consensus_ && consensus_->IsRunning()) {
      break;
    }
    MonoTime now(MonoTime::Now());
    MonoDelta elapsed(now - start);
    if (elapsed > timeout) {
      return Status::TimedOut(Substitute("Raft Consensus is not running after waiting for $0:",
                                         elapsed.ToString()));
    }
    SleepFor(MonoDelta::FromMilliseconds(1L << backoff_exp));
    backoff_exp = std::min(backoff_exp + 1, kMaxBackoffExp);
  }
  return Status::OK();
}

Status TSTabletManager::WaitUntilRunning() {
  TRACE_EVENT0("master", "SysCatalogTable::WaitUntilRunning");
  int seconds_waited = 0;
  while (true) {
    Status status = WaitUntilConsensusRunning(MonoDelta::FromSeconds(1));
    seconds_waited++;
    if (status.ok()) {
      LOG_WITH_PREFIX(INFO) << "configured and running, proceeding with master startup.";
      break;
    }
    if (status.IsTimedOut()) {
      LOG_WITH_PREFIX(INFO) <<  "not online yet (have been trying for "
                               << seconds_waited << " seconds)";
      continue;
    }
    // if the status is not OK or TimedOut return it.
    return status;
  }
  return Status::OK();
}

bool TSTabletManager::IsInitialized() const {
  return state() == MANAGER_INITIALIZED;
}

bool TSTabletManager::IsRunning() const {
  return state() == MANAGER_RUNNING;
}

Status TSTabletManager::Init(bool is_first_run) {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  if (is_first_run) {
    RETURN_NOT_OK_PREPEND(
        CreateNew(server_->fs_manager()),
        "Failed to CreateNew in TabletManager");
  } else {
    RETURN_NOT_OK_PREPEND(
        Load(server_->fs_manager()),
        "Failed to Load in TabletManager");
  }

  set_state(MANAGER_INITIALIZED);
  return Status::OK();
}

Status TSTabletManager::Start() {
  CHECK_EQ(state(), MANAGER_INITIALIZED);

  // set_state(INITIALIZED);
  // SetStatusMessage("Initialized. Waiting to start...");

  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->Load(kSysCatalogTabletId, &cmeta);

  consensus::ConsensusBootstrapInfo bootstrap_info;

  TRACE("Starting consensus");
  VLOG(2) << "T " << kSysCatalogTabletId << " P " << consensus_->peer_uuid() << ": Peer starting";
  VLOG(2) << "RaftConfig before starting: " << SecureDebugString(consensus_->CommittedConfig());

  gscoped_ptr<PeerProxyFactory> peer_proxy_factory;
  scoped_refptr<TimeManager> time_manager;

  peer_proxy_factory.reset(new RpcPeerProxyFactory(server_->messenger()));
  // THIS IS OBVIOUSLY NOT CORRECT.
  // ONLY TO MAKE CODE COMPILE [ Anirban ]
  time_manager.reset(new TimeManager(server_->clock(), Timestamp::kInitialTimestamp));
  //time_manager.reset(new TimeManager(server_->clock(), tablet_->mvcc_manager()->GetCleanTimestamp()));

  // We cannot hold 'lock_' while we call RaftConsensus::Start() because it
  // may invoke TabletReplica::StartFollowerTransaction() during startup,
  // causing a self-deadlock. We take a ref to members protected by 'lock_'
  // before unlocking.
  RETURN_NOT_OK(consensus_->Start(
        bootstrap_info, std::move(peer_proxy_factory),
        log_, std::move(time_manager),
        this, server_->metric_entity(), mark_dirty_clbk_));

  RETURN_NOT_OK_PREPEND(WaitUntilRunning(),
                        "Failed waiting for the raft to run");

  set_state(MANAGER_RUNNING);
  return Status::OK();
}

Status TSTabletManager::SetupRaft() {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  InitLocalRaftPeerPB();

  ConsensusOptions options;
  options.tablet_id = kSysCatalogTabletId;
  shared_ptr<RaftConsensus> consensus;
  TRACE("Creating consensus");
  LOG(INFO) << LogPrefix(kSysCatalogTabletId) << "Creating Raft for the system tablet";
  RETURN_NOT_OK(RaftConsensus::Create(std::move(options),
                                      local_peer_pb_,
                                      cmeta_manager_,
                                      server_->raft_pool(),
                                      &consensus));
  consensus_ = std::move(consensus);
  // set_state(INITIALIZED);
  // SetStatusMessage("Initialized. Waiting to start...");

  // Not sure these 2 lines are required
  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->Load(kSysCatalogTabletId, &cmeta);

  // Open the log, while passing in the factory class.
  // Factory could be empty.
  LogOptions log_options;
  log_options.log_factory = server_->opts().log_factory;
  return Log::Open(log_options, fs_manager_, kSysCatalogTabletId,
      server_->metric_entity(), &log_);
}

void TSTabletManager::Shutdown() {
  {
    std::lock_guard<RWMutex> lock(lock_);
    switch (state_) {
      case MANAGER_QUIESCING: {
        VLOG(1) << "Tablet manager shut down already in progress..";
        return;
      }
      case MANAGER_SHUTDOWN: {
        VLOG(1) << "Tablet manager has already been shut down.";
        return;
      }
      case MANAGER_INITIALIZING:
      case MANAGER_RUNNING: {
        LOG(INFO) << "Shutting down tablet manager...";
        state_ = MANAGER_QUIESCING;
        break;
      }
      default: {
        LOG(FATAL) << "Invalid state: " << TSTabletManagerStatePB_Name(state_);
      }
    }
  }

  if (consensus_) consensus_->Shutdown();

  state_ = MANAGER_SHUTDOWN;
}

const NodeInstancePB& TSTabletManager::NodeInstance() const {
  return server_->instance_pb();
}

void TSTabletManager::InitLocalRaftPeerPB() {
  DCHECK_EQ(state(), MANAGER_INITIALIZING);
  local_peer_pb_.set_permanent_uuid(fs_manager_->uuid());
  Sockaddr addr = server_->first_rpc_address();
  HostPort hp;
  CHECK_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  CHECK_OK(HostPortToPB(hp, local_peer_pb_.mutable_last_known_addr()));
}

string TSTabletManager::LogPrefix(const string& tablet_id, FsManager *fs_manager) {
  DCHECK(fs_manager != nullptr);
  return Substitute("T $0 P $1: ", tablet_id, fs_manager->uuid());
}

string TSTabletManager::LogPrefix() const {
  return LogPrefix(kSysCatalogTabletId);
}

Status TSTabletManager::StartFollowerTransaction(const scoped_refptr<ConsensusRound>& round) {
  // THIS IS CURRENTLY A NO-OP
  consensus::ReplicateMsg* replicate_msg = round->replicate_msg();
  DCHECK(replicate_msg->has_timestamp());
  return Status::OK();
}

void TSTabletManager::FinishConsensusOnlyRound(ConsensusRound* round) {
  consensus::ReplicateMsg* replicate_msg = round->replicate_msg();
  consensus::OperationType op_type = replicate_msg->op_type();
  (void)op_type;
  (void)replicate_msg;
}
} // namespace tserver
} // namespace kudu
