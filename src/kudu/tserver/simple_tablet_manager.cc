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
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/consensus/log.h"
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
using pb_util::SecureDebugString;


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

Status TSTabletManager::Init() {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  InitLocalRaftPeerPB();

  LOG(INFO) << LogPrefix(kSysCatalogTabletId) << "Bootstrapping tablet";
  TRACE("Bootstrapping tablet");

  // Raft config is necessary for initializing the master tablet.
  // In order to get around the peers' count restriction, I have created a dummy
  // peer which is a NON_VOTER so it shouldn't harm us.
  RaftConfigPB config;
  config.set_opid_index(consensus::kInvalidOpIdIndex);
  RaftPeerPB* peer = config.add_peers();
  peer->set_permanent_uuid("dummy_peer");
  peer->set_member_type(RaftPeerPB::NON_VOTER);
  RETURN_NOT_OK_PREPEND(
    cmeta_manager_->Create(kSysCatalogTabletId, config, kMinimumTerm),
    "Unable to create new ConsensusMetadata for tablet " + kSysCatalogTabletId);

  ConsensusOptions options;
  options.tablet_id = kSysCatalogTabletId;
  shared_ptr<RaftConsensus> consensus;
  RETURN_NOT_OK(RaftConsensus::Create(std::move(options),
                                      local_peer_pb_,
                                      cmeta_manager_,
                                      server_->raft_pool(),
                                      &consensus));
  consensus_ = std::move(consensus);

  // set_state(INITIALIZED);
  // SetStatusMessage("Initialized. Waiting to start...");

  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->Load(kSysCatalogTabletId, &cmeta);

  consensus::ConsensusBootstrapInfo bootstrap_info;

  TRACE("Starting consensus");
  VLOG(2) << "T " << kSysCatalogTabletId << " P " << consensus_->peer_uuid() << ": Peer starting";
  VLOG(2) << "RaftConfig before starting: " << SecureDebugString(consensus_->CommittedConfig());

  scoped_refptr<Log> log;
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
        log, std::move(time_manager),
        this, server_->metric_entity(), mark_dirty_clbk_));
  set_state(MANAGER_RUNNING);

  return Status::OK();
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
