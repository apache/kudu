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

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace boost {
template <class T>
class optional;
}

namespace kudu {

class FsManager;
class NodeInstancePB;
class ThreadPool;
class MonoDelta;

namespace log {

class Log;
}

namespace consensus {
class ConsensusMetadataManager;
class OpId;
struct ElectionResult;
} // namespace consensus

namespace tserver {
class TabletServer;
struct TabletServerOptions;

// Keeps track of the tablets hosted on the tablet server side.
//
// TODO(todd): will also be responsible for keeping the local metadata about which
// tablets are hosted on this server persistent on disk, as well as re-opening all
// the tablets at startup, etc.
class TSTabletManager : public consensus::ConsensusRoundHandler {
 public:
  // Construct the tablet manager.
  explicit TSTabletManager(TabletServer* server);

  static const std::string kSysCatalogTabletId;

  virtual ~TSTabletManager();

  Status Load(FsManager *fs_manager);

  // Load all tablet metadata blocks from disk, and open their respective tablets.
  // Upon return of this method all existing tablets are registered, but
  // the bootstrap is performed asynchronously.
  Status Init(bool is_first_run);

  Status Start();

  bool IsInitialized() const;

  bool IsRunning() const;

  // Shut down all of the tablets, gracefully flushing before shutdown.
  void Shutdown();

  virtual const NodeInstancePB& NodeInstance() const;

  // Used by consensus to create and start a new ReplicaTransaction.
  virtual Status StartFollowerTransaction(
      const scoped_refptr<consensus::ConsensusRound>& round) override;

  // Used by consensus to notify the tablet replica that a consensus-only round
  // has finished, advancing MVCC safe time as appropriate.
  virtual void FinishConsensusOnlyRound(consensus::ConsensusRound* round) override;

  virtual Status StartConsensusOnlyRound(
      const scoped_refptr<consensus::ConsensusRound>& round) override;

  std::shared_ptr<consensus::RaftConsensus> shared_consensus() const {
    shared_lock<RWMutex> l(lock_);
    return consensus_;
  }

  consensus::RaftConsensus* consensus() {
    shared_lock<RWMutex> l(lock_);
    return consensus_.get();
  }

  // Marks the tablet as dirty so that it's included in the next heartbeat.
  void MarkTabletDirty(const std::string& reason) {
  }

 private:
  // Standard log prefix, given a tablet id.
  static std::string LogPrefix(const std::string& tablet_id, FsManager *fs_manager);
  std::string LogPrefix(const std::string& tablet_id) const {
    return LogPrefix(tablet_id, fs_manager_);
  }

  std::string LogPrefix() const;

  TSTabletManagerStatePB state() const {
    shared_lock<RWMutex> l(lock_);
    return state_;
  }

  void set_state(TSTabletManagerStatePB s) {
    std::lock_guard<RWMutex> lock(lock_);
    state_ = s;
  }

  // waits in a loop to check that consensus has started to run
  Status WaitUntilRunning();

  // Wait for consensus to start running
  Status WaitUntilConsensusRunning(const MonoDelta& timeout);

  // Create either a standalone or distributed config
  Status CreateNew(FsManager *fs_manager);

  // Helper function to create Raft consensus and log
  // Consensus is yet to be started at the end of this
  // call.
  Status SetupRaft();

  // Initializes the RaftPeerPB for the local peer.
  // Guaranteed to include both uuid and last_seen_addr fields.
  // Crashes with an invariant check if the RPC server is not currently in a
  // running state.
  void InitLocalRaftPeerPB();

  // Use the master options to generate a new consensus configuration.
  // In addition, resolve all UUIDs of this consensus configuration.
  Status CreateDistributedConfig(const TabletServerOptions& options,
                                 consensus::RaftConfigPB* committed_config);

  FsManager* const fs_manager_;

  const scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  // Kudu log, which was created by the passed in
  // factory entity
  scoped_refptr<kudu::log::Log> log_;

  TabletServer* server_;

  MetricRegistry* metric_registry_;

  consensus::RaftPeerPB local_peer_pb_;

  // Lock protecting tablet_map_, dirty_tablets_, state_,
  // transition_in_progress_, perm_deleted_tablet_ids_,
  // tablet_state_counts_, and last_walked_.
  mutable RWMutex lock_;

  TSTabletManagerStatePB state_;

  // Function to mark this TabletReplica's tablet as dirty in the TSTabletManager.
  //
  // Must be called whenever cluster membership or leadership changes, or when
  // the tablet's schema changes.
  const Callback<void(const std::string& reason)> mark_dirty_clbk_;

  std::shared_ptr<consensus::RaftConsensus> consensus_;

  DISALLOW_COPY_AND_ASSIGN(TSTabletManager);
};

} // namespace tserver
} // namespace kudu
