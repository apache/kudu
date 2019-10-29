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

#include <atomic>
#include <cstdint>
#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/threading/thread_collision_warner.h"

namespace kudu {

class FsManager;
class Status;

namespace consensus {

class ConsensusMetadataManager; // IWYU pragma: keep
class ConsensusMetadataTest;    // IWYU pragma: keep

enum class ConsensusMetadataCreateMode {
  FLUSH_ON_CREATE,
  NO_FLUSH_ON_CREATE,
};

// Provides methods to read, write, and persist consensus-related metadata.
// This partly corresponds to Raft Figure 2's "Persistent state on all servers".
//
// In addition to the persistent state, this class also provides access to some
// transient state. This includes the peer that this node considers to be the
// leader of the configuration, as well as the "pending" configuration, if any.
//
// Conceptually, a pending configuration is one that has been proposed via a config
// change operation (AddServer or RemoveServer from Chapter 4 of Diego Ongaro's
// Raft thesis) but has not yet been committed. According to the above spec,
// as soon as a server hears of a new cluster membership configuration, it must
// be adopted (even prior to be committed).
//
// The data structure difference between a committed configuration and a pending one
// is that opid_index (the index in the log of the committed config change
// operation) is always set in a committed configuration, while it is always unset in
// a pending configuration.
//
// Finally, this class exposes the concept of an "active" configuration, which means
// the pending configuration if a pending configuration is set, otherwise the committed
// configuration.
//
// This class is not thread-safe and requires external synchronization.
class ConsensusMetadata : public RefCountedThreadSafe<ConsensusMetadata> {
 public:

  // Specify whether we are allowed to overwrite an existing file when flushing.
  enum FlushMode {
    OVERWRITE,
    NO_OVERWRITE
  };

  // Accessors for current term.
  int64_t current_term() const;
  void set_current_term(int64_t term);

  // Accessors for voted_for.
  bool has_voted_for() const;
  const std::string& voted_for() const;
  void clear_voted_for();
  void set_voted_for(const std::string& uuid);

  // Returns true iff peer with specified uuid is a voter in the specified
  // local Raft config.
  bool IsVoterInConfig(const std::string& uuid, RaftConfigState type);

  // Returns true iff peer with specified uuid is a member of the specified
  // local Raft config.
  bool IsMemberInConfig(const std::string& uuid, RaftConfigState type);

  // Returns a count of the number of voters in the specified local Raft
  // config.
  int CountVotersInConfig(RaftConfigState type);

  // Returns the opid_index of the specified local Raft config.
  int64_t GetConfigOpIdIndex(RaftConfigState type);

  // Accessors for committed configuration.
  const RaftConfigPB& CommittedConfig() const;
  void set_committed_config(const RaftConfigPB& config);

  // Returns whether a pending configuration is set.
  bool has_pending_config() const;

  // Returns the pending configuration if one is set. Otherwise, fires a DCHECK.
  const RaftConfigPB& PendingConfig() const;

  // Set & clear the pending configuration.
  void clear_pending_config();
  void set_pending_config(const RaftConfigPB& config);

  // If a pending configuration is set, return it.
  // Otherwise, return the committed configuration.
  const RaftConfigPB& ActiveConfig() const;

  // Accessors for setting the active leader.
  const std::string& leader_uuid() const;
  void set_leader_uuid(std::string uuid);

  // Returns the currently active role of the current node.
  RaftPeerPB::Role active_role() const;

  // Copy the stored state into a ConsensusStatePB object.
  // To get the active configuration, specify 'type' = ACTIVE.
  // Otherwise, 'type' = COMMITTED will return a version of the
  // ConsensusStatePB using only the committed configuration. In this case, if the
  // current leader is not a member of the committed configuration, then the
  // leader_uuid field of the returned ConsensusStatePB will be cleared.
  ConsensusStatePB ToConsensusStatePB() const;

  // Merge the committed portion of the consensus state from the source node
  // during tablet copy.
  //
  // This method will clear any pending config change, replace the committed
  // consensus config with the one in 'cstate', and clear the currently
  // tracked leader.
  //
  // It will also check whether the current term passed in 'cstate'
  // is greater than the currently recorded one. If so, it will update the
  // local current term to match the passed one and it will clear the voting
  // record for this node. If the current term in 'cstate' is less
  // than the locally recorded term, the locally recorded term and voting
  // record are not changed.
  void MergeCommittedConsensusStatePB(const ConsensusStatePB& cstate);

  // Persist current state of the protobuf to disk.
  Status Flush(FlushMode flush_mode = OVERWRITE);

  int64_t flush_count_for_tests() const {
    return flush_count_for_tests_;
  }

  // The on-disk size of the consensus metadata, as of the last call to
  // Load() or Flush(). This method is thread-safe.
  int64_t on_disk_size() const {
    return on_disk_size_.load(std::memory_order_relaxed);
  }

 private:
  friend class RaftConsensusQuorumTest;
  friend class RefCountedThreadSafe<ConsensusMetadata>;
  friend class ConsensusMetadataManager;

  FRIEND_TEST(ConsensusMetadataTest, TestCreateLoad);
  FRIEND_TEST(ConsensusMetadataTest, TestDeferredCreateLoad);
  FRIEND_TEST(ConsensusMetadataTest, TestCreateNoOverwrite);
  FRIEND_TEST(ConsensusMetadataTest, TestFailedLoad);
  FRIEND_TEST(ConsensusMetadataTest, TestFlush);
  FRIEND_TEST(ConsensusMetadataTest, TestActiveRole);
  FRIEND_TEST(ConsensusMetadataTest, TestToConsensusStatePB);
  FRIEND_TEST(ConsensusMetadataTest, TestMergeCommittedConsensusStatePB);

  ConsensusMetadata(FsManager* fs_manager, std::string tablet_id,
                    std::string peer_uuid);

  // Create a ConsensusMetadata object with provided initial state.
  // If 'create_mode' is set to FLUSH_ON_CREATE, the encoded PB is flushed to
  // disk before returning. Otherwise, if 'create_mode' is set to
  // NO_FLUSH_ON_CREATE, the caller must explicitly call Flush() on the
  // returned object to get the bytes onto disk.
  static Status Create(FsManager* fs_manager,
                       const std::string& tablet_id,
                       const std::string& peer_uuid,
                       const RaftConfigPB& config,
                       int64_t current_term,
                       ConsensusMetadataCreateMode create_mode =
                           ConsensusMetadataCreateMode::FLUSH_ON_CREATE,
                       scoped_refptr<ConsensusMetadata>* cmeta_out = nullptr);

  // Load a ConsensusMetadata object from disk.
  // Returns Status::NotFound if the file could not be found. May return other
  // Status codes if unable to read the file.
  static Status Load(FsManager* fs_manager,
                     const std::string& tablet_id,
                     const std::string& peer_uuid,
                     scoped_refptr<ConsensusMetadata>* cmeta_out = nullptr);

  // Delete the ConsensusMetadata file associated with the given tablet from
  // disk. Returns Status::NotFound if the on-disk data is not found.
  static Status DeleteOnDiskData(FsManager* fs_manager, const std::string& tablet_id);

  // Return the specified config.
  const RaftConfigPB& GetConfig(RaftConfigState type) const;

  std::string LogPrefix() const;

  // Updates the cached active role.
  void UpdateActiveRole();

  // Updates the cached on-disk size of the consensus metadata.
  Status UpdateOnDiskSize();

  FsManager* const fs_manager_;
  const std::string tablet_id_;
  const std::string peer_uuid_;

  // This fake mutex helps ensure that this ConsensusMetadata object stays
  // externally synchronized.
  DFAKE_MUTEX(fake_lock_);

  std::string leader_uuid_; // Leader of the current term (term == pb_.current_term).
  bool has_pending_config_; // Indicates whether there is an as-yet uncommitted
                            // configuration change pending.
  // RaftConfig used by the peers when there is a pending config change operation.
  RaftConfigPB pending_config_;

  // Cached role of the peer_uuid_ within the active configuration.
  RaftPeerPB::Role active_role_;

  // The number of times the metadata has been flushed to disk.
  int64_t flush_count_for_tests_;

  // Durable fields.
  ConsensusMetadataPB pb_;

  // The on-disk size of the consensus metadata, as of the last call to
  // Load() or Flush().
  // The type is int64_t for consistency with other on-disk size metrics,
  // as opposed to uint64_t, which is the return type of the underlying function
  // used to populate this value.
  std::atomic<int64_t> on_disk_size_;

  DISALLOW_COPY_AND_ASSIGN(ConsensusMetadata);
};

} // namespace consensus
} // namespace kudu
