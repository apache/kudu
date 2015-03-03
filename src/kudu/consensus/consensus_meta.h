// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CONSENSUS_CONSENSUS_META_H_
#define KUDU_CONSENSUS_CONSENSUS_META_H_

#include <stdint.h>
#include <string>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace consensus {

// Provides methods to read, write, and persist consensus-related metadata.
// This partly corresponds to Raft Figure 2's "Persistent state on all servers".
//
// In addition to the persistent state, this class also provides access to some
// transient state. This includes the peer that this node considers to be the
// leader of the quorum, as well as the "pending" quorum, if any.
//
// Conceptually, a pending quorum is one that has been proposed via a config
// change operation (AddServer or RemoveServer from Chapter 4 of Diego Ongaro's
// Raft thesis) but has not yet been committed. According to the above spec,
// as soon as a server hears of a new cluster membership configuration, it must
// be adopted (even prior to be committed).
//
// The data structure difference between a committed quorum and a pending one
// is that opid_index (the index in the log of the committed config change
// operation) is always set in a committed quorum, while it is always unset in
// a pending quorum.
//
// Finally, this class exposes the concept of an "active" quorum, which means
// the pending quorum if a pending quorum is set, otherwise the committed
// quorum.
//
// This class is not thread-safe and requires external synchronization.
class ConsensusMetadata {
 public:
  enum ConfigType {
    ACTIVE,   // Active quorum (pending or committed).
    COMMITTED // Committed quorum.
  };

  // Create a ConsensusMetadata object with provided initial state.
  // Encoded PB is flushed to disk before returning.
  static Status Create(FsManager* fs_manager,
                       const std::string& tablet_id,
                       const std::string& peer_uuid,
                       QuorumPB& quorum,
                       int64_t current_term,
                       gscoped_ptr<ConsensusMetadata>* cmeta);

  // Load a ConsensusMetadata object from disk.
  static Status Load(FsManager* fs_manager,
                     const std::string& tablet_id,
                     const std::string& peer_uuid,
                     gscoped_ptr<ConsensusMetadata>* cmeta);

  // Accessors for current term.
  const int64_t current_term() const;
  void set_current_term(int64_t term);

  // Accessors for voted_for.
  bool has_voted_for() const;
  const std::string& voted_for() const;
  void clear_voted_for();
  void set_voted_for(const std::string& uuid);

  // Accessors for committed quorum.
  const QuorumPB& committed_quorum() const;
  void set_committed_quorum(const QuorumPB& quorum);

  // Returns whether a pending quorum is set.
  bool has_pending_quorum() const;

  // Returns the pending quorum if one is set. Otherwise, fires a DCHECK.
  const QuorumPB& pending_quorum() const;

  // Set & clear the pending quorum.
  void clear_pending_quorum();
  void set_pending_quorum(const QuorumPB& quorum);

  // If a pending quorum is set, return it.
  // Otherwise, return the committed quorum.
  const QuorumPB& active_quorum() const;

  // Accessors for setting the active leader.
  const std::string& leader_uuid() const;
  void set_leader_uuid(const std::string& uuid);

  // Returns the currently active role of the current node.
  QuorumPeerPB::Role active_role() const;

  // Copy the stored state into a ConsensusStatePB object.
  // To get the active quorum, specify 'type' = ACTIVE.
  // Otherwise, 'type' = COMMITTED will return a version of the
  // ConsensusStatePB using only the committed quorum. In this case, if the
  // current leader is not a member of the committed quorum, then the
  // leader_uuid field of the returned ConsensusStatePB will be cleared.
  ConsensusStatePB ToConsensusStatePB(ConfigType type) const;

  // Persist current state of the protobuf to disk.
  Status Flush();

 private:
  ConsensusMetadata(FsManager* fs_manager,
                    const std::string& tablet_id,
                    const std::string& peer_uuid);

  std::string LogPrefix() const;

  // Updates the cached active role.
  void UpdateActiveRole();

  // Transient fields.
  // Constants:
  FsManager* const fs_manager_;
  const std::string tablet_id_;
  const std::string peer_uuid_;
  // Mutable:
  std::string leader_uuid_; // Leader of the current term (term == pb_.current_term).
  bool has_pending_quorum_; // Indicates whether there is an as-yet uncommitted
                            // quorum change pending.
  // Quorum used by the peers when there is a pending config change operation.
  QuorumPB pending_quorum_;

  // Cached role of the peer_uuid_ within the active quorum.
  QuorumPeerPB::Role active_role_;

  // Durable fields.
  ConsensusMetadataPB pb_;

  DISALLOW_COPY_AND_ASSIGN(ConsensusMetadata);
};

} // namespace consensus
} // namespace kudu

#endif // KUDU_CONSENSUS_CONSENSUS_META_H_
