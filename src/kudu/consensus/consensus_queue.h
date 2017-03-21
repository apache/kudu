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

#ifndef KUDU_CONSENSUS_CONSENSUS_QUEUE_H_
#define KUDU_CONSENSUS_CONSENSUS_QUEUE_H_

#include <boost/optional.hpp>
#include <iosfwd>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log_cache.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"

namespace kudu {
template<class T>
class AtomicGauge;
class MemTracker;
class MetricEntity;
class ThreadPool;

namespace log {
class Log;
}

namespace consensus {
class PeerMessageQueueObserver;
class TimeManager;

// The id for the server-wide consensus queue MemTracker.
extern const char kConsensusQueueParentTrackerId[];

// Tracks the state of the peers and which transactions they have replicated.
// Owns the LogCache which actually holds the replicate messages which are
// en route to the various peers.
//
// This also takes care of pushing requests to peers as new operations are
// added, and notifying RaftConsensus when the commit index advances.
//
// TODO(todd): Right now this class is able to track one outstanding operation
// per peer. If we want to have more than one outstanding RPC we need to
// modify it.
class PeerMessageQueue {
 public:
  struct TrackedPeer {
    explicit TrackedPeer(std::string uuid)
        : uuid(std::move(uuid)),
          is_new(true),
          next_index(kInvalidOpIdIndex),
          last_received(MinimumOpId()),
          last_known_committed_index(MinimumOpId().index()),
          is_last_exchange_successful(false),
          last_successful_communication_time(MonoTime::Now()),
          needs_tablet_copy(false),
          last_seen_term_(0) {}

    TrackedPeer() = default;

    // Copy a given TrackedPeer.
    TrackedPeer& operator=(const TrackedPeer& tracked_peer) = default;

    // Check that the terms seen from a given peer only increase
    // monotonically.
    void CheckMonotonicTerms(int64_t term) {
      DCHECK_GE(term, last_seen_term_);
      last_seen_term_ = term;
    }

    std::string ToString() const;

    // UUID of the peer.
    std::string uuid;

    // Whether this is a newly tracked peer.
    bool is_new;

    // Next index to send to the peer.
    // This corresponds to "nextIndex" as specified in Raft.
    int64_t next_index;

    // The last operation that we've sent to this peer and that
    // it acked. Used for watermark movement.
    OpId last_received;

    // The last committed index this peer knows about.
    int64_t last_known_committed_index;

    // Whether the last exchange with this peer was successful.
    bool is_last_exchange_successful;

    // The time of the last communication with the peer.
    // Defaults to the time of construction, so does not necessarily mean that
    // successful communication ever took place.
    MonoTime last_successful_communication_time;

    // Whether the follower was detected to need tablet copy.
    bool needs_tablet_copy;

    // Throttler for how often we will log status messages pertaining to this
    // peer (eg when it is lagging, etc).
    logging::LogThrottler status_log_throttler;

   private:
    // The last term we saw from a given peer.
    // This is only used for sanity checking that a peer doesn't
    // go backwards in time.
    int64_t last_seen_term_;
  };

  PeerMessageQueue(const scoped_refptr<MetricEntity>& metric_entity,
                   const scoped_refptr<log::Log>& log,
                   scoped_refptr<TimeManager> time_manager,
                   const RaftPeerPB& local_peer_pb,
                   const std::string& tablet_id);

  // Initialize the queue.
  void Init(const OpId& last_locally_replicated,
            const OpId& last_locally_committed);

  // Changes the queue to leader mode, meaning it tracks majority replicated
  // operations and notifies observers when those change.
  // 'committed_index' corresponds to the id of the last committed operation,
  // i.e. operations with ids <= 'committed_index' should be considered committed.
  //
  // 'current_term' corresponds to the leader's current term, this is different
  // from 'committed_index.term()' if the leader has not yet committed an
  // operation in the current term.
  // 'active_config' is the currently-active Raft config. This must always be
  // a superset of the tracked peers, and that is enforced with runtime CHECKs.
  void SetLeaderMode(int64_t committed_index,
                     int64_t current_term,
                     const RaftConfigPB& active_config);

  // Changes the queue to non-leader mode. Currently tracked peers will still
  // be tracked so that the cache is only evicted when the peers no longer need
  // the operations but the queue will no longer advance the majority replicated
  // index or notify observers of its advancement.
  void SetNonLeaderMode();

  // Makes the queue track this peer.
  void TrackPeer(const std::string& uuid);

  // Makes the queue untrack this peer.
  void UntrackPeer(const std::string& uuid);

  // Appends a single message to be replicated to the peers.
  // Returns OK unless the message could not be added to the queue for some
  // reason (e.g. the queue reached max size).
  // If it returns OK the queue takes ownership of 'msg'.
  //
  // This is thread-safe against all of the read methods, but not thread-safe
  // with concurrent Append calls.
  Status AppendOperation(const ReplicateRefPtr& msg);

  // Appends a vector of messages to be replicated to the peers.
  // Returns OK unless the message could not be added to the queue for some
  // reason (e.g. the queue reached max size), calls 'log_append_callback' when
  // the messages are durable in the local Log.
  // If it returns OK the queue takes ownership of 'msgs'.
  //
  // This is thread-safe against all of the read methods, but not thread-safe
  // with concurrent Append calls.
  Status AppendOperations(const std::vector<ReplicateRefPtr>& msgs,
                          const StatusCallback& log_append_callback);

  // Truncate all operations coming after 'index'. Following this, the 'last_appended'
  // operation is reset to the OpId with this index, and the log cache will be truncated
  // accordingly.
  void TruncateOpsAfter(int64_t index);

  // Return the last OpId in the log.
  // Note that this can move backwards after a truncation (TruncateOpsAfter).
  OpId GetLastOpIdInLog() const;

  // Return the next OpId to be appended to the queue in the current term.
  OpId GetNextOpId() const;

  // Assembles a request for a peer, adding entries past 'op_id' up to
  // 'consensus_max_batch_size_bytes'.
  // Returns OK if the request was assembled, or Status::NotFound() if the
  // peer with 'uuid' was not tracked, of if the queue is not in leader mode.
  // Returns Status::Incomplete if we try to read an operation index from the
  // log that has not been written.
  //
  // WARNING: In order to avoid copying the same messages to every peer,
  // entries are added to 'request' via AddAllocated() methods.
  // The owner of 'request' is expected not to delete the request prior
  // to removing the entries through ExtractSubRange() or any other method
  // that does not delete the entries. The simplest way is to pass the same
  // instance of ConsensusRequestPB to RequestForPeer(): the buffer will
  // replace the old entries with new ones without de-allocating the old
  // ones if they are still required.
  Status RequestForPeer(const std::string& uuid,
                        ConsensusRequestPB* request,
                        std::vector<ReplicateRefPtr>* msg_refs,
                        bool* needs_tablet_copy);

  // Fill in a StartTabletCopyRequest for the specified peer.
  // If that peer should not initiate Tablet Copy, returns a non-OK status.
  // On success, also internally resets peer->needs_tablet_copy to false.
  Status GetTabletCopyRequestForPeer(const std::string& uuid,
                                     StartTabletCopyRequestPB* req);

  // Update the last successful communication timestamp for the given peer
  // to the current time. This should be called when a non-network related
  // error is received from the peer, indicating that it is alive, even if it
  // may not be fully up and running or able to accept updates.
  void NotifyPeerIsResponsiveDespiteError(const std::string& peer_uuid);

  // Updates the request queue with the latest response of a peer, returns
  // whether this peer has more requests pending.
  void ResponseFromPeer(const std::string& peer_uuid,
                        const ConsensusResponsePB& response,
                        bool* more_pending);

  // Called by the consensus implementation to update the queue's watermarks
  // based on information provided by the leader. This is used for metrics and
  // log retention.
  void UpdateFollowerWatermarks(int64_t committed_index,
                                int64_t all_replicated_index);

  // Update the metric that measures how many ops behind the leader the local
  // replica believes it is (0 if leader).
  void UpdateLagMetrics();

  // Updates the last op appended to the leader and the corresponding lag metric.
  // This should not be called by a leader.
  void UpdateLastIndexAppendedToLeader(int64_t last_idx_appended_to_leader);

  // Closes the queue, peers are still allowed to call UntrackPeer() and
  // ResponseFromPeer() but no additional peers can be tracked or messages
  // queued.
  void Close();

  int64_t GetQueuedOperationsSizeBytesForTests() const;

  // Returns the last message replicated by all peers.
  int64_t GetAllReplicatedIndex() const;

  // Returns the committed index. All operations with index less than or equal to
  // this index have been committed.
  int64_t GetCommittedIndex() const;

  // Return true if the committed index falls within the current term.
  bool IsCommittedIndexInCurrentTerm() const;

  // Returns the current majority replicated index, for tests.
  int64_t GetMajorityReplicatedIndexForTests() const;

  // Returns a copy of the TrackedPeer with 'uuid' or crashes if the peer is
  // not being tracked.
  TrackedPeer GetTrackedPeerForTests(const std::string& uuid);

  std::string ToString() const;

  // Dumps the contents of the queue to the provided string vector.
  void DumpToStrings(std::vector<std::string>* lines) const;

  void DumpToHtml(std::ostream& out) const;

  void RegisterObserver(PeerMessageQueueObserver* observer);

  Status UnRegisterObserver(PeerMessageQueueObserver* observer);

  struct Metrics {
    // Keeps track of the number of ops. that are completed by a majority but still need
    // to be replicated to a minority (IsDone() is true, IsAllDone() is false).
    scoped_refptr<AtomicGauge<int64_t> > num_majority_done_ops;
    // Keeps track of the number of ops. that are still in progress (IsDone() returns false).
    scoped_refptr<AtomicGauge<int64_t> > num_in_progress_ops;
    // Keeps track of the number of ops. behind the leader the peer is, measured as the difference
    // between the latest appended op index on this peer versus on the leader (0 if leader).
    scoped_refptr<AtomicGauge<int64_t> > num_ops_behind_leader;

    explicit Metrics(const scoped_refptr<MetricEntity>& metric_entity);
  };

  ~PeerMessageQueue();

 private:
  FRIEND_TEST(ConsensusQueueTest, TestQueueAdvancesCommittedIndex);
  FRIEND_TEST(ConsensusQueueTest, TestQueueMovesWatermarksBackward);
  FRIEND_TEST(ConsensusQueueTest, TestFollowerCommittedIndexAndMetrics);
  FRIEND_TEST(RaftConsensusQuorumTest, TestReplicasEnforceTheLogMatchingProperty);

  // Mode specifies how the queue currently behaves:
  // LEADER - Means the queue tracks remote peers and replicates whatever messages
  //          are appended. Observers are notified of changes.
  // NON_LEADER - Means the queue only tracks the local peer (remote peers are ignored).
  //              Observers are not notified of changes.
  enum Mode {
    LEADER,
    NON_LEADER
  };

  enum State {
    kQueueConstructed,
    kQueueOpen,
    kQueueClosed
  };

  struct QueueState {

    // The first operation that has been replicated to all currently
    // tracked peers.
    int64_t all_replicated_index;

    // The index of the last operation replicated to a majority.
    // This is usually the same as 'committed_index' but might not
    // be if the terms changed.
    int64_t majority_replicated_index;

    // The index of the last operation to be considered committed.
    int64_t committed_index;

    // The index of the last operation appended to the leader. A follower will use this to
    // determine how many ops behind the leader it is, as a soft metric for follower lag.
    int64_t last_idx_appended_to_leader;

    // The opid of the last operation appended to the queue.
    OpId last_appended;

    // The queue's owner current_term.
    // Set by the last appended operation.
    // If the queue owner's term is less than the term observed
    // from another peer the queue owner must step down.
    int64_t current_term;

    // The first index that we saw that was part of this current term.
    // When the term advances, this is set to boost::none, and then set
    // when the first operation is appended in the new term.
    boost::optional<int64_t> first_index_in_current_term;

    // The size of the majority for the queue.
    int majority_size_;

    State state;

    // The current mode of the queue.
    Mode mode;

    // The currently-active raft config. Only set if in LEADER mode.
    gscoped_ptr<RaftConfigPB> active_config;

    std::string ToString() const;
  };

  // Returns true iff given 'desired_op' is found in the local WAL.
  // If the op is not found, returns false.
  // If the log cache returns some error other than NotFound, crashes with a
  // fatal error.
  bool IsOpInLog(const OpId& desired_op) const;

  void NotifyObserversOfCommitIndexChange(int64_t new_commit_index);
  void NotifyObserversOfCommitIndexChangeTask(int64_t new_commit_index);

  void NotifyObserversOfTermChange(int64_t term);
  void NotifyObserversOfTermChangeTask(int64_t term);

  void NotifyObserversOfFailedFollower(const std::string& uuid,
                                       int64_t term,
                                       const std::string& reason);
  void NotifyObserversOfFailedFollowerTask(const std::string& uuid,
                                           int64_t term,
                                           const std::string& reason);

  typedef std::unordered_map<std::string, TrackedPeer*> PeersMap;

  std::string ToStringUnlocked() const;

  std::string LogPrefixUnlocked() const;

  void DumpToStringsUnlocked(std::vector<std::string>* lines) const;

  // Updates the metrics based on index math.
  void UpdateMetrics();

  void ClearUnlocked();

  // Returns the last operation in the message queue, or
  // 'preceding_first_op_in_queue_' if the queue is empty.
  const OpId& GetLastOp() const;

  void TrackPeerUnlocked(const std::string& uuid);

  // Checks that if the queue is in LEADER mode then all registered peers are
  // in the active config. Crashes with a FATAL log message if this invariant
  // does not hold. If the queue is in NON_LEADER mode, does nothing.
  void CheckPeersInActiveConfigIfLeaderUnlocked() const;

  // Callback when a REPLICATE message has finished appending to the local log.
  void LocalPeerAppendFinished(const OpId& id,
                               const StatusCallback& callback,
                               const Status& status);

  // Advances 'watermark' to the smallest op that 'num_peers_required' have.
  void AdvanceQueueWatermark(const char* type,
                             int64_t* watermark,
                             const OpId& replicated_before,
                             const OpId& replicated_after,
                             int num_peers_required,
                             const TrackedPeer* who_caused);

  std::vector<PeerMessageQueueObserver*> observers_;

  // The pool which executes observer notifications.
  // TODO consider reusing a another pool.
  gscoped_ptr<ThreadPool> observers_pool_;

  // PB containing identifying information about the local peer.
  const RaftPeerPB local_peer_pb_;

  // The id of the tablet.
  const std::string tablet_id_;

  QueueState queue_state_;

  // The currently tracked peers.
  PeersMap peers_map_;
  mutable simple_spinlock queue_lock_; // TODO: rename

  // We assume that we never have multiple threads racing to append to the queue.
  // This fake mutex adds some extra assurance that this implementation property
  // doesn't change.
  DFAKE_MUTEX(append_fake_lock_);

  LogCache log_cache_;

  Metrics metrics_;

  scoped_refptr<TimeManager> time_manager_;
};

// The interface between RaftConsensus and the PeerMessageQueue.
class PeerMessageQueueObserver {
 public:
  // Notify the observer that the commit index has advanced to 'committed_index'.
  virtual void NotifyCommitIndex(int64_t committed_index) = 0;

  // Notify the observer that a follower replied with a term
  // higher than that established in the queue.
  virtual void NotifyTermChange(int64_t term) = 0;

  // Notify the observer that a peer is unable to catch up due to falling behind
  // the leader's log GC threshold.
  virtual void NotifyFailedFollower(const std::string& peer_uuid,
                                    int64_t term,
                                    const std::string& reason) = 0;

  virtual ~PeerMessageQueueObserver() {}
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_QUEUE_H_ */
