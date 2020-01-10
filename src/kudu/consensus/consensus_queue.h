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
#include <ostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/consensus/log_cache.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/threading/thread_collision_warner.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {
class ThreadPoolToken;

namespace log {
class Log;
}

namespace logging {
class LogThrottler;
}

namespace consensus {
class ConsensusRequestPB;
class ConsensusResponsePB;
class ConsensusStatusPB;
class PeerMessageQueueObserver;
class StartTabletCopyRequestPB;
class TimeManager;

// The id for the server-wide consensus queue MemTracker.
extern const char kConsensusQueueParentTrackerId[];

// State enum for the last known status of a peer tracked by the
// ConsensusQueue.
enum class PeerStatus {
  // The peer has not yet had a round of communication.
  NEW,

  // The last exchange with the peer was successful. We transmitted
  // an update to the peer and it accepted it.
  OK,

  // Some tserver-level or consensus-level error occurred that didn't
  // fall into any of the below buckets.
  REMOTE_ERROR,

  // Some RPC-layer level error occurred. For example, a network error or timeout
  // occurred while attempting to send the RPC.
  RPC_LAYER_ERROR,

  // The remote tablet server indicated that the tablet was in a FAILED state.
  TABLET_FAILED,

  // The remote tablet server indicated that the tablet was in a NOT_FOUND state.
  TABLET_NOT_FOUND,

  // The remote tablet server indicated that the term of this leader was older
  // than its latest seen term.
  INVALID_TERM,

  // The remote tablet server was unable to prepare any operations in the most recent
  // batch.
  CANNOT_PREPARE,

  // The remote tablet server's log was divergent from the leader's log.
  LMP_MISMATCH,
};

const char* PeerStatusToString(PeerStatus p);

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
    explicit TrackedPeer(RaftPeerPB peer_pb);

    TrackedPeer() = default;

    // Copy a given TrackedPeer.
    TrackedPeer& operator=(const TrackedPeer& tracked_peer) = default;

    // Check that the terms seen from a given peer only increase
    // monotonically.
    void CheckMonotonicTerms(int64_t term) {
      DCHECK_GE(term, last_seen_term_) << "peer info: " << ToString();
      last_seen_term_ = term;
    }

    const std::string& uuid() const {
      return peer_pb.permanent_uuid();
    }

    std::string ToString() const;

    RaftPeerPB peer_pb;

    // Next index to send to the peer.
    // This corresponds to "nextIndex" as specified in Raft.
    int64_t next_index;

    // The last operation that we've sent to this peer and that
    // it acked. Used for watermark movement.
    OpId last_received;

    // The last committed index this peer knows about.
    int64_t last_known_committed_index;

    // The status after our last attempt to communicate with the peer.
    // See the comments within the PeerStatus enum above for details.
    PeerStatus last_exchange_status;

    // The time of the last communication with the peer.
    //
    // NOTE: this does not indicate that the peer successfully made progress at the
    // given time -- this only indicates that we got some indication that the tablet
    // server process was alive. It could be that the tablet was not found, etc.
    // Consult last_exchange_status for details.
    //
    // Defaults to the time of construction, so does not necessarily mean that
    // successful communication ever took place.
    MonoTime last_communication_time;

    // Set to false if it is determined that the remote peer has fallen behind
    // the local peer's WAL.
    bool wal_catchup_possible;

    // The peer's latest overall health status.
    HealthReportPB::HealthStatus last_overall_health_status;

    // Throttler for how often we will log status messages pertaining to this
    // peer (eg when it is lagging, etc).
    std::shared_ptr<logging::LogThrottler> status_log_throttler;

   private:
    // The last term we saw from a given peer.
    // This is only used for sanity checking that a peer doesn't
    // go backwards in time.
    int64_t last_seen_term_;
  };

  PeerMessageQueue(const scoped_refptr<MetricEntity>& metric_entity,
                   scoped_refptr<log::Log> log,
                   scoped_refptr<TimeManager> time_manager,
                   RaftPeerPB local_peer_pb,
                   std::string tablet_id,
                   std::unique_ptr<ThreadPoolToken> raft_pool_observers_token,
                   OpId last_locally_replicated,
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
  void SetNonLeaderMode(const RaftConfigPB& active_config);

  // Makes the queue track this peer.
  void TrackPeer(const RaftPeerPB& peer_pb);

  // Makes the queue untrack this peer.
  void UntrackPeer(const std::string& uuid);

  // Returns a health report for all active peers.
  // Returns IllegalState if the local peer is not the leader of the config.
  std::unordered_map<std::string, HealthReportPB> ReportHealthOfPeers() const;

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

  // Inform the queue of a new status known for one of its peers.
  // 'ps' indicates an interpretation of the status, while 'status'
  // may contain a more specific error message in the case of one of
  // the error statuses.
  void UpdatePeerStatus(const std::string& peer_uuid,
                        PeerStatus ps,
                        const Status& status);

  // Updates the request queue with the latest response from a request to a
  // consensus peer.
  // Returns true iff there are more requests pending in the queue for this
  // peer and another request should be sent immediately, with no intervening
  // delay.
  bool ResponseFromPeer(const std::string& peer_uuid,
                        const ConsensusResponsePB& response);

  // Called by the consensus implementation to update the queue's watermarks
  // based on information provided by the leader. This is used for metrics and
  // log retention.
  void UpdateFollowerWatermarks(int64_t committed_index,
                                int64_t all_replicated_index);

  // Updates the last op appended to the leader and the corresponding lag metric.
  // This should not be called by a leader.
  void UpdateLastIndexAppendedToLeader(int64_t last_idx_appended_to_leader);

  // Closes the queue. Once the queue is closed, peers are still allowed to
  // call UntrackPeer() and ResponseFromPeer(), however no additional peers may
  // be tracked and no additional messages may be enqueued.
  void Close();

  int64_t GetQueuedOperationsSizeBytesForTests() const;

  // Returns the last message replicated by all peers.
  int64_t GetAllReplicatedIndex() const;

  // Returns the committed index. All operations with index less than or equal to
  // this index have been committed.
  int64_t GetCommittedIndex() const;

  // Return true if the committed index falls within the current term.
  bool IsCommittedIndexInCurrentTerm() const;

  // Whether the queue run in the leader mode.
  bool IsInLeaderMode() const;

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

  // Begin or end the watch for an eligible successor. If 'successor_uuid' is
  // boost::none, the queue will notify its observers when 'successor_uuid' is
  // caught up to the leader. Otherwise, it will notify its observers
  // with the UUID of the first voter that is caught up.
  void BeginWatchForSuccessor(const boost::optional<std::string>& successor_uuid);
  void EndWatchForSuccessor();

 private:
  FRIEND_TEST(ConsensusQueueTest, TestQueueAdvancesCommittedIndex);
  FRIEND_TEST(ConsensusQueueTest, TestQueueMovesWatermarksBackward);
  FRIEND_TEST(ConsensusQueueTest, TestFollowerCommittedIndexAndMetrics);
  FRIEND_TEST(ConsensusQueueUnitTest, PeerHealthStatus);
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
    kQueueOpen,
    kQueueClosed
  };

  // Types of replicas to count when advancing a queue watermark.
  enum ReplicaTypes {
    ALL_REPLICAS,
    VOTER_REPLICAS,
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

  // Return true if it would be safe to evict the peer 'evict_uuid' at this
  // point in time.
  bool SafeToEvictUnlocked(const std::string& evict_uuid) const;

  // Update a peer's last_health_status field and trigger the appropriate
  // notifications.
  void UpdatePeerHealthUnlocked(TrackedPeer* peer);

  // Update the peer's last exchange status, and other fields, based on the
  // response. Sets 'lmp_mismatch' to true if the given response indicates
  // there was a log-matching property mismatch on the remote, otherwise sets
  // it to false.
  void UpdateExchangeStatus(TrackedPeer* peer, const TrackedPeer& prev_peer_state,
                            const ConsensusResponsePB& response, bool* lmp_mismatch);

  // Check if the peer is a NON_VOTER candidate ready for promotion. If so,
  // trigger promotion.
  void PromoteIfNeeded(TrackedPeer* peer, const TrackedPeer& prev_peer_state,
                       const ConsensusStatusPB& status);

  // If there is a graceful leadership change underway, notify queue observers
  // to initiate leadership transfer to the specified peer under the following
  // conditions:
  // * 'peer' has fully caught up to the leader
  // * 'peer' is the designated successor, or no successor was designated
  void TransferLeadershipIfNeeded(const TrackedPeer& peer,
                                  const ConsensusStatusPB& status);

  // Calculate a peer's up-to-date health status based on internal fields.
  static HealthReportPB::HealthStatus PeerHealthStatus(const TrackedPeer& peer);

  // Asynchronously trigger various types of observer notifications on a
  // separate thread.
  void NotifyObserversOfCommitIndexChange(int64_t new_commit_index);
  void NotifyObserversOfTermChange(int64_t term);
  void NotifyObserversOfFailedFollower(const std::string& uuid,
                                       int64_t term,
                                       const std::string& reason);
  void NotifyObserversOfPeerToPromote(const std::string& peer_uuid);
  void NotifyObserversOfSuccessor(const std::string& peer_uuid);
  void NotifyObserversOfPeerHealthChange();

  // Notify all PeerMessageQueueObservers using the given callback function.
  void NotifyObserversTask(const std::function<void(PeerMessageQueueObserver*)>& func);

  typedef std::unordered_map<std::string, TrackedPeer*> PeersMap;

  std::string ToStringUnlocked() const;

  std::string LogPrefixUnlocked() const;

  void DumpToStringsUnlocked(std::vector<std::string>* lines) const;

  // Updates the metrics based on index math.
  void UpdateMetricsUnlocked();

  // Update the metric that measures how many ops behind the leader the local
  // replica believes it is (0 if leader).
  void UpdateLagMetricsUnlocked();

  void ClearUnlocked();

  // Returns the last operation in the message queue, or
  // 'preceding_first_op_in_queue_' if the queue is empty.
  const OpId& GetLastOp() const;

  void TrackPeerUnlocked(const RaftPeerPB& peer_pb);

  void UntrackPeerUnlocked(const std::string& uuid);

  // We need the local peer in the config because it contains the current
  // 'member_type' of the local node while 'local_peer_pb_' does not.
  void TrackLocalPeerUnlocked();

  // Checks that if the queue is in LEADER mode then all registered peers are
  // in the active config. Crashes with a FATAL log message if this invariant
  // does not hold. If the queue is in NON_LEADER mode, does nothing.
  void CheckPeersInActiveConfigIfLeaderUnlocked() const;

  // Callback when a REPLICATE message has finished appending to the local log.
  void LocalPeerAppendFinished(const OpId& id,
                               const StatusCallback& callback,
                               const Status& status);

  // Advances 'watermark' to the smallest op that 'num_peers_required' have.
  // If 'replica_types' is set to VOTER_REPLICAS, the 'num_peers_required' is
  // interpreted as "number of voters required". If 'replica_types' is set to
  // ALL_REPLICAS, 'num_peers_required' counts any peer, regardless of its
  // voting status.
  void AdvanceQueueWatermark(const char* type,
                             int64_t* watermark,
                             const OpId& replicated_before,
                             const OpId& replicated_after,
                             int num_peers_required,
                             ReplicaTypes replica_types,
                             const TrackedPeer* who_caused);

  std::vector<PeerMessageQueueObserver*> observers_;

  // The pool token which executes observer notifications.
  std::unique_ptr<ThreadPoolToken> raft_pool_observers_token_;

  // PB containing identifying information about the local peer.
  const RaftPeerPB local_peer_pb_;

  // The id of the tablet.
  const std::string tablet_id_;

  QueueState queue_state_;

  // The currently tracked peers.
  PeersMap peers_map_;
  mutable simple_spinlock queue_lock_; // TODO(todd): rename

  bool successor_watch_in_progress_;
  boost::optional<std::string> designated_successor_uuid_;

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

  // Notify the observer that the specified peer is ready to be promoted from
  // NON_VOTER to VOTER.
  virtual void NotifyPeerToPromote(const std::string& peer_uuid) = 0;

  // Notify the observer that the specified peer is ready to become leader, and
  // and it should be told to run an election.
  virtual void NotifyPeerToStartElection(const std::string& peer_uuid) = 0;

  // Notify the observer that the health of one of the peers has changed.
  virtual void NotifyPeerHealthChange() = 0;

  virtual ~PeerMessageQueueObserver() {}
};

}  // namespace consensus
}  // namespace kudu
