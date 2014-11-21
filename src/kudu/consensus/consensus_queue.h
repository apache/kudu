// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_CONSENSUS_CONSENSUS_QUEUE_H_
#define KUDU_CONSENSUS_CONSENSUS_QUEUE_H_

#include <iosfwd>
#include <map>
#include <string>
#include <tr1/unordered_map>
#include <tr1/memory>
#include <utility>
#include <vector>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log_cache.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {
template<class T>
class AtomicGauge;
class MemTracker;
class MetricContext;

namespace log {
class Log;
class AsyncLogReader;
}

namespace consensus {
class PeerMessageQueueObserver;

// The id for the server-wide consensus queue MemTracker.
extern const char kConsensusQueueParentTrackerId[];

// Tracks the state of the peers and which transactions they have replicated.
// Owns the LogCache which actually holds the replicate messages which are
// en route to the various peers.
//
// This also takes care of pushing requests to peers as new operations are
// added, and notifying RaftConsensus when the commit index advances.
//
// This class is used only on the LEADER side.
//
// TODO Right now this class is able to track one outstanding operation
// per peer. If we want to have more than one outstanding RPC we need to
// modify it.
class PeerMessageQueue {
 public:
  PeerMessageQueue(const MetricContext& metric_ctx,
                   log::Log* log,
                   const std::string& local_uuid,
                   const std::string& parent_tracker_id = kConsensusQueueParentTrackerId);

  // Initialize the queue.
  // 'committed_index' corresponds to the id of the last committed operation,
  // i.e. operations with ids <= 'committed_index' should be considered committed.
  // 'current_term' corresponds to the leader's current term, this is different
  // from 'committed_index.term()' if the leader has not yet committed an
  // operation in the current term.
  // Majority size corresponds to the number of peers that must have replicated
  // a certain operation for it to be considered committed.
  virtual void Init(const OpId& committed_index,
                    const OpId& last_locally_replicated,
                    uint64_t current_term,
                    int majority_size);

  // Appends a message to be replicated to the quorum.
  // Returns OK unless the message could not be added to the queue for some
  // reason (e.g. the queue reached max size).
  virtual Status AppendOperation(gscoped_ptr<ReplicateMsg> replicate);

  // Makes the queue track this peer.
  virtual void TrackPeer(const std::string& uuid);

  // Makes the queue untrack the peer.
  // Requires that the peer was being tracked.
  virtual void UntrackPeer(const std::string& uuid);

  // Assembles a request for a quorum peer, adding entries past 'op_id' up to
  // 'consensus_max_batch_size_bytes'.
  //
  // WARNING: In order to avoid copying the same messages to every peer,
  // entries are added to 'request' via AddAllocated() methods.
  // The owner of 'request' is expected not to delete the request prior
  // to removing the entries through ExtractSubRange() or any other method
  // that does not delete the entries. The simplest way is to pass the same
  // instance of ConsensusRequestPB to RequestForPeer(): the buffer will
  // replace the old entries with new ones without de-allocating the old
  // ones if they are still required.
  virtual void RequestForPeer(const std::string& uuid,
                              ConsensusRequestPB* request);

  // Updates the request queue with the latest response of a peer, returns
  // whether this peer has more requests pending.
  virtual void ResponseFromPeer(const ConsensusResponsePB& response,
                        bool* more_pending);

  // Closes the queue, peers are still allowed to call UntrackPeer() and
  // ResponseFromPeer() but no additional peers can be tracked or messages
  // queued.
  virtual void Close();

  virtual int64_t GetQueuedOperationsSizeBytesForTests() const;

  // Returns the last message replicated by all peers, for tests.
  virtual OpId GetAllReplicatedIndexForTests() const;

  // Returns the current consensus committed index, for tests.
  virtual OpId GetCommittedIndexForTests() const;

  virtual std::string ToString() const;

  // Dumps the contents of the queue to the provided string vector.
  virtual void DumpToStrings(std::vector<std::string>* lines) const;

  virtual void DumpToHtml(std::ostream& out) const;

  virtual void RegisterObserver(PeerMessageQueueObserver* observer);

  virtual Status UnRegisterObserver(PeerMessageQueueObserver* observer);

  struct Metrics {
    // Keeps track of the number of ops. that are completed by a majority but still need
    // to be replicated to a minority (IsDone() is true, IsAllDone() is false).
    AtomicGauge<int64_t>* num_majority_done_ops;
    // Keeps track of the number of ops. that are still in progress (IsDone() returns false).
    AtomicGauge<int64_t>* num_in_progress_ops;

    explicit Metrics(const MetricContext& metric_ctx);
  };

  virtual ~PeerMessageQueue();

 private:

  struct TrackedPeer {
    explicit TrackedPeer(const std::string& uuid)
      : uuid(uuid),
        last_seen_term_(0) {
    }
    std::string uuid;
    ConsensusStatusPB peer_status;

    // Check that the terms seen from a given peer only increase
    // monotonically.
    void CheckMonotonicTerms(uint64_t term) {
      DCHECK_GE(term, last_seen_term_);
      last_seen_term_ = term;
    }

    std::string ToString() const;

   private:
    // The last term we saw from a given peer.
    // This is only used for sanity checking that a peer doesn't
    // go backwards in time.
    uint64_t last_seen_term_;
  };

  enum State {
    kQueueConstructed,
    kQueueOpen,
    kQueueClosed
  };

  struct QueueState {

    // The first operation that has been replicated to all currently
    // tracked peers.
    OpId all_replicated_index;

    // The index of the last operation replicated to a majority.
    // This is usually the same as 'committed_index' but might not
    // be if the terms changed.
    OpId majority_replicated_index;

    // The index of the last operation to be considered committed.
    OpId committed_index;

    // The opid of the last operation appended to the queue.
    OpId last_appended;

    // The queue's owner current_term.
    // Set by the last appended operation.
    // If the queue owner's term is less than the term observed
    // from another peer the queue owner must step down.
    // TODO: it is likely to be cleaner to get this from the ConsensusMetadata
    // rather than by snooping on what operations are appended to the queue.
    uint64_t current_term;

    State state;
  };

  void NotifyObserversOfMajorityReplOpChange(const OpId& new_majority_replicated_op);

  typedef std::tr1::unordered_map<std::string, TrackedPeer*> WatermarksMap;

  std::string ToStringUnlocked() const;

  void DumpToStringsUnlocked(std::vector<std::string>* lines) const;

  // Updates the metrics based on index math.
  void UpdateMetrics();

  void ClearUnlocked();

  // Returns the last operation in the message queue, or
  // 'preceding_first_op_in_queue_' if the queue is empty.
  const OpId& GetLastOp() const;

  void TrackPeerUnlocked(const std::string& uuid);

  // Callback when a REPLICATE message has finished appending to the local
  // log.
  void LocalPeerAppendFinished(const OpId& id,
                               const Status& status);

  void AdvanceQueueWatermark(const char* type,
                             OpId* watermark,
                             const OpId& replicated_before,
                             const OpId& replicated_after,
                             int num_peers_required);

  // Tries to get messages after 'op' from the log cache. If 'op' is not found or if
  // a message with the same index is found but term is not the same as in 'op', it
  // tries another lookup with 'fallback_index'.
  // If either 'op' or 'fallback_index' are found, i.e. if it returns Status::OK(),
  // 'messages' is filled up to 'max_batch_size' bytes and 'preceding_id' is set to
  // the OpId immediately before the first message in 'messages'.
  Status GetOpsFromCacheOrFallback(const OpId& op,
                                   int64_t fallback_index,
                                   int max_batch_size,
                                   std::vector<ReplicateMsg*>* messages,
                                   OpId* preceding_id);

  std::vector<PeerMessageQueueObserver*> observers_;

  // The size of the majority for the queue.
  // TODO support changing majority sizes when quorums change.
  int majority_size_;

  // The UUID of the local peer.
  const std::string local_uuid_;

  QueueState queue_state_;

  // The current watermark for each peer.
  // The queue owns the OpIds.
  WatermarksMap watermarks_;
  mutable simple_spinlock queue_lock_; // TODO: rename

  LogCache log_cache_;

  Metrics metrics_;
};

// The interface between RaftConsensus and the PeerMessageQueue.
class PeerMessageQueueObserver {
 public:
  // Called by the queue each time the response for a peer is handled with
  // the resulting majority replicated index.
  // The consensus implementation decides the commit index based on that
  // and triggers the apply for pending transactions.
  // 'committed_index' is set to the id of the last operation considered
  // committed by consensus.
  // The implementation is idempotent, i.e. independently of the ordering of
  // calls to this method only non-triggered applys will be started.
  virtual void UpdateMajorityReplicated(const OpId& majority_replicated,
                                        OpId* committed_index) = 0;

  // Notify the Consensus implementation that a follower replied with a term
  // higher than that established in the queue.
  virtual void NotifyTermChange(uint64_t term) = 0;

  virtual ~PeerMessageQueueObserver() {}
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_QUEUE_H_ */
