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
class RaftConsensusQueueIface;

// The id for the server-wide consensus queue MemTracker.
extern const char kConsensusQueueParentTrackerId[];

// Tracks all the pending consensus operations on the LEADER side.
// The PeerMessageQueue has the dual goal of keeping a single copy of a
// request in memory (instead of creating a copy for each peer) and
// of centralizing watermark tracking for all peers.
//
// TODO Right now this queue is able to track one outstanding operation
// per peer. If we want to have more than one outstanding RPC we need to
// modify it.
class PeerMessageQueue {
 public:
  explicit PeerMessageQueue(const MetricContext& metric_ctx,
                            const std::string& parent_tracker_id = kConsensusQueueParentTrackerId);

  // Initialize the queue.
  // 'committed_index' corresponds to the id of the last committed operation,
  // i.e. operations with ids <= 'committed_index' should be considered committed.
  // 'current_term' corresponds to the leader's current term, this is different
  // from 'committed_index.term()' if the leader has not yet committed an
  // operation in the current term.
  // Majority size corresponds to the number of peers that must have replicated
  // a certain operation for it to be considered committed.
  virtual void Init(RaftConsensusQueueIface* consensus,
                    const OpId& committed_index,
                    uint64_t current_term,
                    int majority_size);

  // Appends a message to be replicated to the quorum.
  // Returns OK unless the message could not be added to the queue for some
  // reason (e.g. the queue reached max size).
  virtual Status AppendOperation(gscoped_ptr<ReplicateMsg> status);

  // Makes the queue track this peer.
  virtual Status TrackPeer(const std::string& uuid);

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

  // Clears all messages and tracked peers but still leaves the queue in state
  // where it can be used again.
  // Note: Pending messages must be handled before calling this method, i.e.
  // any in flight operations must be either aborted or otherwise referenced
  // elsewhere prior to calling this.
  virtual void Clear();

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

  struct Metrics {
    // Keeps track of the total number of operations in the queue.
    AtomicGauge<int64_t>* total_num_ops;
    // Keeps track of the number of ops. that are completed (IsAllDone() is true) but
    // haven't been deleted from the queue (either because the buffer is not full,
    // because there is a dangling operation with a lower id or just because
    // TrimBuffer() hasn't been called yet).
    AtomicGauge<int64_t>* num_all_done_ops;
    // Keeps track of the number of ops. that are completed by a majority but still need
    // to be replicated to a minority (IsDone() is true, IsAllDone() is false).
    AtomicGauge<int64_t>* num_majority_done_ops;
    // Keeps track of the number of ops. that are still in progress (IsDone() returns false).
    AtomicGauge<int64_t>* num_in_progress_ops;
    // Keeps track of the total size of the queue, in bytes.
    AtomicGauge<int64_t>* queue_size_bytes;

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

    // The index of the last operation to be considered committed.
    OpId committed_index;

    // The id of the operation immediately preceding the first operation
    // in the queue.
    OpId preceding_first_op_in_queue;

    // The queue's owner current_term.
    // Set by the last appended operation.
    // If the queue owner's term is less than the term observed
    // from another peer the queue owner must step down.
    // TODO: it is likely to be cleaner to get this from the ConsensusMetadata
    // rather than by snooping on what operations are appended to the queue.
    uint64_t current_term;

    State state;
  };

  // An ordered map that serves as the buffer for the pending messages.
  typedef std::map<OpId,
                   ReplicateMsg*,
                   OpIdCompareFunctor> MessagesBuffer;

  typedef std::tr1::unordered_map<std::string, TrackedPeer*> WatermarksMap;
  typedef std::tr1::unordered_map<OpId, Status> ErrorsMap;
  typedef std::pair<OpId, Status> ErrorEntry;

  std::string ToStringUnlocked() const;

  void DumpToStringsUnlocked(std::vector<std::string>* lines) const;

  // Trims the buffer to free space, if we can.
  void TrimBuffer();

  // Updates the metrics based on index math.
  void UpdateMetrics();

  // Check whether adding 'bytes' to the consensus queue would violate
  // either the local (per-tablet) hard limit or the global
  // (server-wide) hard limit.
  bool WouldHardLimitBeViolated(size_t bytes) const;

  void ClearUnlocked();

  // Returns the last operation in the message queue, or
  // 'preceding_first_op_in_queue_' if the queue is empty.
  const OpId& GetLastOp() const;

  void AdvanceQueueWatermark(OpId* watermark,
                             const OpId& replicated_before,
                             const OpId& replicated_after,
                             int num_peers_required);

  void EntriesLoadedCallback(const Status& status,
                             const std::vector<ReplicateMsg*>& replicates,
                             const OpId& new_preceding_first_op_in_queue);

  // The size of the majority for the queue.
  // TODO support changing majority sizes when quorums change.
  int majority_size_;

  QueueState queue_state_;

  RaftConsensusQueueIface* consensus_;

  // The total size of consensus entries to keep in memory.
  // This is a hard limit, i.e. messages in the queue are always discarded
  // down to this limit. If a peer has not yet replicated the messages
  // selected to be discarded the peer will be evicted from the quorum.
  uint64_t max_ops_size_bytes_hard_;

  // Server-wide version of 'max_ops_size_bytes_hard_'.
  uint64_t global_max_ops_size_bytes_hard_;

  // The current watermark for each peer.
  // The queue owns the OpIds.
  WatermarksMap watermarks_;
  MessagesBuffer messages_;
  mutable simple_spinlock queue_lock_;

  // Pointer to a parent memtracker for all consensus queues. This
  // exists to compute server-wide queue size and enforce a
  // server-wide memory limit.  When the first instance of a consensus
  // queue is created, a new entry is added to MemTracker's static
  // map; subsequent entries merely increment the refcount, so that
  // the parent tracker can be deleted if all consensus queues are
  // deleted (e.g., if all tablets are deleted from a server, or if
  // the server is shutdown).
  std::tr1::shared_ptr<MemTracker> parent_tracker_;

  // A MemTracker for this instance.
  std::tr1::shared_ptr<MemTracker> tracker_;

  Metrics metrics_;

  gscoped_ptr<log::AsyncLogReader> async_reader_;
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_QUEUE_H_ */
