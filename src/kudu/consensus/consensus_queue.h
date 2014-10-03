// Copyright (c) 2013, Cloudera, inc.

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
}

namespace consensus {

// The id for the server-wide consensus queue MemTracker.
extern const char kConsensusQueueParentTrackerId[];

// The status associated with each single quorum operation.
// This class is ref. counted and takes ownership of the tracked operation.
//
// NOTE: Implementations of this class must be thread safe.
class OperationStatusTracker : public RefCountedThreadSafe<OperationStatusTracker> {
 public:
  explicit OperationStatusTracker(gscoped_ptr<OperationPB> operation);

  // Called by PeerMessageQueue after a peer ACKs this operation.
  //
  // This will never be called concurrently from multiple threads, since it is always
  // called under the PeerMessageQueue lock. However, it may be called concurrently with the
  // IsDone(), IsAllDone(), and Wait().
  //
  // This does not need to be idempotent for a given peer -- the PeerMessageQueue ensures
  // that it is called at most once per peer.
  virtual void AckPeer(const std::string& uuid) = 0;

  // Whether enough/the right peers have ack'd the operation. This might
  // change depending on the operation type or quorum composition.
  // E.g. replication messages need to be ack'd by a majority while commit
  // messages must be at least ack'd by the leader.
  virtual bool IsDone() const = 0;

  // Indicates whether all peers have ack'd the operation meaning it can be
  // freed from the queue.
  virtual bool IsAllDone() const = 0;

  // Callers can use this to block until IsDone() becomes true or until
  // a majority of peers report errors.
  // TODO make this return a status on error.
  virtual void Wait() = 0;

  const OpId& op_id() const {
    return operation_->id();
  }

  OperationPB* operation() {
    return operation_.get();
  }

  virtual std::string ToString() const { return  IsDone() ? "Done" : "NotDone"; }

  virtual ~OperationStatusTracker() {}

 protected:
  gscoped_ptr<OperationPB> operation_;
};

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
  // All operations with ids <= 'committed_index' should be considered committed.
  void Init(const OpId& committed_index);

  // Appends a operation that must be replicated to the quorum and associates
  // it with the provided 'status'.
  // The consensus operation will be associated with the provided 'status'
  // and, on return, 'status' can be inspected to track the operation's
  // progress.
  // Returns OK unless the operation could not be added to the queue for some
  // reason (e.g. the queue reached max size).
  Status AppendOperation(scoped_refptr<OperationStatusTracker> status);

  // Makes the queue track this peer. Used when the peer already has
  // state. The queue assumes the peer has both replicated and committed
  // all messages prior to and including 'initial_watermark'.
  Status TrackPeer(const std::string& uuid, const OpId& initial_watermark);

  // Makes the queue untrack the peer.
  // Requires that the peer was being tracked.
  void UntrackPeer(const std::string& uuid);

  // Assembles a request for a quorum peer, adding entries past 'op_id' up to
  // 'consensus_max_batch_size_bytes'.
  //
  // Getting a request for a peer doesn't mutate the internal state of the
  // queue in any way (which is why this method is marked const). Only
  // when responses are processed are watermarks updated.
  //
  // WARNING: In order to avoid copying the same messages to every peer,
  // entries are added to 'request' via AddAllocated() methods.
  // The owner of 'request' is expected not to delete the request prior
  // to removing the entries through ExtractSubRange() or any other method
  // that does not delete the entries. The simplest way is to pass the same
  // instance of ConsensusRequestPB to RequestForPeer(): the buffer will
  // replace the old entries with new ones without de-allocating the old
  // ones if they are still required.
  void RequestForPeer(const std::string& uuid,
                      ConsensusRequestPB* request) const;

  // Updates the request queue with the latest response of a peer, returns
  // whether this peer has more requests pending.
  void ResponseFromPeer(const ConsensusResponsePB& response,
                        bool* more_pending);

  // Returns the OperationStatusTracker for the operation with id = 'op_id' by
  // setting 'status' to it and returning Status::OK() or returns Status::NotFound
  // if no such operation can be found in the queue.
  Status GetOperationStatus(const OpId& op_id,
                            scoped_refptr<OperationStatusTracker>* status);

  // Clears all messages and tracked peers but still leaves the queue in state
  // where it can be used again.
  // Note: Pending messages must be handled before calling this method, i.e.
  // any in flight operations must be either aborted or otherwise referenced
  // elsewhere prior to calling this.
  void Clear();

  // Closes the queue, peers are still allowed to call UntrackPeer() and
  // ResponseFromPeer() but no additional peers can be tracked or messages
  // queued.
  void Close();

  int64_t GetQueuedOperationsSizeBytesForTests() const;

  // Returns the current consensus committed index, for tests.
  OpId GetCommittedIndexForTests() const;

  std::string ToString() const;

  // Dumps the contents of the queue to the provided string vector.
  void DumpToStrings(std::vector<std::string>* lines) const;

  void DumpToHtml(std::ostream& out) const;

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

  ~PeerMessageQueue();

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

  // An ordered map that serves as the buffer for the pending messages.
  typedef std::map<OpId,
                   scoped_refptr<OperationStatusTracker>,
                   OpIdCompareFunctor> MessagesBuffer;

  typedef std::tr1::unordered_map<std::string, TrackedPeer*> WatermarksMap;
  typedef std::tr1::unordered_map<OpId, Status> ErrorsMap;
  typedef std::pair<OpId, Status> ErrorEntry;

  std::string ToStringUnlocked() const;

  void DumpToStringsUnlocked(std::vector<std::string>* lines) const;

  // Trims the buffer, making sure it can accomodate the provided message.
  // Returns Status::OK() if the buffer was trimmed or otherwise had available
  // space or Status::ServiceUnavailable() if the queue could not free enough space.
  Status TrimBufferForMessage(const OperationPB* operation);

  // Check whether adding 'bytes' to the consensus queue would violate
  // either the local (per-tablet) hard limit or the global
  // (server-wide) hard limit.
  bool CheckHardLimitsNotViolated(size_t bytes) const;

  void ClearUnlocked();

  // The total size of consensus entries to keep in memory.
  // This is a hard limit, i.e. messages in the queue are always discarded
  // down to this limit. If a peer has not yet replicated the messages
  // selected to be discarded the peer will be evicted from the quorum.
  uint64_t max_ops_size_bytes_hard_;

  // Server-wide version of 'max_ops_size_bytes_hard_'.
  uint64_t global_max_ops_size_bytes_hard_;

  // The queue's owner current_term.
  // Set by the last appended operation.
  // If the queue owner's term is less than the term observed
  // from another peer the queue owner must step down.
  // TODO: it is likely to be cleaner to get this from the ConsensusMetadata
  // rather than by snooping on what operations are appended to the queue.
  uint64_t current_term_;

  // The index of the last operation to be considered committed.
  OpId committed_index_;

  // The id of the operation immediately preceding the first operation
  // in the queue.
  OpId preceding_first_op_in_queue_;

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

  enum State {
    kQueueConstructed,
    kQueueOpen,
    kQueueClosed
  };

  State state_;
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_QUEUE_H_ */
