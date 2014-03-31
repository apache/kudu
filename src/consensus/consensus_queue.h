// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_CONSENSUS_QUEUE_H_
#define KUDU_CONSENSUS_CONSENSUS_QUEUE_H_

#include <map>
#include <string>
#include <tr1/unordered_map>
#include <utility>

#include "consensus/consensus.pb.h"
#include "consensus/log_util.h"
#include "gutil/ref_counted.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

namespace log {
class Log;
}

namespace consensus {

// TODO move somewhere common as other places, like bootstrap also need them.
inline int CompareOps(const OpId& first, const OpId& second) {
  if (PREDICT_TRUE(first.term() == second.term())) {
    return first.index() < second.index() ? -1 : first.index() == second.index() ? 0 : 1;
  }
  return first.term() < second.term() ? -1 : 1;
}

// The status associated with each single quorum operation.
class OperationStatus : public base::RefCountedThreadSafe<OperationStatus> {
 public:

  // Peers call this to ack they have executed the operation.
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
  // a majority of peers report errors. The returned status indicates
  // whether the operation was successful or not.
  virtual void Wait() = 0;

  virtual std::string ToString() const { return  IsDone() ? "Done" : "NotDone"; }

  virtual ~OperationStatus() {}
};

// A peer message that is queued for replication to peers.
// Basically a wrapper around an OperationPB (which must be
// replicated) and an OperationStatus (which tracks the replication
// status).
struct PeerMessage {

  PeerMessage(gscoped_ptr<OperationPB> op,
              const scoped_refptr<OperationStatus>& status);

  const OpId& GetOpId() const {
    return op_->id();
  }

  gscoped_ptr<OperationPB> op_;
  scoped_refptr<OperationStatus> status_;
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
  PeerMessageQueue();

  // Appends a operation that must be replicated to the quorum and associates
  // it with the provided 'status'.
  // The consensus operation will be associated with the provided 'status'
  // and, on return, 'status' can be inspected to track the operation's
  // progress.
  // Returns OK unless the operation could not be added to the queue for some
  // reason (e.g. the queue reached max size).
  Status AppendOperation(gscoped_ptr<OperationPB> operation,
                         scoped_refptr<OperationStatus> status);

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
  // WARNING: In order to avoid copying the same messages to every peer,
  // entries are added to 'request' via AddAllocated() methods.
  // The owner of 'request' is expected not to delete the request prior
  // to removing the entries through ExtractSubRange() or any other method
  // that does not delete the entries. The simplest way is to pass the same
  // instance of ConsensusRequestPB to RequestForPeer(): the buffer will
  // replace the old entries with new ones without de-allocating the old
  // ones if they are still required.
  void RequestForPeer(const std::string& uuid,
                      ConsensusRequestPB* request);

  // Updates the request queue with the latest status of a peer, returns
  // whether this peer has more requests pending.
  void ResponseFromPeer(const std::string& uuid,
                        const ConsensusStatusPB& status,
                        bool* more_pending);

  // Closes the queue, peers are still allowed to call UntrackPeer() and
  // ResponseFromPeer() but no additional peers can be tracked or messages
  // queued.
  void Close();

  int64_t GetQueuedOperationsSizeBytesForTests() {
    return queued_ops_size_bytes_;
  }

  void DumpToLog();

  ~PeerMessageQueue();

 private:
  // An ordered map that serves as the buffer for the pending messages.
  typedef std::map<OpId, PeerMessage*, log::OpIdCompareFunctor> MessagesBuffer;
  typedef std::tr1::unordered_map<std::string, ConsensusStatusPB*> WatermarksMap;
  typedef std::tr1::unordered_map<OpId, Status> ErrorsMap;
  typedef std::pair<OpId, Status> ErrorEntry;

  Status TrimBuffer();

  // The current size of the buffer in bytes
  uint64_t queued_ops_size_bytes_;

  // The total size of consensus entries to keep in memory.
  // This is a soft limit, i.e. messages in the queue are discarded
  // down to this limit only if no peer needs to replicate them.
  uint64_t max_ops_size_bytes_soft_;

  // The total size of consensus entries to keep in memory.
  // This is a hard limit, i.e. messages in the queue are always discarded
  // down to this limit. If a peer has not yet replicated the messages
  // selected to be discarded the peer will be evicted from the quorum.
  uint64_t max_ops_size_bytes_hard_;

  // The current watermark for each peer.
  // The queue owns the OpIds.
  WatermarksMap watermarks_;
  MessagesBuffer messages_;
  mutable simple_spinlock queue_lock_;
  OpId low_watermark_;

  enum State {
    kQueueOpen,
    kQueueClosed
  };

  State state_;
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_QUEUE_H_ */
