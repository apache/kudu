// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <gflags/gflags.h>
#include <iostream>
#include <string>
#include <tr1/memory>
#include <utility>

#include "kudu/consensus/consensus_queue.h"

#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/url-coding.h"

DEFINE_int32(consensus_entry_cache_size_soft_limit_mb, 128,
             "The total per-tablet size of consensus entries to keep in memory."
             " This is a soft limit, i.e. messages in the queue are discarded"
             " down to this limit only if no peer needs to replicate them.");
DEFINE_int32(consensus_entry_cache_size_hard_limit_mb, 256,
             "The total per-tablet size of consensus entries to keep in memory."
             " This is a hard limit, i.e. messages in the queue are always discarded"
             " down to this limit. If a peer has not yet replicated the messages"
             " selected to be discarded the peer will be evicted from the quorum.");

DEFINE_int32(global_consensus_entry_cache_size_soft_limit_mb, 1024,
             "Server-wide version of 'consensus_entry_cache_size_soft_limit_mb'");
DEFINE_int32(global_consensus_entry_cache_size_hard_limit_mb, 1024,
             "Server-wide version of 'consensus_entry_cache_size_hard_limit_mb'");

DEFINE_int32(consensus_max_batch_size_bytes, 1024 * 1024,
             "The maximum per-tablet RPC batch size when updating peers.");
DEFINE_bool(consensus_dump_queue_on_full, false,
            "Whether to dump the full contents of the consensus queue to the log"
            " when it gets full. Mostly useful for debugging.");

namespace kudu {
namespace consensus {

using log::Log;
using metadata::QuorumPeerPB;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using strings::Substitute;

METRIC_DEFINE_gauge_int64(total_num_ops, MetricUnit::kCount,
                          "Total number of queued operations in the leader queue.");
METRIC_DEFINE_gauge_int64(num_all_done_ops, MetricUnit::kCount,
                          "Number of operations in the leader queue ack'd by all peers.");
METRIC_DEFINE_gauge_int64(num_majority_done_ops, MetricUnit::kCount,
                          "Number of operations in the leader queue ack'd by a majority but "
                          "not all peers.");
METRIC_DEFINE_gauge_int64(num_in_progress_ops, MetricUnit::kCount,
                          "Number of operations in the leader queue ack'd by a minority of "
                          "peers.");

// TODO expose and register metics via the MemTracker itself, so that
// we don't have to do the accounting in two places.
METRIC_DEFINE_gauge_int64(queue_size_bytes, MetricUnit::kBytes,
                          "Size of the leader queue, in bytes.");

const char kConsensusQueueParentTrackerId[] = "consensus_queue_parent";

OperationStatusTracker::OperationStatusTracker(gscoped_ptr<ReplicateMsg> replicate_msg)
  : replicate_msg_(replicate_msg.Pass()) {
  DCHECK(replicate_msg_->has_id());
}

std::string PeerMessageQueue::TrackedPeer::ToString() const {
  return Substitute("Peer: $0, Status: $1",
                    uuid,
                    peer_status.ShortDebugString());
}

#define INSTANTIATE_METRIC(x) \
  AtomicGauge<int64_t>::Instantiate(x, metric_ctx)
PeerMessageQueue::Metrics::Metrics(const MetricContext& metric_ctx)
  : total_num_ops(INSTANTIATE_METRIC(METRIC_total_num_ops)),
    num_all_done_ops(INSTANTIATE_METRIC(METRIC_num_all_done_ops)),
    num_majority_done_ops(INSTANTIATE_METRIC(METRIC_num_majority_done_ops)),
    num_in_progress_ops(INSTANTIATE_METRIC(METRIC_num_in_progress_ops)),
    queue_size_bytes(INSTANTIATE_METRIC(METRIC_queue_size_bytes)) {
}
#undef INSTANTIATE_METRIC

PeerMessageQueue::PeerMessageQueue(RaftConsensusQueueIface* consensus,
                                   const MetricContext& metric_ctx,
                                   const std::string& parent_tracker_id)
    : consensus_(DCHECK_NOTNULL(consensus)),
      max_ops_size_bytes_hard_(FLAGS_consensus_entry_cache_size_hard_limit_mb * 1024 * 1024),
      global_max_ops_size_bytes_hard_(
          FLAGS_global_consensus_entry_cache_size_hard_limit_mb * 1024 * 1024),
      current_term_(MinimumOpId().term()),
      committed_index_(MinimumOpId()),
      metrics_(metric_ctx),
      state_(kQueueConstructed) {
  uint64_t max_ops_size_bytes_soft = FLAGS_consensus_entry_cache_size_soft_limit_mb * 1024 * 1024;
  uint64_t global_max_ops_size_bytes_soft =
      FLAGS_global_consensus_entry_cache_size_soft_limit_mb * 1024 * 1024;

  // If no tracker is registered for kConsensusQueueMemTrackerId,
  // create one using the global soft limit.
  parent_tracker_ = MemTracker::FindOrCreateTracker(global_max_ops_size_bytes_soft,
                                                    parent_tracker_id,
                                                    NULL);

  tracker_ = MemTracker::CreateTracker(max_ops_size_bytes_soft,
                                       Substitute("$0-$1", parent_tracker_id, metric_ctx.prefix()),
                                       parent_tracker_.get());
}

void PeerMessageQueue::Init(const OpId& committed_index,
                            uint64_t current_term) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  CHECK_EQ(state_, kQueueConstructed);
  CHECK(committed_index.IsInitialized());
  committed_index_ = committed_index;
  current_term_ = current_term;
  preceding_first_op_in_queue_ = committed_index;
  state_ = kQueueOpen;
}

Status PeerMessageQueue::TrackPeer(const string& uuid, const OpId& initial_watermark) {
  CHECK(!uuid.empty()) << "Got request to track peer with empty UUID";
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  // TODO allow the queue to go and fetch requests from the log
  // up to a point.
  DCHECK(initial_watermark.IsInitialized());

  TrackedPeer* tracked_peer = new TrackedPeer(uuid);
  tracked_peer->CheckMonotonicTerms(initial_watermark.term());
  tracked_peer->peer_status.mutable_last_received()->CopyFrom(initial_watermark);
  tracked_peer->uuid = uuid;

  InsertOrDie(&watermarks_, uuid, tracked_peer);
  return Status::OK();
}

void PeerMessageQueue::UntrackPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  TrackedPeer* peer = EraseKeyReturnValuePtr(&watermarks_, uuid);
  if (peer != NULL) {
    delete peer;
  }
}

Status PeerMessageQueue::AppendOperation(scoped_refptr<OperationStatusTracker> status) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  const ReplicateMsg* msg = status->replicate_msg();

  // Before we change the queue's term, in debug mode, check that the indexes
  // in the queue are consecutive.
  DCHECK_EQ(GetLastOp().index() + 1, status->op_id().index())
    << "Last op in the queue: " << GetLastOp().ShortDebugString()
    << " operation being appended: " << status->op_id().ShortDebugString();

  // Check that terms are monotonically increasing
  DCHECK_GE(status->op_id().term(), current_term_);
  if (status->op_id().term() > current_term_) {
    current_term_ = status->op_id().term();
  }

  // Once either the local or global soft limit is exceeded...
  if (tracker_->AnyLimitExceeded()) {
    // .. try to trim the queue.
    Status s  = TrimBufferForMessage(msg);
    if (PREDICT_FALSE(!s.ok() && (VLOG_IS_ON(2) || FLAGS_consensus_dump_queue_on_full))) {
      queue_lock_.unlock();
      LOG(INFO) << "Queue Full: Dumping State:";
      vector<string>  queue_dump;
      DumpToStringsUnlocked(&queue_dump);
      BOOST_FOREACH(const string& line, queue_dump) {
        LOG(INFO) << line;
      }
    }
    RETURN_NOT_OK(s);
  }

  // If we get here, then either:
  //
  // 1) We were able to trim the queue such that no local or global
  // soft limit was exceeded
  // 2) We were unable to trim the queue to below any soft limits, but
  // hard limits were not violated.
  //
  // See also: TrimBufferForMessage() in this class.
  metrics_.queue_size_bytes->IncrementBy(msg->SpaceUsed());
  tracker_->Consume(msg->SpaceUsed());

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Appended REPLICATE to queue: " << msg->ShortDebugString() <<
        " Operation Status: " << status->ToString();
  }

  InsertOrDieNoPrint(&messages_, status->op_id(), status);
  metrics_.total_num_ops->Increment();

  // In tests some operations might already be IsAllDone().
  if (PREDICT_FALSE(status->IsAllDone())) {
    metrics_.num_all_done_ops->Increment();
  // If we're just replicating to learners, some operations might already
  // be IsDone().
  } else if (PREDICT_FALSE(status->IsDone())) {
    metrics_.num_majority_done_ops->Increment();
  } else {
    metrics_.num_in_progress_ops->Increment();
  }

  return Status::OK();
}

void PeerMessageQueue::RequestForPeer(const string& uuid,
                                      ConsensusRequestPB* request) const {
  // Clear the requests without deleting the entries, as they may be in use by other peers.
  request->mutable_ops()->ExtractSubrange(0, request->ops_size(), NULL);
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  const TrackedPeer* peer;
  if (PREDICT_FALSE(!FindCopy(watermarks_, uuid, &peer))) {
    LOG(FATAL) << "Unable to find peer with UUID " << uuid
               << ". Queue status: " << ToStringUnlocked();
  }

  // We don't actually start sending on 'lower_bound' but we seek to
  // it to get the preceding_id.
  MessagesBuffer::const_iterator iter = messages_.lower_bound(peer->peer_status.last_received());

  // If "lower_bound" points to the beginning of the queue and it is not the exact same as the
  // 'peer_status.last_received()' it means we're sending a batch including the very first message
  // in the queue, meaning we'll have to use to 'preceeding_first_op_in_queue_' to get the
  // 'preceding_id'.
  if (messages_.empty() ||
      (iter == messages_.begin() &&
       !OpIdEquals((*iter).first, peer->peer_status.last_received()))) {
    request->mutable_preceding_id()->CopyFrom(preceding_first_op_in_queue_);
  // ... otherwise 'preceding_id' is the first element in the iterator and we start sending
  // on the element after that.
  } else {
    request->mutable_preceding_id()->CopyFrom((*iter).first);
    iter++;
  }

  request->mutable_committed_index()->CopyFrom(committed_index_);
  request->set_caller_term(current_term_);

  // Add as many operations as we can to a request.
  OperationStatusTracker* ost = NULL;
  for (; iter != messages_.end(); iter++) {
    ost = (*iter).second.get();

    request->mutable_ops()->AddAllocated(ost->replicate_msg());
    if (request->ByteSize() > FLAGS_consensus_max_batch_size_bytes) {

      // Allow overflowing the max batch size in the case that we are sending
      // exactly one op. Otherwise we would never send the batch!
      if (request->ops_size() > 1) {
        request->mutable_ops()->ReleaseLast();
      }
      if (PREDICT_FALSE(VLOG_IS_ON(2))) {
        VLOG(2) << "Request reached max size for peer: "
            << uuid << " trimmed to: " << request->ops_size()
            << " ops and " << request->ByteSize() << " bytes."
            << " max is: " << FLAGS_consensus_max_batch_size_bytes;
      }
      break;
    }
  }
  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    if (request->ops_size() > 0) {
      VLOG(2) << "Sending request with operations to Peer: " << uuid
              << ". Size: " << request->ops_size()
              << ". From: " << request->ops(0).id().ShortDebugString() << ". To: "
              << request->ops(request->ops_size() - 1).id().ShortDebugString();
    } else {
      VLOG(2) << "Sending status only request to Peer: " << uuid;
    }
  }
}

void PeerMessageQueue::ResponseFromPeer(const ConsensusResponsePB& response,
                                        bool* more_pending) {
  CHECK(response.has_responder_uuid() && !response.responder_uuid().empty())
      << "Got response from peer with empty UUID";

  OpId updated_commit_index;
  {
    boost::lock_guard<simple_spinlock> lock(queue_lock_);

    TrackedPeer* peer = FindPtrOrNull(watermarks_, response.responder_uuid());
    if (PREDICT_FALSE(state_ == kQueueClosed || peer == NULL)) {
      LOG(WARNING) << "Queue is closed or peer was untracked, disregarding peer response."
          << " Response: " << response.ShortDebugString();
      *more_pending = false;
      return;
    }

    // Sanity checks.
    // Some of these can be eventually removed, but they are handy for now.

    // Application level errors should be handled elsewhere
    DCHECK(!response.has_error());

    // We're not handling any consensus level errors yet so we check that
    // consensus level errors are not present.
    DCHECK(!response.status().has_error());

    // Response must have a status as we're not yet handling consensus level
    // errors.
    DCHECK(response.has_status());

    // The peer must have responded with a term that is greater than or equal to
    // the last known term for that peer.
    peer->CheckMonotonicTerms(response.responder_term());

    // Right now we don't support leader election so the receiver must have the
    // same term as our own.
    CHECK_EQ(response.responder_term(), current_term_);

    MessagesBuffer::iterator iter = messages_.upper_bound(peer->peer_status.last_received());

    MessagesBuffer::iterator end_iter = messages_.upper_bound(response.status().last_received());

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Received Response from Peer: " << response.responder_uuid()
          << ". Current Status: " << peer->peer_status.ShortDebugString()
          << ". Response: " << response.ShortDebugString();
    }

    // The id of the last operation that becomes committed with this response, i.e. the
    // id of the last message acknowledged by the responding peer that makes a replicate
    // message committed.
    const OpId* last_committed = &committed_index_;

    OperationStatusTracker* ost = NULL;
    for (;iter != end_iter; iter++) {
      ost = (*iter).second.get();
      bool was_done = ost->IsDone();
      bool was_all_done = ost->IsAllDone();

      // Acknowledge that the peer logged the operation
      ost->AckPeer(response.responder_uuid());

      if (ost->IsAllDone() && !was_all_done) {
        metrics_.num_all_done_ops->Increment();
        metrics_.num_majority_done_ops->Decrement();
      }
      if (ost->IsDone() && !was_done) {
        // If this operation became IsDone() with this response
        // update 'last_committed_' to match.
        // Because the replication queue is traversed in order
        // we're sure to update 'last_committed_' to monotonically
        // increasing values.
        last_committed = &ost->replicate_msg()->id();
        metrics_.num_majority_done_ops->Increment();
        metrics_.num_in_progress_ops->Decrement();
      }
    }

    peer->peer_status.CopyFrom(response.status());

    // Update the last_committed operation.
    // This only changes the last_committed_ operation if one of the replicates became
    // committed when this response was processed.

    // TODO This is assuming no leader election, the rules for advancing the
    // committed_index will have to change when we have it.
    committed_index_.CopyFrom(*last_committed);
    updated_commit_index.CopyFrom(committed_index_);

    // check if there are more messages pending.
    *more_pending = (iter != messages_.end());
  }

  // It's OK that we do this without the lock as this is idempotent.
  consensus_->UpdateCommittedIndex(updated_commit_index);
}

OpId PeerMessageQueue::GetCommittedIndexForTests() const {
  return committed_index_;
}

Status PeerMessageQueue::GetOperationStatus(const OpId& op_id,
                                            scoped_refptr<OperationStatusTracker>* status) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  MessagesBuffer::iterator iter = messages_.find(op_id);
  if (iter == messages_.end()) {
    return Status::NotFound("Operation is not in the queue.");
  }
  *status = (*iter).second;
  return Status::OK();
}

Status PeerMessageQueue::TrimBufferForMessage(const ReplicateMsg* msg) {
  // TODO for now we're just trimming the buffer, but we need to handle when
  // the buffer is full but there is a peer hanging on to the queue (very delayed)
  int32_t bytes = msg->SpaceUsed();

  MessagesBuffer::iterator iter = messages_.begin();

  // If adding 'msg' to the queue would violate either a local
  // or a global soft limit, try to trim any finished messages from
  // the queue and release the memory used to the mem tracker.
  while (bytes > tracker_->SpareCapacity()) {
    OperationStatusTracker* ost = NULL;
    // Handle the situation where this tablet's consensus queue is
    // empty, but the global limits may have already been violated due
    // to the other queues' memory consumption.
    if (iter != messages_.end()) {
      ost = (*iter).second.get();
    }
    if (ost == NULL || !ost->IsAllDone()) {
      // return OK if we could trim the queue
      if (CheckHardLimitsNotViolated(bytes)) {
        // parent_tracker_->consumption() in this case returns total
        // consumption by _ALL_ all consensus queues, i.e., the
        // server-wide consensus queue memory consumption.
        return Status::OK();
      } else {
        return Status::ServiceUnavailable("Cannot append replicate message. Queue is full.");
      }
    }
    uint64_t bytes_to_decrement = ost->replicate_msg()->SpaceUsed();
    metrics_.total_num_ops->Decrement();
    metrics_.num_all_done_ops->Decrement();
    metrics_.queue_size_bytes->DecrementBy(bytes_to_decrement);

    tracker_->Release(bytes_to_decrement);
    preceding_first_op_in_queue_.CopyFrom((*iter).first);
    messages_.erase(iter++);
  }
  return Status::OK();
}


bool PeerMessageQueue::CheckHardLimitsNotViolated(size_t bytes) const {
  bool local_limit_violated = (bytes + tracker_->consumption()) > max_ops_size_bytes_hard_;
  bool global_limit_violated = (bytes + parent_tracker_->consumption())
      > global_max_ops_size_bytes_hard_;
#ifndef NDEBUG
  if (VLOG_IS_ON(1)) {
    DVLOG(1) << "global consumption: "
             << HumanReadableNumBytes::ToString(parent_tracker_->consumption());
    string human_readable_bytes = HumanReadableNumBytes::ToString(bytes);
    if (local_limit_violated) {
      DVLOG(1) << "adding " << human_readable_bytes
               << " would violate local hard limit ("
               << HumanReadableNumBytes::ToString(max_ops_size_bytes_hard_) << ").";
    }
    if (global_limit_violated) {
      DVLOG(1) << "adding " << human_readable_bytes
               << " would violate global hard limit ("
               << HumanReadableNumBytes::ToString(global_max_ops_size_bytes_hard_) << ").";
    }
  }
#endif
  return !local_limit_violated && !global_limit_violated;
}

const OpId& PeerMessageQueue::GetLastOp() {
  return messages_.empty() ? preceding_first_op_in_queue_ : (*messages_.rbegin()).first;
}

void PeerMessageQueue::DumpToStrings(vector<string>* lines) const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DumpToStringsUnlocked(lines);
}

void PeerMessageQueue::DumpToStringsUnlocked(vector<string>* lines) const {
  lines->push_back("Watermarks:");
  BOOST_FOREACH(const WatermarksMap::value_type& entry, watermarks_) {
    lines->push_back(
        Substitute("Peer: $0 Watermark: $1", entry.first, entry.second->ToString()));
  }
  int counter = 0;
  lines->push_back("Messages:");
  BOOST_FOREACH(const MessagesBuffer::value_type entry, messages_) {
    const OpId& id = entry.second->op_id();
    ReplicateMsg* msg = entry.second->replicate_msg();
    lines->push_back(
      Substitute("Message[$0] $1.$2 : REPLICATE. Type: $3, Size: $4, Status: $5",
                 counter++, id.term(), id.index(),
                 OperationType_Name(msg->op_type()),
                 msg->ByteSize(), entry.second->ToString()));
  }
}

void PeerMessageQueue::DumpToHtml(std::ostream& out) const {
  using std::endl;

  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  out << "<h3>Watermarks</h3>" << endl;
  out << "<table>" << endl;;
  out << "  <tr><th>Peer</th><th>Watermark</th></tr>" << endl;
  BOOST_FOREACH(const WatermarksMap::value_type& entry, watermarks_) {
    out << Substitute("  <tr><td>$0</td><td>$1</td></tr>",
                      EscapeForHtmlToString(entry.first),
                      EscapeForHtmlToString(entry.second->ToString())) << endl;
  }
  out << "</table>" << endl;

  out << "<h3>Messages:</h3>" << endl;
  out << "<table>" << endl;
  out << "<tr><th>Entry</th><th>OpId</th><th>Type</th><th>Size</th><th>Status</th></tr>" << endl;

  int counter = 0;
  BOOST_FOREACH(const MessagesBuffer::value_type entry, messages_) {
    const OpId& id = entry.second->op_id();
    ReplicateMsg* msg = entry.second->replicate_msg();
    out << Substitute("<tr><th>$0</th><th>$1.$2</th><td>REPLICATE $3</td>"
                      "<td>$4</td><td>$5</td></tr>",
                      counter++, id.term(), id.index(),
                      OperationType_Name(msg->op_type()),
                      msg->ByteSize(), entry.second->ToString()) << endl;
  }
  out << "</table>";
}

void PeerMessageQueue::Clear() {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  ClearUnlocked();
}

void PeerMessageQueue::ClearUnlocked() {
  messages_.clear();
  STLDeleteValues(&watermarks_);
}

void PeerMessageQueue::Close() {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  state_ = kQueueClosed;
  ClearUnlocked();
}

int64_t PeerMessageQueue::GetQueuedOperationsSizeBytesForTests() const {
  return tracker_->consumption();
}

string PeerMessageQueue::ToString() const {
  // Even though metrics are thread-safe obtain the lock so that we get
  // a "consistent" snapshot of the metrics.
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return ToStringUnlocked();
}

string PeerMessageQueue::ToStringUnlocked() const {
  return Substitute("Consensus queue metrics: Total Ops: $0, All Done Ops: $1, "
      "Only Majority Done Ops: $2, In Progress Ops: $3, Queue Size (bytes): $4/$5",
      metrics_.total_num_ops->value(), metrics_.num_all_done_ops->value(),
      metrics_.num_majority_done_ops->value(), metrics_.num_in_progress_ops->value(),
      metrics_.queue_size_bytes->value(), max_ops_size_bytes_hard_);
}

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

}  // namespace consensus
}  // namespace kudu
