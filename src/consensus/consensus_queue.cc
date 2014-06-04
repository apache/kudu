// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <string>
#include <tr1/memory>
#include <utility>

#include "consensus/consensus_queue.h"
#include "consensus/log_util.h"

#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/strcat.h"
#include "util/auto_release_pool.h"
#include "util/metrics.h"

DEFINE_int32(consensus_entry_cache_size_soft_limit_mb, 128,
             "The total size of consensus entries to keep in memory."
             " This is a soft limit, i.e. messages in the queue are discarded"
             " down to this limit only if no peer needs to replicate them.");
DEFINE_int32(consensus_entry_cache_size_hard_limit_mb, 256,
             "The total size of consensus entries to keep in memory."
             " This is a hard limit, i.e. messages in the queue are always discarded"
             " down to this limit. If a peer has not yet replicated the messages"
             " selected to be discarded the peer will be evicted from the quorum.");
DEFINE_int32(consensus_max_batch_size_bytes, 1024 * 1024,
             "The maximum RPC batch size when updating peers.");
DEFINE_bool(consensus_dump_queue_on_full, false,
            "Whether to dump the full contents of the consensus queue to the log"
            " when it gets full. Mostly useful for debugging.");

namespace kudu {
namespace consensus {

using log::Log;
using log::OpIdCompare;
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
METRIC_DEFINE_gauge_int64(queue_size_bytes, MetricUnit::kBytes,
                          "Size of the leader queue, in bytes.");

PeerMessage::PeerMessage(gscoped_ptr<OperationPB> op,
                         const scoped_refptr<OperationStatusTracker>& status)
    : op_(op.Pass()),
      status_(status) {
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

PeerMessageQueue::PeerMessageQueue(const MetricContext& metric_ctx)
    : // TODO manage these on a server-wide basis
      max_ops_size_bytes_soft_(FLAGS_consensus_entry_cache_size_soft_limit_mb * 1024 * 1024),
      max_ops_size_bytes_hard_(FLAGS_consensus_entry_cache_size_hard_limit_mb * 1024 * 1024),
      metrics_(metric_ctx),
      state_(kQueueOpen) {
}

Status PeerMessageQueue::TrackPeer(const string& uuid, const OpId& initial_watermark) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  // TODO allow the queue to go and fetch requests from the log
  // up to a point.
  DCHECK(initial_watermark.IsInitialized());
  ConsensusStatusPB* status = new ConsensusStatusPB();
  status->mutable_safe_commit_watermark()->CopyFrom(initial_watermark);
  status->mutable_replicated_watermark()->CopyFrom(initial_watermark);
  status->mutable_received_watermark()->CopyFrom(initial_watermark);
  InsertOrDie(&watermarks_, uuid, status);
  return Status::OK();
}

void PeerMessageQueue::UntrackPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  ConsensusStatusPB* status = EraseKeyReturnValuePtr(&watermarks_, uuid);
  if (status != NULL) {
    delete status;
  }
}

Status PeerMessageQueue::AppendOperation(gscoped_ptr<OperationPB> operation,
                                         scoped_refptr<OperationStatusTracker> status) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  DCHECK(operation->has_commit()
         || operation->has_replicate()) << "Operation must be a commit or a replicate: "
             << operation->DebugString();

  int bytes = operation->ByteSize();

  if (metrics_.queue_size_bytes->value() >= max_ops_size_bytes_soft_) {
    Status s  = TrimBufferForMessage(bytes);
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

  metrics_.queue_size_bytes->IncrementBy(operation->ByteSize());

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Appended operation to queue: " << operation->ShortDebugString() <<
        " Operation Status: " << status->ToString();
  }
  PeerMessage* msg = new PeerMessage(operation.Pass(), status);
  InsertOrDieNoPrint(&messages_, msg->GetOpId(), msg);
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

  if (metrics_.queue_size_bytes->value() < max_ops_size_bytes_soft_) {
    return Status::OK();
  }

  return Status::OK();
}

void PeerMessageQueue::RequestForPeer(const string& uuid,
                                        ConsensusRequestPB* request) {
  // Clear the requests without deleting the entries, as they may be in use by other peers.
  // TODO consider adding an utility for this.
  request->mutable_ops()->ExtractSubrange(0, request->ops_size(), NULL);
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  const ConsensusStatusPB* current_status = FindOrDie(watermarks_, uuid);

  MessagesBuffer::iterator iter = messages_.upper_bound(current_status->received_watermark());

  // Add as many operations as we can to a request.
  PeerMessage* msg = NULL;
  for (; iter != messages_.end(); iter++) {
    msg = (*iter).second;
    request->mutable_ops()->AddAllocated(msg->op_.get());
    if (request->ByteSize() > FLAGS_consensus_max_batch_size_bytes) {
      request->mutable_ops()->ReleaseLast();
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
          << ". From: " << request->ops(0).ShortDebugString() << ". To: "
          << request->ops(request->ops_size() -1).ShortDebugString();
    } else {
      VLOG(2) << "Sending status only request to Peer: " << uuid;
    }
  }
}

void PeerMessageQueue::ResponseFromPeer(const string& uuid,
                                        const ConsensusStatusPB& new_status,
                                        bool* more_pending) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  ConsensusStatusPB* current_status = FindPtrOrNull(watermarks_, uuid);
  if (PREDICT_FALSE(state_ == kQueueClosed || current_status == NULL)) {
    LOG(WARNING) << "Queue is closed or peer was untracked, disregarding peer response.";
    *more_pending = false;
    return;
  }
  MessagesBuffer::iterator iter;
  // We always start processing messages from the lowest watermark
  // (which might be the replicated or the committed one)
  const OpId* lowest_watermark = &std::min(current_status->replicated_watermark(),
                                           current_status->safe_commit_watermark(),
                                           OpIdCompare);
  iter = messages_.upper_bound(*lowest_watermark);

  MessagesBuffer::iterator end_iter = messages_.upper_bound(new_status.received_watermark());

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Received Response from Peer: " << uuid << ". Current Status: "
        << current_status->ShortDebugString() << ". New Status: " << new_status.ShortDebugString();
  }

  // We need to ack replicates and commits separately (commits are executed asynchonously).
  // So for instance in the case of commits:
  // - Check that the op is a commit
  // - Check that it falls between the last ack'd commit watermark and the
  //   incoming commit watermark
  // If both checks pass ack it. The case for replicates is similar.
  PeerMessage* msg = NULL;
  for (;iter != end_iter; iter++) {
    msg = (*iter).second;
    bool was_done = msg->status_->IsDone();
    bool was_all_done = msg->status_->IsAllDone();

    if (msg->op_->has_commit() &&
        OpIdCompare(msg->op_->id(), current_status->safe_commit_watermark()) > 0 &&
        OpIdCompare(msg->op_->id(), new_status.safe_commit_watermark()) <= 0) {
      msg->status_->AckPeer(uuid);
    } else if (msg->op_->has_replicate() &&
        OpIdCompare(msg->op_->id(), current_status->replicated_watermark()) > 0 &&
        OpIdCompare(msg->op_->id(), new_status.replicated_watermark()) <= 0) {
      msg->status_->AckPeer(uuid);
    }

    if (msg->status_->IsAllDone() && !was_all_done) {
      metrics_.num_all_done_ops->Increment();
      metrics_.num_majority_done_ops->Decrement();
    }
    if (msg->status_->IsDone() && !was_done) {
      metrics_.num_majority_done_ops->Increment();
      metrics_.num_in_progress_ops->Decrement();
    }
  }

  if (current_status == NULL) {
    InsertOrUpdate(&watermarks_, uuid, new ConsensusStatusPB(new_status));
  } else {
    current_status->CopyFrom(new_status);
  }

  // check if there are more messages pending.
  *more_pending = (iter != messages_.end());
}

Status PeerMessageQueue::GetOperationStatus(const OpId& op_id,
                                            scoped_refptr<OperationStatusTracker>* status) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  MessagesBuffer::iterator iter = messages_.find(op_id);
  if (iter == messages_.end()) {
    return Status::NotFound("Operation is not in the queue.");
  }
  *status = (*iter).second->status_;
  return Status::OK();
}

Status PeerMessageQueue::TrimBufferForMessage(uint64_t bytes) {
  // TODO for now we're just trimming the buffer, but we need to handle when
  // the buffer is full but there is a peer hanging on to the queue (very delayed)
  MessagesBuffer::iterator iter = messages_.begin();
  uint64_t new_size = metrics_.queue_size_bytes->value() + bytes;
  while (new_size > max_ops_size_bytes_soft_ && iter != messages_.end()) {
    PeerMessage* message = (*iter).second;
    if (!message->status_->IsAllDone()) {
      if (new_size < max_ops_size_bytes_hard_) {
        return Status::OK();
      } else {
        return Status::ServiceUnavailable("Queue is full.");
      }
    }
    uint64_t bytes_to_decrement = (*iter).second->op_->ByteSize();
    metrics_.total_num_ops->Decrement();
    metrics_.num_all_done_ops->Decrement();
    metrics_.queue_size_bytes->DecrementBy(bytes_to_decrement);
    PeerMessage* msg = (*iter).second;
    messages_.erase(iter++);
    delete msg;
    new_size = metrics_.queue_size_bytes->value() + bytes;
  }
  return Status::OK();
}

void PeerMessageQueue::DumpToStrings(vector<string>* lines) const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DumpToStringsUnlocked(lines);
}

void PeerMessageQueue::DumpToStringsUnlocked(vector<string>* lines) const {
  lines->push_back("Watermarks:");
  BOOST_FOREACH(const WatermarksMap::value_type& entry, watermarks_) {
    lines->push_back(
        Substitute("Peer: $0 Watermark: $1",
                   entry.first,
                   (entry.second != NULL ? entry.second->ShortDebugString() : "NULL")));
  }
  int counter = 0;
  lines->push_back("Messages:");
  BOOST_FOREACH(const MessagesBuffer::value_type entry, messages_) {

    const OpId& id = entry.second->op_->id();
    if (entry.second->op_->has_replicate()) {
      lines->push_back(
          Substitute("Message[$0] $1.$2 : REPLICATE. Type: $3, Size: $4, Status: $5",
                     counter++, id.term(), id.index(),
                     OperationType_Name(entry.second->op_->replicate().op_type()),
                     entry.second->op_->ByteSize(), entry.second->status_->ToString()));
    } else {
      const OpId& committed_op_id = entry.second->op_->commit().commited_op_id();
      lines->push_back(
          Substitute("Message[$0] $1.$2 : COMMIT. Committed OpId: $3.$4 "
              "Type: $5, Size: $6, Status: $7",
                     counter++, id.term(), id.index(), committed_op_id.index(),
                     committed_op_id.term(),
                     OperationType_Name(entry.second->op_->replicate().op_type()),
                     entry.second->op_->ByteSize(), entry.second->status_->ToString()));
    }
  }
}

void PeerMessageQueue::Close() {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  state_ = kQueueClosed;
  STLDeleteValues(&messages_);
  STLDeleteValues(&watermarks_);
}

int64_t PeerMessageQueue::GetQueuedOperationsSizeBytesForTests() const {
  return metrics_.queue_size_bytes->value();
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

