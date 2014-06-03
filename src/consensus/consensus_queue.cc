// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <gutil/map-util.h>
#include <gutil/stl_util.h>
#include <gutil/strings/substitute.h>
#include <string>
#include <tr1/memory>
#include <utility>
#include <vector>

#include "consensus/consensus_queue.h"
#include "consensus/log_util.h"

#include "common/wire_protocol.h"
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
  // TODO trim the buffer before we add to the queue
  return TrimBuffer();
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

Status PeerMessageQueue::TrimBuffer() {
  // TODO for now we're just trimming the buffer, but we need to handle when
  // the buffer is full but there is a peer hanging on to the queue (very delayed)
  MessagesBuffer::iterator iter = messages_.begin();
  while (metrics_.queue_size_bytes->value() > max_ops_size_bytes_soft_ &&
      iter != messages_.end()) {
    PeerMessage* message = (*iter).second;
    if (!message->status_->IsAllDone()) {
      if (metrics_.queue_size_bytes->value() < max_ops_size_bytes_hard_) {
        return Status::OK();
      } else {
        LOG(FATAL) << "Queue reached hard limit: " << max_ops_size_bytes_hard_;
      }
    }
    PeerMessage* msg = (*iter).second;
    messages_.erase(iter++);
    delete msg;
    metrics_.total_num_ops->Decrement();
    metrics_.num_all_done_ops->Decrement();
    metrics_.queue_size_bytes->DecrementBy((*iter).second->op_->ByteSize());
  }
  return Status::OK();
}

void PeerMessageQueue::DumpToLog() {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  LOG(INFO) << "Watermarks:";
  BOOST_FOREACH(const WatermarksMap::value_type& entry, watermarks_) {
    LOG(INFO) << "Peer: " << entry.first << " Watermark: "
      << (entry.second != NULL ? entry.second->ShortDebugString() : "NULL");
  }
  int counter = 0;
  LOG(INFO) << "Messages:";
  BOOST_FOREACH(const MessagesBuffer::value_type entry, messages_) {
    LOG(INFO) << "Message[" << counter++ << "]: " << entry.first.ShortDebugString()
      << " Status: " << entry.second->status_->ToString()
      << " Op: " << entry.second->op_->ShortDebugString();
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
  // a "consistent" snaphsot of the metrics.
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return Substitute("Consensus queue metrics: Total Ops: $0, All Done Ops: $1, "
      "Only Majority Done Ops: $2, In Progress Ops: $3, Queue Size (bytes): $4",
      metrics_.total_num_ops->value(), metrics_.num_all_done_ops->value(),
      metrics_.num_majority_done_ops->value(), metrics_.num_in_progress_ops->value(),
      metrics_.queue_size_bytes->value());
}

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

}  // namespace consensus
}  // namespace kudu

