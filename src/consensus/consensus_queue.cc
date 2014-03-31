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

#include "common/wire_protocol.h"
#include "util/auto_release_pool.h"

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
using metadata::QuorumPeerPB;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using strings::Substitute;

PeerMessage::PeerMessage(gscoped_ptr<OperationPB> op,
                         const scoped_refptr<OperationStatus>& status)
    : op_(op.Pass()),
      status_(status) {
}

PeerMessageQueue::PeerMessageQueue()
    : queued_ops_size_bytes_(0),
      // TODO manage these on a server-wide basis
      max_ops_size_bytes_soft_(FLAGS_consensus_entry_cache_size_soft_limit_mb * 1024 * 1024),
      max_ops_size_bytes_hard_(FLAGS_consensus_entry_cache_size_hard_limit_mb * 1024 * 1024),
      state_(kQueueOpen) {
}

Status PeerMessageQueue::TrackPeer(const string& uuid, const OpId& initial_watermark) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  // TODO allow the queue to go and fetch requests from the log
  // up to a point.
  DCHECK(initial_watermark.IsInitialized());
  ConsensusStatusPB* status = new ConsensusStatusPB();
  status->mutable_committed_watermark()->CopyFrom(initial_watermark);
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
                                         scoped_refptr<OperationStatus> status) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(state_, kQueueOpen);
  DCHECK(operation->has_commit()
         || operation->has_replicate()) << "Operation must be a commit or a replicate: "
             << operation->DebugString();
  queued_ops_size_bytes_ += operation->ByteSize();
  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Appended operation to queue: " << operation->ShortDebugString() <<
        " Operation Status: " << status->ToString();
  }
  PeerMessage* msg = new PeerMessage(operation.Pass(), status);
  InsertOrDieNoPrint(&messages_, msg->GetOpId(), msg);

  if (queued_ops_size_bytes_ < max_ops_size_bytes_soft_) {
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
                                           current_status->committed_watermark(),
                                           CompareOps);
  iter = messages_.upper_bound(*lowest_watermark);

  MessagesBuffer::iterator end_iter = messages_.upper_bound(new_status.received_watermark());

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Received Response from Peer: " << uuid << ". Current Watermark: "
        << (*iter).first.ShortDebugString() << ". New Status: " << new_status.ShortDebugString();
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
    if (msg->op_->has_commit() &&
        CompareOps(msg->op_->id(), current_status->committed_watermark()) > 0 &&
        CompareOps(msg->op_->id(), new_status.committed_watermark()) <= 0) {
      msg->status_->AckPeer(uuid);
      continue;
    }
    if (msg->op_->has_replicate() &&
        CompareOps(msg->op_->id(), current_status->replicated_watermark()) > 0 &&
        CompareOps(msg->op_->id(), new_status.replicated_watermark()) <= 0) {
      msg->status_->AckPeer(uuid);
      continue;
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

Status PeerMessageQueue::TrimBuffer() {
  // TODO for now we're just trimming the buffer, but we need to handle when
  // the buffer is full but there is a peer hanging on to the queue (very delayed)
  MessagesBuffer::iterator iter = messages_.begin();
  while (queued_ops_size_bytes_ > max_ops_size_bytes_soft_ && iter != messages_.end()) {
    PeerMessage* message = (*iter).second;
    if (!message->status_->IsAllDone()) {
      if (queued_ops_size_bytes_ < max_ops_size_bytes_hard_) {
        return Status::OK();
      } else {
        LOG(FATAL) << "Queue reached hard limit: " << max_ops_size_bytes_hard_;
      }
    }
    queued_ops_size_bytes_ -= (*iter).second->op_->ByteSize();
    PeerMessage* msg = (*iter).second;
    messages_.erase(iter++);
    delete msg;
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

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

}  // namespace consensus
}  // namespace kudu

