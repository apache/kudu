// Copyright (c) 2013, Cloudera, inc.

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
      max_ops_size_bytes_hard_(FLAGS_consensus_entry_cache_size_hard_limit_mb * 1024 * 1024) {
}

Status PeerMessageQueue::TrackPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  // TODO allow the queue to go and fetch requests from the log
  // up to a point.
  InsertOrDie(&watermarks_, uuid, NULL);
  return Status::OK();
}

Status PeerMessageQueue::TrackPeer(const string& uuid, const OpId& replicated_watermark) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  // TODO allow the queue to go and fetch requests from the log
  // up to a point.
  DCHECK(replicated_watermark.IsInitialized());
  InsertOrDie(&watermarks_, uuid, new OpId(replicated_watermark));
  return Status::OK();
}

void PeerMessageQueue::UntrackPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  OpId* op = EraseKeyReturnValuePtr(&watermarks_, uuid);
  delete DCHECK_NOTNULL(op);
}

Status PeerMessageQueue::AppendOperation(gscoped_ptr<OperationPB> operation,
                                         scoped_refptr<OperationStatus> status) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  queued_ops_size_bytes_ += operation->ByteSize();
  PeerMessage* msg = new PeerMessage(operation.Pass(), status);
  InsertOrDieNoPrint(&messages_, msg->GetOpId(), msg);

  if (queued_ops_size_bytes_ < max_ops_size_bytes_soft_) {
    return Status::OK();
  }
  // TODO trim the buffer before we add to the queue
  return TrimBuffer();
}

Status PeerMessageQueue::RequestForPeer(const string& uuid,
                                        ConsensusRequestPB* request) {
  // Clear the requests without deleting the entries, as they may be in use by other peers.
  // TODO consider adding an utility for this.
  request->mutable_ops()->ExtractSubrange(0, request->ops_size(), NULL);
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  OpId* peer_watermark = FindPtrOrNull(watermarks_, uuid);

  MessagesBuffer::iterator iter;
  if (!peer_watermark) {
    iter = messages_.begin();
  } else {
    iter = messages_.upper_bound(*peer_watermark);
  }

  // Add as many operations as we can to a request.
  PeerMessage* msg = NULL;
  for (; iter != messages_.end(); iter++) {
    msg = (*iter).second;
    request->mutable_ops()->AddAllocated(msg->op_.get());
    if (request->ByteSize() > FLAGS_consensus_max_batch_size_bytes) {
      request->mutable_ops()->ReleaseLast();
      break;
    }
  }
  if (VLOG_IS_ON(2)) {
    if (request->ops_size() > 0) {
      VLOG(2) << "Sending request with operations to Peer: " << uuid
          << ". Size: " << request->ops_size()
          << ". From: " << request->ops(0).ShortDebugString() << ". To: "
          << request->ops(request->ops_size() -1).ShortDebugString();
    } else {
      VLOG(2) << "Sending status only request to Peer: " << uuid;
    }
  }
  return Status::OK();
}

// TODO do peer wise queue cleanup
// TODO track peer commits
// TODO prob only ack commits when the peer is done committing them
void PeerMessageQueue::ResponseFromPeer(const string& uuid,
                                        const ConsensusStatusPB& status,
                                        bool* more_pending) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  OpId* peer_watermark = FindPtrOrNull(watermarks_, uuid);

  MessagesBuffer::iterator iter;
  if (!peer_watermark) {
    iter = messages_.begin();
  } else {
    iter = messages_.lower_bound(*peer_watermark);
  }

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Received Response from Peer: " << uuid << ". Current Watermark: "
        << (*iter).first.ShortDebugString() << ". Status: " << status.ShortDebugString();
  }

  MessagesBuffer::iterator end_iter = messages_.upper_bound(status.replicated_watermark());

  // ack the messages this peer just processed
  PeerMessage* msg = NULL;
  for (;iter != end_iter; iter++) {
    msg = (*iter).second;
    msg->status_.get()->AckPeer(uuid);
  }

  if (peer_watermark == NULL) {
    InsertOrUpdate(&watermarks_, uuid, new OpId(status.replicated_watermark()));
  } else {
    peer_watermark->CopyFrom(status.replicated_watermark());
  }

  // check if there are more messages pending
  *more_pending = (iter != messages_.end());
}

Status PeerMessageQueue::TrimBuffer() {
  // TODO for now we're just trimming the buffer, but we need to handle when
  // the buffer is full but there is a peer hanging on to the queue (very delayed)
  MessagesBuffer::iterator iter = messages_.begin();
  while (queued_ops_size_bytes_ > max_ops_size_bytes_soft_ && iter != messages_.end()) {
    PeerMessage* message = (*iter).second;
    if (!message->status_->IsDone()) {
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
  LOG(INFO)<< "Watermarks:";
  BOOST_FOREACH(const WatermarksMap::value_type& entry, watermarks_) {
    LOG(INFO) << "Peer: " << entry.first << " Watermark: "
      << (entry.second != NULL ? entry.second->ShortDebugString() : "NULL");
  }
  LOG(INFO)<< "Messages:";
  BOOST_FOREACH(const MessagesBuffer::value_type entry, messages_) {
    LOG(INFO) << "Message: " << entry.first.ShortDebugString()
      << " Status: " << entry.second->status_->ToString()
      << " Op: " << entry.second->op_->ShortDebugString();
  }
}

PeerMessageQueue::~PeerMessageQueue() {
  STLDeleteValues(&messages_);
  STLDeleteValues(&watermarks_);
}

}  // namespace consensus
}  // namespace kudu

