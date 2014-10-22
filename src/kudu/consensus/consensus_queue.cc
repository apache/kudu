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
#include "kudu/consensus/async_log_reader.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
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

using log::AsyncLogReader;
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
    : majority_size_(0),
      consensus_(DCHECK_NOTNULL(consensus)),
      max_ops_size_bytes_hard_(FLAGS_consensus_entry_cache_size_hard_limit_mb * 1024 * 1024),
      global_max_ops_size_bytes_hard_(
          FLAGS_global_consensus_entry_cache_size_hard_limit_mb * 1024 * 1024),
      metrics_(metric_ctx) {
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

  queue_state_.current_term = MinimumOpId().term();
  queue_state_.committed_index = MinimumOpId();
  queue_state_.all_replicated_index = MinimumOpId();
  queue_state_.state = kQueueConstructed;
}

void PeerMessageQueue::Init(const OpId& committed_index,
                            uint64_t current_term,
                            int majority_size) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  CHECK_EQ(queue_state_.state, kQueueConstructed);
  DCHECK_GE(majority_size, 0);
  CHECK(committed_index.IsInitialized());
  queue_state_.committed_index = committed_index;
  queue_state_.current_term = current_term;
  queue_state_.preceding_first_op_in_queue = committed_index;
  queue_state_.state = kQueueOpen;
  majority_size_ = majority_size;
  async_reader_.reset(new AsyncLogReader(consensus_->log()->GetLogReader()));
}

Status PeerMessageQueue::TrackPeer(const string& uuid) {
  CHECK(!uuid.empty()) << "Got request to track peer with empty UUID";
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(queue_state_.state, kQueueOpen);
  // TODO allow the queue to go and fetch requests from the log
  // up to a point.

  // We leave the peer's last_received watermark unitialized, we'll
  // initialize it when we get the first response back from the peer.
  TrackedPeer* tracked_peer = new TrackedPeer(uuid);
  tracked_peer->uuid = uuid;
  InsertOrDie(&watermarks_, uuid, tracked_peer);
  queue_state_.all_replicated_index = MinimumOpId();
  return Status::OK();
}

void PeerMessageQueue::UntrackPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  TrackedPeer* peer = EraseKeyReturnValuePtr(&watermarks_, uuid);
  if (peer != NULL) {
    delete peer;
  }
}

Status PeerMessageQueue::AppendOperation(gscoped_ptr<ReplicateMsg> msg) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(queue_state_.state, kQueueOpen);

  ReplicateMsg* msg_ptr = DCHECK_NOTNULL(msg.get());

  // Before we change the queue's term, in debug mode, check that the indexes
  // in the queue are consecutive.
  DCHECK_EQ(GetLastOp().index() + 1, msg_ptr->id().index())
    << "Last op in the queue: " << GetLastOp().ShortDebugString()
    << " operation being appended: " << msg_ptr->id().ShortDebugString();

  // Check that terms are monotonically increasing
  DCHECK_GE(msg_ptr->id().term(), queue_state_.current_term);
  if (msg_ptr->id().term() > queue_state_.current_term) {
    queue_state_.current_term = msg_ptr->id().term();
  }

  // Once either the local or global soft limit is exceeded...
  if (tracker_->AnyLimitExceeded()) {
    // .. try to trim the queue.
    Status s;
    if (WouldHardLimitBeViolated(msg_ptr->SpaceUsed())) {
      s = Status::ServiceUnavailable("Cannot append replicate message. Queue is full.");
    }
    if (PREDICT_FALSE(!s.ok() && (VLOG_IS_ON(2) || FLAGS_consensus_dump_queue_on_full))) {
      queue_lock_.unlock();
      LOG(INFO) << "Queue Full. Can't Append: " << msg_ptr->id().ShortDebugString()
          << ". Dumping State:";
      vector<string>  queue_dump;
      DumpToStringsUnlocked(&queue_dump);
      BOOST_FOREACH(const string& line, queue_dump) {
        LOG(INFO) << line;
      }
    }
    RETURN_NOT_OK(s);
  }

  InsertOrDieNoPrint(&messages_, msg_ptr->id(), msg.release());

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Appended REPLICATE to queue: " << msg_ptr->ShortDebugString();
  }

  metrics_.queue_size_bytes->IncrementBy(msg_ptr->SpaceUsed());
  tracker_->Consume(msg_ptr->SpaceUsed());
  metrics_.total_num_ops->Increment();
  metrics_.num_in_progress_ops->Increment();
  return Status::OK();
}

void PeerMessageQueue::EntriesLoadedCallback(const Status& status,
                                             const vector<ReplicateMsg*>& replicates,
                                             const OpId& new_preceding_first_op_in_queue) {

  // TODO deal with errors when loading operations.
  CHECK_OK(status);

  // OK, we're all done, we can now bulk load the operations into the queue.

  // Note that we don't check queue limits. Were we to stop adding operations
  // in the middle of the sequence the queue would have holes so it is possible
  // that we're breaking queue limits right here.

  // TODO enforce some sort of limit on how much can be loaded from disk
  {
    boost::lock_guard<simple_spinlock> lock(queue_lock_);
    // We only actually append to the queue if it is still open.
    if (queue_state_.state != PeerMessageQueue::kQueueOpen) {
      // .. otherwise we delete the messages
      BOOST_FOREACH(ReplicateMsg* replicate, replicates) {
        delete replicate;
      }

      LOG(WARNING) << "Tried to load operations from disk but queue was not open.";
      return;
    }
    BOOST_FOREACH(ReplicateMsg* replicate, replicates) {
      InsertOrDieNoPrint(&messages_, replicate->id(), replicate);
      tracker_->Consume(replicate->SpaceUsed());
    }

    CHECK(OpIdEquals(replicates.back()->id(),
                     queue_state_.preceding_first_op_in_queue))
      << "Expected: " << queue_state_.preceding_first_op_in_queue.ShortDebugString()
      << " got: " << replicates.back()->id();

    queue_state_.preceding_first_op_in_queue = new_preceding_first_op_in_queue;
    queue_state_.all_replicated_index = new_preceding_first_op_in_queue;
    UpdateMetrics();
    LOG(INFO) << "Loaded operations in the queue form: "
        << replicates.front()->id().ShortDebugString()
        << " to: " << replicates.back()->id().ShortDebugString()
        << " for a total of: " << replicates.size();
  }
}

void PeerMessageQueue::RequestForPeer(const string& uuid,
                                      ConsensusRequestPB* request) {
  // Clear the requests without deleting the entries, as they may be in use by other peers.
  request->mutable_ops()->ExtractSubrange(0, request->ops_size(), NULL);
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  DCHECK_EQ(queue_state_.state, kQueueOpen);
  const TrackedPeer* peer;
  if (PREDICT_FALSE(!FindCopy(watermarks_, uuid, &peer))) {
    LOG(FATAL) << "Unable to find peer with UUID " << uuid
               << ". Queue status: " << ToStringUnlocked();
  }

  request->mutable_committed_index()->CopyFrom(queue_state_.committed_index);
  request->set_caller_term(queue_state_.current_term);

  // If the peer's watermark is not initialized we set 'last_received'
  // to the last operation in the queue, as per the raft paper. In this
  // case this will end up being a status-only request.
  OpId last_received;
  if (!peer->peer_status.has_last_received()) {
    last_received.CopyFrom(GetLastOp());
  } else {
    last_received.CopyFrom(peer->peer_status.last_received());
  }

  // If the messages the peer needs haven't been loaded into the queue yet,
  // load them.
  if (OpIdCompare(last_received,
                  queue_state_.preceding_first_op_in_queue) < 0) {
    Status status = async_reader_->EnqueueAsyncRead(last_received,
                                                    queue_state_.preceding_first_op_in_queue,
                                                    Bind(&PeerMessageQueue::EntriesLoadedCallback,
                                                         Unretained(this)));
    // The loader is already loading something, or if the enqueue was successful
    // just send a status only.
    if (status.ok() || status.IsAlreadyPresent()) {
      request->mutable_preceding_id()->CopyFrom(peer->peer_status.last_received());
      return;
    }
    LOG(FATAL) << "Unimplemented: Unable to load operations into the queue for peer: "
        << uuid << ". Status: " << status.ToString();
  }

  // We don't actually start sending on 'lower_bound' but we seek to
  // it to get the preceding_id.
  MessagesBuffer::const_iterator iter = messages_.lower_bound(last_received);

  // If "lower_bound" points to the beginning of the queue and it is not the exact same as the
  // 'peer_status.last_received()' it means we're sending a batch including the very first message
  // in the queue, meaning we'll have to use to 'preceeding_first_op_in_queue_' to get the
  // 'preceding_id'.
  if (messages_.empty() ||
      (iter == messages_.begin() && !OpIdEquals((*iter).first, last_received))) {
    request->mutable_preceding_id()->CopyFrom(queue_state_.preceding_first_op_in_queue);
  // ... otherwise 'preceding_id' is the first element in the iterator and we start sending
  // on the element after that.
  } else {
    request->mutable_preceding_id()->CopyFrom((*iter).first);
    iter++;
  }

  // Add as many operations as we can to a request.
  ReplicateMsg* msg = NULL;
  for (; iter != messages_.end(); iter++) {
    msg = (*iter).second;

    request->mutable_ops()->AddAllocated(msg);
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

void PeerMessageQueue::AdvanceQueueWatermark(OpId* watermark,
                                             const OpId& replicated_before,
                                             const OpId& replicated_after,
                                             int num_peers_required) {

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Updating watermark: " << watermark->ShortDebugString()
        << "\nPeer last: " << replicated_before.ShortDebugString()
        << " Peer current: " << replicated_after.ShortDebugString()
        << "\nNum Peers required: " << num_peers_required;
  }

  // Update 'watermark', e.g. the commit index.
  // We look at the last 'watermark', this response may only impact 'watermark'
  // if it is currently lower than 'replicated_after'.
  if (OpIdLessThan(*watermark, replicated_after)) {

    // Go through the peer's watermarks, we want the highest watermark that
    // 'num_peers_required' of peers has replicated. To find this we do the
    // following:
    // - Store all the peer's 'last_received' in a vector
    // - Sort the vector
    // - Find the vector.size() - 'num_peers_required' position, this
    //   will be the new 'watermark'.
    OpId min = MinimumOpId();
    vector<const OpId*> watermarks;
    BOOST_FOREACH(const WatermarksMap::value_type& peer, watermarks_) {
      ConsensusStatusPB* status = &peer.second->peer_status;
      if (status->has_last_received()) {
        watermarks.push_back(&status->last_received());
      } else {
        watermarks.push_back(&min);
      }
    }
    std::sort(watermarks.begin(), watermarks.end(), OpIdLessThanPtrFunctor());
    watermark->CopyFrom(*watermarks[watermarks.size() - num_peers_required]);

    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Updated watermark. New value: " << watermark->ShortDebugString();
      VLOG(1) << "Peers: ";
      BOOST_FOREACH(const WatermarksMap::value_type& peer, watermarks_) {
        VLOG(1) << "Peer: " << peer.first  << " Watermark: "
            << peer.second->peer_status.ShortDebugString();
      }
      VLOG(1) << "Sorted watermarks:";
      BOOST_FOREACH(const OpId* watermark, watermarks) {
        VLOG(1) << "Watermark: " << watermark->ShortDebugString();
      }
    }
  }

}

void PeerMessageQueue::ResponseFromPeer(const ConsensusResponsePB& response,
                                        bool* more_pending) {
  CHECK(response.has_responder_uuid() && !response.responder_uuid().empty())
      << "Got response from peer with empty UUID";
  DCHECK(response.IsInitialized()) << "Response: " << response.ShortDebugString();

  OpId updated_commit_index;
  {
    boost::lock_guard<simple_spinlock> lock(queue_lock_);

    TrackedPeer* peer = FindPtrOrNull(watermarks_, response.responder_uuid());
    if (PREDICT_FALSE(queue_state_.state == kQueueClosed || peer == NULL)) {
      LOG(WARNING) << "Queue is closed or peer was untracked, disregarding peer response."
          << " Response: " << response.ShortDebugString();
      *more_pending = false;
      return;
    }

    // Sanity checks.
    // Some of these can be eventually removed, but they are handy for now.

    // Application level errors should be handled elsewhere
    DCHECK(!response.has_error());
    // Responses should always have a status.
    DCHECK(response.has_status());

    const ConsensusStatusPB& status = response.status();

    if (PREDICT_FALSE(status.has_error())) {
      switch (status.error().code()) {
        case ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH: {
          if (peer->peer_status.has_last_received()) {
            LOG(INFO) << "Peer: " << response.responder_uuid() << " replied that the "
                "Log Matching Property check failed. Queue's last received watermark for peer: "
                << peer->peer_status.last_received().ShortDebugString()
                << ". Peer's actual last received watermark: "
                << status.last_received().ShortDebugString();
          } else {
            // That's currently how we can detect that we able to connect to a peer.
            LOG(INFO) << "Connected to peer: " << response.responder_uuid()
                << " whose last received watermark is: "
                << status.last_received().ShortDebugString();
          }
          peer->peer_status.mutable_last_received()->CopyFrom(status.last_received());
          if (OpIdLessThan(status.last_received(), queue_state_.all_replicated_index)) {
            queue_state_.all_replicated_index.CopyFrom(status.last_received());
          }
          *more_pending = true;
          return;
        }
        case ConsensusErrorPB::INVALID_TERM: {
          CHECK(response.has_responder_term());
          consensus_->NotifyTermChange(response.responder_term());
          *more_pending = true;
          return;
        }
        default: {
          LOG(FATAL) << "Unexpected consensus error. Response: " << response.ShortDebugString();
        }
      }
    }

    // If the peer's last received id is less than or equal to the queue's preceding we
    // should have triggered queue loading before.
    if (OpIdCompare(response.status().last_received(),
                    queue_state_.preceding_first_op_in_queue) < 0) {
      CHECK(async_reader_->IsReading());
      CHECK(OpIdEquals(peer->peer_status.last_received(),
                       response.status().last_received()));
      // Ok we're still loading. We set more_pending to false so that
      // there queue/peer don't spin back and forth. Likely when there's
      // a new heartbeat to the peer the messages will be available.
      *more_pending = false;
      return;
    }

    // The peer must have responded with a term that is greater than or equal to
    // the last known term for that peer.
    peer->CheckMonotonicTerms(response.responder_term());

    // If the responder didn't send an error back that must mean that it has
    // the same term as ourselves.
    CHECK_EQ(response.responder_term(), queue_state_.current_term);

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Received Response from Peer: " << response.responder_uuid()
          << ". Current Status: " << peer->peer_status.ShortDebugString()
          << ". Response: " << response.ShortDebugString();
    }

    OpId last_received;
    if (!peer->peer_status.has_last_received()) {
      last_received = MinimumOpId();
    } else {
      last_received = peer->peer_status.last_received();
    }

    peer->peer_status.mutable_last_received()->CopyFrom(response.status().last_received());

    // Advance the commit index.
    AdvanceQueueWatermark(&queue_state_.committed_index,
                          last_received,
                          response.status().last_received(),
                          majority_size_);

    // Advance the all replicated index.
    AdvanceQueueWatermark(&queue_state_.all_replicated_index,
                          last_received,
                          response.status().last_received(),
                          watermarks_.size());

    updated_commit_index.CopyFrom(queue_state_.committed_index);
    *more_pending = !OpIdEquals(response.status().last_received(), GetLastOp());

    TrimBuffer();
    UpdateMetrics();
  }

  // It's OK that we do this without the lock as this is idempotent.
  consensus_->UpdateCommittedIndex(updated_commit_index);
}

OpId PeerMessageQueue::GetAllReplicatedIndexForTests() const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.all_replicated_index;
}

OpId PeerMessageQueue::GetCommittedIndexForTests() const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.committed_index;
}

void PeerMessageQueue::TrimBuffer() {
  MessagesBuffer::iterator iter = messages_.begin();

  VLOG(1) << "Trimming buffer. Before stats: " << ToStringUnlocked();
  while (iter != messages_.end() &&
      OpIdCompare((*iter).first, queue_state_.all_replicated_index) <= 0) {
    ReplicateMsg* msg = (*iter).second;
    metrics_.queue_size_bytes->DecrementBy(msg->SpaceUsed());
    queue_state_.preceding_first_op_in_queue.CopyFrom((*iter).first);
    tracker_->Release(msg->SpaceUsed());
    VLOG(1) << "Trimming queue. Deleting: " << msg->id().ShortDebugString();
    delete msg;
    messages_.erase(iter++);
  }
  VLOG(1) << "Trimming buffer. After stats: " << ToStringUnlocked();
}

void PeerMessageQueue::UpdateMetrics() {
  // Since operations have consecutive indices we can update the metrics based
  // on simple index math.
  metrics_.total_num_ops->set_value(messages_.size());
  metrics_.num_all_done_ops->set_value(
      queue_state_.all_replicated_index.index() -
      queue_state_.preceding_first_op_in_queue.index());
  metrics_.num_majority_done_ops->set_value(
      queue_state_.committed_index.index() -
      queue_state_.preceding_first_op_in_queue.index());
  metrics_.num_in_progress_ops->set_value(
      messages_.empty() ? 0 :
          (*messages_.rend()).first.index() -
          queue_state_.committed_index.index());
}


bool PeerMessageQueue::WouldHardLimitBeViolated(size_t bytes) const {
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
  return local_limit_violated || global_limit_violated;
}

const OpId& PeerMessageQueue::GetLastOp() const {
  return messages_.empty() ? queue_state_.committed_index : (*messages_.rbegin()).first;
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
    const OpId& id = entry.second->id();
    ReplicateMsg* msg = entry.second;
    lines->push_back(
      Substitute("Message[$0] $1.$2 : REPLICATE. Type: $3, Size: $4, Id: $5",
                 counter++, id.term(), id.index(),
                 OperationType_Name(msg->op_type()),
                 msg->ByteSize(), entry.second->id().ShortDebugString()));
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
    const OpId& id = entry.second->id();
    ReplicateMsg* msg = entry.second;
    out << Substitute("<tr><th>$0</th><th>$1.$2</th><td>REPLICATE $3</td>"
                      "<td>$4</td><td>$5</td></tr>",
                      counter++, id.term(), id.index(),
                      OperationType_Name(msg->op_type()),
                      msg->ByteSize(), entry.second->id().ShortDebugString()) << endl;
  }
  out << "</table>";
}

void PeerMessageQueue::Clear() {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  ClearUnlocked();
}

void PeerMessageQueue::ClearUnlocked() {
  STLDeleteValues(&messages_);
  STLDeleteValues(&watermarks_);
}

void PeerMessageQueue::Close() {
  if (async_reader_) async_reader_->Shutdown();
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  queue_state_.state = kQueueClosed;
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
