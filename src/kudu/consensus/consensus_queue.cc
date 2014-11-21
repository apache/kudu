// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

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
  : num_majority_done_ops(INSTANTIATE_METRIC(METRIC_num_majority_done_ops)),
    num_in_progress_ops(INSTANTIATE_METRIC(METRIC_num_in_progress_ops)) {
}
#undef INSTANTIATE_METRIC

PeerMessageQueue::PeerMessageQueue(const MetricContext& metric_ctx,
                                   log::Log* log,
                                   const std::string& parent_tracker_id)
    : majority_size_(0),
      log_cache_(metric_ctx, log, parent_tracker_id),
      metrics_(metric_ctx) {

  queue_state_.current_term = MinimumOpId().term();
  queue_state_.committed_index = MinimumOpId();
  queue_state_.all_replicated_index = MinimumOpId();
  queue_state_.majority_replicated_index = MinimumOpId();
  queue_state_.state = kQueueConstructed;
}

void PeerMessageQueue::Init(const OpId& committed_index,
                            uint64_t current_term,
                            int majority_size) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  CHECK_EQ(queue_state_.state, kQueueConstructed);
  DCHECK_GE(majority_size, 0);
  CHECK(committed_index.IsInitialized());
  log_cache_.Init(committed_index);
  queue_state_.last_appended = committed_index;
  queue_state_.committed_index = committed_index;
  queue_state_.majority_replicated_index = committed_index;
  queue_state_.current_term = current_term;
  queue_state_.state = kQueueOpen;
  majority_size_ = majority_size;
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
  boost::unique_lock<simple_spinlock> lock(queue_lock_);
  if (queue_state_.state != kQueueOpen) {
    return Status::IllegalState("Queue closed");
  }

  ReplicateMsg* msg_ptr = DCHECK_NOTNULL(msg.get());

  if (msg_ptr->id().term() > queue_state_.current_term) {
    queue_state_.current_term = msg_ptr->id().term();
  }


  if (!log_cache_.AppendOperation(&msg)) {
    lock.unlock();
    if (PREDICT_FALSE((VLOG_IS_ON(2) || FLAGS_consensus_dump_queue_on_full))) {
      LOG(INFO) << "Queue Full. Can't Append: " << msg_ptr->id().ShortDebugString()
                << ". Dumping State:";
      log_cache_.DumpToLog();
    }
    return Status::ServiceUnavailable("Cannot append replicate message. Queue is full.");
  }

  queue_state_.last_appended = msg_ptr->id();
  UpdateMetrics();

  return Status::OK();
}

Status PeerMessageQueue::GetOpsFromCacheOrFallback(const OpId& op,
                                                   int64_t fallback_index,
                                                   int max_batch_size,
                                                   vector<ReplicateMsg*>* messages,
                                                   OpId* preceding_id) {

  OpId new_preceding;

  Status s = log_cache_.ReadOps(op.index(),
                                max_batch_size,
                                messages,
                                &new_preceding);
  // If we could get the index we wanted, but the terms were different or if
  // we couldn't get the index we wanter at all, try the fallback index.
  if ((s.ok() && op.term() != new_preceding.term()) || s.IsNotFound()) {
    messages->clear();
    new_preceding.Clear();
    s = log_cache_.ReadOps(fallback_index,
                           max_batch_size,
                           messages,
                           &new_preceding);
  }

  if (s.ok()) {
    DCHECK(new_preceding.IsInitialized());
    preceding_id->CopyFrom(new_preceding);
  }
  return s;
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

  // This is initialized to the queue's last appended op but gets set to the id of the
  // log entry preceding the first one in 'messages' if messages are found for the peer.
  OpId preceding_id = queue_state_.last_appended;

  // If we've never communicated with the peer, we don't know what messages to
  // send, so we'll send a status-only request. Otherwise, we grab requests
  // from the log starting at the last_received point.
  if (peer->peer_status.has_last_received()) {

    // The batch of messages to send to the peer.
    vector<ReplicateMsg*> messages;
    int max_batch_size = FLAGS_consensus_max_batch_size_bytes - request->ByteSize();

    // We try to get the peer's last received op. If that fails because the op
    // cannot be found in our log we fall back to the last committed index.
    Status s = GetOpsFromCacheOrFallback(peer->peer_status.last_received(),
                                         peer->peer_status.last_committed_idx(),
                                         max_batch_size,
                                         &messages,
                                         &preceding_id);

    if (PREDICT_FALSE(!s.ok() && !s.IsIncomplete())) {
      CHECK(messages.empty());
      LOG(DFATAL) << "Error while reading the log: " << s.ToString();
    }

    // We use AddAllocated rather than copy, because we pin the log cache at the
    // "all replicated" point. At some point we may want to allow partially loading
    // (and not pinning) earlier messages. At that point we'll need to do something
    // smarter here, like copy or ref-count.
    BOOST_FOREACH(ReplicateMsg* msg, messages) {
      request->mutable_ops()->AddAllocated(msg);
    }
    DCHECK_LE(request->ByteSize(), FLAGS_consensus_max_batch_size_bytes);
  }

  DCHECK(preceding_id.IsInitialized());
  request->mutable_preceding_id()->CopyFrom(preceding_id);
  request->mutable_committed_index()->CopyFrom(queue_state_.committed_index);
  request->set_caller_term(queue_state_.current_term);

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    if (request->ops_size() > 0) {
      VLOG(2) << "Sending request with operations to Peer: " << uuid
              << ". Size: " << request->ops_size()
              << ". From: " << request->ops(0).id().ShortDebugString() << ". To: "
              << request->ops(request->ops_size() - 1).id().ShortDebugString();
    } else {
      VLOG(2) << "Sending status only request to Peer: " << uuid
              << ": " << request->DebugString();
    }
  }
}

void PeerMessageQueue::AdvanceQueueWatermark(const char* type,
                                             OpId* watermark,
                                             const OpId& replicated_before,
                                             const OpId& replicated_after,
                                             int num_peers_required) {

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Updating " << type << " watermark: "
            << " peer changed from " << replicated_before << " to " << replicated_after;
  }

  // Update 'watermark', e.g. the commit index.
  // We look at the last 'watermark', this response may only impact 'watermark'
  // if it is currently lower than 'replicated_after'.
  if (watermark->index() < replicated_after.index()) {

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

    OpId new_watermark = *watermarks[watermarks.size() - num_peers_required];
    OpId old_watermark = *watermark;
    watermark->CopyFrom(new_watermark);

    VLOG(1) << "Updated " << type << " watermark "
            << "from " << old_watermark << " to " << new_watermark;
    if (VLOG_IS_ON(3)) {
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

  OpId updated_majority_replicated_index;
  {
    unique_lock<simple_spinlock> scoped_lock(&queue_lock_);

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
          peer->peer_status.set_last_committed_idx(status.last_committed_idx());
          if (status.last_received().index() < queue_state_.all_replicated_index.index()) {
            queue_state_.all_replicated_index = status.last_received();
          }
          *more_pending = true;
          return;
        }
        case ConsensusErrorPB::INVALID_TERM: {
          CHECK(response.has_responder_term());
          // TODO maybe we should notify the leader here, but it will find out eventually
          // anyway and right now it's simpler not to do it due to ownership/locking
          // issues.
          *more_pending = false;
          return;
        }
        default: {
          LOG(FATAL) << "Unexpected consensus error. Response: " << response.ShortDebugString();
        }
      }
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

    OpId old_last_received;
    if (!peer->peer_status.has_last_received()) {
      old_last_received = MinimumOpId();
    } else {
      old_last_received = peer->peer_status.last_received();
    }

    const OpId& new_last_received = response.status().last_received();

    peer->peer_status.mutable_last_received()->CopyFrom(new_last_received);
    peer->peer_status.set_last_committed_idx(response.status().last_committed_idx());

    // Advance the commit index.
    AdvanceQueueWatermark("majority_replicated",
                          &queue_state_.majority_replicated_index,
                          old_last_received,
                          new_last_received,
                          majority_size_);

    // Advance the all replicated index.
    AdvanceQueueWatermark("all_replicated",
                          &queue_state_.all_replicated_index,
                          old_last_received,
                          new_last_received,
                          watermarks_.size());

    updated_majority_replicated_index.CopyFrom(queue_state_.majority_replicated_index);

    *more_pending = log_cache_.HasOpIndex(new_last_received.index() + 1);

    log_cache_.SetPinnedOp(queue_state_.all_replicated_index.index());
    UpdateMetrics();
  }

  NotifyObserversOfMajorityReplOpChange(updated_majority_replicated_index);
}

OpId PeerMessageQueue::GetAllReplicatedIndexForTests() const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.all_replicated_index;
}

OpId PeerMessageQueue::GetCommittedIndexForTests() const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.committed_index;
}


void PeerMessageQueue::UpdateMetrics() {
  // Since operations have consecutive indices we can update the metrics based
  // on simple index math.
  metrics_.num_majority_done_ops->set_value(
      queue_state_.committed_index.index() -
      queue_state_.all_replicated_index.index());
  metrics_.num_in_progress_ops->set_value(
    queue_state_.last_appended.index() -
    queue_state_.committed_index.index());
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

  log_cache_.DumpToStrings(lines);
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

  log_cache_.DumpToHtml(out);
}

void PeerMessageQueue::ClearUnlocked() {
  log_cache_.Clear();
  STLDeleteValues(&watermarks_);
  queue_state_.current_term = MinimumOpId().term();
  queue_state_.committed_index = MinimumOpId();
  queue_state_.all_replicated_index = MinimumOpId();
  queue_state_.majority_replicated_index = MinimumOpId();
  queue_state_.state = kQueueConstructed;
}

void PeerMessageQueue::Close() {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  log_cache_.Close();
  ClearUnlocked();
  log_cache_.Close();
}

int64_t PeerMessageQueue::GetQueuedOperationsSizeBytesForTests() const {
  return log_cache_.BytesUsed();
}

string PeerMessageQueue::ToString() const {
  // Even though metrics are thread-safe obtain the lock so that we get
  // a "consistent" snapshot of the metrics.
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return ToStringUnlocked();
}

string PeerMessageQueue::ToStringUnlocked() const {
  return Substitute("Consensus queue metrics:"
                    "Only Majority Done Ops: $0, In Progress Ops: $1, Cache: $2",
                    metrics_.num_majority_done_ops->value(), metrics_.num_in_progress_ops->value(),
                    log_cache_.StatsString());
}

void PeerMessageQueue::RegisterObserver(PeerMessageQueueObserver* observer) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  observers_.push_back(observer);
}

Status PeerMessageQueue::UnRegisterObserver(PeerMessageQueueObserver* observer) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  std::vector<PeerMessageQueueObserver*>::iterator iter =
      std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    return Status::NotFound("Can't find observer.");
  }
  observers_.erase(iter);
  return Status::OK();
}

void PeerMessageQueue::NotifyObserversOfMajorityReplOpChange(
    const OpId& new_majority_replicated_op) {
  std::vector<PeerMessageQueueObserver*> copy;
  {
    boost::lock_guard<simple_spinlock> lock(queue_lock_);
    copy = observers_;
  }

  // TODO move commit index advancement here so that the queue is not dependent on
  // consensus at all, but that requires a bit more work.
  OpId new_committed_index;
  BOOST_FOREACH(PeerMessageQueueObserver* observer, copy) {
    observer->UpdateMajorityReplicated(new_majority_replicated_op, &new_committed_index);
  }

  {
    boost::lock_guard<simple_spinlock> lock(queue_lock_);
    if (new_committed_index.IsInitialized() &&
        new_committed_index.index() > queue_state_.committed_index.index()) {
      queue_state_.committed_index.CopyFrom(new_committed_index);
    }
  }
}

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

}  // namespace consensus
}  // namespace kudu
