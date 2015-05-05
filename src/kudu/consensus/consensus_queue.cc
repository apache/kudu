// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/consensus_queue.h"

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <gflags/gflags.h>
#include <iostream>
#include <string>
#include <tr1/memory>
#include <utility>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/url-coding.h"

DEFINE_int32(consensus_max_batch_size_bytes, 1024 * 1024,
             "The maximum per-tablet RPC batch size when updating peers.");

namespace kudu {
namespace consensus {

using log::AsyncLogReader;
using log::Log;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using strings::Substitute;

METRIC_DEFINE_gauge_int64(tablet, majority_done_ops, "Leader Operations Acked by Majority",
                          MetricUnit::kOperations,
                          "Number of operations in the leader queue ack'd by a majority but "
                          "not all peers.");
METRIC_DEFINE_gauge_int64(tablet, in_progress_ops, "Leader Operations in Progress",
                          MetricUnit::kOperations,
                          "Number of operations in the leader queue ack'd by a minority of "
                          "peers.");

std::string PeerMessageQueue::TrackedPeer::ToString() const {
  return Substitute("Peer: $0, New: $1, Last received: $2, Last sent $3, "
      "Last known committed idx: $4 Last exchange result: $5", uuid, is_new,
      OpIdToString(log_tail), OpIdToString(last_received), last_known_committed_idx,
      is_last_exchange_successful ? "SUCCESS" : "ERROR");
}

#define INSTANTIATE_METRIC(x) \
  x.Instantiate(metric_entity, 0)
PeerMessageQueue::Metrics::Metrics(const scoped_refptr<MetricEntity>& metric_entity)
  : num_majority_done_ops(INSTANTIATE_METRIC(METRIC_majority_done_ops)),
    num_in_progress_ops(INSTANTIATE_METRIC(METRIC_in_progress_ops)) {
}
#undef INSTANTIATE_METRIC

PeerMessageQueue::PeerMessageQueue(const scoped_refptr<MetricEntity>& metric_entity,
                                   const scoped_refptr<log::Log>& log,
                                   const string& local_uuid,
                                   const string& tablet_id,
                                   const shared_ptr<MemTracker>& parent_mem_tracker)
    : local_uuid_(local_uuid),
      tablet_id_(tablet_id),
      log_cache_(metric_entity, log, local_uuid, tablet_id, parent_mem_tracker),
      metrics_(metric_entity) {
  queue_state_.current_term = MinimumOpId().term();
  queue_state_.committed_index = MinimumOpId();
  queue_state_.all_replicated_opid = MinimumOpId();
  queue_state_.majority_replicated_opid = MinimumOpId();
  queue_state_.state = kQueueConstructed;
  queue_state_.mode = NON_LEADER;
  queue_state_.majority_size_ = 1;
  CHECK_OK(ThreadPoolBuilder("queue-observers-pool").set_max_threads(1).Build(&observers_pool_));
}

void PeerMessageQueue::Init(const OpId& last_locally_replicated) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  CHECK_EQ(queue_state_.state, kQueueConstructed);
  log_cache_.Init(last_locally_replicated);
  queue_state_.last_appended = last_locally_replicated;
  queue_state_.state = kQueueOpen;
  TrackPeerUnlocked(local_uuid_);
}

void PeerMessageQueue::SetLeaderMode(const OpId& committed_index,
                                     uint64_t current_term,
                                     int majority_size) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  CHECK(committed_index.IsInitialized());
  queue_state_.current_term = current_term;
  queue_state_.committed_index = committed_index;
  queue_state_.majority_replicated_opid = committed_index;
  queue_state_.majority_size_ = majority_size;
  queue_state_.mode = LEADER;

  LOG_WITH_PREFIX_UNLOCKED(INFO) << " queue going to LEADER mode. State: "
      << queue_state_.ToString();
}

void PeerMessageQueue::SetNonLeaderMode() {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  queue_state_.mode = NON_LEADER;
  queue_state_.majority_size_ = 1;
  LOG_WITH_PREFIX_UNLOCKED(INFO) << " queue going to NON_LEADER mode. State: "
      << queue_state_.ToString();
}

void PeerMessageQueue::TrackPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  TrackPeerUnlocked(uuid);
}


void PeerMessageQueue::TrackPeerUnlocked(const string& uuid) {
  CHECK(!uuid.empty()) << "Got request to track peer with empty UUID";
  DCHECK_EQ(queue_state_.state, kQueueOpen);

  TrackedPeer* tracked_peer = new TrackedPeer(uuid);
  tracked_peer->uuid = uuid;
  // We don't know the last operation received by the peer so, following
  // the raft protocol, we set it to the queue's last appended operation.
  // If this is not the last operation the peer has received it will reset
  // it accordingly.
  tracked_peer->log_tail = queue_state_.last_appended;
  InsertOrDie(&peers_map_, uuid, tracked_peer);

  // We don't know how far back this peer is, so set the all replicated watermark to
  // MinimumOpId. We'll advance it when we know how far along the peer is.
  queue_state_.all_replicated_opid = MinimumOpId();
}

void PeerMessageQueue::UntrackPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  TrackedPeer* peer = EraseKeyReturnValuePtr(&peers_map_, uuid);
  if (peer != NULL) {
    delete peer;
  }
}

void PeerMessageQueue::LocalPeerAppendFinished(const OpId& id,
                                               const StatusCallback& callback,
                                               const Status& status) {
  CHECK_OK(status);

  // Fake an RPC response from the local peer.
  // TODO: we should probably refactor the ResponseFromPeer function
  // so that we don't need to construct this fake response, but this
  // seems to work for now.
  ConsensusResponsePB fake_response;
  fake_response.set_responder_uuid(local_uuid_);
  fake_response.mutable_status()->mutable_last_received()->CopyFrom(id);
  {
    boost::unique_lock<simple_spinlock> lock(queue_lock_);
    fake_response.mutable_status()->set_last_committed_idx(queue_state_.committed_index.index());
  }
  bool junk;
  ResponseFromPeer(id, fake_response, &junk);

  callback.Run(status);
}

Status PeerMessageQueue::AppendOperation(const ReplicateRefPtr& msg) {
  return AppendOperations(boost::assign::list_of(msg), Bind(DoNothingStatusCB));
}

Status PeerMessageQueue::AppendOperations(const vector<ReplicateRefPtr>& msgs,
                                          const StatusCallback& log_append_callback) {

  DFAKE_SCOPED_LOCK(append_fake_lock_);
  boost::unique_lock<simple_spinlock> lock(queue_lock_);

  OpId last_id = msgs.back()->get()->id();

  if (last_id.term() > queue_state_.current_term) {
    queue_state_.current_term = last_id.term();
  }

  // Unlock ourselves during Append to prevent a deadlock: it's possible that
  // the log buffer is full, in which case AppendOperations would block. However,
  // for the log buffer to empty, it may need to call LocalPeerAppendFinished()
  // which also needs queue_lock_.
  lock.unlock();
  RETURN_NOT_OK(log_cache_.AppendOperations(msgs,
                                            Bind(&PeerMessageQueue::LocalPeerAppendFinished,
                                                 Unretained(this),
                                                 last_id,
                                                 log_append_callback)));
  lock.lock();
  queue_state_.last_appended = last_id;
  UpdateMetrics();

  return Status::OK();
}

Status PeerMessageQueue::GetOpsFromCacheOrFallback(const OpId& op,
                                                   int64_t fallback_index,
                                                   int max_batch_size,
                                                   vector<ReplicateRefPtr>* messages,
                                                   OpId* preceding_id) {

  OpId new_preceding;

  Status s = log_cache_.ReadOps(op.index(),
                                max_batch_size,
                                messages,
                                &new_preceding);
  // If we could get the index we wanted, but the terms were different or if
  // we couldn't get the index we wanter at all, try the fallback index.
  if ((s.ok() && op.term() != new_preceding.term()) || s.IsNotFound()) {
    if (s.ok()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Tried to read ops starting at " << op << " from cache, "
                            << "but found op " << new_preceding << " (term mismatch). "
                            << "Falling back to index " << fallback_index;
    } else {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Tried to read ops starting at " << op << " from cache, "
                            << "but could not find that index in the log. "
                            << "Falling back to index " << fallback_index;
    }
    messages->clear();
    new_preceding.Clear();
    s = log_cache_.ReadOps(fallback_index,
                           max_batch_size,
                           messages,
                           &new_preceding);
    if (s.ok()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Successfully fell back and found op " << new_preceding;
    }
  }

  if (s.ok()) {
    DCHECK(new_preceding.IsInitialized());
    preceding_id->CopyFrom(new_preceding);
  }
  return s;
}

Status PeerMessageQueue::RequestForPeer(const string& uuid,
                                        ConsensusRequestPB* request,
                                        vector<ReplicateRefPtr>* msg_refs) {
  kudu::unique_lock<simple_spinlock> lock(&queue_lock_);
  DCHECK_EQ(queue_state_.state, kQueueOpen);
  DCHECK_NE(uuid, local_uuid_);

  TrackedPeer* peer = FindPtrOrNull(peers_map_, uuid);
  if (PREDICT_FALSE(peer == NULL || queue_state_.mode == NON_LEADER)) {
    return Status::NotFound("Peer not tracked or queue not in leader mode.");
  }

  // Clear the requests without deleting the entries, as they may be in use by other peers.
  request->mutable_ops()->ExtractSubrange(0, request->ops_size(), NULL);

  // This is initialized to the queue's last appended op but gets set to the id of the
  // log entry preceding the first one in 'messages' if messages are found for the peer.
  OpId preceding_id = queue_state_.last_appended;
  request->mutable_committed_index()->CopyFrom(queue_state_.committed_index);
  request->set_caller_term(queue_state_.current_term);
  lock.unlock();

  // If we've never communicated with the peer, we don't know what messages to
  // send, so we'll send a status-only request. Otherwise, we grab requests
  // from the log starting at the last_received point.
  if (!peer->is_new) {

    OpId send_after;
    if (peer->last_received.IsInitialized()) {
      send_after = peer->last_received;
    } else {
      send_after = peer->log_tail;
    }

    // The batch of messages to send to the peer.
    vector<ReplicateRefPtr> messages;
    int max_batch_size = FLAGS_consensus_max_batch_size_bytes - request->ByteSize();

    // We try to get the op after the one we last sent. If that fails because the op
    // cannot be found in our log we fall back to the last committed index.
    Status s = GetOpsFromCacheOrFallback(send_after,
                                         peer->last_known_committed_idx,
                                         max_batch_size,
                                         &messages,
                                         &preceding_id);

    if (PREDICT_FALSE(!s.ok())) {
      CHECK(messages.empty());
      LOG_WITH_PREFIX_UNLOCKED(DFATAL) << "Error while reading the log: " << s.ToString();
    }

    // We use AddAllocated rather than copy, because we pin the log cache at the
    // "all replicated" point. At some point we may want to allow partially loading
    // (and not pinning) earlier messages. At that point we'll need to do something
    // smarter here, like copy or ref-count.
    BOOST_FOREACH(const ReplicateRefPtr& msg, messages) {
      request->mutable_ops()->AddAllocated(msg->get());
    }
    msg_refs->swap(messages);
    DCHECK_LE(request->ByteSize(), FLAGS_consensus_max_batch_size_bytes);
  }

  DCHECK(preceding_id.IsInitialized());
  request->mutable_preceding_id()->CopyFrom(preceding_id);

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    if (request->ops_size() > 0) {
      VLOG_WITH_PREFIX_UNLOCKED(2) << "Sending request with operations to Peer: " << uuid
          << ". Size: " << request->ops_size()
          << ". From: " << request->ops(0).id().ShortDebugString() << ". To: "
          << request->ops(request->ops_size() - 1).id().ShortDebugString();
    } else {
      VLOG_WITH_PREFIX_UNLOCKED(2) << "Sending status only request to Peer: " << uuid
          << ": " << request->DebugString();
    }
  }

  return Status::OK();
}

void PeerMessageQueue::AdvanceQueueWatermark(const char* type,
                                             OpId* watermark,
                                             const OpId& replicated_before,
                                             const OpId& replicated_after,
                                             int num_peers_required) {

  if (VLOG_IS_ON(2)) {
    VLOG_WITH_PREFIX_UNLOCKED(2) << "Updating " << type << " watermark: " << " peer changed from "
        << replicated_before << " to " << replicated_after << ". Current value: "
        << watermark->ShortDebugString();
  }

  // Go through the peer's watermarks, we want the highest watermark that
  // 'num_peers_required' of peers has replicated. To find this we do the
  // following:
  // - Store all the peer's 'last_received' in a vector
  // - Sort the vector
  // - Find the vector.size() - 'num_peers_required' position, this
  //   will be the new 'watermark'.
  vector<const OpId*> watermarks;
  BOOST_FOREACH(const PeersMap::value_type& peer, peers_map_) {
    if (peer.second->is_last_exchange_successful) {
      watermarks.push_back(&peer.second->last_received);
    }
  }

  // If we haven't enough peers to calculate the watermark return.
  if (watermarks.size() < num_peers_required) {
    return;
  }

  std::sort(watermarks.begin(), watermarks.end(), OpIdIndexLessThanPtrFunctor());

  OpId new_watermark = *watermarks[watermarks.size() - num_peers_required];
  OpId old_watermark = *watermark;
  watermark->CopyFrom(new_watermark);

  VLOG_WITH_PREFIX_UNLOCKED(1) << "Updated " << type << " watermark "
      << "from " << old_watermark << " to " << new_watermark;
  if (VLOG_IS_ON(3)) {
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Peers: ";
    BOOST_FOREACH(const PeersMap::value_type& peer, peers_map_) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Peer: " << peer.second->ToString();
    }
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Sorted watermarks:";
    BOOST_FOREACH(const OpId* watermark, watermarks) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermark: " << watermark->ShortDebugString();
    }
  }
}

void PeerMessageQueue::ResponseFromPeer(const OpId& last_sent,
                                        const ConsensusResponsePB& response,
                                        bool* more_pending) {
  CHECK(response.has_responder_uuid() && !response.responder_uuid().empty())
      << "Got response from peer with empty UUID";
  DCHECK(response.IsInitialized()) << "Response: " << response.ShortDebugString();

  OpId updated_majority_replicated_opid;
  Mode mode_copy;
  {
    unique_lock<simple_spinlock> scoped_lock(&queue_lock_);
    DCHECK_EQ(queue_state_.state, kQueueOpen);

    TrackedPeer* peer = FindPtrOrNull(peers_map_, response.responder_uuid());
    if (PREDICT_FALSE(peer == NULL)) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Queue is closed or peer was untracked, disregarding "
          "peer response. Response: " << response.ShortDebugString();
      *more_pending = false;
      return;
    }

    // Sanity checks.
    // Some of these can be eventually removed, but they are handy for now.

    // Application level errors should be handled elsewhere
    DCHECK(!response.has_error());
    // Responses should always have a status.
    DCHECK(response.has_status());
    // The status must always have a last received op id and a last committed index.
    DCHECK(response.status().has_last_received());
    DCHECK(response.status().has_last_committed_idx());

    const ConsensusStatusPB& status = response.status();

    // Take a snapshot of the current peer status.
    TrackedPeer previous = *peer;

    // Update the peer status based on the response.
    peer->log_tail.CopyFrom(status.last_received());
    peer->last_known_committed_idx = status.last_committed_idx();
    peer->is_new = false;

    if (PREDICT_FALSE(status.has_error())) {
      peer->is_last_exchange_successful = false;
      switch (status.error().code()) {
        case ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH: {
          DCHECK(status.has_last_received());
          if (previous.is_new) {
            // That's currently how we can detect that we able to connect to a peer.
            LOG_WITH_PREFIX_UNLOCKED(INFO) << "Connected to new peer: " << peer->ToString();
          } else {
            LOG_WITH_PREFIX_UNLOCKED(INFO) << "Got LMP mismatch error from peer: "
                                           << peer->ToString();
          }
          *more_pending = true;
          return;
        }
        case ConsensusErrorPB::INVALID_TERM: {
          CHECK(response.has_responder_term());
          LOG_WITH_PREFIX_UNLOCKED(INFO) << "Peer responded invalid term: " << peer->ToString();
          NotifyObserversOfTermChange(response.responder_term());
          *peer = previous;

          *more_pending = false;
          return;
        }
        default: {
          LOG_WITH_PREFIX_UNLOCKED(FATAL) << "Unexpected consensus error. Response: "
              << response.ShortDebugString();
        }
      }
    }

    // On a successful response we update the last sent.
    peer->last_received = last_sent;
    peer->is_last_exchange_successful = true;

    if (response.has_responder_term()) {
      // The peer must have responded with a term that is greater than or equal to
      // the last known term for that peer.
      peer->CheckMonotonicTerms(response.responder_term());

      // If the responder didn't send an error back that must mean that it has
      // a term that is the same or lower than ours.
      CHECK_LE(response.responder_term(), queue_state_.current_term);
    }

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG_WITH_PREFIX_UNLOCKED(2) << "Received Response from Peer: " << peer->ToString()
          << ". Response: " << response.ShortDebugString();
    }

    *more_pending = log_cache_.HasOpBeenWritten(peer->log_tail.index() + 1);

    mode_copy = queue_state_.mode;
    if (mode_copy == LEADER) {
      // Advance the majority replicated index.
      AdvanceQueueWatermark("majority_replicated",
                            &queue_state_.majority_replicated_opid,
                            previous.last_received,
                            peer->last_received,
                            queue_state_.majority_size_);

      updated_majority_replicated_opid.CopyFrom(queue_state_.majority_replicated_opid);
    }

    // Advance the all replicated index.
    AdvanceQueueWatermark("all_replicated",
                          &queue_state_.all_replicated_opid,
                          previous.last_received,
                          peer->last_received,
                          peers_map_.size());

    log_cache_.EvictThroughOp(queue_state_.all_replicated_opid.index());

    UpdateMetrics();
  }

  if (mode_copy == LEADER) {
    NotifyObserversOfMajorityReplOpChange(updated_majority_replicated_opid);
  }
}

PeerMessageQueue::TrackedPeer PeerMessageQueue::GetTrackedPeerForTests(string uuid) {
  unique_lock<simple_spinlock> scoped_lock(&queue_lock_);
  TrackedPeer* tracked = FindOrDie(peers_map_, uuid);
  return *tracked;
}

OpId PeerMessageQueue::GetAllReplicatedIndexForTests() const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.all_replicated_opid;
}

OpId PeerMessageQueue::GetCommittedIndexForTests() const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.committed_index;
}

OpId PeerMessageQueue::GetMajorityReplicatedOpIdForTests() const {
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.majority_replicated_opid;
}


void PeerMessageQueue::UpdateMetrics() {
  // Since operations have consecutive indices we can update the metrics based
  // on simple index math.
  metrics_.num_majority_done_ops->set_value(
      queue_state_.committed_index.index() -
      queue_state_.all_replicated_opid.index());
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
  BOOST_FOREACH(const PeersMap::value_type& entry, peers_map_) {
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
  BOOST_FOREACH(const PeersMap::value_type& entry, peers_map_) {
    out << Substitute("  <tr><td>$0</td><td>$1</td></tr>",
                      EscapeForHtmlToString(entry.first),
                      EscapeForHtmlToString(entry.second->ToString())) << endl;
  }
  out << "</table>" << endl;

  log_cache_.DumpToHtml(out);
}

void PeerMessageQueue::ClearUnlocked() {
  STLDeleteValues(&peers_map_);
  queue_state_.state = kQueueClosed;
}

void PeerMessageQueue::Close() {
  observers_pool_->Shutdown();
  boost::lock_guard<simple_spinlock> lock(queue_lock_);
  ClearUnlocked();
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
  std::vector<PeerMessageQueueObserver*>::iterator iter =
        std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    observers_.push_back(observer);
  }
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
    const OpId new_majority_replicated_op) {
  WARN_NOT_OK(observers_pool_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversOfMajorityReplOpChangeTask,
           Unretained(this), new_majority_replicated_op)),
              LogPrefixUnlocked() + " Unable to notify peers of majority replicated op change.");
}

void PeerMessageQueue::NotifyObserversOfTermChange(uint64_t term) {
  WARN_NOT_OK(observers_pool_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversOfTermChangeTask,
           Unretained(this), term)),
              LogPrefixUnlocked() + " Unable to notify peers of term change.");
}

void PeerMessageQueue::NotifyObserversOfMajorityReplOpChangeTask(
    const OpId new_majority_replicated_op) {
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

void PeerMessageQueue::NotifyObserversOfTermChangeTask(uint64_t term) {
  std::vector<PeerMessageQueueObserver*> copy;
  {
    boost::lock_guard<simple_spinlock> lock(queue_lock_);
    copy = observers_;
  }
  OpId new_committed_index;
  BOOST_FOREACH(PeerMessageQueueObserver* observer, copy) {
    observer->NotifyTermChange(term);
  }
}

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

string PeerMessageQueue::LogPrefixUnlocked() const {
  return Substitute("T $0 P $1 [$2]: ",
                    tablet_id_,
                    local_uuid_,
                    queue_state_.mode == LEADER ? "LEADER" : "NON_LEADER");
}

string PeerMessageQueue::QueueState::ToString() const {
  return Substitute("All replicated op: $0, Majority replicated op: $1, "
      "Committed index: $2, Last appended: $3, Current term: $4, Majority size: $5, "
      "State: $6, Mode: $7",
      OpIdToString(all_replicated_opid), OpIdToString(majority_replicated_opid),
      OpIdToString(committed_index), OpIdToString(last_appended), current_term,
      majority_size_, state, (mode == LEADER ? "LEADER" : "NON_LEADER"));
}

}  // namespace consensus
}  // namespace kudu
