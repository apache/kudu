// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "kudu/consensus/consensus_queue.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include <boost/optional/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <gflags/gflags.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/url-coding.h"

DEFINE_int32(consensus_max_batch_size_bytes, 1024 * 1024,
             "The maximum per-tablet RPC batch size when updating peers.");
TAG_FLAG(consensus_max_batch_size_bytes, advanced);

DEFINE_int32(follower_unavailable_considered_failed_sec, 300,
             "Seconds that a leader is unable to successfully heartbeat to a "
             "follower after which the follower is considered to be failed and "
             "evicted from the config.");
TAG_FLAG(follower_unavailable_considered_failed_sec, advanced);
TAG_FLAG(follower_unavailable_considered_failed_sec, runtime);

DEFINE_int32(consensus_inject_latency_ms_in_notifications, 0,
             "Injects a random sleep between 0 and this many milliseconds into "
             "asynchronous notifications from the consensus queue back to the "
             "consensus implementation.");
TAG_FLAG(consensus_inject_latency_ms_in_notifications, hidden);
TAG_FLAG(consensus_inject_latency_ms_in_notifications, unsafe);

DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_bool(safe_time_advancement_without_writes);
DECLARE_bool(raft_prepare_replacement_before_eviction);

using kudu::log::Log;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace consensus {

METRIC_DEFINE_gauge_int64(tablet, majority_done_ops, "Leader Operations Acked by Majority",
                          MetricUnit::kOperations,
                          "Number of operations in the leader queue ack'd by a majority but "
                          "not all peers. This metric is always zero for followers.",
                          kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_int64(tablet, in_progress_ops, "Operations in Progress",
                          MetricUnit::kOperations,
                          "Number of operations in the peer's queue ack'd by a minority of "
                          "peers.",
                          kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_int64(tablet, ops_behind_leader, "Operations Behind Leader",
                          MetricUnit::kOperations,
                          "Number of operations this server believes it is behind the leader.",
                          kudu::MetricLevel::kWarn);

const char* PeerStatusToString(PeerStatus p) {
  switch (p) {
    case PeerStatus::OK: return "OK";
    case PeerStatus::REMOTE_ERROR: return "REMOTE_ERROR";
    case PeerStatus::RPC_LAYER_ERROR: return "RPC_LAYER_ERROR";
    case PeerStatus::TABLET_FAILED: return "TABLET_FAILED";
    case PeerStatus::TABLET_NOT_FOUND: return "TABLET_NOT_FOUND";
    case PeerStatus::INVALID_TERM: return "INVALID_TERM";
    case PeerStatus::LMP_MISMATCH: return "LMP_MISMATCH";
    case PeerStatus::CANNOT_PREPARE: return "CANNOT_PREPARE";
    case PeerStatus::NEW: return "NEW";
  }
  DCHECK(false);
  return "<unknown>";
}

PeerMessageQueue::TrackedPeer::TrackedPeer(RaftPeerPB peer_pb)
    : peer_pb(std::move(peer_pb)),
      next_index(kInvalidOpIdIndex),
      last_received(MinimumOpId()),
      last_known_committed_index(MinimumOpId().index()),
      last_exchange_status(PeerStatus::NEW),
      last_communication_time(MonoTime::Now()),
      wal_catchup_possible(true),
      last_overall_health_status(HealthReportPB::UNKNOWN),
      status_log_throttler(std::make_shared<logging::LogThrottler>()),
      last_seen_term_(0) {
}

std::string PeerMessageQueue::TrackedPeer::ToString() const {
  return Substitute("Peer: $0, Status: $1, Last received: $2, Next index: $3, "
                    "Last known committed idx: $4, Time since last communication: $5",
                    SecureShortDebugString(peer_pb),
                    PeerStatusToString(last_exchange_status),
                    OpIdToString(last_received), next_index,
                    last_known_committed_index,
                    (MonoTime::Now() - last_communication_time).ToString());
}

#define INSTANTIATE_METRIC(x) \
  x.Instantiate(metric_entity, 0)
PeerMessageQueue::Metrics::Metrics(const scoped_refptr<MetricEntity>& metric_entity)
  : num_majority_done_ops(INSTANTIATE_METRIC(METRIC_majority_done_ops)),
    num_in_progress_ops(INSTANTIATE_METRIC(METRIC_in_progress_ops)),
    num_ops_behind_leader(INSTANTIATE_METRIC(METRIC_ops_behind_leader)) {
}
#undef INSTANTIATE_METRIC

PeerMessageQueue::PeerMessageQueue(const scoped_refptr<MetricEntity>& metric_entity,
                                   scoped_refptr<log::Log> log,
                                   scoped_refptr<TimeManager> time_manager,
                                   RaftPeerPB local_peer_pb,
                                   string tablet_id,
                                   unique_ptr<ThreadPoolToken> raft_pool_observers_token,
                                   OpId last_locally_replicated,
                                   const OpId& last_locally_committed)
    : raft_pool_observers_token_(std::move(raft_pool_observers_token)),
      local_peer_pb_(std::move(local_peer_pb)),
      tablet_id_(std::move(tablet_id)),
      successor_watch_in_progress_(false),
      log_cache_(metric_entity, std::move(log), local_peer_pb_.permanent_uuid(), tablet_id_),
      metrics_(metric_entity),
      time_manager_(std::move(time_manager)) {
  DCHECK(local_peer_pb_.has_permanent_uuid());
  DCHECK(local_peer_pb_.has_last_known_addr());
  DCHECK(last_locally_replicated.IsInitialized());
  DCHECK(last_locally_committed.IsInitialized());
  queue_state_.current_term = 0;
  queue_state_.first_index_in_current_term = boost::none;
  queue_state_.committed_index = 0;
  queue_state_.all_replicated_index = 0;
  queue_state_.majority_replicated_index = 0;
  queue_state_.last_idx_appended_to_leader = 0;
  queue_state_.mode = NON_LEADER;
  queue_state_.majority_size_ = -1;
  queue_state_.last_appended = std::move(last_locally_replicated);
  queue_state_.committed_index = last_locally_committed.index();
  queue_state_.state = kQueueOpen;
  // TODO(mpercy): Merge LogCache::Init() with its constructor.
  log_cache_.Init(queue_state_.last_appended);
}

void PeerMessageQueue::SetLeaderMode(int64_t committed_index,
                                     int64_t current_term,
                                     const RaftConfigPB& active_config) {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  if (current_term != queue_state_.current_term) {
    CHECK_GT(current_term, queue_state_.current_term) << "Terms should only increase";
    queue_state_.first_index_in_current_term = boost::none;
    queue_state_.current_term = current_term;
  }

  queue_state_.committed_index = committed_index;
  queue_state_.majority_replicated_index = committed_index;
  queue_state_.active_config.reset(new RaftConfigPB(active_config));
  queue_state_.majority_size_ = MajoritySize(CountVoters(*queue_state_.active_config));
  queue_state_.mode = LEADER;

  TrackLocalPeerUnlocked();
  CheckPeersInActiveConfigIfLeaderUnlocked();

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Queue going to LEADER mode. State: "
                                 << queue_state_.ToString();

  // Reset last communication time with all peers to reset the clock on the
  // failure timeout.
  const auto now = MonoTime::Now();
  for (const PeersMap::value_type& entry : peers_map_) {
    entry.second->last_communication_time = now;
  }
  time_manager_->SetLeaderMode();
}

void PeerMessageQueue::SetNonLeaderMode(const RaftConfigPB& active_config) {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  queue_state_.active_config.reset(new RaftConfigPB(active_config));
  queue_state_.mode = NON_LEADER;
  queue_state_.majority_size_ = -1;

  // Update this when stepping down, since it doesn't get tracked as LEADER.
  queue_state_.last_idx_appended_to_leader = queue_state_.last_appended.index();

  TrackLocalPeerUnlocked();

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Queue going to NON_LEADER mode. State: "
                                 << queue_state_.ToString();

  time_manager_->SetNonLeaderMode();
}

void PeerMessageQueue::TrackPeer(const RaftPeerPB& peer_pb) {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  TrackPeerUnlocked(peer_pb);
}

void PeerMessageQueue::TrackPeerUnlocked(const RaftPeerPB& peer_pb) {
  CHECK(!peer_pb.permanent_uuid().empty()) << SecureShortDebugString(peer_pb);
  CHECK(peer_pb.has_member_type()) << SecureShortDebugString(peer_pb);
  DCHECK(queue_lock_.is_locked());
  DCHECK_EQ(queue_state_.state, kQueueOpen);

  TrackedPeer* tracked_peer = new TrackedPeer(peer_pb);
  // We don't know the last operation received by the peer so, following the
  // Raft protocol, we set next_index to one past the end of our own log. This
  // way, if calling this method is the result of a successful leader election
  // and the logs between the new leader and remote peer match, the
  // peer->next_index will point to the index of the soon-to-be-written NO_OP
  // entry that is used to assert leadership. If we guessed wrong, and the peer
  // does not have a log that matches ours, the normal queue negotiation
  // process will eventually find the right point to resume from.
  tracked_peer->next_index = queue_state_.last_appended.index() + 1;
  InsertOrDie(&peers_map_, tracked_peer->uuid(), tracked_peer);

  CheckPeersInActiveConfigIfLeaderUnlocked();

  // We don't know how far back this peer is, so set the all replicated watermark to
  // 0. We'll advance it when we know how far along the peer is.
  queue_state_.all_replicated_index = 0;
}

void PeerMessageQueue::UntrackPeer(const string& uuid) {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  UntrackPeerUnlocked(uuid);
}

void PeerMessageQueue::UntrackPeerUnlocked(const string& uuid) {
  DCHECK(queue_lock_.is_locked());
  TrackedPeer* peer = EraseKeyReturnValuePtr(&peers_map_, uuid);
  delete peer; // Deleting a nullptr is safe.
}

void PeerMessageQueue::TrackLocalPeerUnlocked() {
  DCHECK(queue_lock_.is_locked());
  RaftPeerPB* local_peer_in_config;
  Status s = GetRaftConfigMember(queue_state_.active_config.get(),
                                 local_peer_pb_.permanent_uuid(),
                                 &local_peer_in_config);
  auto local_copy = local_peer_pb_;
  if (!s.ok()) {
    // The local peer is not a member of the config. The queue requires the
    // 'member_type' field to be set for any tracked peer, so we explicitly
    // mark the local peer as a NON_VOTER. This case is only possible when the
    // local peer is not the leader, so the choice is not particularly
    // important, but NON_VOTER is the most reasonable option.
    local_copy.set_member_type(RaftPeerPB::NON_VOTER);
    local_peer_in_config = &local_copy;
  }
  CHECK(local_peer_in_config->member_type() == RaftPeerPB::VOTER ||
        queue_state_.mode != LEADER)
      << "local peer " << local_peer_pb_.permanent_uuid()
      << " is not a voter in config: " << queue_state_.ToString();
  if (ContainsKey(peers_map_, local_peer_pb_.permanent_uuid())) {
    UntrackPeerUnlocked(local_peer_pb_.permanent_uuid());
  }
  TrackPeerUnlocked(*local_peer_in_config);
}

unordered_map<string, HealthReportPB> PeerMessageQueue::ReportHealthOfPeers() const {
  unordered_map<string, HealthReportPB> reports;
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  for (const auto& entry : peers_map_) {
    const string& peer_uuid = entry.first;
    const TrackedPeer* peer = entry.second;
    HealthReportPB report;
    auto overall_health = peer->last_overall_health_status;
    // We always consider the local peer (ourselves) to be healthy.
    // TODO(mpercy): Is this always a safe assumption?
    if (peer_uuid == local_peer_pb_.permanent_uuid()) {
      overall_health = HealthReportPB::HEALTHY;
    }
    report.set_overall_health(overall_health);
    reports.emplace(peer_uuid, std::move(report));
  }
  return reports;
}

void PeerMessageQueue::CheckPeersInActiveConfigIfLeaderUnlocked() const {
  DCHECK(queue_lock_.is_locked());
  if (queue_state_.mode != LEADER) return;
  std::unordered_set<string> config_peer_uuids;
  for (const RaftPeerPB& peer_pb : queue_state_.active_config->peers()) {
    InsertOrDie(&config_peer_uuids, peer_pb.permanent_uuid());
  }
  for (const PeersMap::value_type& entry : peers_map_) {
    if (!ContainsKey(config_peer_uuids, entry.first)) {
      LOG_WITH_PREFIX_UNLOCKED(FATAL) << Substitute("Peer $0 is not in the active config. "
                                                    "Queue state: $1",
                                                    entry.first,
                                                    queue_state_.ToString());
    }
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
  fake_response.set_responder_uuid(local_peer_pb_.permanent_uuid());
  *fake_response.mutable_status()->mutable_last_received() = id;
  *fake_response.mutable_status()->mutable_last_received_current_leader() = id;
  {
    std::lock_guard<simple_spinlock> lock(queue_lock_);
    fake_response.mutable_status()->set_last_committed_idx(queue_state_.committed_index);
  }
  ResponseFromPeer(local_peer_pb_.permanent_uuid(), fake_response);

  callback.Run(status);
}

Status PeerMessageQueue::AppendOperation(const ReplicateRefPtr& msg) {
  return AppendOperations({ msg }, Bind(CrashIfNotOkStatusCB,
                                        "Enqueued replicate operation failed to write to WAL"));
}

Status PeerMessageQueue::AppendOperations(const vector<ReplicateRefPtr>& msgs,
                                          const StatusCallback& log_append_callback) {

  DFAKE_SCOPED_LOCK(append_fake_lock_);
  std::unique_lock<simple_spinlock> lock(queue_lock_);

  OpId last_id = msgs.back()->get()->id();

  // "Snoop" on the appended operations to watch for term changes (as follower)
  // and to determine the first index in our term (as leader).
  //
  // TODO: it would be a cleaner design to explicitly set the first index in the
  // leader term as part of SetLeaderMode(). However, we are currently also
  // using that method to handle refreshing the peer list during configuration
  // changes, so the refactor isn't trivial.
  for (const auto& msg : msgs) {
    const auto& id = msg->get()->id();
    if (id.term() > queue_state_.current_term) {
      queue_state_.current_term = id.term();
      queue_state_.first_index_in_current_term = id.index();
    } else if (id.term() == queue_state_.current_term &&
               queue_state_.first_index_in_current_term == boost::none) {
      queue_state_.first_index_in_current_term = id.index();
    }
  }

  // Update safe time in the TimeManager if we're leader.
  // This will 'unpin' safe time advancement, which had stopped since we assigned a timestamp to
  // the message.
  // Until we have leader leases, replicas only call this when the message is committed.
  if (queue_state_.mode == LEADER) {
    time_manager_->AdvanceSafeTimeWithMessage(*msgs.back()->get());
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
  DCHECK(last_id.IsInitialized());
  queue_state_.last_appended = last_id;
  UpdateMetricsUnlocked();

  return Status::OK();
}

void PeerMessageQueue::TruncateOpsAfter(int64_t index) {
  DFAKE_SCOPED_LOCK(append_fake_lock_); // should not race with append.
  OpId op;
  CHECK_OK_PREPEND(log_cache_.LookupOpId(index, &op),
                   Substitute("$0: cannot truncate ops after bad index $1",
                              LogPrefixUnlocked(),
                              index));
  {
    std::unique_lock<simple_spinlock> lock(queue_lock_);
    DCHECK(op.IsInitialized());
    queue_state_.last_appended = op;
  }
  log_cache_.TruncateOpsAfter(op.index());
}

OpId PeerMessageQueue::GetLastOpIdInLog() const {
  std::unique_lock<simple_spinlock> lock(queue_lock_);
  DCHECK(queue_state_.last_appended.IsInitialized());
  return queue_state_.last_appended;
}

OpId PeerMessageQueue::GetNextOpId() const {
  std::unique_lock<simple_spinlock> lock(queue_lock_);
  DCHECK(queue_state_.last_appended.IsInitialized());
  return MakeOpId(queue_state_.current_term,
                  queue_state_.last_appended.index() + 1);
}

bool PeerMessageQueue::SafeToEvictUnlocked(const string& evict_uuid) const {
  DCHECK(queue_lock_.is_locked());
  DCHECK_EQ(LEADER, queue_state_.mode);
  auto now = MonoTime::Now();

  int remaining_voters = 0;
  int remaining_viable_voters = 0;

  for (const auto& e : peers_map_) {
    const auto& uuid = e.first;
    const auto& peer = e.second;
    if (uuid == evict_uuid) {
      continue;
    }
    if (!IsRaftConfigVoter(uuid, *queue_state_.active_config)) {
      continue;
    }
    remaining_voters++;

    bool viable = true;
    // Being alive, the local peer itself (the leader) is always a viable
    // voter: the criteria below apply only to non-local peers.
    if (uuid != local_peer_pb_.permanent_uuid()) {
      // Only consider a peer to be a viable voter if...
      // ...its last exchange was successful
      viable &= peer->last_exchange_status == PeerStatus::OK;

      // ...the peer is up to date with the latest majority.
      //
      //    This indicates that it's actively participating in majorities and likely to
      //    replicate a config change immediately when we propose it.
      viable &= peer->last_received.index() >= queue_state_.majority_replicated_index;

      // ...we have communicated successfully with it recently.
      //
      //    This handles the case where the tablet has had no recent writes and therefore
      //    even a replica that is down would have participated in the latest majority.
      auto unreachable_time = now - peer->last_communication_time;
      viable &= unreachable_time.ToMilliseconds() < FLAGS_consensus_rpc_timeout_ms;
    }
    if (viable) {
      remaining_viable_voters++;
    }
  }

  // We never drop from 2 to 1 automatically, at least for now. We may want
  // to revisit this later, we're just being cautious with this.
  if (remaining_voters <= 1) {
    VLOG(2) << LogPrefixUnlocked() << "Not evicting P $0 (only one voter would remain)";
    return false;
  }
  // Don't evict anything if the remaining number of viable voters is not enough
  // to form a quorum.
  if (remaining_viable_voters < MajoritySize(remaining_voters)) {
    VLOG(2) << LogPrefixUnlocked() << Substitute(
        "Not evicting P $0 (only $1/$2 remaining voters appear viable)",
        evict_uuid, remaining_viable_voters, remaining_voters);
    return false;
  }

  return true;
}

void PeerMessageQueue::UpdatePeerHealthUnlocked(TrackedPeer* peer) {
  DCHECK(queue_lock_.is_locked());
  DCHECK_EQ(LEADER, queue_state_.mode);

  auto overall_health_status = PeerHealthStatus(*peer);

  // Prepare error messages for different conditions.
  string error_msg;
  if (overall_health_status == HealthReportPB::FAILED ||
      overall_health_status == HealthReportPB::FAILED_UNRECOVERABLE) {
    if (peer->last_exchange_status == PeerStatus::TABLET_FAILED) {
      error_msg = Substitute("The tablet replica hosted on peer $0 has failed", peer->uuid());
    } else if (!peer->wal_catchup_possible) {
      error_msg = Substitute("The logs necessary to catch up peer $0 have been "
                             "garbage collected. The replica will never be able "
                             "to catch up", peer->uuid());
    } else {
      error_msg = Substitute("Leader has been unable to successfully communicate "
                             "with peer $0 for more than $1 seconds ($2)",
                             peer->uuid(),
                             FLAGS_follower_unavailable_considered_failed_sec,
                             (MonoTime::Now() - peer->last_communication_time).ToString());
    }
  }

  bool changed = overall_health_status != peer->last_overall_health_status;
  peer->last_overall_health_status = overall_health_status;

  if (FLAGS_raft_prepare_replacement_before_eviction) {
    // Only take action when there is a change.
    if (changed) {
      // Only log a message when the status changes to some flavor of failure.
      if (overall_health_status == HealthReportPB::FAILED ||
          overall_health_status == HealthReportPB::FAILED_UNRECOVERABLE) {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << error_msg;
      }
      NotifyObserversOfPeerHealthChange();
    }
  } else {
    if ((overall_health_status == HealthReportPB::FAILED ||
         overall_health_status == HealthReportPB::FAILED_UNRECOVERABLE) &&
        SafeToEvictUnlocked(peer->uuid())) {
      NotifyObserversOfFailedFollower(peer->uuid(), queue_state_.current_term, error_msg);
    }
  }
}

// While reporting on the replica health status, it's important to report on
// the 'definitive' health statuses once they surface. That allows the system
// to expedite decisions on replica replacement because the more 'definitive'
// statuses have less uncertainty and provide more information (compared
// with less 'definitive' statuses). Informally, the level of 'definitiveness'
// could be measured by the number of possible state transitions on the replica
// health status state diagram.
//
// The health status chain below has increasing level of 'definitiveness'
// left to right:
//
//   UNKNOWN --> HEALTHY --> FAILED --> FAILED_UNRECOVERABLE
//
// For example, in the case when a replica has been unreachable longer than the
// time interval specified by the --follower_unavailable_considered_failed_sec
// flag, the system should start reporting its health status as FAILED.
// However, once the replica falls behind the WAL log GC threshold, the system
// should start reporting its healths status as FAILED_UNRECOVERABLE. The code
// below is written to adhere to that informal policy.
HealthReportPB::HealthStatus PeerMessageQueue::PeerHealthStatus(const TrackedPeer& peer) {
  // Replicas that have fallen behind the leader's retained WAL segments are
  // failed irrecoverably and will not come back because they cannot ever catch
  // up with the leader replica.
  if (!peer.wal_catchup_possible) {
    return HealthReportPB::FAILED_UNRECOVERABLE;
  }

  // Replicas returning TABLET_FAILED status are considered irrecoverably
  // failed because the TABLED_FAILED status manifests about IO failures
  // caused by disk corruption, etc.
  if (peer.last_exchange_status == PeerStatus::TABLET_FAILED) {
    return HealthReportPB::FAILED_UNRECOVERABLE;
  }

  // Replicas which have been unreachable for too long are considered failed,
  // unless it's known that they have failed irrecoverably (see above). They
  // might come back at some point and successfully catch up with the leader.
  auto max_unreachable = MonoDelta::FromSeconds(FLAGS_follower_unavailable_considered_failed_sec);
  if (MonoTime::Now() - peer.last_communication_time > max_unreachable) {
    return HealthReportPB::FAILED;
  }

  // The happy case: replicas returned OK during the recent exchange are considered healthy.
  if (peer.last_exchange_status == PeerStatus::OK) {
    return HealthReportPB::HEALTHY;
  }

  // Other cases are for various situations when there hasn't been a contact
  // with the replica yet or it's impossible to definitely tell the health
  // status of the replica based on the last exchange status (transient error,
  // etc.). For such cases, the replica health status is reported as UNKNOWN.
  return HealthReportPB::UNKNOWN;
}

Status PeerMessageQueue::RequestForPeer(const string& uuid,
                                        ConsensusRequestPB* request,
                                        vector<ReplicateRefPtr>* msg_refs,
                                        bool* needs_tablet_copy) {
  // Maintain a thread-safe copy of necessary members.
  OpId preceding_id;
  int64_t current_term;
  TrackedPeer peer_copy;
  MonoDelta unreachable_time;
  {
    std::lock_guard<simple_spinlock> lock(queue_lock_);
    DCHECK_EQ(queue_state_.state, kQueueOpen);
    DCHECK_NE(uuid, local_peer_pb_.permanent_uuid());

    TrackedPeer* peer = FindPtrOrNull(peers_map_, uuid);
    if (PREDICT_FALSE(peer == nullptr || queue_state_.mode == NON_LEADER)) {
      return Status::NotFound(Substitute("peer $0 is no longer tracked or "
                                         "queue is not in leader mode", uuid));
    }
    peer_copy = *peer;

    // Clear the requests without deleting the entries, as they may be in use by other peers.
    request->mutable_ops()->ExtractSubrange(0, request->ops_size(), nullptr);

    // This is initialized to the queue's last appended op but gets set to the id of the
    // log entry preceding the first one in 'messages' if messages are found for the peer.
    preceding_id = queue_state_.last_appended;
    current_term = queue_state_.current_term;

    request->set_committed_index(queue_state_.committed_index);
    request->set_all_replicated_index(queue_state_.all_replicated_index);
    request->set_last_idx_appended_to_leader(queue_state_.last_appended.index());
    request->set_caller_term(current_term);
    unreachable_time = MonoTime::Now() - peer_copy.last_communication_time;
  }

  // Always trigger a health status update check at the end of this function.
  bool wal_catchup_progress = false;
  bool wal_catchup_failure = false;
  SCOPED_CLEANUP({
      std::lock_guard<simple_spinlock> lock(queue_lock_);
      TrackedPeer* peer = FindPtrOrNull(peers_map_, uuid);
      if (PREDICT_FALSE(peer == nullptr || queue_state_.mode == NON_LEADER)) {
        VLOG(1) << LogPrefixUnlocked() << "peer " << uuid
                << " is no longer tracked or queue is not in leader mode";
        return;
      }
      if (wal_catchup_progress) peer->wal_catchup_possible = true;
      if (wal_catchup_failure) peer->wal_catchup_possible = false;
      UpdatePeerHealthUnlocked(peer);
    });

  if (peer_copy.last_exchange_status == PeerStatus::TABLET_NOT_FOUND) {
    VLOG(3) << LogPrefixUnlocked() << "Peer " << uuid << " needs tablet copy" << THROTTLE_MSG;
    *needs_tablet_copy = true;
    return Status::OK();
  }
  *needs_tablet_copy = false;

  // If we've never communicated with the peer, we don't know what messages to
  // send, so we'll send a status-only request. Otherwise, we grab requests
  // from the log starting at the last_received point.
  if (peer_copy.last_exchange_status != PeerStatus::NEW) {

    // The batch of messages to send to the peer.
    vector<ReplicateRefPtr> messages;
    int max_batch_size = FLAGS_consensus_max_batch_size_bytes - request->ByteSize();

    // We try to get the follower's next_index from our log.
    Status s = log_cache_.ReadOps(peer_copy.next_index - 1,
                                  max_batch_size,
                                  &messages,
                                  &preceding_id);
    if (PREDICT_FALSE(!s.ok())) {
      // It's normal to have a NotFound() here if a follower falls behind where
      // the leader has GCed its logs. The follower replica will hang around
      // for a while until it's evicted.
      if (PREDICT_TRUE(s.IsNotFound())) {
        KLOG_EVERY_N_SECS_THROTTLER(INFO, 60, *peer_copy.status_log_throttler, "logs_gced")
            << LogPrefixUnlocked()
            << Substitute("The logs necessary to catch up peer $0 have been "
                          "garbage collected. The follower will never be able "
                          "to catch up ($1)", uuid, s.ToString());
        wal_catchup_failure = true;
        return s;
      }
      if (s.IsIncomplete()) {
        // IsIncomplete() means that we tried to read beyond the head of the log
        // (in the future). This is usually a sign that this peer is under load
        // and is about to step down as leader. See KUDU-1078.
        LOG_WITH_PREFIX_UNLOCKED(INFO) << "Error trying to read ahead of the log "
                                       << "while preparing peer request: "
                                       << s.ToString() << ". Destination peer: "
                                       << peer_copy.ToString();
        return s;
      }
      LOG_WITH_PREFIX_UNLOCKED(FATAL) << "Error reading the log while preparing peer request: "
                                      << s.ToString() << ". Destination peer: "
                                      << peer_copy.ToString();
    }

    // Since we were able to read ops through the log cache, we know that
    // catchup is possible.
    wal_catchup_progress = true;

    // We use AddAllocated rather than copy, because we pin the log cache at the
    // "all replicated" point. At some point we may want to allow partially loading
    // (and not pinning) earlier messages. At that point we'll need to do something
    // smarter here, like copy or ref-count.
    for (const ReplicateRefPtr& msg : messages) {
      request->mutable_ops()->AddAllocated(msg->get());
    }
    msg_refs->swap(messages);
  }

  DCHECK(preceding_id.IsInitialized());
  request->mutable_preceding_id()->CopyFrom(preceding_id);

  // If we are sending ops to the follower, but the batch doesn't reach the current
  // committed index, we can consider the follower lagging, and it's worth
  // logging this fact periodically.
  if (request->ops_size() > 0) {
    int64_t last_op_sent = request->ops(request->ops_size() - 1).id().index();
    if (last_op_sent < request->committed_index()) {
      KLOG_EVERY_N_SECS_THROTTLER(INFO, 3, *peer_copy.status_log_throttler, "lagging")
          << LogPrefixUnlocked() << "Peer " << uuid << " is lagging by at least "
          << (request->committed_index() - last_op_sent)
          << " ops behind the committed index " << THROTTLE_MSG;
    }
  // If we're not sending ops to the follower, set the safe time on the request.
  // TODO(dralves) When we have leader leases, send this all the time.
  } else {
    if (PREDICT_TRUE(FLAGS_safe_time_advancement_without_writes)) {
      request->set_safe_timestamp(time_manager_->GetSafeTime().value());
    } else {
      KLOG_EVERY_N_SECS(WARNING, 300) << "Safe time advancement without writes is disabled. "
            "Snapshot reads on non-leader replicas may stall if there are no writes in progress.";
    }
  }

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    if (request->ops_size() > 0) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << Substitute("Sending request with operations to Peer: $0. Size: $1. From: $2. To: $3",
                        uuid,
                        request->ops_size(),
                        SecureShortDebugString(request->ops(0).id()),
                        SecureShortDebugString(request->ops(request->ops_size() - 1).id()));
    } else {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << Substitute("Sending status only request to Peer: $0: $1",
                        uuid,
                        SecureDebugString(*request));
    }
  }

  return Status::OK();
}

Status PeerMessageQueue::GetTabletCopyRequestForPeer(const string& uuid,
                                                     StartTabletCopyRequestPB* req) {
  TrackedPeer* peer = nullptr;
  int64_t current_term;
  {
    std::lock_guard<simple_spinlock> lock(queue_lock_);
    DCHECK_EQ(queue_state_.state, kQueueOpen);
    DCHECK_NE(uuid, local_peer_pb_.permanent_uuid());
    peer = FindPtrOrNull(peers_map_, uuid);
    current_term = queue_state_.current_term;
    if (PREDICT_FALSE(peer == nullptr || queue_state_.mode == NON_LEADER)) {
      return Status::NotFound("Peer not tracked or queue not in leader mode.");
    }
    if (PREDICT_FALSE(peer->last_exchange_status != PeerStatus::TABLET_NOT_FOUND)) {
      return Status::IllegalState("Peer does not need to initiate Tablet Copy", uuid);
    }
  }
  req->Clear();
  req->set_dest_uuid(uuid);
  req->set_tablet_id(tablet_id_);
  req->set_copy_peer_uuid(local_peer_pb_.permanent_uuid());
  *req->mutable_copy_peer_addr() = local_peer_pb_.last_known_addr();
  req->set_caller_term(current_term);
  return Status::OK();
}

void PeerMessageQueue::AdvanceQueueWatermark(const char* type,
                                             int64_t* watermark,
                                             const OpId& replicated_before,
                                             const OpId& replicated_after,
                                             int num_peers_required,
                                             ReplicaTypes replica_types,
                                             const TrackedPeer* who_caused) {
  DCHECK(queue_lock_.is_locked());
  VLOG_WITH_PREFIX_UNLOCKED(2)
      << Substitute("Updating $0 watermark: Peer ($1) changed from $2 to $3. "
                    "Current value: $4",
                    type,
                    who_caused->ToString(),
                    OpIdToString(replicated_before),
                    OpIdToString(replicated_after),
                    *watermark);

  // Go through the peer's watermarks, we want the highest watermark that
  // 'num_peers_required' of peers has replicated. To find this we do the
  // following:
  // - Store all the peer's 'last_received' in a vector
  // - Sort the vector
  // - Find the vector.size() - 'num_peers_required' position, this
  //   will be the new 'watermark'.
  vector<int64_t> watermarks;
  for (const PeersMap::value_type& peer : peers_map_) {
    if (replica_types == VOTER_REPLICAS &&
        peer.second->peer_pb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    // TODO(todd): The fact that we only consider peers whose last exchange was
    // successful can cause the "all_replicated" watermark to lag behind
    // farther than necessary. For example:
    // - local peer has replicated opid 100
    // - remote peer A has replicated opid 100
    // - remote peer B has replication opid 10 and is catching up
    // - remote peer A goes down
    // Here we'd start getting a non-OK last_exchange_status for peer A.
    // In that case, the 'all_replicated_watermark', which requires 3 peers, would not
    // be updateable, even once we've replicated peer 'B' up to opid 100. It would
    // get "stuck" at 10. In fact, in this case, the 'majority_replicated_watermark' would
    // also move *backwards* when peer A started getting errors.
    //
    // The issue with simply removing this condition is that 'last_received' does not
    // perfectly correspond to the 'match_index' in Raft Figure 2. It is simply the
    // highest operation in a peer's log, regardless of whether that peer currently
    // holds a prefix of the leader's log. So, in the case that the last exchange
    // was an error (LMP mismatch, for example), the 'last_received' is _not_ usable
    // for watermark calculation. This could be fixed by separately storing the
    // 'match_index' on a per-peer basis and using that for watermark calculation.
    if (peer.second->last_exchange_status == PeerStatus::OK) {
      watermarks.push_back(peer.second->last_received.index());
    }
  }

  // If we haven't enough peers to calculate the watermark return.
  if (watermarks.size() < num_peers_required) {
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermarks size: " << watermarks.size() << ", "
                                 << "Num peers required: " << num_peers_required;
    return;
  }

  std::sort(watermarks.begin(), watermarks.end());

  int64_t new_watermark = watermarks[watermarks.size() - num_peers_required];
  int64_t old_watermark = *watermark;
  *watermark = new_watermark;

  VLOG_WITH_PREFIX_UNLOCKED(1)
      << Substitute("Updated $0 watermark from $1 to $2",
                    type, old_watermark, new_watermark);
  if (VLOG_IS_ON(3)) {
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Peers: ";
    for (const auto& peer : peers_map_) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Peer: " << peer.second->ToString();
    }
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Sorted watermarks:";
    for (const auto watermark : watermarks) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermark: " << watermark;
    }
  }
}

void PeerMessageQueue::BeginWatchForSuccessor(
    const boost::optional<string>& successor_uuid) {
  std::lock_guard<simple_spinlock> l(queue_lock_);
  successor_watch_in_progress_ = true;
  designated_successor_uuid_ = successor_uuid;
}

void PeerMessageQueue::EndWatchForSuccessor() {
  std::lock_guard<simple_spinlock> l(queue_lock_);
  successor_watch_in_progress_ = false;
}

void PeerMessageQueue::UpdateFollowerWatermarks(int64_t committed_index,
                                                int64_t all_replicated_index) {
  std::lock_guard<simple_spinlock> l(queue_lock_);
  DCHECK_EQ(queue_state_.mode, NON_LEADER);
  queue_state_.committed_index = committed_index;
  queue_state_.all_replicated_index = all_replicated_index;
  UpdateMetricsUnlocked();
}

void PeerMessageQueue::UpdateLastIndexAppendedToLeader(int64_t last_idx_appended_to_leader) {
  std::lock_guard<simple_spinlock> l(queue_lock_);
  DCHECK_EQ(queue_state_.mode, NON_LEADER);
  queue_state_.last_idx_appended_to_leader = last_idx_appended_to_leader;
  UpdateLagMetricsUnlocked();
}

void PeerMessageQueue::UpdatePeerStatus(const string& peer_uuid,
                                        PeerStatus ps,
                                        const Status& status) {
  std::unique_lock<simple_spinlock> l(queue_lock_);
  TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
  if (PREDICT_FALSE(peer == nullptr || queue_state_.mode == NON_LEADER)) {
    VLOG(1) << LogPrefixUnlocked() << "peer " << peer_uuid
            << " is no longer tracked or queue is not in leader mode";
    return;
  }
  peer->last_exchange_status = ps;

  if (ps != PeerStatus::RPC_LAYER_ERROR) {
    // So long as we got _any_ response from the follower, we consider it a 'communication'.
    // RPC_LAYER_ERROR indicates something like a connection failure, indicating that the
    // host itself is likely down.
    //
    // This indicates that the node is at least online.
    peer->last_communication_time = MonoTime::Now();
  }

  switch (ps) {
    case PeerStatus::NEW:
      LOG_WITH_PREFIX_UNLOCKED(DFATAL) << "Should not update an existing peer to 'NEW' state";
      break;

    case PeerStatus::RPC_LAYER_ERROR:
      // Most controller errors are caused by network issues or corner cases
      // like shutdown and failure to deserialize a protobuf. Therefore, we
      // generally consider these errors to indicate an unreachable peer.
      DCHECK(!status.ok());
      break;

    case PeerStatus::TABLET_NOT_FOUND:
      VLOG_WITH_PREFIX_UNLOCKED(1) << "Peer needs tablet copy: " << peer->ToString();
      break;

    case PeerStatus::TABLET_FAILED: {
      UpdatePeerHealthUnlocked(peer);
      return;
    }

    case PeerStatus::REMOTE_ERROR:
    case PeerStatus::INVALID_TERM:
    case PeerStatus::LMP_MISMATCH:
    case PeerStatus::CANNOT_PREPARE:
      // No special handling here for these - we assume that we'll just retry until
      // we make progress.
      break;

    case PeerStatus::OK:
      DCHECK(status.ok());
      break;
  }
}

void PeerMessageQueue::UpdateExchangeStatus(TrackedPeer* peer,
                                            const TrackedPeer& prev_peer_state,
                                            const ConsensusResponsePB& response,
                                            bool* lmp_mismatch) {
  DCHECK(queue_lock_.is_locked());
  const ConsensusStatusPB& status = response.status();

  peer->last_communication_time = MonoTime::Now();
  peer->last_known_committed_index = status.last_committed_idx();

  if (PREDICT_TRUE(!status.has_error())) {
    peer->last_exchange_status = PeerStatus::OK;
    *lmp_mismatch = false;
    return;
  }

  switch (status.error().code()) {
    case ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH:
      peer->last_exchange_status = PeerStatus::LMP_MISMATCH;
      DCHECK(status.has_last_received());
      if (prev_peer_state.last_exchange_status == PeerStatus::NEW) {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << "Connected to new peer: " << peer->ToString();
      } else {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << "Got LMP mismatch error from peer: "
                                       << peer->ToString();
      }
      *lmp_mismatch = true;
      return;

    case ConsensusErrorPB::INVALID_TERM:
      peer->last_exchange_status = PeerStatus::INVALID_TERM;
      CHECK(response.has_responder_term());
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Peer responded invalid term: " << peer->ToString();
      NotifyObserversOfTermChange(response.responder_term());
      *lmp_mismatch = false;
      return;

    default:
      // Other ConsensusStatusPB error codes (such as remote errors) are
      // supposed to be handled higher up in the stack.
      LOG_WITH_PREFIX_UNLOCKED(FATAL) << "Unexpected consensus error. Code: "
          << ConsensusErrorPB::Code_Name(status.error().code()) << ". Response: "
          << SecureShortDebugString(response);
  }
}

void PeerMessageQueue::PromoteIfNeeded(TrackedPeer* peer, const TrackedPeer& prev_peer_state,
                                       const ConsensusStatusPB& status) {
  DCHECK(queue_lock_.is_locked());
  if (queue_state_.mode != PeerMessageQueue::LEADER ||
      peer->last_exchange_status != PeerStatus::OK) {
    return;
  }

  // TODO(mpercy): It would be more efficient to cache the member type in the
  // TrackedPeer data structure.
  RaftPeerPB* peer_pb;
  Status s = GetRaftConfigMember(DCHECK_NOTNULL(queue_state_.active_config.get()),
                                 peer->uuid(), &peer_pb);
  if (s.ok() &&
      peer_pb->member_type() == RaftPeerPB::NON_VOTER &&
      peer_pb->attrs().promote()) {

    // Only promote the peer if it is within one round-trip of being fully
    // caught-up with the current commit index, as measured by recent
    // UpdateConsensus() operation batch sizes.

    // If we had never previously contacted this peer, wait until the second
    // time we contact them to try to promote them.
    if (prev_peer_state.last_received.index() == 0) return;

    int64_t last_batch_size =
        std::max<int64_t>(0, peer->last_received.index() - prev_peer_state.last_received.index());
    bool peer_caught_up =
        !OpIdEquals(status.last_received_current_leader(), MinimumOpId()) &&
        status.last_received_current_leader().index() + last_batch_size
            >= queue_state_.committed_index;
    if (!peer_caught_up) return;

    // TODO(mpercy): Implement a SafeToPromote() check to ensure that we only
    // try to promote a NON_VOTER to VOTER if we will be able to commit the
    // resulting config change operation.
    NotifyObserversOfPeerToPromote(peer->uuid());
  }
}

void PeerMessageQueue::TransferLeadershipIfNeeded(const TrackedPeer& peer,
                                                  const ConsensusStatusPB& status) {
  DCHECK(queue_lock_.is_locked());
  if (!successor_watch_in_progress_) {
    return;
  }

  if (designated_successor_uuid_ && peer.uuid() != designated_successor_uuid_.get()) {
    return;
  }

  if (queue_state_.mode != PeerMessageQueue::LEADER ||
      peer.last_exchange_status != PeerStatus::OK) {
    return;
  }

  RaftPeerPB* peer_pb;
  Status s = GetRaftConfigMember(DCHECK_NOTNULL(queue_state_.active_config.get()),
                                 peer.uuid(), &peer_pb);
  if (!s.ok() || peer_pb->member_type() != RaftPeerPB::VOTER) {
    return;
  }

  bool peer_caught_up =
      !OpIdEquals(status.last_received_current_leader(), MinimumOpId()) &&
      OpIdEquals(status.last_received_current_leader(), queue_state_.last_appended);
  if (!peer_caught_up) {
    return;
  }

  VLOG(1) << "Successor watch: peer " << peer.uuid() << " is caught up to "
          << "the leader at OpId " << OpIdToString(status.last_received_current_leader());
  successor_watch_in_progress_ = false;
  NotifyObserversOfSuccessor(peer.uuid());
}

bool PeerMessageQueue::ResponseFromPeer(const std::string& peer_uuid,
                                        const ConsensusResponsePB& response) {
  DCHECK(response.IsInitialized()) << "Error: Uninitialized: "
      << response.InitializationErrorString() << ". Response: " << SecureShortDebugString(response);
  CHECK(!response.has_error());

  bool send_more_immediately = false;
  boost::optional<int64_t> updated_commit_index;
  Mode mode_copy;
  {
    std::lock_guard<simple_spinlock> scoped_lock(queue_lock_);

    TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
    if (PREDICT_FALSE(queue_state_.state != kQueueOpen || peer == nullptr)) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Queue is closed or peer was untracked, disregarding "
          "peer response. Response: " << SecureShortDebugString(response);
      return send_more_immediately;
    }

    // Sanity checks.
    // Some of these can be eventually removed, but they are handy for now.
    DCHECK(response.status().IsInitialized())
        << "Error: Uninitialized: " << response.InitializationErrorString()
        << ". Response: "<< SecureShortDebugString(response);
    // TODO(mpercy): Include uuid in error messages as well.
    DCHECK(response.has_responder_uuid() && !response.responder_uuid().empty())
        << "Got response from peer with empty UUID";

    DCHECK(!response.has_error()); // Application-level errors should be handled elsewhere.
    DCHECK(response.has_status()); // Responses should always have a status.
    // The status must always have a last received op id and a last committed index.
    const ConsensusStatusPB& status = response.status();
    DCHECK(status.has_last_received());
    DCHECK(status.has_last_received_current_leader());
    DCHECK(status.has_last_committed_idx());

    // Take a snapshot of the previously-recorded peer state.
    const TrackedPeer prev_peer_state = *peer;

    // Update the peer's last exchange status based on the response.
    // In this case, if there is a log matching property (LMP) mismatch, we
    // want to immediately send another request as we attempt to sync the log
    // offset between the local leader and the remote peer.
    UpdateExchangeStatus(peer, prev_peer_state, response, &send_more_immediately);

    // If the reported last-received op for the replica is in our local log,
    // then resume sending entries from that point onward. Otherwise, resume
    // after the last op they received from us. If we've never successfully
    // sent them anything, start after the last-committed op in their log, which
    // is guaranteed by the Raft protocol to be a valid op.

    bool peer_has_prefix_of_log = IsOpInLog(status.last_received());
    if (peer_has_prefix_of_log) {
      // If the latest thing in their log is in our log, we are in sync.
      peer->last_received = status.last_received();
      peer->next_index = peer->last_received.index() + 1;

      // Check if the peer is a NON_VOTER candidate ready for promotion.
      PromoteIfNeeded(peer, prev_peer_state, status);

      TransferLeadershipIfNeeded(*peer, status);
    } else if (!OpIdEquals(status.last_received_current_leader(), MinimumOpId())) {
      // Their log may have diverged from ours, however we are in the process
      // of replicating our ops to them, so continue doing so. Eventually, we
      // will cause the divergent entry in their log to be overwritten.
      peer->last_received = status.last_received_current_leader();
      peer->next_index = peer->last_received.index() + 1;

    } else {
      // The peer is divergent and they have not (successfully) received
      // anything from us yet. Start sending from their last committed index.
      // This logic differs from the Raft spec slightly because instead of
      // stepping back one-by-one from the end until we no longer have an LMP
      // error, we jump back to the last committed op indicated by the peer with
      // the hope that doing so will result in a faster catch-up process.
      DCHECK_GE(peer->last_known_committed_index, 0);
      peer->next_index = peer->last_known_committed_index + 1;
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Peer " << peer_uuid << " log is divergent from this leader: "
          << "its last log entry " << OpIdToString(status.last_received()) << " is not in "
          << "this leader's log and it has not received anything from this leader yet. "
          << "Falling back to committed index " << peer->last_known_committed_index;
    }

    if (peer->last_exchange_status != PeerStatus::OK) {
      // In this case, 'send_more_immediately' has already been set by
      // UpdateExchangeStatus() to true in the case of an LMP mismatch, false
      // otherwise.
      return send_more_immediately;
    }

    if (response.has_responder_term()) {
      // The peer must have responded with a term that is greater than or equal to
      // the last known term for that peer.
      peer->CheckMonotonicTerms(response.responder_term());

      // If the responder didn't send an error back that must mean that it has
      // a term that is the same or lower than ours.
      CHECK_LE(response.responder_term(), queue_state_.current_term);
    }

    VLOG_WITH_PREFIX_UNLOCKED(2)
        << Substitute("Received Response from Peer ($0). Response: $1",
                      peer->ToString(), SecureShortDebugString(response));

    mode_copy = queue_state_.mode;

    // If we're the leader, we can compute the new watermarks based on the progress
    // of our followers.
    // NOTE: it's possible this node might have lost its leadership (and the notification
    // is just pending behind the lock we're holding), but any future leader will observe
    // the same watermarks and make the same advancement, so this is safe.
    if (mode_copy == LEADER) {
      // Advance the majority replicated index.
      AdvanceQueueWatermark("majority_replicated",
                            &queue_state_.majority_replicated_index,
                            /*replicated_before=*/ prev_peer_state.last_received,
                            /*replicated_after=*/ peer->last_received,
                            /*num_peers_required=*/ queue_state_.majority_size_,
                            VOTER_REPLICAS,
                            peer);

      // Advance the all replicated index.
      AdvanceQueueWatermark("all_replicated",
                            &queue_state_.all_replicated_index,
                            /*replicated_before=*/ prev_peer_state.last_received,
                            /*replicated_after=*/ peer->last_received,
                            /*num_peers_required=*/ peers_map_.size(),
                            ALL_REPLICAS,
                            peer);

      // If the majority-replicated index is in our current term,
      // and it is above our current committed index, then
      // we can advance the committed index.
      //
      // It would seem that the "it is above our current committed index"
      // check is redundant (and could be a CHECK), but in fact the
      // majority-replicated index can currently go down, since we don't
      // consider peers whose last contact was an error in the watermark
      // calculation. See the TODO in AdvanceQueueWatermark() for more details.
      int64_t commit_index_before = queue_state_.committed_index;
      if (queue_state_.first_index_in_current_term != boost::none &&
          queue_state_.majority_replicated_index >= queue_state_.first_index_in_current_term &&
          queue_state_.majority_replicated_index > queue_state_.committed_index) {
        queue_state_.committed_index = queue_state_.majority_replicated_index;
      } else {
        VLOG_WITH_PREFIX_UNLOCKED(2) << "Cannot advance commit index, waiting for > "
                                     << "first index in current leader term: "
                                     << queue_state_.first_index_in_current_term << ". "
                                     << "current majority_replicated_index: "
                                     << queue_state_.majority_replicated_index << ", "
                                     << "current committed_index: "
                                     << queue_state_.committed_index;

      }

      // Only notify observers if the commit index actually changed.
      if (queue_state_.committed_index != commit_index_before) {
        DCHECK_GT(queue_state_.committed_index, commit_index_before);
        updated_commit_index = queue_state_.committed_index;
        VLOG_WITH_PREFIX_UNLOCKED(2) << "Commit index advanced from "
                                     << commit_index_before << " to "
                                     << *updated_commit_index;
      }
    }

    // If the peer's committed index is lower than our own, or if our log has
    // the next request for the peer, set 'send_more_immediately' to true.
    send_more_immediately = peer->last_known_committed_index < queue_state_.committed_index ||
                            log_cache_.HasOpBeenWritten(peer->next_index);

    log_cache_.EvictThroughOp(queue_state_.all_replicated_index);

    UpdateMetricsUnlocked();
  }

  if (mode_copy == LEADER && updated_commit_index != boost::none) {
  // Suppress false positive about 'updated_commit_index' used when uninitialized.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    NotifyObserversOfCommitIndexChange(*updated_commit_index);
#pragma GCC diagnostic pop
  }

  return send_more_immediately;
}

PeerMessageQueue::TrackedPeer PeerMessageQueue::GetTrackedPeerForTests(const string& uuid) {
  std::lock_guard<simple_spinlock> scoped_lock(queue_lock_);
  TrackedPeer* tracked = FindOrDie(peers_map_, uuid);
  return *tracked;
}

int64_t PeerMessageQueue::GetAllReplicatedIndex() const {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.all_replicated_index;
}

int64_t PeerMessageQueue::GetCommittedIndex() const {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.committed_index;
}

bool PeerMessageQueue::IsCommittedIndexInCurrentTerm() const {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.first_index_in_current_term != boost::none &&
      queue_state_.committed_index >= *queue_state_.first_index_in_current_term;
}

bool PeerMessageQueue::IsInLeaderMode() const {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.mode == Mode::LEADER;
}

int64_t PeerMessageQueue::GetMajorityReplicatedIndexForTests() const {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  return queue_state_.majority_replicated_index;
}


void PeerMessageQueue::UpdateMetricsUnlocked() {
  DCHECK(queue_lock_.is_locked());
  // Since operations have consecutive indices we can update the metrics based
  // on simple index math.
  // For non-leaders, majority_done_ops isn't meaningful because followers don't
  // track when an op is replicated to all peers.
  metrics_.num_majority_done_ops->set_value(queue_state_.mode == LEADER ?
    queue_state_.committed_index - queue_state_.all_replicated_index
    : 0);
  metrics_.num_in_progress_ops->set_value(
    queue_state_.last_appended.index() - queue_state_.committed_index);

  UpdateLagMetricsUnlocked();
}

void PeerMessageQueue::UpdateLagMetricsUnlocked() {
  DCHECK(queue_lock_.is_locked());
  metrics_.num_ops_behind_leader->set_value(queue_state_.mode == LEADER ? 0 :
      queue_state_.last_idx_appended_to_leader - queue_state_.last_appended.index());
}

void PeerMessageQueue::DumpToStrings(vector<string>* lines) const {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  DumpToStringsUnlocked(lines);
}

void PeerMessageQueue::DumpToStringsUnlocked(vector<string>* lines) const {
  DCHECK(queue_lock_.is_locked());
  lines->push_back("Watermarks:");
  for (const PeersMap::value_type& entry : peers_map_) {
    lines->push_back(
        Substitute("Peer: $0 Watermark: $1", entry.first, entry.second->ToString()));
  }

  log_cache_.DumpToStrings(lines);
}

void PeerMessageQueue::DumpToHtml(std::ostream& out) const {
  using std::endl;

  std::lock_guard<simple_spinlock> lock(queue_lock_);
  out << "<h3>Watermarks</h3>" << endl;
  out << "<table>" << endl;
  out << "  <tr><th>Peer</th><th>Watermark</th></tr>" << endl;
  for (const PeersMap::value_type& entry : peers_map_) {
    out << Substitute("  <tr><td>$0</td><td>$1</td></tr>",
                      EscapeForHtmlToString(entry.first),
                      EscapeForHtmlToString(entry.second->ToString())) << endl;
  }
  out << "</table>" << endl;
  out << "<p>" << queue_state_.ToString() << "</p>" << endl;

  log_cache_.DumpToHtml(out);
}

void PeerMessageQueue::ClearUnlocked() {
  DCHECK(queue_lock_.is_locked());
  STLDeleteValues(&peers_map_);
  queue_state_.state = kQueueClosed;
}

void PeerMessageQueue::Close() {
  raft_pool_observers_token_->Shutdown();

  std::lock_guard<simple_spinlock> lock(queue_lock_);
  ClearUnlocked();
}

int64_t PeerMessageQueue::GetQueuedOperationsSizeBytesForTests() const {
  return log_cache_.BytesUsed();
}

string PeerMessageQueue::ToString() const {
  // Even though metrics are thread-safe obtain the lock so that we get
  // a "consistent" snapshot of the metrics.
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  return ToStringUnlocked();
}

string PeerMessageQueue::ToStringUnlocked() const {
  DCHECK(queue_lock_.is_locked());
  return Substitute("Consensus queue metrics: "
                    "Only Majority Done Ops: $0, In Progress Ops: $1, Cache: $2",
                    metrics_.num_majority_done_ops->value(), metrics_.num_in_progress_ops->value(),
                    log_cache_.StatsString());
}

void PeerMessageQueue::RegisterObserver(PeerMessageQueueObserver* observer) {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  auto iter = std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    observers_.push_back(observer);
  }
}

Status PeerMessageQueue::UnRegisterObserver(PeerMessageQueueObserver* observer) {
  std::lock_guard<simple_spinlock> lock(queue_lock_);
  auto iter = std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    return Status::NotFound("Can't find observer.");
  }
  observers_.erase(iter);
  return Status::OK();
}

bool PeerMessageQueue::IsOpInLog(const OpId& desired_op) const {
  OpId log_op;
  Status s = log_cache_.LookupOpId(desired_op.index(), &log_op);
  if (PREDICT_TRUE(s.ok())) {
    return OpIdEquals(desired_op, log_op);
  }
  if (PREDICT_TRUE(s.IsNotFound() || s.IsIncomplete())) {
    return false;
  }
  LOG_WITH_PREFIX_UNLOCKED(FATAL) << "Error while reading the log: " << s.ToString();
  return false; // Unreachable; here to squelch GCC warning.
}

void PeerMessageQueue::NotifyObserversOfCommitIndexChange(int64_t new_commit_index) {
  WARN_NOT_OK(raft_pool_observers_token_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversTask, Unretained(this),
           [=](PeerMessageQueueObserver* observer) {
             observer->NotifyCommitIndex(new_commit_index);
           })),
      LogPrefixUnlocked() + "Unable to notify RaftConsensus of commit index change.");
}

void PeerMessageQueue::NotifyObserversOfTermChange(int64_t term) {
  WARN_NOT_OK(raft_pool_observers_token_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversTask, Unretained(this),
           [=](PeerMessageQueueObserver* observer) {
             observer->NotifyTermChange(term);
           })),
      LogPrefixUnlocked() + "Unable to notify RaftConsensus of term change.");
}

void PeerMessageQueue::NotifyObserversOfFailedFollower(const string& uuid,
                                                       int64_t term,
                                                       const string& reason) {
  WARN_NOT_OK(raft_pool_observers_token_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversTask, Unretained(this),
           [=](PeerMessageQueueObserver* observer) {
             observer->NotifyFailedFollower(uuid, term, reason);
           })),
      LogPrefixUnlocked() + "Unable to notify RaftConsensus of abandoned follower.");
}

void PeerMessageQueue::NotifyObserversOfPeerToPromote(const string& peer_uuid) {
  WARN_NOT_OK(raft_pool_observers_token_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversTask, Unretained(this),
           [=](PeerMessageQueueObserver* observer) {
             observer->NotifyPeerToPromote(peer_uuid);
           })),
      LogPrefixUnlocked() + "Unable to notify RaftConsensus of peer to promote.");
}

void PeerMessageQueue::NotifyObserversOfSuccessor(const string& peer_uuid) {
  DCHECK(queue_lock_.is_locked());
  WARN_NOT_OK(raft_pool_observers_token_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversTask, Unretained(this),
           [=](PeerMessageQueueObserver* observer) {
             observer->NotifyPeerToStartElection(peer_uuid);
           })),
      LogPrefixUnlocked() + "Unable to notify RaftConsensus of available successor.");
}

void PeerMessageQueue::NotifyObserversOfPeerHealthChange() {
  WARN_NOT_OK(raft_pool_observers_token_->SubmitClosure(
      Bind(&PeerMessageQueue::NotifyObserversTask, Unretained(this),
           [](PeerMessageQueueObserver* observer) {
             observer->NotifyPeerHealthChange();
           })),
      LogPrefixUnlocked() + "Unable to notify RaftConsensus peer health change.");
}

void PeerMessageQueue::NotifyObserversTask(
    const std::function<void(PeerMessageQueueObserver*)>& func) {
  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_consensus_inject_latency_ms_in_notifications);
  std::vector<PeerMessageQueueObserver*> observers_copy;
  {
    std::lock_guard<simple_spinlock> lock(queue_lock_);
    observers_copy = observers_;
  }
  for (PeerMessageQueueObserver* observer : observers_copy) {
    func(observer);
  }
}

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

string PeerMessageQueue::LogPrefixUnlocked() const {
  // TODO: we should probably use an atomic here. We'll just annotate
  // away the TSAN error for now, since the worst case is a slightly out-of-date
  // log message, and not very likely.
  Mode mode = ANNOTATE_UNPROTECTED_READ(queue_state_.mode);
  return Substitute("T $0 P $1 [$2]: ",
                    tablet_id_,
                    local_peer_pb_.permanent_uuid(),
                    mode == LEADER ? "LEADER" : "NON_LEADER");
}

string PeerMessageQueue::QueueState::ToString() const {
  return Substitute("All replicated index: $0, Majority replicated index: $1, "
      "Committed index: $2, Last appended: $3, Last appended by leader: $4, Current term: $5, "
      "Majority size: $6, State: $7, Mode: $8$9",
      all_replicated_index, majority_replicated_index,
      committed_index, OpIdToString(last_appended), last_idx_appended_to_leader, current_term,
      majority_size_, state, (mode == LEADER ? "LEADER" : "NON_LEADER"),
      active_config ? ", active raft config: " + SecureShortDebugString(*active_config) : "");
}

}  // namespace consensus
}  // namespace kudu
