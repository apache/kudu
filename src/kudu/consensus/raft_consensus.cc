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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#include "kudu/consensus/raft_consensus.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <google/protobuf/util/message_differencer.h>

#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/leader_election.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/peer_manager.h"
#include "kudu/consensus/pending_rounds.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/routing.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/periodic.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/async_util.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"

DEFINE_int32(raft_heartbeat_interval_ms, 500,
             "The heartbeat interval for Raft replication. The leader produces heartbeats "
             "to followers at this interval. The followers expect a heartbeat at this interval "
             "and consider a leader to have failed if it misses several in a row.");
TAG_FLAG(raft_heartbeat_interval_ms, advanced);

DEFINE_double(leader_failure_max_missed_heartbeat_periods, 3.0,
             "Maximum heartbeat periods that the leader can fail to heartbeat in before we "
             "consider the leader to be failed. The total failure timeout in milliseconds is "
             "raft_heartbeat_interval_ms times leader_failure_max_missed_heartbeat_periods. "
             "The value passed to this flag may be fractional.");
TAG_FLAG(leader_failure_max_missed_heartbeat_periods, advanced);

DEFINE_double(snooze_for_leader_ban_ratio, 1.0,
             "Failure detector for this instance should be at a higher ratio than other instances"
             ". This will prevent this instance from initiating an election");
TAG_FLAG(snooze_for_leader_ban_ratio, advanced);

DEFINE_int32(leader_failure_exp_backoff_max_delta_ms, 20 * 1000,
             "Maximum time to sleep in between leader election retries, in addition to the "
             "regular timeout. When leader election fails the interval in between retries "
             "increases exponentially, up to this value.");
TAG_FLAG(leader_failure_exp_backoff_max_delta_ms, experimental);

DEFINE_bool(enable_leader_failure_detection, true,
            "Whether to enable failure detection of tablet leaders. If enabled, attempts will be "
            "made to elect a follower as a new leader when the leader is detected to have failed.");
TAG_FLAG(enable_leader_failure_detection, unsafe);

DEFINE_bool(evict_failed_followers, true,
            "Whether to evict followers from the Raft config that have fallen "
            "too far behind the leader's log to catch up normally or have been "
            "unreachable by the leader for longer than "
            "follower_unavailable_considered_failed_sec");
TAG_FLAG(evict_failed_followers, advanced);

DEFINE_bool(follower_reject_update_consensus_requests, false,
            "Whether a follower will return an error for all UpdateConsensus() requests. "
            "Warning! This is only intended for testing.");
TAG_FLAG(follower_reject_update_consensus_requests, unsafe);

DEFINE_bool(follower_fail_all_prepare, false,
            "Whether a follower will fail preparing all transactions. "
            "Warning! This is only intended for testing.");
TAG_FLAG(follower_fail_all_prepare, unsafe);

DEFINE_bool(raft_enable_pre_election, true,
            "When enabled, candidates will call a pre-election before "
            "running a real leader election.");
TAG_FLAG(raft_enable_pre_election, experimental);
TAG_FLAG(raft_enable_pre_election, runtime);

DEFINE_bool(raft_enable_tombstoned_voting, true,
            "When enabled, tombstoned tablets may vote in elections.");
TAG_FLAG(raft_enable_tombstoned_voting, experimental);
TAG_FLAG(raft_enable_tombstoned_voting, runtime);

// Enable improved re-replication (KUDU-1097).
DEFINE_bool(raft_prepare_replacement_before_eviction, true,
            "When enabled, failed replicas will only be evicted after a "
            "replacement has been prepared for them.");
TAG_FLAG(raft_prepare_replacement_before_eviction, advanced);
TAG_FLAG(raft_prepare_replacement_before_eviction, experimental);

DEFINE_bool(raft_attempt_to_replace_replica_without_majority, false,
            "When enabled, the replica replacement logic attempts to perform "
            "desired Raft configuration changes even if the majority "
            "of voter replicas is reported failed or offline. "
            "Warning! This is only intended for testing.");
TAG_FLAG(raft_attempt_to_replace_replica_without_majority, unsafe);

DEFINE_bool(raft_enable_multi_hop_proxy_routing, false,
            "Enables multi-hop routing. When disabled, causes any tablet "
            "server acting as a proxy to forward any incoming replication "
            "request directly to the destination node, if it is part of the "
            "active config.");
TAG_FLAG(raft_enable_multi_hop_proxy_routing, advanced);
TAG_FLAG(raft_enable_multi_hop_proxy_routing, runtime);

DEFINE_int32(raft_log_cache_proxy_wait_time_ms, 500,
             "Maximum wait time for proxied messages to wait for events to "
             "appear in the local log cache");
TAG_FLAG(raft_log_cache_proxy_wait_time_ms, advanced);
TAG_FLAG(raft_log_cache_proxy_wait_time_ms, runtime);

DECLARE_int32(memory_limit_warn_threshold_percentage);
DECLARE_int32(consensus_max_batch_size_bytes); // defined in consensus_queue (expose as method?)
DECLARE_int32(consensus_rpc_timeout_ms);

DEFINE_bool(raft_derived_log_mode, false,
            "When derived log mode is turned on, certain functions"
            " inside kudu raft become invalid");

DEFINE_bool(enable_flexi_raft, false,
            "Enables flexi raft mode. All the configurations need to be already"
            " present and setup before the flag can be enabled.");

// Metrics
// ---------
METRIC_DEFINE_counter(server, follower_memory_pressure_rejections,
                      "Follower Memory Pressure Rejections",
                      kudu::MetricUnit::kRequests,
                      "Number of RPC requests rejected due to "
                      "memory pressure while FOLLOWER.");
METRIC_DEFINE_gauge_int64(server, raft_term,
                          "Current Raft Consensus Term",
                          kudu::MetricUnit::kUnits,
                          "Current Term of the Raft Consensus algorithm. This number increments "
                          "each time a leader election is started.");
METRIC_DEFINE_gauge_int64(server, failed_elections_since_stable_leader,
                          "Failed Elections Since Stable Leader",
                          kudu::MetricUnit::kUnits,
                          "Number of failed elections on this node since there was a stable "
                          "leader. This number increments on each failed election and resets on "
                          "each successful one.");
METRIC_DEFINE_gauge_int64(server, time_since_last_leader_heartbeat,
                          "Time Since Last Leader Heartbeat",
                          kudu::MetricUnit::kMilliseconds,
                          "The time elapsed since the last heartbeat from the leader "
                          "in milliseconds. This metric is identically zero on a leader replica.");


using boost::optional;
using google::protobuf::util::MessageDifferencer;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::PeriodicTimer;
//using kudu::ServerErrorPB;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using std::weak_ptr;
using strings::Substitute;

namespace kudu {
namespace consensus {

RaftConsensus::RaftConsensus(
    ConsensusOptions options,
    RaftPeerPB local_peer_pb,
    scoped_refptr<ConsensusMetadataManager> cmeta_manager,
    ThreadPool* raft_pool)
    : options_(std::move(options)),
      local_peer_pb_(std::move(local_peer_pb)),
      cmeta_manager_(std::move(cmeta_manager)),
      raft_pool_(raft_pool),
      state_(kNew),
      rng_(GetRandomSeed32()),
      leader_transfer_in_progress_(false),
      withhold_votes_until_(MonoTime::Min()),
      last_received_cur_leader_(MinimumOpId()),
      failed_elections_since_stable_leader_(0),
      disable_noop_(false),
      shutdown_(false),
      update_calls_for_tests_(0) {
  DCHECK(local_peer_pb_.has_permanent_uuid());
  DCHECK(cmeta_manager_ != NULL);
}

Status RaftConsensus::Init() {
  DCHECK_EQ(kNew, state_) << State_Name(state_);
  RETURN_NOT_OK(cmeta_manager_->LoadCMeta(options_.tablet_id, &cmeta_));
  RETURN_NOT_OK(cmeta_manager_->LoadDRT(options_.tablet_id, cmeta_->ActiveConfig(),
                                        &routing_table_));
  SetStateUnlocked(kInitialized);
  return Status::OK();
}

RaftConsensus::~RaftConsensus() {
  Shutdown();
}

Status RaftConsensus::Create(ConsensusOptions options,
                             RaftPeerPB local_peer_pb,
                             scoped_refptr<ConsensusMetadataManager> cmeta_manager,
                             ThreadPool* raft_pool,
                             shared_ptr<RaftConsensus>* consensus_out) {
  shared_ptr<RaftConsensus> consensus(RaftConsensus::make_shared(std::move(options),
                                                                 std::move(local_peer_pb),
                                                                 std::move(cmeta_manager),
                                                                 raft_pool));
  RETURN_NOT_OK_PREPEND(consensus->Init(), "Unable to initialize Raft consensus");
  *consensus_out = std::move(consensus);
  return Status::OK();
}

Status RaftConsensus::Start(const ConsensusBootstrapInfo& info,
                            gscoped_ptr<PeerProxyFactory> peer_proxy_factory,
                            scoped_refptr<log::Log> log,
                            scoped_refptr<TimeManager> time_manager,
                            ConsensusRoundHandler* round_handler,
                            const scoped_refptr<MetricEntity>& metric_entity,
                            Callback<void(const std::string& reason)> mark_dirty_clbk) {
  DCHECK(metric_entity);

  peer_proxy_factory_ = std::move(peer_proxy_factory);
  log_ = std::move(log);
  time_manager_ = std::move(time_manager);
  round_handler_ = DCHECK_NOTNULL(round_handler);
  mark_dirty_clbk_ = std::move(mark_dirty_clbk);

  DCHECK(peer_proxy_factory_ != NULL);
  DCHECK(log_ != NULL);
  DCHECK(time_manager_ != NULL);

  term_metric_ = metric_entity->FindOrCreateGauge(&METRIC_raft_term, CurrentTerm());
  follower_memory_pressure_rejections_ =
      metric_entity->FindOrCreateCounter(&METRIC_follower_memory_pressure_rejections);

  num_failed_elections_metric_ =
      metric_entity->FindOrCreateGauge(&METRIC_failed_elections_since_stable_leader,
                                       failed_elections_since_stable_leader_);

  METRIC_time_since_last_leader_heartbeat.InstantiateFunctionGauge(
    metric_entity, Bind(&RaftConsensus::GetMillisSinceLastLeaderHeartbeat, Unretained(this)))
    ->AutoDetach(&metric_detacher_);

  // A single Raft thread pool token is shared between RaftConsensus and
  // PeerManager. Because PeerManager is owned by RaftConsensus, it receives a
  // raw pointer to the token, to emphasize that RaftConsensus is responsible
  // for destroying the token.
  raft_pool_token_ = raft_pool_->NewToken(ThreadPool::ExecutionMode::CONCURRENT);

  // The message queue that keeps track of which operations need to be replicated
  // where.
  //
  // Note: the message queue receives a dedicated Raft thread pool token so that
  // its submissions don't block other submissions by RaftConsensus (such as
  // heartbeat processing).
  //
  // TODO(adar): the token is SERIAL to match the previous single-thread
  // observer pool behavior, but CONCURRENT may be safe here.
  unique_ptr<PeerMessageQueue> queue(new PeerMessageQueue(
      metric_entity,
      log_,
      time_manager_,
      local_peer_pb_,
      options_.tablet_id,
      raft_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL),
      info.last_id,
      info.last_committed_id));

  // A manager for the set of peers that actually send the operations both remotely
  // and to the local wal.
  unique_ptr<PeerManager> peer_manager(new PeerManager(options_.tablet_id,
                                                       peer_uuid(),
                                                       peer_proxy_factory_.get(),
                                                       queue.get(),
                                                       raft_pool_token_.get(),
                                                       log_));

  unique_ptr<PendingRounds> pending(new PendingRounds(LogPrefixThreadSafe(), time_manager_));

  // Capture a weak_ptr reference into the functor so it can safely handle
  // outliving the consensus instance.
  weak_ptr<RaftConsensus> w = shared_from_this();
  failure_detector_ = PeriodicTimer::Create(
      peer_proxy_factory_->messenger(),
      [w]() {
        if (auto consensus = w.lock()) {
          consensus->ReportFailureDetected();
        }
      },
      MinimumElectionTimeout());

  PeriodicTimer::Options opts;
  opts.one_shot = true;
  transfer_period_timer_ = PeriodicTimer::Create(
      peer_proxy_factory_->messenger(),
      [w]() {
        if (auto consensus = w.lock()) {
          consensus->EndLeaderTransferPeriod();
        }
      },
      MinimumElectionTimeout(),
      opts);

  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    CHECK_EQ(kInitialized, state_) << LogPrefixUnlocked() << "Illegal state for Start(): "
                                   << State_Name(state_);

    queue_ = std::move(queue);
    peer_manager_ = std::move(peer_manager);
    pending_ = std::move(pending);

    ClearLeaderUnlocked();

    // Our last persisted term can be higher than the last persisted operation
    // (i.e. if we called an election) but reverse should never happen.
    if (info.last_id.term() > CurrentTermUnlocked()) {
      return Status::Corruption(Substitute("Unable to start RaftConsensus: "
          "The last op in the WAL with id $0 has a term ($1) that is greater "
          "than the latest recorded term, which is $2",
          OpIdToString(info.last_id),
          info.last_id.term(),
          CurrentTermUnlocked()));
    }

    // Append any uncommitted replicate messages found during log replay to the queue.
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Replica starting. Triggering "
                                   << info.orphaned_replicates.size()
                                   << " pending transactions. Active config: "
                                   << SecureShortDebugString(cmeta_->ActiveConfig());
    for (ReplicateMsg* replicate : info.orphaned_replicates) {
      ReplicateRefPtr replicate_ptr = make_scoped_refptr_replicate(new ReplicateMsg(*replicate));
      RETURN_NOT_OK(StartFollowerTransactionUnlocked(replicate_ptr));
    }

    // Set the initial committed opid for the PendingRounds only after
    // appending any uncommitted replicate messages to the queue.
    pending_->SetInitialCommittedOpId(info.last_committed_id);

    // If this is the first term expire the FD immediately so that we have a
    // fast first election, otherwise we just let the timer expire normally.
    boost::optional<MonoDelta> fd_initial_delta;
    if (CurrentTermUnlocked() == 0) {
      // The failure detector is initialized to a low value to trigger an early
      // election (unless someone else requested a vote from us first, which
      // resets the election timer).
      //
      // We do it this way instead of immediately running an election to get a
      // higher likelihood of enough servers being available when the first one
      // attempts an election to avoid multiple election cycles on startup,
      // while keeping that "waiting period" random.
      if (PREDICT_TRUE(FLAGS_enable_leader_failure_detection)) {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << "Consensus starting up: Expiring failure detector timer "
                                          "to make a prompt election more likely";
        fd_initial_delta = MonoDelta::FromMilliseconds(
            rng_.Uniform(FLAGS_raft_heartbeat_interval_ms));
      }
    }

    // Now assume non-leader replica duties.
    RETURN_NOT_OK(BecomeReplicaUnlocked(fd_initial_delta));

    SetStateUnlocked(kRunning);
  }

  if (IsSingleVoterConfig() && FLAGS_enable_leader_failure_detection) {
    LOG_WITH_PREFIX(INFO) << "Only one voter in the Raft config. Triggering election immediately";
    RETURN_NOT_OK(StartElection(NORMAL_ELECTION, INITIAL_SINGLE_NODE_ELECTION));
  }

  // Report become visible to the Master.
  MarkDirty("RaftConsensus started");

  return Status::OK();
}

bool RaftConsensus::IsRunning() const {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  return state_ == kRunning;
}

Status RaftConsensus::EmulateElection() {
  TRACE_EVENT2("consensus", "RaftConsensus::EmulateElection",
               "peer", peer_uuid(),
               "tablet", options_.tablet_id);

  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  RETURN_NOT_OK(CheckRunningUnlocked());

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Emulating election...";

  // Assume leadership of new term.
  RETURN_NOT_OK(HandleTermAdvanceUnlocked(CurrentTermUnlocked() + 1));
  SetLeaderUuidUnlocked(peer_uuid());
  return BecomeLeaderUnlocked();
}

namespace {
const char* ModeString(RaftConsensus::ElectionMode mode) {
  switch (mode) {
    case RaftConsensus::NORMAL_ELECTION:
      return "leader election";
    case RaftConsensus::PRE_ELECTION:
      return "pre-election";
    case RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE:
      return "forced leader election";
  }
  __builtin_unreachable(); // silence gcc warnings
}
string ReasonString(RaftConsensus::ElectionReason reason, StringPiece leader_uuid) {
  switch (reason) {
    case RaftConsensus::INITIAL_SINGLE_NODE_ELECTION:
      return "initial election of a single-replica configuration";
    case RaftConsensus::EXTERNAL_REQUEST:
      return "received explicit request";
    case RaftConsensus::ELECTION_TIMEOUT_EXPIRED:
      if (leader_uuid.empty()) {
        return "no leader contacted us within the election timeout";
      }
      return Substitute("detected failure of leader $0", leader_uuid);
  }
  __builtin_unreachable(); // silence gcc warnings
}
} // anonymous namespace

Status RaftConsensus::StartElection(ElectionMode mode, ElectionReason reason) {
  const char* const mode_str = ModeString(mode);

  TRACE_EVENT2("consensus", "RaftConsensus::StartElection",
               "peer", LogPrefixThreadSafe(),
               "mode", mode_str);
  scoped_refptr<LeaderElection> election;
  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    RETURN_NOT_OK(CheckRunningUnlocked());

    RaftPeerPB::Role active_role = cmeta_->active_role();
    if (active_role == RaftPeerPB::LEADER) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << Substitute(
          "Not starting $0 -- already a leader", mode_str);
      return Status::OK();
    }
    if (PREDICT_FALSE(!consensus::IsVoterRole(active_role))) {
      // A non-voter should not start leader elections. The leader failure
      // detector should be re-enabled once the non-voter replica is promoted
      // to voter replica.
      return Status::IllegalState("only voting members can start elections",
          SecureShortDebugString(cmeta_->ActiveConfig()));
    }
    if (PREDICT_FALSE(active_role == RaftPeerPB::NON_PARTICIPANT)) {
      SnoozeFailureDetector();
      return Status::IllegalState("Not starting election: node is currently "
                                  "a non-participant in the Raft config",
                                  SecureShortDebugString(cmeta_->ActiveConfig()));
    }
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Starting " << mode_str
        << " (" << ReasonString(reason, GetLeaderUuidUnlocked()) << ")";

    // Snooze to avoid the election timer firing again as much as possible.
    // We do not disable the election timer while running an election, so that
    // if the election times out, we will try again.
    MonoDelta timeout = LeaderElectionExpBackoffDeltaUnlocked();
    SnoozeFailureDetector(string("starting election"), timeout);

    // Increment the term and vote for ourselves, unless it's a pre-election.
    if (mode != PRE_ELECTION) {
      // TODO(mpercy): Consider using a separate Mutex for voting, which must sync to disk.

      // We skip flushing the term to disk because setting the vote just below also
      // flushes to disk, and the double fsync doesn't buy us anything.
      RETURN_NOT_OK(HandleTermAdvanceUnlocked(CurrentTermUnlocked() + 1,
                                              SKIP_FLUSH_TO_DISK));
      RETURN_NOT_OK(SetVotedForCurrentTermUnlocked(peer_uuid()));
    }

    RaftConfigPB active_config = cmeta_->ActiveConfig();
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Starting " << mode_str << " with config: "
                                   << SecureShortDebugString(active_config);

    // Initialize the VoteCounter.
    gscoped_ptr<VoteCounter> counter;

    if (!FLAGS_enable_flexi_raft) {
      int num_voters = CountVoters(active_config);
      int majority_size = MajoritySize(num_voters);
      counter.reset(new VoteCounter(num_voters, majority_size));
    } else {
      counter.reset(new FlexibleVoteCounter(active_config));
    }

    // Vote for ourselves.
    bool duplicate;
    RETURN_NOT_OK(counter->RegisterVote(peer_uuid(), VOTE_GRANTED, &duplicate));
    CHECK(!duplicate) << LogPrefixUnlocked()
                      << "Inexplicable duplicate self-vote for term "
                      << CurrentTermUnlocked();

    VoteRequestPB request;
    request.set_ignore_live_leader(mode == ELECT_EVEN_IF_LEADER_IS_ALIVE);
    request.set_candidate_uuid(peer_uuid());
    if (mode == PRE_ELECTION) {
      // In a pre-election, we haven't bumped our own term yet, so we need to be
      // asking for votes for the next term.
      request.set_is_pre_election(true);
      request.set_candidate_term(CurrentTermUnlocked() + 1);
    } else {
      request.set_candidate_term(CurrentTermUnlocked());
    }
    request.set_tablet_id(options_.tablet_id);
    *request.mutable_candidate_status()->mutable_last_received() =
        queue_->GetLastOpIdInLog();

    election.reset(new LeaderElection(
        std::move(active_config),
        // The RaftConsensus ref passed below ensures that this raw pointer
        // remains safe to use for the entirety of LeaderElection's life.
        peer_proxy_factory_.get(),
        std::move(request), std::move(counter), timeout,
        std::bind(&RaftConsensus::ElectionCallback,
                  shared_from_this(),
                  reason, std::placeholders::_1)));
  }

  // Start the election outside the lock.
  election->Run();

  return Status::OK();
}

Status RaftConsensus::WaitUntilLeaderForTests(const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;
  while (role() != consensus::RaftPeerPB::LEADER) {
    if (MonoTime::Now() >= deadline) {
      return Status::TimedOut(Substitute("Peer $0 is not leader of tablet $1 after $2. Role: $3",
                                         peer_uuid(), options_.tablet_id, timeout.ToString(),
                                         role()));
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::OK();
}

Status RaftConsensus::StepDown(LeaderStepDownResponsePB* resp) {
  TRACE_EVENT0("consensus", "RaftConsensus::StepDown");
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  DCHECK((queue_->IsInLeaderMode() && cmeta_->active_role() == RaftPeerPB::LEADER) ||
         (!queue_->IsInLeaderMode() && cmeta_->active_role() != RaftPeerPB::LEADER));
  RETURN_NOT_OK(CheckRunningUnlocked());
  if (cmeta_->active_role() != RaftPeerPB::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Rejecting request to step down while not leader";
    resp->mutable_error()->set_code(ServerErrorPB::NOT_THE_LEADER);
    StatusToPB(Status::IllegalState("Not currently leader"),
               resp->mutable_error()->mutable_status());
    // We return OK so that the tablet service won't overwrite the error code.
    return Status::OK();
  }
  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Received request to step down";
  RETURN_NOT_OK(HandleTermAdvanceUnlocked(CurrentTermUnlocked() + 1,
                                          SKIP_FLUSH_TO_DISK));
  // Snooze the failure detector for an extra leader failure timeout.
  // This should ensure that a different replica is elected leader after this one steps down.
  SnoozeFailureDetector(string("explicit stepdown request"), MonoDelta::FromMilliseconds(
      2 * MinimumElectionTimeout().ToMilliseconds()));
  return Status::OK();
}

Status RaftConsensus::TransferLeadership(const boost::optional<string>& new_leader_uuid,
    const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
    LeaderStepDownResponsePB* resp) {
  TRACE_EVENT0("consensus", "RaftConsensus::TransferLeadership");
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Received request to transfer leadership"
                                 << (new_leader_uuid ?
                                    Substitute(" to TS $0", *new_leader_uuid) :
                                    "");
  DCHECK((queue_->IsInLeaderMode() && cmeta_->active_role() == RaftPeerPB::LEADER) ||
         (!queue_->IsInLeaderMode() && cmeta_->active_role() != RaftPeerPB::LEADER));
  RETURN_NOT_OK(CheckRunningUnlocked());
  if (cmeta_->active_role() != RaftPeerPB::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Rejecting request to transer leadership while not leader";
    resp->mutable_error()->set_code(ServerErrorPB::NOT_THE_LEADER);
    StatusToPB(Status::IllegalState("not currently leader"),
               resp->mutable_error()->mutable_status());
    // We return OK so that the tablet service won't overwrite the error code.
    return Status::OK();
  }
  if (new_leader_uuid) {
    if (*new_leader_uuid == peer_uuid()) {
      // Short-circuit as we are transferring leadership to ourselves and we
      // already checked that we are leader.
      return Status::OK();
    }
    if (!IsRaftConfigVoter(*new_leader_uuid, cmeta_->ActiveConfig())) {
      const string msg = Substitute("tablet server $0 is not a voter in the active config",
                                    *new_leader_uuid);
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Rejecting request to transfer leadership "
                                     << "because " << msg;
      return Status::InvalidArgument(msg);
    }
  }
  return BeginLeaderTransferPeriodUnlocked(new_leader_uuid, filter_fn);
}

Status RaftConsensus::BeginLeaderTransferPeriodUnlocked(
    const boost::optional<string>& successor_uuid,
    const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn) {
  DCHECK(lock_.is_locked());
  if (leader_transfer_in_progress_.CompareAndSwap(false, true)) {
    return Status::ServiceUnavailable(
        Substitute("leadership transfer for $0 already in progress",
                   options_.tablet_id));
  }
  leader_transfer_in_progress_.Store(true, kMemOrderAcquire);
  queue_->BeginWatchForSuccessor(successor_uuid, filter_fn);
  transfer_period_timer_->Start();
  return Status::OK();
}

void RaftConsensus::EndLeaderTransferPeriod() {
  transfer_period_timer_->Stop();
  queue_->EndWatchForSuccessor();
  leader_transfer_in_progress_.Store(false, kMemOrderRelease);
}

scoped_refptr<ConsensusRound> RaftConsensus::NewRound(
    gscoped_ptr<ReplicateMsg> replicate_msg,
    ConsensusReplicatedCallback replicated_cb) {
  return make_scoped_refptr(new ConsensusRound(this,
                                               std::move(replicate_msg),
                                               std::move(replicated_cb)));
}

void RaftConsensus::ReportFailureDetectedTask() {
  std::unique_lock<simple_spinlock> try_lock(failure_detector_election_lock_,
                                             std::try_to_lock);
  if (try_lock.owns_lock()) {
    WARN_NOT_OK(StartElection(FLAGS_raft_enable_pre_election ?
        PRE_ELECTION : NORMAL_ELECTION, ELECTION_TIMEOUT_EXPIRED),
                LogPrefixThreadSafe() + "failed to trigger leader election");
  }
}

void RaftConsensus::ReportFailureDetected() {
  // We're running on a timer thread; start an election on a different thread pool.
  WARN_NOT_OK(raft_pool_token_->SubmitFunc(std::bind(
      &RaftConsensus::ReportFailureDetectedTask, shared_from_this())),
              LogPrefixThreadSafe() + "failed to submit failure detected task");
}

Status RaftConsensus::BecomeLeaderUnlocked() {
  DCHECK(lock_.is_locked());

  TRACE_EVENT2("consensus", "RaftConsensus::BecomeLeaderUnlocked",
               "peer", peer_uuid(),
               "tablet", options_.tablet_id);
  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Becoming Leader. State: " << ToStringUnlocked();

  // Disable FD while we are leader.
  DisableFailureDetector();

  // Don't vote for anyone if we're a leader.
  withhold_votes_until_ = MonoTime::Max();

  // Leadership never starts in a transfer period.
  EndLeaderTransferPeriod();

  queue_->RegisterObserver(this);
  RETURN_NOT_OK(RefreshConsensusQueueAndPeersUnlocked());

  if (disable_noop_) {
    return Status::OK();
  }

  // Initiate a NO_OP transaction that is sent at the beginning of every term
  // change in raft.
  auto replicate = new ReplicateMsg;
  replicate->set_op_type(NO_OP);
  replicate->mutable_noop_request(); // Define the no-op request field.
  CHECK_OK(time_manager_->AssignTimestamp(replicate));

  scoped_refptr<ConsensusRound> round(
      new ConsensusRound(this, make_scoped_refptr(new RefCountedReplicate(replicate))));
  round->SetConsensusReplicatedCallback(std::bind(
      &RaftConsensus::NonTxRoundReplicationFinished,
      this,
      round.get(),
      &DoNothingStatusCB,
      std::placeholders::_1));

  last_leader_communication_time_micros_ = 0;

  return AppendNewRoundToQueueUnlocked(round);
}

Status RaftConsensus::BecomeReplicaUnlocked(boost::optional<MonoDelta> fd_delta) {
  DCHECK(lock_.is_locked());

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Becoming Follower/Learner. State: "
                                 << ToStringUnlocked();
  ClearLeaderUnlocked();

  // Enable/disable leader failure detection if becoming VOTER/NON_VOTER replica
  // correspondingly.
  UpdateFailureDetectorState(std::move(fd_delta));

  // Now that we're a replica, we can allow voting for other nodes.
  withhold_votes_until_ = MonoTime::Min();

  // Deregister ourselves from the queue. We no longer need to track what gets
  // replicated since we're stepping down.
  queue_->UnRegisterObserver(this);
  queue_->SetNonLeaderMode(cmeta_->ActiveConfig());
  peer_manager_->Close();

  return Status::OK();
}

Status RaftConsensus::Replicate(const scoped_refptr<ConsensusRound>& round) {

  std::lock_guard<simple_spinlock> lock(update_lock_);
  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    RETURN_NOT_OK(CheckSafeToReplicateUnlocked(*round->replicate_msg()));
    RETURN_NOT_OK(round->CheckBoundTerm(CurrentTermUnlocked()));
    RETURN_NOT_OK(AppendNewRoundToQueueUnlocked(round));
  }

  peer_manager_->SignalRequest();
  return Status::OK();
}

Status RaftConsensus::CheckLeadershipAndBindTerm(const scoped_refptr<ConsensusRound>& round) {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  RETURN_NOT_OK(CheckSafeToReplicateUnlocked(*round->replicate_msg()));
  round->BindToTerm(CurrentTermUnlocked());
  return Status::OK();
}

Status RaftConsensus::AppendNewRoundToQueueUnlocked(const scoped_refptr<ConsensusRound>& round) {
  DCHECK(lock_.is_locked());

  // If index was set in the ReplicateMsgg Round before starting
  // ::Replicate() we need to check that the ground has not shifted
  // under our feet. The term and index has also been serialized
  // into the WRITE_OP which would make it inconsistent to
  // replicate this message
  if (PREDICT_TRUE(round->replicate_msg()->id().index() != 0)) {
    if (PREDICT_FALSE(round->replicate_msg()->id().index() != queue_->GetNextOpId().index())) {
      return Status::Aborted(
        strings::Substitute(
          "Transaction submitted with index $0 mismatches with queue index $1",
          round->replicate_msg()->id().index(),
          queue_->GetNextOpId().index()));
    }
  } else {
    *round->replicate_msg()->mutable_id() = queue_->GetNextOpId();
  }
  RETURN_NOT_OK(AddPendingOperationUnlocked(round));

  // The only reasons for a bad status would be if the log itself were shut down,
  // or if we had an actual IO error, which we currently don't handle.
  CHECK_OK_PREPEND(queue_->AppendOperation(round->replicate_scoped_refptr()),
                   Substitute("$0: could not append to queue", LogPrefixUnlocked()));
  return Status::OK();
}

Status RaftConsensus::AddPendingOperationUnlocked(const scoped_refptr<ConsensusRound>& round) {
  DCHECK(lock_.is_locked());
  DCHECK(pending_);

  // If we are adding a pending config change, we need to propagate it to the
  // metadata.
  if (PREDICT_FALSE(round->replicate_msg()->op_type() == CHANGE_CONFIG_OP)) {
    // Fill in the opid for the proposed new configuration. This has to be done
    // here rather than when it's first created because we didn't yet have an
    // OpId assigned at creation time.
    ChangeConfigRecordPB* change_record = round->replicate_msg()->mutable_change_config_record();
    change_record->mutable_new_config()->set_opid_index(round->replicate_msg()->id().index());

    DCHECK(change_record->IsInitialized())
        << "change_config_record missing required fields: "
        << change_record->InitializationErrorString();

    const RaftConfigPB& new_config = change_record->new_config();

    if (!new_config.unsafe_config_change()) {
      Status s = CheckNoConfigChangePendingUnlocked();
      if (PREDICT_FALSE(!s.ok())) {
        s = s.CloneAndAppend(Substitute("\n  New config: $0", SecureShortDebugString(new_config)));
        LOG_WITH_PREFIX_UNLOCKED(INFO) << s.ToString();
        return s;
      }
    }
    // Check if the pending Raft config has an OpId less than the committed
    // config. If so, this is a replay at startup in which the COMMIT
    // messages were delayed.
    int64_t committed_config_opid_index = cmeta_->GetConfigOpIdIndex(COMMITTED_CONFIG);
    if (round->replicate_msg()->id().index() > committed_config_opid_index) {
      RETURN_NOT_OK(SetPendingConfigUnlocked(new_config));
      if (cmeta_->active_role() == RaftPeerPB::LEADER) {
        RETURN_NOT_OK(RefreshConsensusQueueAndPeersUnlocked());
      }
    } else {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Ignoring setting pending config change with OpId "
          << round->replicate_msg()->id() << " because the committed config has OpId index "
          << committed_config_opid_index << ". The config change we are ignoring is: "
          << "Old config: { " << SecureShortDebugString(change_record->old_config()) << " }. "
          << "New config: { " << SecureShortDebugString(new_config) << " }";
    }
  }

  return pending_->AddPendingOperation(round);
}

void RaftConsensus::NotifyCommitIndex(int64_t commit_index) {
  TRACE_EVENT2("consensus", "RaftConsensus::NotifyCommitIndex",
               "tablet", options_.tablet_id,
               "commit_index", commit_index);

  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  // We will process commit notifications while shutting down because a replica
  // which has initiated a Prepare() / Replicate() may eventually commit even if
  // its state has changed after the initial Append() / Update().
  if (PREDICT_FALSE(state_ != kRunning && state_ != kStopping)) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Unable to update committed index: "
                                      << "Replica not in running state: "
                                      << State_Name(state_);
    return;
  }

  pending_->AdvanceCommittedIndex(commit_index);

  if (cmeta_->active_role() == RaftPeerPB::LEADER) {
    peer_manager_->SignalRequest(false);
  }
}

void RaftConsensus::NotifyTermChange(int64_t term) {
  TRACE_EVENT2("consensus", "RaftConsensus::NotifyTermChange",
               "tablet", options_.tablet_id,
               "term", term);

  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  Status s = CheckRunningUnlocked();
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Unable to handle notification of new term "
                                      << "(" << term << "): " << s.ToString();
    return;
  }
  WARN_NOT_OK(HandleTermAdvanceUnlocked(term), "Couldn't advance consensus term.");
}

void RaftConsensus::NotifyFailedFollower(const string& uuid,
                                         int64_t term,
                                         const std::string& reason) {
  // Common info used in all of the log messages within this method.
  string fail_msg = Substitute("Processing failure of peer $0 in term $1 ($2): ",
                               uuid, term, reason);

  if (!FLAGS_evict_failed_followers) {
    LOG(INFO) << LogPrefixThreadSafe() << fail_msg
              << "Eviction of failed followers is disabled. Doing nothing.";
    return;
  }

  RaftConfigPB committed_config;
  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    int64_t current_term = CurrentTermUnlocked();
    if (current_term != term) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << fail_msg << "Notified about a follower failure in "
                                     << "previous term " << term << ", but a leader election "
                                     << "likely occurred since the failure was detected. "
                                     << "Doing nothing.";
      return;
    }

    if (cmeta_->has_pending_config()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << fail_msg << "There is already a config change operation "
                                     << "in progress. Unable to evict follower until it completes. "
                                     << "Doing nothing.";
      return;
    }
    committed_config = cmeta_->CommittedConfig();
  }

  // Run config change on thread pool after dropping lock.
  WARN_NOT_OK(raft_pool_token_->SubmitFunc(std::bind(&RaftConsensus::TryRemoveFollowerTask,
                                                     shared_from_this(),
                                                     uuid,
                                                     committed_config,
                                                     reason)),
              LogPrefixThreadSafe() + "Unable to start TryRemoveFollowerTask");
}

void RaftConsensus::NotifyPeerToPromote(const std::string& peer_uuid) {
  // Run the config change on the raft thread pool.
  WARN_NOT_OK(raft_pool_token_->SubmitFunc(std::bind(&RaftConsensus::TryPromoteNonVoterTask,
                                                     shared_from_this(),
                                                     peer_uuid)),
              LogPrefixThreadSafe() + "Unable to start TryPromoteNonVoterTask");
}

void RaftConsensus::NotifyPeerToStartElection(const string& peer_uuid) {
  LOG(INFO) << "Instructing follower " << peer_uuid << " to start an election";
  WARN_NOT_OK(raft_pool_token_->SubmitFunc(std::bind(&RaftConsensus::TryStartElectionOnPeerTask,
                                                     shared_from_this(),
                                                     peer_uuid)),
              LogPrefixThreadSafe() + "Unable to start TryStartElectionOnPeerTask");
}

void RaftConsensus::NotifyPeerHealthChange() {
  MarkDirty("Peer health change");
}

void RaftConsensus::TryRemoveFollowerTask(const string& uuid,
                                          const RaftConfigPB& committed_config,
                                          const std::string& reason) {
  ChangeConfigRequestPB req;
  req.set_tablet_id(options_.tablet_id);
  req.mutable_server()->set_permanent_uuid(uuid);
  req.set_type(REMOVE_PEER);
  req.set_cas_config_opid_index(committed_config.opid_index());
  LOG(INFO) << LogPrefixThreadSafe() << "Attempting to remove follower "
            << uuid << " from the Raft config. Reason: " << reason;
  boost::optional<ServerErrorPB::Code> error_code;
  WARN_NOT_OK(ChangeConfig(req, &DoNothingStatusCB, &error_code),
              LogPrefixThreadSafe() + "Unable to remove follower " + uuid);
}

void RaftConsensus::TryPromoteNonVoterTask(const std::string& peer_uuid) {
  string msg = Substitute("attempt to promote peer $0: ", peer_uuid);
  int64_t current_committed_config_index;
  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);

    if (cmeta_->has_pending_config()) {
     LOG_WITH_PREFIX_UNLOCKED(INFO) << msg << "there is already a config change operation "
                                     << "in progress. Unable to promote follower until it "
                                     << "completes. Doing nothing.";
      return;
    }

    // Check if the peer is still part of the current committed config.
    RaftConfigPB committed_config = cmeta_->CommittedConfig();
    current_committed_config_index = committed_config.opid_index();

    RaftPeerPB* peer_pb;
    Status s = GetRaftConfigMember(&committed_config, peer_uuid, &peer_pb);
    if (!s.ok()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << msg << "can't find peer in the "
                                     << "current committed config: "
                                     << committed_config.ShortDebugString()
                                     << ". Doing nothing.";
      return;
    }

    // Also check if the peer it still a NON_VOTER waiting for promotion.
    if (peer_pb->member_type() != RaftPeerPB::NON_VOTER || !peer_pb->attrs().promote()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << msg << "peer is either no longer a NON_VOTER "
                                     << "or not marked for promotion anymore. Current "
                                     << "config: " << committed_config.ShortDebugString()
                                     << ". Doing nothing.";
      return;
    }
  }

  ChangeConfigRequestPB req;
  req.set_tablet_id(options_.tablet_id);
  req.set_type(MODIFY_PEER);
  req.mutable_server()->set_permanent_uuid(peer_uuid);
  req.mutable_server()->set_member_type(RaftPeerPB::VOTER);
  req.mutable_server()->mutable_attrs()->set_promote(false);
  req.set_cas_config_opid_index(current_committed_config_index);
  LOG(INFO) << LogPrefixThreadSafe() << "attempting to promote NON_VOTER "
            << peer_uuid << " to VOTER";
  boost::optional<ServerErrorPB::Code> error_code;
  WARN_NOT_OK(ChangeConfig(req, &DoNothingStatusCB, &error_code),
              LogPrefixThreadSafe() + Substitute("Unable to promote non-voter $0", peer_uuid));
}

void RaftConsensus::TryStartElectionOnPeerTask(const string& peer_uuid) {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  // Double-check that the peer is a voter in the active config.
  if (!IsRaftConfigVoter(peer_uuid, cmeta_->ActiveConfig())) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Not signalling peer " << peer_uuid
                                   << "to start an election: it's not a voter "
                                   << "in the active config.";
    return;
  }
  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Signalling peer " << peer_uuid
                                 << "to start an election";
  WARN_NOT_OK(peer_manager_->StartElection(peer_uuid),
              Substitute("unable to start election on peer $0", peer_uuid));
}

Status RaftConsensus::Update(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response) {
  update_calls_for_tests_.Increment();

  if (PREDICT_FALSE(FLAGS_follower_reject_update_consensus_requests)) {
    return Status::IllegalState("Rejected: --follower_reject_update_consensus_requests "
                                "is set to true.");
  }

  response->set_responder_uuid(peer_uuid());

  VLOG_WITH_PREFIX(2) << "Replica received request: " << SecureShortDebugString(*request);

  // see var declaration
  std::lock_guard<simple_spinlock> lock(update_lock_);
  Status s = UpdateReplica(request, response);
  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    if (request->ops().empty()) {
      VLOG_WITH_PREFIX(1) << "Replica replied to status only request. Replica: "
                          << ToString() << ". Response: "
                          << SecureShortDebugString(*response);
    }
  }
  return s;
}

// Helper function to check if the op is a non-Transaction op.
static bool IsConsensusOnlyOperation(OperationType op_type) {
  return op_type == NO_OP || op_type == CHANGE_CONFIG_OP;
}

Status RaftConsensus::StartFollowerTransactionUnlocked(const ReplicateRefPtr& msg) {
  DCHECK(lock_.is_locked());

  if (IsConsensusOnlyOperation(msg->get()->op_type())) {
    return StartConsensusOnlyRoundUnlocked(msg);
  }

  if (PREDICT_FALSE(FLAGS_follower_fail_all_prepare)) {
    return Status::IllegalState("Rejected: --follower_fail_all_prepare "
                                "is set to true.");
  }

  VLOG_WITH_PREFIX_UNLOCKED(1) << "Starting transaction: "
                               << SecureShortDebugString(msg->get()->id());
  scoped_refptr<ConsensusRound> round(new ConsensusRound(this, msg));
  ConsensusRound* round_ptr = round.get();
  RETURN_NOT_OK(round_handler_->StartFollowerTransaction(round));
  return AddPendingOperationUnlocked(round_ptr);
}

bool RaftConsensus::IsSingleVoterConfig() const {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  return cmeta_->CountVotersInConfig(COMMITTED_CONFIG) == 1 &&
         cmeta_->IsVoterInConfig(peer_uuid(), COMMITTED_CONFIG);
}

std::string RaftConsensus::LeaderRequest::OpsRangeString() const {
  std::string ret;
  ret.reserve(100);
  ret.push_back('[');
  if (!messages.empty()) {
    const OpId& first_op = (*messages.begin())->get()->id();
    const OpId& last_op = (*messages.rbegin())->get()->id();
    strings::SubstituteAndAppend(&ret, "$0.$1-$2.$3",
                                 first_op.term(), first_op.index(),
                                 last_op.term(), last_op.index());
  }
  ret.push_back(']');
  return ret;
}

void RaftConsensus::DeduplicateLeaderRequestUnlocked(ConsensusRequestPB* rpc_req,
                                                     LeaderRequest* deduplicated_req) {
  DCHECK(lock_.is_locked());

  // TODO(todd): use queue committed index?
  int64_t last_committed_index = pending_->GetCommittedIndex();

  // The leader's preceding id.
  deduplicated_req->preceding_opid = &rpc_req->preceding_id();

  int64_t dedup_up_to_index = queue_->GetLastOpIdInLog().index();

  deduplicated_req->first_message_idx = -1;

  // In this loop we discard duplicates and advance the leader's preceding id
  // accordingly.
  for (int i = 0; i < rpc_req->ops_size(); i++) {
    ReplicateMsg* leader_msg = rpc_req->mutable_ops(i);

    if (leader_msg->id().index() <= last_committed_index) {
      VLOG_WITH_PREFIX_UNLOCKED(2) << "Skipping op id " << leader_msg->id()
                                   << " (already committed)";
      deduplicated_req->preceding_opid = &leader_msg->id();
      continue;
    }

    if (leader_msg->id().index() <= dedup_up_to_index) {
      // If the index is uncommitted and below our match index, then it must be in the
      // pendings set.
      scoped_refptr<ConsensusRound> round =
          pending_->GetPendingOpByIndexOrNull(leader_msg->id().index());
      DCHECK(round) << "Could not find op with index " << leader_msg->id().index()
                    << " in pending set. committed= " << last_committed_index
                    << " dedup=" << dedup_up_to_index;

      // If the OpIds match, i.e. if they have the same term and id, then this is just
      // duplicate, we skip...
      if (OpIdEquals(round->replicate_msg()->id(), leader_msg->id())) {
        VLOG_WITH_PREFIX_UNLOCKED(2) << "Skipping op id " << leader_msg->id()
                                     << " (already replicated)";
        deduplicated_req->preceding_opid = &leader_msg->id();
        continue;
      }

      // ... otherwise we must adjust our match index, i.e. all messages from now on
      // are "new"
      dedup_up_to_index = leader_msg->id().index();
    }

    if (deduplicated_req->first_message_idx == - 1) {
      deduplicated_req->first_message_idx = i;
    }
    deduplicated_req->messages.push_back(make_scoped_refptr_replicate(leader_msg));
  }

  if (deduplicated_req->messages.size() != rpc_req->ops_size()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Deduplicated request from leader. Original: "
                          << rpc_req->preceding_id() << "->" << OpsRangeString(*rpc_req)
                          << "   Dedup: " << *deduplicated_req->preceding_opid << "->"
                          << deduplicated_req->OpsRangeString();
  }

}

Status RaftConsensus::HandleLeaderRequestTermUnlocked(const ConsensusRequestPB* request,
                                                      ConsensusResponsePB* response) {
  DCHECK(lock_.is_locked());
  // Do term checks first:
  if (PREDICT_FALSE(request->caller_term() != CurrentTermUnlocked())) {

    // If less, reject.
    if (request->caller_term() < CurrentTermUnlocked()) {
      string msg = Substitute("Rejecting Update request from peer $0 for earlier term $1. "
                              "Current term is $2. Ops: $3",
                              request->caller_uuid(),
                              request->caller_term(),
                              CurrentTermUnlocked(),
                              OpsRangeString(*request));
      LOG_WITH_PREFIX_UNLOCKED(INFO) << msg;
      FillConsensusResponseError(response,
                                 ConsensusErrorPB::INVALID_TERM,
                                 Status::IllegalState(msg));
      return Status::OK();
    }
    RETURN_NOT_OK(HandleTermAdvanceUnlocked(request->caller_term()));
  }
  return Status::OK();
}

Status RaftConsensus::EnforceLogMatchingPropertyMatchesUnlocked(const LeaderRequest& req,
                                                                ConsensusResponsePB* response) {
  DCHECK(lock_.is_locked());

  bool term_mismatch;
  if (pending_->IsOpCommittedOrPending(*req.preceding_opid, &term_mismatch)) {
    return Status::OK();
  }

  string error_msg = Substitute(
    "Log matching property violated."
    " Preceding OpId in replica: $0. Preceding OpId from leader: $1. ($2 mismatch)",
    SecureShortDebugString(queue_->GetLastOpIdInLog()),
    SecureShortDebugString(*req.preceding_opid),
    term_mismatch ? "term" : "index");


  FillConsensusResponseError(response,
                             ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH,
                             Status::IllegalState(error_msg));

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Refusing update from remote peer "
                        << req.leader_uuid << ": " << error_msg;

  // If the terms mismatch we abort down to the index before the leader's preceding,
  // since we know that is the last opid that has a chance of not being overwritten.
  // Aborting preemptively here avoids us reporting a last received index that is
  // possibly higher than the leader's causing an avoidable cache miss on the leader's
  // queue.
  //
  // TODO: this isn't just an optimization! if we comment this out, we get
  // failures on raft_consensus-itest a couple percent of the time! Should investigate
  // why this is actually critical to do here, as opposed to just on requests that
  // append some ops.
  if (term_mismatch) {
    TruncateAndAbortOpsAfterUnlocked(req.preceding_opid->index() - 1);
  }

  return Status::OK();
}

void RaftConsensus::TruncateAndAbortOpsAfterUnlocked(int64_t truncate_after_index) {
  DCHECK(lock_.is_locked());
  pending_->AbortOpsAfter(truncate_after_index);
  queue_->TruncateOpsAfter(truncate_after_index);
}

Status RaftConsensus::CheckLeaderRequestUnlocked(const ConsensusRequestPB* request,
                                                 ConsensusResponsePB* response,
                                                 LeaderRequest* deduped_req) {
  DCHECK(lock_.is_locked());

  if (request->has_deprecated_committed_index() ||
      !request->has_all_replicated_index()) {
    return Status::InvalidArgument("Leader appears to be running an earlier version "
                                   "of Kudu. Please shut down and upgrade all servers "
                                   "before restarting.");
  }

  ConsensusRequestPB* mutable_req = const_cast<ConsensusRequestPB*>(request);
  DeduplicateLeaderRequestUnlocked(mutable_req, deduped_req);

  // This is an additional check for KUDU-639 that makes sure the message's index
  // and term are in the right sequence in the request, after we've deduplicated
  // them. We do this before we change any of the internal state.
  //
  // TODO move this to raft_consensus-state or whatever we transform that into.
  // We should be able to do this check for each append, but right now the way
  // we initialize raft_consensus-state is preventing us from doing so.
  Status s;
  const OpId* prev = deduped_req->preceding_opid;
  for (const ReplicateRefPtr& message : deduped_req->messages) {
    s = PendingRounds::CheckOpInSequence(*prev, message->get()->id());
    if (PREDICT_FALSE(!s.ok())) {
      LOG_WITH_PREFIX_UNLOCKED(ERROR) << "Leader request contained out-of-sequence messages. "
          << "Status: " << s.ToString() << ". Leader Request: " << SecureShortDebugString(*request);
      break;
    }
    prev = &message->get()->id();
  }

  // We only release the messages from the request after the above check so that
  // that we can print the original request, if it fails.
  if (!deduped_req->messages.empty()) {
    // We take ownership of the deduped ops.
    DCHECK_GE(deduped_req->first_message_idx, 0);
    mutable_req->mutable_ops()->ExtractSubrange(
        deduped_req->first_message_idx,
        deduped_req->messages.size(),
        nullptr);
  }

  RETURN_NOT_OK(s);

  RETURN_NOT_OK(HandleLeaderRequestTermUnlocked(request, response));

  if (response->status().has_error()) {
    return Status::OK();
  }

  RETURN_NOT_OK(EnforceLogMatchingPropertyMatchesUnlocked(*deduped_req, response));

  if (response->status().has_error()) {
    return Status::OK();
  }

  // If the first of the messages to apply is not in our log, either it follows the last
  // received message or it replaces some in-flight.
  if (!deduped_req->messages.empty()) {

    bool term_mismatch;
    CHECK(!pending_->IsOpCommittedOrPending(deduped_req->messages[0]->get()->id(), &term_mismatch));

    // If the index is in our log but the terms are not the same abort down to the leader's
    // preceding id.
    if (term_mismatch) {
      TruncateAndAbortOpsAfterUnlocked(deduped_req->preceding_opid->index());
    }
  }

  // If all of the above logic was successful then we can consider this to be
  // the effective leader of the configuration. If they are not currently marked as
  // the leader locally, mark them as leader now.
  const string& caller_uuid = request->caller_uuid();
  if (PREDICT_FALSE(HasLeaderUnlocked() &&
                    GetLeaderUuidUnlocked() != caller_uuid)) {
    LOG_WITH_PREFIX_UNLOCKED(FATAL) << "Unexpected new leader in same term! "
        << "Existing leader UUID: " << GetLeaderUuidUnlocked() << ", "
        << "new leader UUID: " << caller_uuid;
  }
  if (PREDICT_FALSE(!HasLeaderUnlocked())) {
    SetLeaderUuidUnlocked(request->caller_uuid());
    new_leader_detected_failsafe_ = true;
  }

  return Status::OK();
}

Status RaftConsensus::UpdateReplica(const ConsensusRequestPB* request,
                                    ConsensusResponsePB* response) {
  TRACE_EVENT2("consensus", "RaftConsensus::UpdateReplica",
               "peer", peer_uuid(),
               "tablet", options_.tablet_id);
  Synchronizer log_synchronizer;
  StatusCallback sync_status_cb = log_synchronizer.AsStatusCallback();
  // this is a temp variable. reset every time.
  new_leader_detected_failsafe_ = false;

  // The ordering of the following operations is crucial, read on for details.
  //
  // The main requirements explained in more detail below are:
  //
  //   1) We must enqueue the prepares before we write to our local log.
  //   2) If we were able to enqueue a prepare then we must be able to log it.
  //   3) If we fail to enqueue a prepare, we must not attempt to enqueue any
  //      later-indexed prepare or apply.
  //
  // See below for detailed rationale.
  //
  // The steps are:
  //
  // 0 - Split/Dedup
  //
  // We split the operations into replicates and commits and make sure that we don't
  // don't do anything on operations we've already received in a previous call.
  // This essentially makes this method idempotent.
  //
  // 1 - We mark as many pending transactions as committed as we can.
  //
  // We may have some pending transactions that, according to the leader, are now
  // committed. We Apply them early, because:
  // - Soon (step 2) we may reject the call due to excessive memory pressure. One
  //   way to relieve the pressure is by flushing the MRS, and applying these
  //   transactions may unblock an in-flight Flush().
  // - The Apply and subsequent Prepares (step 2) can take place concurrently.
  //
  // 2 - We enqueue the Prepare of the transactions.
  //
  // The actual prepares are enqueued in order but happen asynchronously so we don't
  // have decoding/acquiring locks on the critical path.
  //
  // We need to do this now for a number of reasons:
  // - Prepares, by themselves, are inconsequential, i.e. they do not mutate the
  //   state machine so, were we to crash afterwards, having the prepares in-flight
  //   won't hurt.
  // - Prepares depend on factors external to consensus (the transaction drivers and
  //   the TabletReplica) so if for some reason they cannot be enqueued we must know
  //   before we try write them to the WAL. Once enqueued, we assume that prepare will
  //   always succeed on a replica transaction (because the leader already prepared them
  //   successfully, and thus we know they are valid).
  // - The prepares corresponding to every operation that was logged must be in-flight
  //   first. This because should we need to abort certain transactions (say a new leader
  //   says they are not committed) we need to have those prepares in-flight so that
  //   the transactions can be continued (in the abort path).
  // - Failure to enqueue prepares is OK, we can continue and let the leader know that
  //   we only went so far. The leader will re-send the remaining messages.
  // - Prepares represent new transactions, and transactions consume memory. Thus, if the
  //   overall memory pressure on the server is too high, we will reject the prepares.
  //
  // 3 - We enqueue the writes to the WAL.
  //
  // We enqueue writes to the WAL, but only the operations that were successfully
  // enqueued for prepare (for the reasons introduced above). This means that even
  // if a prepare fails to enqueue, if any of the previous prepares were successfully
  // submitted they must be written to the WAL.
  // If writing to the WAL fails, we're in an inconsistent state and we crash. In this
  // case, no one will ever know of the transactions we previously prepared so those are
  // inconsequential.
  //
  // 4 - We mark the transactions as committed.
  //
  // For each transaction which has been committed by the leader, we update the
  // transaction state to reflect that. If the logging has already succeeded for that
  // transaction, this will trigger the Apply phase. Otherwise, Apply will be triggered
  // when the logging completes. In both cases the Apply phase executes asynchronously.
  // This must, of course, happen after the prepares have been triggered as the same batch
  // can both replicate/prepare and commit/apply an operation.
  //
  // Currently, if a prepare failed to enqueue we still trigger all applies for operations
  // with an id lower than it (if we have them). This is important now as the leader will
  // not re-send those commit messages. This will be moot when we move to the commit
  // commitIndex way of doing things as we can simply ignore the applies as we know
  // they will be triggered with the next successful batch.
  //
  // 5 - We wait for the writes to be durable.
  //
  // Before replying to the leader we wait for the writes to be durable. We then
  // just update the last replicated watermark and respond.
  //
  // TODO - These failure scenarios need to be exercised in an unit
  //        test. Moreover we need to add more fault injection spots (well that
  //        and actually use the) for each of these steps.
  //        This will be done in a follow up patch.
  TRACE("Updating replica for $0 ops", request->ops_size());

  // The deduplicated request.
  LeaderRequest deduped_req;
  auto& messages = deduped_req.messages;
  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    RETURN_NOT_OK(CheckRunningUnlocked());
    if (!cmeta_->IsMemberInConfig(peer_uuid(), ACTIVE_CONFIG)) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Allowing update even though not a member of the config";
    }

    deduped_req.leader_uuid = request->caller_uuid();

    RETURN_NOT_OK(CheckLeaderRequestUnlocked(request, response, &deduped_req));
    if (response->status().has_error()) {
      // We had an error, like an invalid term, we still fill the response.
      FillConsensusResponseOKUnlocked(response);
      return Status::OK();
    }

    // Snooze the failure detector as soon as we decide to accept the message.
    // We are guaranteed to be acting as a FOLLOWER at this point by the above
    // sanity check.
    // If this particular instance is banned from cluster manager,
    // then we snooze for longer to give other instances an opportunity to win
    // the election
    SnoozeFailureDetector(boost::none, MinimumElectionTimeoutWithBan());

    last_leader_communication_time_micros_ = GetMonoTimeMicros();

    // Reset the 'failed_elections_since_stable_leader' metric now that we've
    // accepted an update from the established leader. This is done in addition
    // to the reset of the value in SetLeaderUuidUnlocked() because there is
    // a potential race between resetting the failed elections count in
    // SetLeaderUuidUnlocked() and incrementing after a failed election
    // if another replica was elected leader in an election concurrent with
    // the one called by this replica.
    failed_elections_since_stable_leader_ = 0;
    num_failed_elections_metric_->set_value(failed_elections_since_stable_leader_);

    // We update the lag metrics here in addition to after appending to the queue so the
    // metrics get updated even when the operation is rejected.
    queue_->UpdateLastIndexAppendedToLeader(request->last_idx_appended_to_leader());

    // Also prohibit voting for anyone for the minimum election timeout.
    withhold_votes_until_ = MonoTime::Now() + MinimumElectionTimeout();

    // 1 - Early commit pending (and committed) transactions

    // What should we commit?
    // 1. As many pending transactions as we can, except...
    // 2. ...if we commit beyond the preceding index, we'd regress KUDU-639, and...
    // 3. ...the leader's committed index is always our upper bound.
    const int64_t early_apply_up_to = std::min({
        pending_->GetLastPendingTransactionOpId().index(),
        deduped_req.preceding_opid->index(),
        request->committed_index()});

    VLOG_WITH_PREFIX_UNLOCKED(1) << "Early marking committed up to " << early_apply_up_to
                                 << ", Last pending opid index: "
                                 << pending_->GetLastPendingTransactionOpId().index()
                                 << ", preceding opid index: "
                                 << deduped_req.preceding_opid->index()
                                 << ", requested index: " << request->committed_index();
    TRACE("Early marking committed up to index $0", early_apply_up_to);
    CHECK_OK(pending_->AdvanceCommittedIndex(early_apply_up_to));

    // 2 - Enqueue the prepares

    TRACE("Triggering prepare for $0 ops", messages.size());

    if (PREDICT_TRUE(!messages.empty())) {
      // This request contains at least one message, and is likely to increase
      // our memory pressure.
      double capacity_pct;
      if (process_memory::SoftLimitExceeded(&capacity_pct)) {
        if (follower_memory_pressure_rejections_) follower_memory_pressure_rejections_->Increment();
        string msg = StringPrintf(
            "Soft memory limit exceeded (at %.2f%% of capacity)",
            capacity_pct);
        if (capacity_pct >= FLAGS_memory_limit_warn_threshold_percentage) {
          KLOG_EVERY_N_SECS(WARNING, 1) << "Rejecting consensus request: " << msg
                                        << THROTTLE_MSG;
        } else {
          KLOG_EVERY_N_SECS(INFO, 1) << "Rejecting consensus request: " << msg
                                     << THROTTLE_MSG;
        }
        return Status::ServiceUnavailable(msg);
      }
    }

    Status prepare_status;
    auto iter = messages.begin();
    while (iter != messages.end()) {
      OperationType op_type = (*iter)->get()->op_type();
      if (op_type == NO_OP) {
        new_leader_detected_failsafe_ = false;
      }
      prepare_status = StartFollowerTransactionUnlocked(*iter);
      if (PREDICT_FALSE(!prepare_status.ok())) {
        break;
      }
      // TODO(dralves) Without leader leases this shouldn't be allowed to fail.
      // Once we have that functionality we'll have to revisit this.
      CHECK_OK(time_manager_->MessageReceivedFromLeader(*(*iter)->get()));
      ++iter;
    }

    // If we stopped before reaching the end we failed to prepare some message(s) and need
    // to perform cleanup, namely trimming deduped_req.messages to only contain the messages
    // that were actually prepared, and deleting the other ones since we've taken ownership
    // when we first deduped.
    if (iter != messages.end()) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING) << Substitute(
          "Could not prepare transaction for op '$0' and following $1 ops. "
          "Status for this op: $2",
          (*iter)->get()->id().ShortDebugString(),
          std::distance(iter, messages.end()) - 1,
          prepare_status.ToString());
      iter = messages.erase(iter, messages.end());

      // If this is empty, it means we couldn't prepare a single de-duped message. There is nothing
      // else we can do. The leader will detect this and retry later.
      if (messages.empty()) {
        string msg = Substitute("Rejecting Update request from peer $0 for term $1. "
                                "Could not prepare a single transaction due to: $2",
                                request->caller_uuid(),
                                request->caller_term(),
                                prepare_status.ToString());
        LOG_WITH_PREFIX_UNLOCKED(INFO) << msg;
        FillConsensusResponseError(response, ConsensusErrorPB::CANNOT_PREPARE,
                                   Status::IllegalState(msg));
        FillConsensusResponseOKUnlocked(response);
        return Status::OK();
      }
    }

    // All transactions that are going to be prepared were started, advance the safe timestamp.
    // TODO(dralves) This is only correct because the queue only sets safe time when the request is
    // an empty heartbeat. If we actually start setting this on a consensus request along with
    // actual messages we need to be careful to ignore it if any of the messages fails to prepare.
    if (request->has_safe_timestamp()) {
      time_manager_->AdvanceSafeTime(Timestamp(request->safe_timestamp()));
    }

    OpId last_from_leader;
    // 3 - Enqueue the writes.
    // Now that we've triggered the prepares enqueue the operations to be written
    // to the WAL.
    if (PREDICT_TRUE(!messages.empty())) {
      last_from_leader = messages.back()->get()->id();
      // Trigger the log append asap, if fsync() is on this might take a while
      // and we can't reply until this is done.
      //
      // Since we've prepared, we need to be able to append (or we risk trying to apply
      // later something that wasn't logged). We crash if we can't.
      CHECK_OK(queue_->AppendOperations(messages, sync_status_cb));
    } else {
      last_from_leader = *deduped_req.preceding_opid;
    }

    // 4 - Mark transactions as committed

    // Choose the last operation to be applied. This will either be 'committed_index', if
    // no prepare enqueuing failed, or the minimum between 'committed_index' and the id of
    // the last successfully enqueued prepare, if some prepare failed to enqueue.
    int64_t apply_up_to;
    if (last_from_leader.index() < request->committed_index()) {
      // we should never apply anything later than what we received in this request
      apply_up_to = last_from_leader.index();

      VLOG_WITH_PREFIX_UNLOCKED(2) << "Received commit index "
          << request->committed_index() << " from the leader but only"
          << " marked up to " << apply_up_to << " as committed.";
    } else {
      apply_up_to = request->committed_index();
    }

    VLOG_WITH_PREFIX_UNLOCKED(1) << "Marking committed up to " << apply_up_to;
    TRACE("Marking committed up to $0", apply_up_to);
    CHECK_OK(pending_->AdvanceCommittedIndex(apply_up_to));
    queue_->UpdateFollowerWatermarks(apply_up_to, request->all_replicated_index());

    // If any messages failed to be started locally, then we already have removed them
    // from 'deduped_req' at this point. So, 'last_from_leader' is the last one that
    // we might apply.
    last_received_cur_leader_ = last_from_leader;

    // Fill the response with the current state. We will not mutate anymore state until
    // we actually reply to the leader, we'll just wait for the messages to be durable.
    FillConsensusResponseOKUnlocked(response);
  }
  // Release the lock while we wait for the log append to finish so that commits can go through.
  // We'll re-acquire it before we update the state again.

  // Update the last replicated op id
  if (!messages.empty()) {

    // 5 - We wait for the writes to be durable.

    // Note that this is safe because dist consensus now only supports a single outstanding
    // request at a time and this way we can allow commits to proceed while we wait.
    TRACE("Waiting on the replicates to finish logging");
    TRACE_EVENT0("consensus", "Wait for log");
    Status s;
    do {
      s = log_synchronizer.WaitFor(
          MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms));
      // If just waiting for our log append to finish lets snooze the timer.
      // We don't want to fire leader election because we're waiting on our own log.
      if (s.IsTimedOut()) {
        SnoozeFailureDetector();
      }
    } while (s.IsTimedOut());
    RETURN_NOT_OK(s);

    TRACE("finished");
  }

  VLOG_WITH_PREFIX(2) << "Replica updated. " << ToString()
                      << ". Request: " << SecureShortDebugString(*request);

  if (new_leader_detected_failsafe_) {
    ScheduleLeaderDetectedCallback();
  }
  TRACE("UpdateReplicas() finished");
  return Status::OK();
}

void RaftConsensus::FillConsensusResponseOKUnlocked(ConsensusResponsePB* response) {
  DCHECK(lock_.is_locked());
  TRACE("Filling consensus response to leader.");
  response->set_responder_term(CurrentTermUnlocked());

  // if RESPONSE STATUS does not have error - i.e. common case
  // and there are messages in the request, then last_received_cur_leader_
  // = last_from_leader
  // and AppendOperations also uses the same OpId (last_id) to
  // update queue_state_.last_appended.
  // So in COMMON case both the first and second OpId's should be the same.
  response->mutable_status()->mutable_last_received()->CopyFrom(
      queue_->GetLastOpIdInLog());
  response->mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      last_received_cur_leader_);
  response->mutable_status()->set_last_committed_idx(
      queue_->GetCommittedIndex());
}

void RaftConsensus::FillConsensusResponseError(ConsensusResponsePB* response,
                                               ConsensusErrorPB::Code error_code,
                                               const Status& status) {
  ConsensusErrorPB* error = response->mutable_status()->mutable_error();
  error->set_code(error_code);
  StatusToPB(status, error->mutable_status());
}

Status RaftConsensus::RequestVote(const VoteRequestPB* request,
                                  TabletVotingState tablet_voting_state,
                                  VoteResponsePB* response) {
  TRACE_EVENT2("consensus", "RaftConsensus::RequestVote",
               "peer", peer_uuid(),
               "tablet", options_.tablet_id);
  response->set_responder_uuid(peer_uuid());

  // We must acquire the update lock in order to ensure that this vote action
  // takes place between requests.
  // Lock ordering: update_lock_ must be acquired before lock_.
  std::unique_lock<simple_spinlock> update_guard(update_lock_, std::defer_lock);
  if (FLAGS_enable_leader_failure_detection) {
    update_guard.try_lock();
  } else {
    // If failure detection is not enabled, then we can't just reject the vote,
    // because there will be no automatic retry later. So, block for the lock.
    update_guard.lock();
  }
  if (!update_guard.owns_lock()) {
    // There is another vote or update concurrent with the vote. In that case, that
    // other request is likely to reset the timer, and we'll end up just voting
    // "NO" after waiting. To avoid starving RPC handlers and causing cascading
    // timeouts, just vote a quick NO.
    //
    // We still need to take the state lock in order to respond with term info, etc.
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    return RequestVoteRespondIsBusy(request, response);
  }

  // Acquire the replica state lock so we can read / modify the consensus state.
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);

  // Ensure our lifecycle state is compatible with voting.
  // If RaftConsensus is running, we use the latest OpId from the WAL to vote.
  // Otherwise, we must be voting while tombstoned.
  OpId local_last_logged_opid;
  switch (state_) {
    case kShutdown:
      return Status::IllegalState("cannot vote while shut down");
    case kRunning:
      // Note: it is (theoretically) possible for 'tombstone_last_logged_opid'
      // to be passed in and by the time we reach here the state is kRunning.
      // That may occur when a vote request comes in at the end of a tablet
      // copy and then tablet bootstrap completes quickly. In that case, we
      // ignore the passed-in value and use the latest OpId from our queue.
      local_last_logged_opid = queue_->GetLastOpIdInLog();
      break;
    default:
      if (!tablet_voting_state.tombstone_last_logged_opid_) {
        return Status::IllegalState("must be running to vote when last-logged opid is not known");
      }
      if (!FLAGS_raft_enable_tombstoned_voting) {
        return Status::IllegalState("must be running to vote when tombstoned voting is disabled");
      }
      local_last_logged_opid = *(tablet_voting_state.tombstone_last_logged_opid_);
#ifdef FB_DO_NOT_REMOVE
      if (tablet_voting_state.data_state_ == tablet::TABLET_DATA_COPYING) {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << "voting while copying based on last-logged opid "
                                       << local_last_logged_opid;
      } else if (tablet_voting_state.data_state_ == tablet::TABLET_DATA_TOMBSTONED) {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << "voting while tombstoned based on last-logged opid "
                                       << local_last_logged_opid;
      }
#endif
      break;
  }
  DCHECK(local_last_logged_opid.IsInitialized());

  // If the node is not in the configuration, allow the vote (this is required by Raft)
  // but log an informational message anyway.
  if (!cmeta_->IsMemberInConfig(request->candidate_uuid(), ACTIVE_CONFIG)) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Handling vote request from an unknown peer "
                                   << request->candidate_uuid();
  }

  // If we've heard recently from the leader, then we should ignore the request.
  // It might be from a "disruptive" server. This could happen in a few cases:
  //
  // 1) Network partitions
  // If the leader can talk to a majority of the nodes, but is partitioned from a
  // bad node, the bad node's failure detector will trigger. If the bad node is
  // able to reach other nodes in the cluster, it will continuously trigger elections.
  //
  // 2) An abandoned node
  // It's possible that a node has fallen behind the log GC mark of the leader. In that
  // case, the leader will stop sending it requests. Eventually, the the configuration
  // will change to eject the abandoned node, but until that point, we don't want the
  // abandoned follower to disturb the other nodes.
  //
  // See also https://ramcloud.stanford.edu/~ongaro/thesis.pdf
  // section 4.2.3.
  if (!request->ignore_live_leader() && MonoTime::Now() < withhold_votes_until_) {
    return RequestVoteRespondLeaderIsAlive(request, response);
  }

  // Candidate is running behind.
  if (request->candidate_term() < CurrentTermUnlocked()) {
    return RequestVoteRespondInvalidTerm(request, response);
  }

  // We already voted this term.
  if (request->candidate_term() == CurrentTermUnlocked() &&
      HasVotedCurrentTermUnlocked()) {

    // Already voted for the same candidate in the current term.
    if (GetVotedForCurrentTermUnlocked() == request->candidate_uuid()) {
      return RequestVoteRespondVoteAlreadyGranted(request, response);
    }

    // Voted for someone else in current term.
    return RequestVoteRespondAlreadyVotedForOther(request, response);
  }

  // Candidate must have last-logged OpId at least as large as our own to get
  // our vote.
  bool vote_yes = !OpIdLessThan(request->candidate_status().last_received(),
                                local_last_logged_opid);

  // Record the term advancement if necessary. We don't do so in the case of
  // pre-elections because it's possible that the node who called the pre-election
  // has actually now successfully become leader of the prior term, in which case
  // bumping our term here would disrupt it.
  if (!request->is_pre_election() &&
      request->candidate_term() > CurrentTermUnlocked()) {
    // If we are going to vote for this peer, then we will flush the consensus metadata
    // to disk below when we record the vote, and we can skip flushing the term advancement
    // to disk here.
    auto flush = vote_yes ? SKIP_FLUSH_TO_DISK : FLUSH_TO_DISK;
    RETURN_NOT_OK_PREPEND(HandleTermAdvanceUnlocked(request->candidate_term(), flush),
        Substitute("Could not step down in RequestVote. Current term: $0, candidate term: $1",
                   CurrentTermUnlocked(), request->candidate_term()));
  }

  if (!vote_yes) {
    return RequestVoteRespondLastOpIdTooOld(local_last_logged_opid, request, response);
  }

  // Passed all our checks. Vote granted.
  return RequestVoteRespondVoteGranted(request, response);
}

Status RaftConsensus::ChangeConfig(const ChangeConfigRequestPB& req,
                                   StdStatusCallback client_cb,
                                   boost::optional<ServerErrorPB::Code>* error_code) {
  TRACE_EVENT2("consensus", "RaftConsensus::ChangeConfig",
               "peer", peer_uuid(),
               "tablet", options_.tablet_id);

  BulkChangeConfigRequestPB bulk_req;
  *bulk_req.mutable_tablet_id() = req.tablet_id();

  if (req.has_dest_uuid()) {
    *bulk_req.mutable_dest_uuid() = req.dest_uuid();
  }
  if (req.has_cas_config_opid_index()) {
    bulk_req.set_cas_config_opid_index(req.cas_config_opid_index());
  }
  auto* change = bulk_req.add_config_changes();
  if (req.has_type()) {
    change->set_type(req.type());
  }
  if (req.has_server()) {
    *change->mutable_peer() = req.server();
  }

  return BulkChangeConfig(bulk_req, std::move(client_cb), error_code);
}

Status RaftConsensus::BulkChangeConfig(const BulkChangeConfigRequestPB& req,
                                       StdStatusCallback client_cb,
                                       boost::optional<ServerErrorPB::Code>* error_code) {
  TRACE_EVENT2("consensus", "RaftConsensus::BulkChangeConfig",
               "peer", peer_uuid(),
               "tablet", options_.tablet_id);
  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    RETURN_NOT_OK(CheckRunningUnlocked());
    RETURN_NOT_OK(CheckActiveLeaderUnlocked());
    RETURN_NOT_OK(CheckNoConfigChangePendingUnlocked());

    // We are required by Raft to reject config change operations until we have
    // committed at least one operation in our current term as leader.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (!queue_->IsCommittedIndexInCurrentTerm()) {
      return Status::IllegalState("Leader has not yet committed an operation in its own term");
    }

    const RaftConfigPB committed_config = cmeta_->CommittedConfig();

    // Support atomic ChangeConfig requests.
    if (req.has_cas_config_opid_index()) {
      if (committed_config.opid_index() != req.cas_config_opid_index()) {
        *error_code = ServerErrorPB::CAS_FAILED;
        return Status::IllegalState(Substitute("Request specified cas_config_opid_index "
                                               "of $0 but the committed config has opid_index "
                                               "of $1",
                                               req.cas_config_opid_index(),
                                               committed_config.opid_index()));
      }
    }

    // 'new_config' will be modified in-place and validated before being used
    // as the new Raft configuration.
    RaftConfigPB new_config = committed_config;

    // Enforce the "one by one" config change rules, even with the bulk API.
    // Keep track of total voters added, including non-voters promoted to
    // voters, and removed, including voters demoted to non-voters.
    int num_voters_modified = 0;

    // A record of the peers being modified so that we can enforce only one
    // change per peer per request.
    unordered_set<string> peers_modified;

    for (const auto& item : req.config_changes()) {
      if (PREDICT_FALSE(!item.has_type())) {
        *error_code = ServerErrorPB::INVALID_CONFIG;
        return Status::InvalidArgument("Must specify 'type' argument",
                                       SecureShortDebugString(req));
      }
      if (PREDICT_FALSE(!item.has_peer())) {
        *error_code = ServerErrorPB::INVALID_CONFIG;
        return Status::InvalidArgument("Must specify 'peer' argument",
                                       SecureShortDebugString(req));
      }

      ChangeConfigType type = item.type();
      const RaftPeerPB& peer = item.peer();

      if (PREDICT_FALSE(!peer.has_permanent_uuid())) {
        return Status::InvalidArgument("peer must have permanent_uuid specified",
                                       SecureShortDebugString(req));
      }

      if (!InsertIfNotPresent(&peers_modified, peer.permanent_uuid())) {
        return Status::InvalidArgument(
            Substitute("only one change allowed per peer: peer $0 appears more "
                       "than once in the config change request",
                       peer.permanent_uuid()),
            SecureShortDebugString(req));
      }

      const string& server_uuid = peer.permanent_uuid();
      switch (type) {
        case ADD_PEER:
          // Ensure the peer we are adding is not already a member of the configuration.
          if (IsRaftConfigMember(server_uuid, committed_config)) {
            return Status::InvalidArgument(
                Substitute("Server with UUID $0 is already a member of the config. RaftConfig: $1",
                           server_uuid, SecureShortDebugString(committed_config)));
          }
          if (!peer.has_member_type()) {
            return Status::InvalidArgument("peer must have member_type specified",
                                           SecureShortDebugString(req));
          }
          if (!peer.has_last_known_addr()) {
            return Status::InvalidArgument("peer must have last_known_addr specified",
                                           SecureShortDebugString(req));
          }
          if (peer.member_type() == RaftPeerPB::VOTER) {
            num_voters_modified++;
          }
          *new_config.add_peers() = peer;
          break;

        case REMOVE_PEER:
          if (server_uuid == peer_uuid()) {
            return Status::InvalidArgument(
                Substitute("Cannot remove peer $0 from the config because it is the leader. "
                           "Force another leader to be elected to remove this peer. "
                           "Consensus state: $1",
                           server_uuid,
                           SecureShortDebugString(cmeta_->ToConsensusStatePB())));
          }
          if (!RemoveFromRaftConfig(&new_config, server_uuid)) {
            return Status::NotFound(
                Substitute("Server with UUID $0 not a member of the config. RaftConfig: $1",
                           server_uuid, SecureShortDebugString(committed_config)));
          }
          if (IsRaftConfigVoter(server_uuid, committed_config)) {
            num_voters_modified++;
          }
          break;

        case MODIFY_PEER: {
          LOG(INFO) << "modifying peer" << peer.ShortDebugString();
          RaftPeerPB* modified_peer;
          RETURN_NOT_OK(GetRaftConfigMember(&new_config, server_uuid, &modified_peer));
          const RaftPeerPB orig_peer(*modified_peer);
          // Override 'member_type' and items within 'attrs' only if they are
          // explicitly passed in the request. At least one field must be
          // modified to be a valid request.
          if (peer.has_member_type() && peer.member_type() != modified_peer->member_type()) {
            if (modified_peer->member_type() == RaftPeerPB::VOTER ||
                peer.member_type() == RaftPeerPB::VOTER) {
              // This is a 'member_type' change involving a VOTER, i.e. a
              // promotion or demotion.
              num_voters_modified++;
            }
            // A leader must be forced to step down before demoting it.
            if (server_uuid == peer_uuid()) {
              return Status::InvalidArgument(
                  Substitute("Cannot modify member type of peer $0 because it is the leader. "
                              "Cause another leader to be elected to modify this peer. "
                              "Consensus state: $1",
                              server_uuid,
                              SecureShortDebugString(cmeta_->ToConsensusStatePB())));
            }
            modified_peer->set_member_type(peer.member_type());
          }
          if (peer.attrs().has_promote()) {
            modified_peer->mutable_attrs()->set_promote(peer.attrs().promote());
          }
          if (peer.attrs().has_replace()) {
            modified_peer->mutable_attrs()->set_replace(peer.attrs().replace());
          }
          // Ensure that MODIFY_PEER actually modified something.
          if (MessageDifferencer::Equals(orig_peer, *modified_peer)) {
            return Status::InvalidArgument("must modify a field when calling MODIFY_PEER");
          }
          break;
        }

        default:
          return Status::NotSupported(Substitute(
              "$0: unsupported type of configuration change",
              ChangeConfigType_Name(type)));
      }
    }

    // Don't allow no-op config changes to be committed.
    if (MessageDifferencer::Equals(committed_config, new_config)) {
      return Status::InvalidArgument("requested configuration change does not "
                                     "actually modify the config",
                                     SecureShortDebugString(req));
    }

    // Ensure this wasn't an illegal bulk change.
    if (num_voters_modified > 1) {
      return Status::InvalidArgument("it is not safe to modify the VOTER status "
                                     "of more than one peer at a time",
                                     SecureShortDebugString(req));
    }

    // We'll assign a new opid_index to this config change.
    new_config.clear_opid_index();

    RETURN_NOT_OK(ReplicateConfigChangeUnlocked(
        committed_config, std::move(new_config), std::bind(
            &RaftConsensus::MarkDirtyOnSuccess,
            this,
            string("Config change replication complete"),
            std::move(client_cb),
            std::placeholders::_1)));
  } // Release lock before signaling request.
  peer_manager_->SignalRequest();
  return Status::OK();
}

Status RaftConsensus::UnsafeChangeConfig(
    const UnsafeChangeConfigRequestPB& req,
    boost::optional<ServerErrorPB::Code>* error_code) {
  if (PREDICT_FALSE(!req.has_new_config())) {
    *error_code = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument("Request must contain 'new_config' argument "
                                   "to UnsafeChangeConfig()", SecureShortDebugString(req));
  }
  if (PREDICT_FALSE(!req.has_caller_id())) {
    *error_code = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument("Must specify 'caller_id' argument to UnsafeChangeConfig()",
                                   SecureShortDebugString(req));
  }

  // Grab the committed config and current term on this node.
  int64_t current_term;
  RaftConfigPB committed_config;
  int64_t all_replicated_index;
  int64_t last_committed_index;
  OpId preceding_opid;
  uint64_t msg_timestamp;
  {
    // Take the snapshot of the replica state and queue state so that
    // we can stick them in the consensus update request later.
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    current_term = CurrentTermUnlocked();
    committed_config = cmeta_->CommittedConfig();
    if (cmeta_->has_pending_config()) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING)
            << "Replica has a pending config, but the new config "
            << "will be unsafely changed anyway. "
            << "Currently pending config on the node: "
            << SecureShortDebugString(cmeta_->PendingConfig());
    }
    all_replicated_index = queue_->GetAllReplicatedIndex();
    last_committed_index = queue_->GetCommittedIndex();
    preceding_opid = queue_->GetLastOpIdInLog();
    msg_timestamp = time_manager_->GetSerialTimestamp().value();
  }

  // Validate that passed replica uuids are part of the committed config
  // on this node.  This allows a manual recovery tool to only have to specify
  // the uuid of each replica in the new config without having to know the
  // addresses of each server (since we can get the address information from
  // the committed config). Additionally, only a subset of the committed config
  // is required for typical cluster repair scenarios.
  std::unordered_set<string> retained_peer_uuids;
  const RaftConfigPB& config = req.new_config();
  for (const RaftPeerPB& new_peer : config.peers()) {
    const string& peer_uuid = new_peer.permanent_uuid();
    retained_peer_uuids.insert(peer_uuid);
    if (!IsRaftConfigMember(peer_uuid, committed_config)) {
      *error_code = ServerErrorPB::INVALID_CONFIG;
      return Status::InvalidArgument(Substitute("Peer with uuid $0 is not in the committed  "
                                                "config on this replica, rejecting the  "
                                                "unsafe config change request for tablet $1. "
                                                "Committed config: $2",
                                                peer_uuid, req.tablet_id(),
                                                SecureShortDebugString(committed_config)));
    }
  }

  RaftConfigPB new_config = committed_config;
  for (const auto& peer : committed_config.peers()) {
    const string& peer_uuid = peer.permanent_uuid();
    if (!ContainsKey(retained_peer_uuids, peer_uuid)) {
      CHECK(RemoveFromRaftConfig(&new_config, peer_uuid));
    }
  }
  // Check that local peer is part of the new config and is a VOTER.
  // Although it is valid for a local replica to not have itself
  // in the committed config, it is rare and a replica without itself
  // in the latest config is definitely not caught up with the latest leader's log.
  if (!IsRaftConfigVoter(peer_uuid(), new_config)) {
    *error_code = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument(Substitute("Local replica uuid $0 is not "
                                              "a VOTER in the new config, "
                                              "rejecting the unsafe config "
                                              "change request for tablet $1. "
                                              "Rejected config: $2" ,
                                              peer_uuid(), req.tablet_id(),
                                              SecureShortDebugString(new_config)));
  }
  new_config.set_unsafe_config_change(true);
  int64_t replicate_opid_index = preceding_opid.index() + 1;
  new_config.set_opid_index(replicate_opid_index);

  // Sanity check the new config. 'type' is irrelevant here.
  Status s = VerifyRaftConfig(new_config);
  if (!s.ok()) {
    *error_code = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument(Substitute("The resulting new config for tablet $0  "
                                              "from passed parameters has failed raft "
                                              "config sanity check: $1",
                                              req.tablet_id(), s.ToString()));
  }

  // Prepare the consensus request as if the request is being generated
  // from a different leader.
  ConsensusRequestPB consensus_req;
  consensus_req.set_caller_uuid(req.caller_id());
  // Bumping up the term for the consensus request being generated.
  // This makes this request appear to come from a new leader that
  // the local replica doesn't know about yet. If the local replica
  // happens to be the leader, this will cause it to step down.
  const int64_t new_term = current_term + 1;
  consensus_req.set_caller_term(new_term);
  consensus_req.mutable_preceding_id()->CopyFrom(preceding_opid);
  consensus_req.set_committed_index(last_committed_index);
  consensus_req.set_all_replicated_index(all_replicated_index);

  // Prepare the replicate msg to be replicated.
  ReplicateMsg* replicate = consensus_req.add_ops();
  ChangeConfigRecordPB* cc_req = replicate->mutable_change_config_record();
  cc_req->set_tablet_id(req.tablet_id());
  *cc_req->mutable_old_config() = committed_config;
  *cc_req->mutable_new_config() = new_config;
  OpId* id = replicate->mutable_id();
  // Bumping up both the term and the opid_index from what's found in the log.
  id->set_term(new_term);
  id->set_index(replicate_opid_index);
  replicate->set_op_type(CHANGE_CONFIG_OP);
  replicate->set_timestamp(msg_timestamp);

  VLOG_WITH_PREFIX(3) << "UnsafeChangeConfig: Generated consensus request: "
                      << SecureShortDebugString(consensus_req);

  LOG_WITH_PREFIX(WARNING)
        << "PROCEEDING WITH UNSAFE CONFIG CHANGE ON THIS SERVER, "
        << "COMMITTED CONFIG: " << SecureShortDebugString(committed_config)
        << "NEW CONFIG: " << SecureShortDebugString(new_config);

  ConsensusResponsePB consensus_resp;
  return Update(&consensus_req, &consensus_resp).AndThen([&consensus_resp]{
    return consensus_resp.has_error()
        ? StatusFromPB(consensus_resp.error().status()) : Status::OK();
  });
}

Status RaftConsensus::ChangeProxyTopology(const ChangeProxyTopologyRequestPB& req) {
  Status s = routing_table_->UpdateProxyTopology(req.new_config());
  if (FLAGS_raft_enable_multi_hop_proxy_routing && s.ok()) {
    LOG_WITH_PREFIX(INFO) << "updated routing table: \n" << routing_table_->ToString();
  }
  return s;
}

void RaftConsensus::Stop() {
  TRACE_EVENT2("consensus", "RaftConsensus::Shutdown",
               "peer", peer_uuid(),
               "tablet", options_.tablet_id);

  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    if (state_ == kStopping || state_ == kStopped || state_ == kShutdown) return;
    // Transition to kStopping state.
    SetStateUnlocked(kStopping);
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Raft consensus shutting down.";
  }

  // Close the peer manager.
  if (peer_manager_) peer_manager_->Close();

  // We must close the queue after we close the peers.
  if (queue_) queue_->Close();

  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    if (pending_) CHECK_OK(pending_->CancelPendingTransactions());
    SetStateUnlocked(kStopped);

    // Clear leader status on Stop(), in case this replica was the leader. If
    // we don't do this, the log messages still show this node as the leader.
    // No need to sync it since it's not persistent state.
    if (cmeta_) {
      ClearLeaderUnlocked();
    }

    // If we were the leader, stop witholding votes.
    if (withhold_votes_until_ == MonoTime::Max()) {
      withhold_votes_until_ = MonoTime::Min();
    }

    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Raft consensus is shut down!";
  }

  // Shut down things that might acquire locks during destruction.
  if (raft_pool_token_) raft_pool_token_->Shutdown();
  if (failure_detector_) DisableFailureDetector();
}

void RaftConsensus::Shutdown() {
  // Avoid taking locks if already shut down so we don't violate
  // ThreadRestrictions assertions in the case where the RaftConsensus
  // destructor runs on the reactor thread due to an election callback being
  // the last outstanding reference.
  if (shutdown_.Load(kMemOrderAcquire)) return;

  Stop();
  {
    LockGuard l(lock_);
    SetStateUnlocked(kShutdown);
  }
  shutdown_.Store(true, kMemOrderRelease);
}

Status RaftConsensus::StartConsensusOnlyRoundUnlocked(const ReplicateRefPtr& msg) {
  DCHECK(lock_.is_locked());
  OperationType op_type = msg->get()->op_type();
  CHECK(IsConsensusOnlyOperation(op_type))
      << "Expected a consensus-only op type, got " << OperationType_Name(op_type)
      << ": " << SecureShortDebugString(*msg->get());
  if (op_type == NO_OP) {
    ScheduleNoOpReceivedCallback(msg);
  }
  VLOG_WITH_PREFIX_UNLOCKED(1) << "Starting consensus round: "
                               << SecureShortDebugString(msg->get()->id());
  scoped_refptr<ConsensusRound> round(new ConsensusRound(this, msg));
  RETURN_NOT_OK(round_handler_->StartConsensusOnlyRound(round));

  // Using disable_noop_ mode as a proxy for special NORCB handling
  // When in disable_noop_ mode, the SetConsensusReplicatedCallback
  // will be enqueued in the MySQL plugin.
  // In this case we should not enqueue NonTxRoundReplicationFinished
  // below, as it also does unsupported things, e.g. enqueing a CommitMsg
  if (!disable_noop_) {
    StdStatusCallback client_cb = std::bind(&RaftConsensus::MarkDirtyOnSuccess,
                                            this,
                                            string("Replicated consensus-only round"),
                                            &DoNothingStatusCB,
                                            std::placeholders::_1);
    round->SetConsensusReplicatedCallback(std::bind(
        &RaftConsensus::NonTxRoundReplicationFinished,
        this,
        round.get(),
        std::move(client_cb),
        std::placeholders::_1));
  }
  return AddPendingOperationUnlocked(round);
}

Status RaftConsensus::AdvanceTermForTests(int64_t new_term) {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  CHECK_OK(CheckRunningUnlocked());
  return HandleTermAdvanceUnlocked(new_term);
}

std::string RaftConsensus::GetRequestVoteLogPrefixUnlocked(const VoteRequestPB& request) const {
  DCHECK(lock_.is_locked());
  return Substitute("$0Leader $1election vote request",
                    LogPrefixUnlocked(),
                    request.is_pre_election() ? "pre-" : "");
}

void RaftConsensus::FillVoteResponseVoteGranted(VoteResponsePB* response) {
  response->set_responder_term(CurrentTermUnlocked());
  response->set_vote_granted(true);
}

void RaftConsensus::FillVoteResponseVoteDenied(ConsensusErrorPB::Code error_code,
                                               VoteResponsePB* response) {
  response->set_responder_term(CurrentTermUnlocked());
  response->set_vote_granted(false);
  response->mutable_consensus_error()->set_code(error_code);
}

Status RaftConsensus::RequestVoteRespondInvalidTerm(const VoteRequestPB* request,
                                                    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::INVALID_TERM, response);
  string msg = Substitute("$0: Denying vote to candidate $1 for earlier term $2. "
                          "Current term is $3.",
                          GetRequestVoteLogPrefixUnlocked(*request),
                          request->candidate_uuid(),
                          request->candidate_term(),
                          CurrentTermUnlocked());
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondVoteAlreadyGranted(const VoteRequestPB* request,
                                                           VoteResponsePB* response) {
  FillVoteResponseVoteGranted(response);
  LOG(INFO) << Substitute("$0: Already granted yes vote for candidate $1 in term $2. "
                          "Re-sending same reply.",
                          GetRequestVoteLogPrefixUnlocked(*request),
                          request->candidate_uuid(),
                          request->candidate_term());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondAlreadyVotedForOther(const VoteRequestPB* request,
                                                             VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::ALREADY_VOTED, response);
  string msg = Substitute("$0: Denying vote to candidate $1 in current term $2: "
                          "Already voted for candidate $3 in this term.",
                          GetRequestVoteLogPrefixUnlocked(*request),
                          request->candidate_uuid(),
                          CurrentTermUnlocked(),
                          GetVotedForCurrentTermUnlocked());
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondLastOpIdTooOld(const OpId& local_last_logged_opid,
                                                       const VoteRequestPB* request,
                                                       VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::LAST_OPID_TOO_OLD, response);
  string msg = Substitute("$0: Denying vote to candidate $1 for term $2 because "
                          "replica has last-logged OpId of $3, which is greater than that of the "
                          "candidate, which has last-logged OpId of $4.",
                          GetRequestVoteLogPrefixUnlocked(*request),
                          request->candidate_uuid(),
                          request->candidate_term(),
                          SecureShortDebugString(local_last_logged_opid),
                          SecureShortDebugString(request->candidate_status().last_received()));
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondLeaderIsAlive(const VoteRequestPB* request,
                                                      VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::LEADER_IS_ALIVE, response);
  string msg = Substitute("$0: Denying vote to candidate $1 for term $2 because "
                          "replica is either leader or believes a valid leader to "
                          "be alive.",
                          GetRequestVoteLogPrefixUnlocked(*request),
                          request->candidate_uuid(),
                          request->candidate_term());
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondIsBusy(const VoteRequestPB* request,
                                               VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::CONSENSUS_BUSY, response);
  string msg = Substitute("$0: Denying vote to candidate $1 for term $2 because "
                          "replica is already servicing an update from a current leader "
                          "or another vote.",
                          GetRequestVoteLogPrefixUnlocked(*request),
                          request->candidate_uuid(),
                          request->candidate_term());
  LOG(INFO) << msg;
  StatusToPB(Status::ServiceUnavailable(msg),
             response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondVoteGranted(const VoteRequestPB* request,
                                                    VoteResponsePB* response) {
  // We know our vote will be "yes", so avoid triggering an election while we
  // persist our vote to disk. We use an exponential backoff to avoid too much
  // split-vote contention when nodes display high latencies.
  MonoDelta backoff = LeaderElectionExpBackoffDeltaUnlocked();
  SnoozeFailureDetector(string("vote granted"), backoff);

  if (!request->is_pre_election()) {
    // Persist our vote to disk.
    RETURN_NOT_OK(SetVotedForCurrentTermUnlocked(request->candidate_uuid()));
  }

  FillVoteResponseVoteGranted(response);

  // Give peer time to become leader. Snooze one more time after persisting our
  // vote. When disk latency is high, this should help reduce churn.
  SnoozeFailureDetector(/*reason_for_log=*/boost::none, backoff);

  LOG(INFO) << Substitute("$0: Granting yes vote for candidate $1 in term $2.",
                          GetRequestVoteLogPrefixUnlocked(*request),
                          request->candidate_uuid(),
                          CurrentTermUnlocked());
  return Status::OK();
}

RaftPeerPB::Role RaftConsensus::role() const {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  return cmeta_->active_role();
}

int64_t RaftConsensus::CurrentTerm() const {
  LockGuard l(lock_);
  return CurrentTermUnlocked();
}

string RaftConsensus::GetLeaderUuid() const {
  LockGuard l(lock_);
  return GetLeaderUuidUnlocked();
}

std::pair<string, unsigned int> RaftConsensus::GetLeaderHostPort() const
{
  LockGuard l(lock_);
  return cmeta_->leader_hostport();
}

void RaftConsensus::SetStateUnlocked(State new_state) {
  switch (new_state) {
    case kInitialized:
      CHECK_EQ(kNew, state_);
      break;
    case kRunning:
      CHECK_EQ(kInitialized, state_);
      break;
    case kStopping:
      CHECK(state_ != kStopped && state_ != kShutdown) << "State = " << State_Name(state_);
      break;
    case kStopped:
      CHECK_EQ(kStopping, state_);
      break;
    case kShutdown:
      CHECK(state_ == kStopped || state_ == kShutdown) << "State = " << State_Name(state_);
      break;
    default:
      LOG(FATAL) << "Disallowed transition to state = " << State_Name(new_state);
      break;
  }
  state_ = new_state;
}

const char* RaftConsensus::State_Name(State state) {
  switch (state) {
    case kNew:
      return "New";
    case kInitialized:
      return "Initialized";
    case kRunning:
      return "Running";
    case kStopping:
      return "Stopping";
    case kStopped:
      return "Stopped";
    case kShutdown:
      return "Shut down";
    default:
      LOG(DFATAL) << "Unknown State value: " << state;
      return "Unknown";
  }
}

void RaftConsensus::SetLeaderUuidUnlocked(const string& uuid) {
  DCHECK(lock_.is_locked());
  failed_elections_since_stable_leader_ = 0;
  num_failed_elections_metric_->set_value(failed_elections_since_stable_leader_);
  cmeta_->set_leader_uuid(uuid);
  routing_table_->UpdateLeader(uuid);
  MarkDirty(Substitute("New leader $0", uuid));
}

Status RaftConsensus::ReplicateConfigChangeUnlocked(
    RaftConfigPB old_config,
    RaftConfigPB new_config,
    StdStatusCallback client_cb) {
  DCHECK(lock_.is_locked());
  auto cc_replicate = new ReplicateMsg();
  cc_replicate->set_op_type(CHANGE_CONFIG_OP);
  ChangeConfigRecordPB* cc_req = cc_replicate->mutable_change_config_record();
  cc_req->set_tablet_id(options_.tablet_id);
  *cc_req->mutable_old_config() = std::move(old_config);
  *cc_req->mutable_new_config() = std::move(new_config);
  CHECK_OK(time_manager_->AssignTimestamp(cc_replicate));

  scoped_refptr<ConsensusRound> round(
      new ConsensusRound(this, make_scoped_refptr(new RefCountedReplicate(cc_replicate))));
  round->SetConsensusReplicatedCallback(std::bind(
      &RaftConsensus::NonTxRoundReplicationFinished,
      this,
      round.get(),
      std::move(client_cb),
      std::placeholders::_1));

  return AppendNewRoundToQueueUnlocked(round);
}

Status RaftConsensus::RefreshConsensusQueueAndPeersUnlocked() {
  DCHECK(lock_.is_locked());
  DCHECK_EQ(RaftPeerPB::LEADER, cmeta_->active_role());
  const RaftConfigPB& active_config = cmeta_->ActiveConfig();

  // Change the peers so that we're able to replicate messages remotely and
  // locally. The peer manager must be closed before updating the active config
  // in the queue -- when the queue is in LEADER mode, it checks that all
  // registered peers are a part of the active config.
  peer_manager_->Close();
  // TODO(todd): should use queue committed index here? in that case do
  // we need to pass it in at all?
  queue_->SetLeaderMode(pending_->GetCommittedIndex(),
                        CurrentTermUnlocked(),
                        active_config);
  RETURN_NOT_OK(peer_manager_->UpdateRaftConfig(active_config));
  return Status::OK();
}

const string& RaftConsensus::peer_uuid() const {
  return local_peer_pb_.permanent_uuid();
}

std::pair<string, unsigned int> RaftConsensus::peer_hostport() const {
  if (local_peer_pb_.has_last_known_addr()) {
    const ::kudu::HostPortPB& host_port = local_peer_pb_.last_known_addr();
    std::string host = host_port.host();
    return std::make_pair(host_port.host(), host_port.port());
  }
  return {};
}

const string& RaftConsensus::tablet_id() const {
  return options_.tablet_id;
}

Status RaftConsensus::ConsensusState(ConsensusStatePB* cstate,
                                     IncludeHealthReport report_health) const {
  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock l(lock_);
  if (state_ == kShutdown) {
    return Status::IllegalState("Tablet replica is shutdown");
  }
  ConsensusStatePB cstate_tmp = cmeta_->ToConsensusStatePB();

  // If we need to include the health report, merge it into the committed
  // config iff we believe we are the current leader of the config.
  if (report_health == INCLUDE_HEALTH_REPORT &&
      cmeta_->active_role() == RaftPeerPB::LEADER) {
    auto reports = queue_->ReportHealthOfPeers();

    // We don't need to access the queue anymore, so drop the consensus lock.
    l.unlock();

    // Iterate through each peer in the committed config and attach the health
    // report to it.
    RaftConfigPB* committed_raft_config = cstate_tmp.mutable_committed_config();
    for (int i = 0; i < committed_raft_config->peers_size(); i++) {
      RaftPeerPB* peer = committed_raft_config->mutable_peers(i);
      const HealthReportPB* report = FindOrNull(reports, peer->permanent_uuid());
      if (!report) continue; // Only attach details if we know about the peer.
      *peer->mutable_health_report() = *report;
    }
  }
  *cstate = std::move(cstate_tmp);
  return Status::OK();
}

RaftConfigPB RaftConsensus::CommittedConfig() const {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  return cmeta_->CommittedConfig();
}

void RaftConsensus::DumpStatusHtml(std::ostream& out) const {
  RaftPeerPB::Role role;
  {
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    if (state_ != kRunning) {
      out << "Tablet " << EscapeForHtmlToString(tablet_id()) << " not running" << std::endl;
      return;
    }
    role = cmeta_->active_role();
  }

  out << "<h1>Raft Consensus State</h1>" << std::endl;

  out << "<h2>State</h2>" << std::endl;
  out << "<pre>" << EscapeForHtmlToString(ToString()) << "</pre>" << std::endl;
  out << "<h2>Queue</h2>" << std::endl;
  out << "<pre>" << EscapeForHtmlToString(queue_->ToString()) << "</pre>" << std::endl;

  // Dump the queues on a leader.
  if (role == RaftPeerPB::LEADER) {
    out << "<h2>Queue overview</h2>" << std::endl;
    out << "<pre>" << EscapeForHtmlToString(queue_->ToString()) << "</pre>" << std::endl;
    out << "<hr/>" << std::endl;
    out << "<h2>Queue details</h2>" << std::endl;
    queue_->DumpToHtml(out);
  }
}

void RaftConsensus::ElectionCallback(ElectionReason reason, const ElectionResult& result) {
  // The election callback runs on a reactor thread, so we need to defer to our
  // threadpool. If the threadpool is already shut down for some reason, it's OK --
  // we're OK with the callback never running.
  WARN_NOT_OK(raft_pool_token_->SubmitFunc(std::bind(&RaftConsensus::NestedElectionDecisionCallback,
                                                     shared_from_this(),
                                                     reason,
                                                     result)),
              LogPrefixThreadSafe() + "Unable to run election callback");
}

void RaftConsensus::DoElectionCallback(ElectionReason reason, const ElectionResult& result) {
  const int64_t election_term = result.vote_request.candidate_term();
  const bool was_pre_election = result.vote_request.is_pre_election();
  const char* election_type = was_pre_election ? "pre-election" : "election";

  // The vote was granted, become leader.
  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock lock(lock_);
  Status s = CheckRunningUnlocked();
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Received " << election_type << " callback for term "
                                   << election_term << " while not running: "
                                   << s.ToString();
    return;
  }

  // Snooze to avoid the election timer firing again as much as possible.
  // We need to snooze when we win and when we lose:
  // - When we win because we're about to disable the timer and become leader.
  // - When we lose or otherwise we can fall into a cycle, where everyone keeps
  //   triggering elections but no election ever completes because by the time they
  //   finish another one is triggered already.
  SnoozeFailureDetector(string("election complete"), LeaderElectionExpBackoffDeltaUnlocked());

  if (result.decision == VOTE_DENIED) {
    failed_elections_since_stable_leader_++;
    num_failed_elections_metric_->set_value(failed_elections_since_stable_leader_);

    // If we called an election and one of the voters had a higher term than we did,
    // we should bump our term before we potentially try again. This is particularly
    // important with pre-elections to avoid getting "stuck" in a case like:
    //    Peer A: has ops through 1.10, term = 2, voted in term 2 for peer C
    //    Peer B: has ops through 1.15, term = 1
    // In this case, Peer B will reject peer A's pre-elections for term 3 because
    // the local log is longer. Peer A will reject B's pre-elections for term 2
    // because it already voted in term 2. The check below ensures that peer B
    // will bump to term 2 when it gets the vote rejection, such that its
    // next pre-election (for term 3) would succeed.
    if (result.highest_voter_term > CurrentTermUnlocked()) {
      HandleTermAdvanceUnlocked(result.highest_voter_term);
    }

    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Leader " << election_type << " lost for term " << election_term
        << ". Reason: "
        << (!result.message.empty() ? result.message : "None given");
    return;
  }

  // In a pre-election, we collected votes for the _next_ term.
  // So, we need to adjust our expectations of what the current term should be.
  int64_t election_started_in_term = election_term;
  if (was_pre_election) {
    election_started_in_term--;
  }

  if (election_started_in_term != CurrentTermUnlocked()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Leader " << election_type << " decision vote started in "
        << "defunct term " << election_started_in_term << ": "
        << (result.decision == VOTE_GRANTED ? "won" : "lost");
    return;
  }

  if (!cmeta_->IsVoterInConfig(peer_uuid(), ACTIVE_CONFIG)) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Leader " << election_type
                                      << " decision while not in active config. "
                                      << "Result: Term " << election_term << ": "
                                      << (result.decision == VOTE_GRANTED ? "won" : "lost")
                                      << ". RaftConfig: "
                                      << SecureShortDebugString(cmeta_->ActiveConfig());
    return;
  }

  if (cmeta_->active_role() == RaftPeerPB::LEADER) {
    // If this was a pre-election, it's possible to see the following interleaving:
    //
    //  1. Term N (follower): send a real election for term N
    //  2. Election callback expires again
    //  3. Term N (follower): send a pre-election for term N+1
    //  4. Election callback for real election from term N completes.
    //     Peer is now leader for term N.
    //  5. Pre-election callback from term N+1 completes, even though
    //     we are currently a leader of term N.
    // In this case, we should just ignore the pre-election, since we're
    // happily the leader of the prior term.
    if (was_pre_election) return;
    LOG_WITH_PREFIX_UNLOCKED(DFATAL)
        << "Leader " << election_type << " callback while already leader! "
        << "Result: Term " << election_term << ": "
        << (result.decision == VOTE_GRANTED ? "won" : "lost");
    return;
  }

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Leader " << election_type << " won for term " << election_term;

  if (was_pre_election) {
    // We just won the pre-election. So, we need to call a real election.
    lock.unlock();
    WARN_NOT_OK(StartElection(NORMAL_ELECTION, reason),
                "Couldn't start leader election after successful pre-election");
  } else {
    // We won a real election. Convert role to LEADER.
    SetLeaderUuidUnlocked(peer_uuid());

    // TODO(todd): BecomeLeaderUnlocked() can fail due to state checks during shutdown.
    // It races with the above state check.
    // This could be a problem during tablet deletion.
    CHECK_OK(BecomeLeaderUnlocked());
  }
}

void RaftConsensus::NestedElectionDecisionCallback(
    ElectionReason reason, const ElectionResult& result) {
  DoElectionCallback(reason, result);
  if (!result.vote_request.is_pre_election() && edcb_) {
    edcb_(result);
  }
}

boost::optional<OpId> RaftConsensus::GetNextOpId() const {
  LockGuard l(lock_);
  if (!queue_) return boost::none;
  return queue_->GetNextOpId();
}

boost::optional<OpId> RaftConsensus::GetLastOpId(OpIdType type) {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  return GetLastOpIdUnlocked(type);
}

boost::optional<OpId> RaftConsensus::GetLastOpIdUnlocked(OpIdType type) {
  // Return early if this method is called on an instance of RaftConsensus that
  // has not yet been started, failed during Init(), or failed during Start().
  if (!queue_ || !pending_) return boost::none;

  switch (type) {
    case RECEIVED_OPID:
      return queue_->GetLastOpIdInLog();
    case COMMITTED_OPID:
      return MakeOpId(pending_->GetTermWithLastCommittedOp(),
                      pending_->GetCommittedIndex());
    default:
      LOG(DFATAL) << LogPrefixUnlocked() << "Invalid OpIdType " << type;
      return boost::none;
  }
}

log::RetentionIndexes RaftConsensus::GetRetentionIndexes() {
  // Grab the watermarks from the queue. It's OK to fetch these two watermarks
  // separately -- the worst case is we see a relatively "out of date" watermark
  // which just means we'll retain slightly more than necessary in this invocation
  // of log GC.
  return log::RetentionIndexes(queue_->GetCommittedIndex(), // for durability
                               queue_->GetAllReplicatedIndex()); // for peers
}

void RaftConsensus::MarkDirty(const std::string& reason) {
  WARN_NOT_OK(raft_pool_token_->SubmitClosure(Bind(mark_dirty_clbk_, reason)),
              LogPrefixThreadSafe() + "Unable to run MarkDirty callback");
}

void RaftConsensus::MarkDirtyOnSuccess(const string& reason,
                                       const StdStatusCallback& client_cb,
                                       const Status& status) {
  if (PREDICT_TRUE(status.ok())) {
    MarkDirty(reason);
  }
  client_cb(status);
}

void RaftConsensus::NonTxRoundReplicationFinished(ConsensusRound* round,
                                                  const StdStatusCallback& client_cb,
                                                  const Status& status) {
  // NOTE: lock_ is held here because this is triggered by
  // PendingRounds::AbortOpsAfter() and AdvanceCommittedIndex().
  DCHECK(lock_.is_locked());
  OperationType op_type = round->replicate_msg()->op_type();
  const string& op_type_str = OperationType_Name(op_type);
  CHECK(IsConsensusOnlyOperation(op_type)) << "Unexpected op type: " << op_type_str;

  if (op_type == CHANGE_CONFIG_OP) {
    CompleteConfigChangeRoundUnlocked(round, status);
    // Fall through to the generic handling.
  }

  // TODO(mpercy): May need some refactoring to unlock 'lock_' before invoking
  // the client callback.

  if (!status.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << op_type_str << " replication failed: "
                                   << status.ToString();
    client_cb(status);
    return;
  }
  VLOG_WITH_PREFIX_UNLOCKED(1) << "Committing " << op_type_str << " with op id "
                               << round->id();
  round_handler_->FinishConsensusOnlyRound(round);
  gscoped_ptr<CommitMsg> commit_msg(new CommitMsg);
  commit_msg->set_op_type(round->replicate_msg()->op_type());
  *commit_msg->mutable_commited_op_id() = round->id();

  CHECK_OK(log_->AsyncAppendCommit(std::move(commit_msg),
                                   Bind(CrashIfNotOkStatusCB,
                                        "Enqueued commit operation failed to write to WAL")));

  client_cb(status);
}

void RaftConsensus::CompleteConfigChangeRoundUnlocked(ConsensusRound* round, const Status& status) {
  DCHECK(lock_.is_locked());
  const OpId& op_id = round->replicate_msg()->id();

  if (!status.ok()) {
    // If the config change being aborted is the current pending one, abort it.
    if (cmeta_->has_pending_config() &&
        cmeta_->GetConfigOpIdIndex(PENDING_CONFIG) == op_id.index()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Aborting config change with OpId "
                                     << op_id << ": " << status.ToString();
      cmeta_->clear_pending_config();
      // We should not forget to "abort" the config change in the routing table as well.
      CHECK_OK(routing_table_->UpdateRaftConfig(cmeta_->ActiveConfig()));

      // Disable leader failure detection if transitioning from VOTER to
      // NON_VOTER and vice versa.
      UpdateFailureDetectorState();
    } else {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Skipping abort of non-pending config change with OpId "
          << op_id << ": " << status.ToString();
    }

    // It's possible to abort a config change which isn't the pending one in the following
    // sequence:
    // - replicate a config change
    // - it gets committed, so we write the new config to disk as the Committed configuration
    // - we crash before the COMMIT message hits the WAL
    // - we restart the server, and the config change is added as a pending round again,
    //   but isn't set as Pending because it's already committed.
    // - we delete the tablet before committing it
    // See KUDU-1735.
    return;
  }

  // Commit the successful config change.

  DCHECK(round->replicate_msg()->change_config_record().has_old_config());
  DCHECK(round->replicate_msg()->change_config_record().has_new_config());
  const RaftConfigPB& old_config = round->replicate_msg()->change_config_record().old_config();
  const RaftConfigPB& new_config = round->replicate_msg()->change_config_record().new_config();
  DCHECK(old_config.has_opid_index());
  DCHECK(new_config.has_opid_index());
  // Check if the pending Raft config has an OpId less than the committed
  // config. If so, this is a replay at startup in which the COMMIT
  // messages were delayed.
  int64_t committed_config_opid_index = cmeta_->GetConfigOpIdIndex(COMMITTED_CONFIG);
  if (new_config.opid_index() > committed_config_opid_index) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Committing config change with OpId "
        << op_id << ": "
        << DiffRaftConfigs(old_config, new_config)
        << ". New config: { " << SecureShortDebugString(new_config) << " }";
    CHECK_OK(SetCommittedConfigUnlocked(new_config));
  } else {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Ignoring commit of config change with OpId "
        << op_id << " because the committed config has OpId index "
        << committed_config_opid_index << ". The config change we are ignoring is: "
        << "Old config: { " << SecureShortDebugString(old_config) << " }. "
        << "New config: { " << SecureShortDebugString(new_config) << " }";
  }
}

void RaftConsensus::EnableFailureDetector(boost::optional<MonoDelta> delta) {
  if (PREDICT_TRUE(FLAGS_enable_leader_failure_detection)) {
    failure_detector_->Start(std::move(delta));
  }
}

void RaftConsensus::DisableFailureDetector() {
  if (PREDICT_TRUE(FLAGS_enable_leader_failure_detection)) {
    failure_detector_->Stop();
  }
}

void RaftConsensus::UpdateFailureDetectorState(boost::optional<MonoDelta> delta) {
  DCHECK(lock_.is_locked());
  const auto& uuid = peer_uuid();
  if (uuid != cmeta_->leader_uuid() &&
      cmeta_->IsVoterInConfig(uuid, ACTIVE_CONFIG)) {
    // A voter that is not the leader should run the failure detector.
    EnableFailureDetector(std::move(delta));
  } else {
    // Otherwise, the local peer should not start leader elections
    // (e.g. if it is the leader, a non-voter, a non-participant, etc).
    DisableFailureDetector();
  }
}

void RaftConsensus::SnoozeFailureDetector(boost::optional<string> reason_for_log,
                                          boost::optional<MonoDelta> delta) {
  if (PREDICT_TRUE(failure_detector_ && FLAGS_enable_leader_failure_detection)) {
    if (reason_for_log) {
      LOG(INFO) << LogPrefixThreadSafe()
                << Substitute("Snoozing failure detection for $0 ($1)",
                              delta ? delta->ToString() : "election timeout",
                              *reason_for_log);
    }

    if (!delta) {
      delta = MinimumElectionTimeout();
    }
    failure_detector_->Snooze(std::move(delta));
  }
}

MonoDelta RaftConsensus::MinimumElectionTimeout() const {
  int32_t failure_timeout = FLAGS_leader_failure_max_missed_heartbeat_periods *
      FLAGS_raft_heartbeat_interval_ms;
  return MonoDelta::FromMilliseconds(failure_timeout);
}

MonoDelta RaftConsensus::MinimumElectionTimeoutWithBan() const {
  int32_t failure_timeout = FLAGS_leader_failure_max_missed_heartbeat_periods *
      FLAGS_raft_heartbeat_interval_ms * FLAGS_snooze_for_leader_ban_ratio;
  return MonoDelta::FromMilliseconds(failure_timeout);
}

MonoDelta RaftConsensus::LeaderElectionExpBackoffDeltaUnlocked() {
  DCHECK(lock_.is_locked());
  // Compute a backoff factor based on how many leader elections have
  // failed since a stable leader was last seen.
  double backoff_factor = pow(1.5, failed_elections_since_stable_leader_ + 1);
  double min_timeout = MinimumElectionTimeout().ToMilliseconds();
  double max_timeout = std::min<double>(
      min_timeout * backoff_factor,
      FLAGS_leader_failure_exp_backoff_max_delta_ms);

  // Randomize the timeout between the minimum and the calculated value.
  // We do this after the above capping to the max. Otherwise, after a
  // churny period, we'd end up highly likely to backoff exactly the max
  // amount.
  double timeout = min_timeout + (max_timeout - min_timeout) * rng_.NextDoubleFraction();
  DCHECK_GE(timeout, min_timeout);

  return MonoDelta::FromMilliseconds(timeout);
}

Status RaftConsensus::HandleTermAdvanceUnlocked(ConsensusTerm new_term,
                                                FlushToDisk flush) {
  DCHECK(lock_.is_locked());
  if (new_term <= CurrentTermUnlocked()) {
    return Status::IllegalState(Substitute("Can't advance term to: $0 current term: $1 is higher.",
                                           new_term, CurrentTermUnlocked()));
  }
  if (cmeta_->active_role() == RaftPeerPB::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Stepping down as leader of term "
                                   << CurrentTermUnlocked();
    RETURN_NOT_OK(BecomeReplicaUnlocked());
  }

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Advancing to term " << new_term;
  RETURN_NOT_OK(SetCurrentTermUnlocked(new_term, flush));
  if (term_metric_) term_metric_->set_value(new_term);
  last_received_cur_leader_ = MinimumOpId();
  return Status::OK();
}

Status RaftConsensus::CheckSafeToReplicateUnlocked(const ReplicateMsg& msg) const {
  DCHECK(lock_.is_locked());
#ifdef FB_DO_NOT_REMOVE
  DCHECK(!msg.has_id()) << "Should not have an ID yet: " << SecureShortDebugString(msg);
#endif
  RETURN_NOT_OK(CheckRunningUnlocked());
  return CheckActiveLeaderUnlocked();
}

Status RaftConsensus::CheckRunningUnlocked() const {
  DCHECK(lock_.is_locked());
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("RaftConsensus is not running",
                                Substitute("State = $0", State_Name(state_)));
  }
  return Status::OK();
}

Status RaftConsensus::CheckActiveLeaderUnlocked() const {
  DCHECK(lock_.is_locked());
  RaftPeerPB::Role role = cmeta_->active_role();
  switch (role) {
    case RaftPeerPB::LEADER:
      // Check for the consistency of the information in the consensus metadata
      // and the state of the consensus queue.
      DCHECK(queue_->IsInLeaderMode());
      if (leader_transfer_in_progress_.Load()) {
        return Status::ServiceUnavailable("leader transfer in progress");
      }
      return Status::OK();

    default:
      // Check for the consistency of the information in the consensus metadata
      // and the state of the consensus queue.
      DCHECK(!queue_->IsInLeaderMode());
      return Status::IllegalState(Substitute("Replica $0 is not leader of this config. Role: $1. "
                                             "Consensus state: $2",
                                             peer_uuid(),
                                             RaftPeerPB::Role_Name(role),
                                             SecureShortDebugString(cmeta_->ToConsensusStatePB())));
  }
}

Status RaftConsensus::CheckNoConfigChangePendingUnlocked() const {
  DCHECK(lock_.is_locked());
  if (cmeta_->has_pending_config()) {
    return Status::IllegalState(
        Substitute("RaftConfig change currently pending. Only one is allowed at a time.\n"
                   "  Committed config: $0.\n  Pending config: $1",
                   SecureShortDebugString(cmeta_->CommittedConfig()),
                   SecureShortDebugString(cmeta_->PendingConfig())));
  }
  return Status::OK();
}

Status RaftConsensus::SetPendingConfigUnlocked(const RaftConfigPB& new_config) {
  DCHECK(lock_.is_locked());
  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(new_config),
                        "Invalid config to set as pending");
  if (!new_config.unsafe_config_change()) {
    CHECK(!cmeta_->has_pending_config())
        << "Attempt to set pending config while another is already pending! "
        << "Existing pending config: " << SecureShortDebugString(cmeta_->PendingConfig()) << "; "
        << "Attempted new pending config: " << SecureShortDebugString(new_config);
  } else if (cmeta_->has_pending_config()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Allowing unsafe config change even though there is a pending config! "
        << "Existing pending config: " << SecureShortDebugString(cmeta_->PendingConfig()) << "; "
        << "New pending config: " << SecureShortDebugString(new_config);
  }
  cmeta_->set_pending_config(new_config);
  RETURN_NOT_OK(routing_table_->UpdateRaftConfig(cmeta_->ActiveConfig()));

  UpdateFailureDetectorState();

  return Status::OK();
}

Status RaftConsensus::SetCommittedConfigUnlocked(const RaftConfigPB& config_to_commit) {
  TRACE_EVENT0("consensus", "RaftConsensus::SetCommittedConfigUnlocked");
  DCHECK(lock_.is_locked());
  DCHECK(config_to_commit.IsInitialized());
  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(config_to_commit),
                        "Invalid config to set as committed");

  // Compare committed with pending configuration, ensure that they are the same.
  // In the event of an unsafe config change triggered by an administrator,
  // it is possible that the config being committed may not match the pending config
  // because unsafe config change allows multiple pending configs to exist.
  // Therefore we only need to validate that 'config_to_commit' matches the pending config
  // if the pending config does not have its 'unsafe_config_change' flag set.
  if (cmeta_->has_pending_config()) {
    RaftConfigPB pending_config = cmeta_->PendingConfig();
    if (!pending_config.unsafe_config_change()) {
      // Quorums must be exactly equal, even w.r.t. peer ordering.
      CHECK_EQ(pending_config.SerializeAsString(),
               config_to_commit.SerializeAsString())
          << Substitute("New committed config must equal pending config, but does not. "
                        "Pending config: $0, committed config: $1",
                        SecureShortDebugString(pending_config),
                        SecureShortDebugString(config_to_commit));
    }
  }
  cmeta_->set_committed_config(config_to_commit);
  cmeta_->clear_pending_config();
  CHECK_OK(cmeta_->Flush());
  RETURN_NOT_OK(routing_table_->UpdateRaftConfig(cmeta_->ActiveConfig()));
  return Status::OK();
}

void RaftConsensus::ScheduleTermAdvancementCallback(int64_t new_term) {
  WARN_NOT_OK(
      raft_pool_token_->SubmitFunc(
        std::bind(&RaftConsensus::DoTermAdvancmentCallback,
                  shared_from_this(),
                  new_term)),
      LogPrefixThreadSafe() + "Unable to run term advancement callback");
}

void RaftConsensus::DoTermAdvancmentCallback(int64_t new_term) {
  // Simply execute the registered callback for term advancement.
  if (tacb_) tacb_(new_term);
}

void RaftConsensus::ScheduleNoOpReceivedCallback(const ReplicateRefPtr& msg) {
  WARN_NOT_OK(
      raft_pool_token_->SubmitFunc(
        std::bind(&RaftConsensus::DoNoOpReceivedCallback,
                  shared_from_this(),
                  msg->get()->id())),
      LogPrefixThreadSafe() + "Unable to run no op received callback");
}

void RaftConsensus::DoNoOpReceivedCallback(const OpId id) {
  if (norcb_) norcb_(id);
}

void RaftConsensus::ScheduleLeaderDetectedCallback() {
  WARN_NOT_OK(
      raft_pool_token_->SubmitFunc(
        std::bind(&RaftConsensus::DoLeaderDetectedCallback,
                  shared_from_this())),
      LogPrefixThreadSafe() + "Unable to run leader detected callback");
}

void RaftConsensus::DoLeaderDetectedCallback() {
  if (ldcb_) ldcb_();
}

Status RaftConsensus::SetCurrentTermUnlocked(int64_t new_term,
                                            FlushToDisk flush) {
  TRACE_EVENT1("consensus", "RaftConsensus::SetCurrentTermUnlocked",
               "term", new_term);
  DCHECK(lock_.is_locked());
  if (PREDICT_FALSE(new_term <= CurrentTermUnlocked())) {
    return Status::IllegalState(
        Substitute("Cannot change term to a term that is lower than or equal to the current one. "
                   "Current: $0, Proposed: $1", CurrentTermUnlocked(), new_term));
  }
  cmeta_->set_current_term(new_term);
  cmeta_->clear_voted_for();
  if (flush == FLUSH_TO_DISK) {
    CHECK_OK(cmeta_->Flush());
  }

  ClearLeaderUnlocked();

  // Trigger term advancement callback
  ScheduleTermAdvancementCallback(new_term);

  return Status::OK();
}

const int64_t RaftConsensus::CurrentTermUnlocked() const {
  DCHECK(lock_.is_locked());
  return cmeta_->current_term();
}

string RaftConsensus::GetLeaderUuidUnlocked() const {
  DCHECK(lock_.is_locked());
  return cmeta_->leader_uuid();
}

bool RaftConsensus::HasLeaderUnlocked() const {
  DCHECK(lock_.is_locked());
  return !GetLeaderUuidUnlocked().empty();
}

void RaftConsensus::ClearLeaderUnlocked() {
  DCHECK(lock_.is_locked());
  cmeta_->set_leader_uuid("");
}

const bool RaftConsensus::HasVotedCurrentTermUnlocked() const {
  DCHECK(lock_.is_locked());
  return cmeta_->has_voted_for();
}

Status RaftConsensus::SetVotedForCurrentTermUnlocked(const std::string& uuid) {
  TRACE_EVENT1("consensus", "RaftConsensus::SetVotedForCurrentTermUnlocked",
               "uuid", uuid);
  DCHECK(lock_.is_locked());
  cmeta_->set_voted_for(uuid);
  CHECK_OK(cmeta_->Flush());
  return Status::OK();
}

const std::string& RaftConsensus::GetVotedForCurrentTermUnlocked() const {
  DCHECK(lock_.is_locked());
  DCHECK(cmeta_->has_voted_for());
  return cmeta_->voted_for();
}

const ConsensusOptions& RaftConsensus::GetOptions() const {
  return options_;
}

string RaftConsensus::LogPrefix() const {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  return LogPrefixUnlocked();
}

string RaftConsensus::LogPrefixUnlocked() const {
  DCHECK(lock_.is_locked());
  // 'cmeta_' may not be set if initialization failed.
  string cmeta_info;
  if (cmeta_) {
    cmeta_info = Substitute(" [term $0 $1]",
                            cmeta_->current_term(),
                            RaftPeerPB::Role_Name(cmeta_->active_role()));
  }
  return Substitute("T $0 P $1$2: ", options_.tablet_id, peer_uuid(), cmeta_info);
}

string RaftConsensus::LogPrefixThreadSafe() const {
  return Substitute("T $0 P $1: ",
                    options_.tablet_id,
                    peer_uuid());
}

string RaftConsensus::ToString() const {
  ThreadRestrictions::AssertWaitAllowed();
  LockGuard l(lock_);
  return ToStringUnlocked();
}

string RaftConsensus::ToStringUnlocked() const {
  DCHECK(lock_.is_locked());
  return Substitute("Replica: $0, State: $1, Role: $2",
                    peer_uuid(), State_Name(state_), RaftPeerPB::Role_Name(cmeta_->active_role()));
}

int64_t RaftConsensus::MetadataOnDiskSize() const {
  return cmeta_->on_disk_size();
}

ConsensusMetadata* RaftConsensus::consensus_metadata_for_tests() const {
  return cmeta_.get();
}

int64_t RaftConsensus::GetMillisSinceLastLeaderHeartbeat() const {
    return last_leader_communication_time_micros_ == 0 ?
        0 : (GetMonoTimeMicros() - last_leader_communication_time_micros_) / 1000;
}

void RaftConsensus::SetElectionDecisionCallback(ElectionDecisionCallback edcb) {
  CHECK(edcb);
  edcb_ = std::move(edcb);
}

void RaftConsensus::SetTermAdvancementCallback(TermAdvancementCallback tacb) {
  CHECK(tacb);
  tacb_ = std::move(tacb);
}

void RaftConsensus::SetNoOpReceivedCallback(NoOpReceivedCallback norcb) {
  CHECK(norcb);
  norcb_ = std::move(norcb);
}

void RaftConsensus::SetLeaderDetectedCallback(LeaderDetectedCallback ldcb) {
  CHECK(ldcb);
  ldcb_ = std::move(ldcb);
}

bool RaftConsensus::IsProxyRequest(const ConsensusRequestPB* request) const {
  // We expect proxy_uuid to reflect the uuid of the local node if it's a proxy
  // request, or to be empty otherwise.
  return !request->proxy_dest_uuid().empty();
}

// Set an error and respond.
// Stolen (mostly) from tablet_service.cc
static void SetupErrorAndRespond(const Status& s,
                                 ServerErrorPB::Code code,
                                 ConsensusResponsePB* response,
                                 rpc::RpcContext* context) {
  // Generic "service unavailable" errors will cause the client to retry later.
  if ((code == ServerErrorPB::UNKNOWN_ERROR /*||
       code == TabletServerErrorPB::THROTTLED */) && s.IsServiceUnavailable()) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY, s);
    return;
  }

  StatusToPB(s, response->mutable_error()->mutable_status());
  response->mutable_error()->set_code(code);
  context->RespondNoCache();
}

// Respond with an error and return if 's' is not OK.
#define RET_RESPOND_ERROR_NOT_OK(s) \
  do { \
    const kudu::Status& _s = (s); \
    if (PREDICT_FALSE(!_s.ok())) { \
      SetupErrorAndRespond(_s, ServerErrorPB::UNKNOWN_ERROR, response, context); \
      return; \
    } \
  } while (0)

void RaftConsensus::HandleProxyRequest(const ConsensusRequestPB* request,
                                       ConsensusResponsePB* response,
                                       rpc::RpcContext* context) {
  MonoDelta wal_wait_timeout = MonoDelta::FromMilliseconds(FLAGS_raft_log_cache_proxy_wait_time_ms);
  MonoTime wal_wait_deadline = MonoTime::Now() + wal_wait_timeout;

  // TODO(mpercy): Remove this config lookup when refactoring DRT to return a
  // RaftPeerPB, which will prevent a validation race.
  RaftConfigPB active_config;
  {
    // Snapshot the active Raft config so we know how to route proxied messages.
    ThreadRestrictions::AssertWaitAllowed();
    LockGuard l(lock_);
    RET_RESPOND_ERROR_NOT_OK(CheckRunningUnlocked());
    active_config = cmeta_->ActiveConfig();
  }

  // Initial implementation:
  //
  // Synchronously:
  // 1. Validate that the request is addressed to the local node via 'proxy_dest_uuid'.
  // 2. Reconstitute each message from the local cache.
  //
  // Asynchronously:
  // 4. Deliver the reconstituted request directly to the remote (async).
  // 5. Proxy the response from the remote back to the caller.

  // Validate the request.
  if (request->proxy_dest_uuid() != peer_uuid()) {
    Status s = Status::InvalidArgument(Substitute("Wrong proxy destination UUID requested. "
                                                  "Local UUID: $1. Requested UUID: $2",
                                                  peer_uuid(), request->proxy_dest_uuid()));
    LOG_WITH_PREFIX(WARNING) << s.ToString() << ": from " << context->requestor_string()
                 << ": " << SecureShortDebugString(*request);
    SetupErrorAndRespond(s, ServerErrorPB::WRONG_SERVER_UUID, response, context);
    return;
  }
  if (request->dest_uuid() == peer_uuid()) {
    LOG_WITH_PREFIX(WARNING) << "dest_uuid and proxy_dest_uuid are the same: "
                             << request->proxy_dest_uuid() << ": "
                             << request->ShortDebugString();
    context->RespondFailure(Status::InvalidArgument("proxy and desination must be different"));
    return;
  }

  // Construct the downstream request; copy the relevant fields from the
  // proxied request.
  ConsensusRequestPB downstream_request;
  auto prevent_ops_deletion = MakeScopedCleanup([&]() {
    // Prevent double-deletion of these requests.
    downstream_request.mutable_ops()->ExtractSubrange(
      /*start=*/ 0, /*num=*/ downstream_request.ops_size(), /*elements=*/ nullptr);
  });
  downstream_request.set_dest_uuid(request->dest_uuid());
  downstream_request.set_tablet_id(request->tablet_id());
  downstream_request.set_caller_uuid(request->caller_uuid());
  downstream_request.set_caller_term(request->caller_term());

  if (request->has_preceding_id()) {
    *downstream_request.mutable_preceding_id() = request->preceding_id();
  }
  if (request->has_committed_index()) {
    downstream_request.set_committed_index(request->committed_index());
  }
  if (request->has_all_replicated_index()) {
    downstream_request.set_all_replicated_index(request->all_replicated_index());
  }
  if (request->has_safe_timestamp()) {
    downstream_request.set_safe_timestamp(request->safe_timestamp());
  }
  if (request->has_last_idx_appended_to_leader()) {
    downstream_request.set_last_idx_appended_to_leader(request->last_idx_appended_to_leader());
  }

  downstream_request.set_proxy_caller_uuid(peer_uuid());

  string next_uuid = request->dest_uuid();
  if (FLAGS_raft_enable_multi_hop_proxy_routing) {
    RET_RESPOND_ERROR_NOT_OK(routing_table_->NextHop(peer_uuid(), request->dest_uuid(), &next_uuid));
  }

  if (request->dest_uuid() != next_uuid) {
    // Multi-hop proxy request.
    downstream_request.set_proxy_dest_uuid(next_uuid);
    // Forward the existing PROXY_OP ops.
    for (int i = 0; i < request->ops_size(); i++) {
      *downstream_request.add_ops() = request->ops(i);
    }
    prevent_ops_deletion.cancel(); // The ops we copy here are not pre-allocated.
  } else {

    // Reconstitute proxied events from the local cache.
    // If the cache does not have all events, we retry up until the specified
    // retry timeout.
    // TODO(mpercy): Switch this from polling to event-triggered.

    vector<ReplicateRefPtr> messages;
    do {

      // We assume and enforce that a single request is composed of a range of ops.
      int64_t first_op_index = -1;

      int64_t max_batch_size = FLAGS_consensus_max_batch_size_bytes - request->ByteSizeLong();
      for (int i = 0; i < request->ops_size(); i++) {
        auto& msg = request->ops(i);
        if (PREDICT_FALSE(msg.op_type() != PROXY_OP)) {
          RET_RESPOND_ERROR_NOT_OK(Status::InvalidArgument(Substitute(
              "proxy expected PROXY_OP but received opid {} of type {}",
              OpIdToString(msg.id()),
              OperationType_Name(msg.op_type()))));
        }
        if (i == 0) {
          first_op_index = msg.id().index();
        } else {
          // TODO(mpercy): It would be nice not to require consecutive indexes in the batch.
          // We should see if we can support it without a big perf penalty in IOPS.
          if (PREDICT_FALSE(msg.id().index() != first_op_index + i)) {
            RET_RESPOND_ERROR_NOT_OK(Status::InvalidArgument(Substitute(
                "proxy requires consecutive indexes in batch, but received {} after index {}",
                OpIdToString(msg.id()),
                first_op_index + i - 1)));
          }
        }
      }
      // Now we know that all ops we are reconstituting are consecutive.

      // TODO(mpercy): Check whether we have the event in our LogCache yet. If not,
      // wait and retry, or subscribe to the event being available. Most likely we
      // should try be simple / greedy and wait until we have all events in the
      // cache? Or we could be aggressive and fill what we can?

      OpId preceding_id;
      // TODO(mpercy): Add an API for ReadOps() to take number of ops we want,
      // instead of the max batch size.
      if (request->ops_size() > 0) {
        RET_RESPOND_ERROR_NOT_OK(queue_->log_cache()->ReadOps(first_op_index - 1,
                                                              max_batch_size,
                                                              &messages,
                                                              &preceding_id));
      }

      if (messages.size() >= request->ops_size()) {
        break;
      }

      if (MonoTime::Now() > wal_wait_deadline) {
        // TODO(mpercy): Increment a counter for how often we time out.
        break;
      }

      // Sleep and retry.
      SleepFor(MonoDelta::FromMilliseconds(5));

    } while (true);

    if (request->ops_size() > 0 && messages.size() == 0) {
      // We got nothing from the cache, go back into hiding until the expected
      // timeout when we turn into a heartbeat. TODO(mpercy): Increment a
      // counter for how often we degrade to a heartbeat.
      LOG_WITH_PREFIX(WARNING) << "no relevant events found in the log cache";
    }

    // Reconstitute the proxied ops. We silently tolerate proxying a subset of
    // the requested batch.
    for (int i = 0; i < request->ops_size() && i < messages.size(); i++) {
      // Ensure that the OpIds match. We don't expect a mismatch to ever
      // happen, so we log an error locally before reponding to the caller.
      if (!OpIdEquals(request->ops(i).id(), messages[i]->get()->id())) {
          Status s = Status::IllegalState(Substitute(
              "log cache returned non-consecutive OpId indexes: expected {}, found {}",
              OpIdToString(request->ops(i).id()),
              OpIdToString(messages[i]->get()->id())));
          LOG_WITH_PREFIX(ERROR) << s.ToString();
          RET_RESPOND_ERROR_NOT_OK(s);
      }
      downstream_request.mutable_ops()->AddAllocated(messages[i]->get());
    }
  }

  VLOG_WITH_PREFIX(3) << "Downstream proxy request: " << SecureShortDebugString(downstream_request);

  // Asynchronously:
  // Send the request to the remote.
  //
  // Find the address of the remote given our local config.
  RaftPeerPB* next_peer_pb;
  Status s = GetRaftConfigMember(&active_config, next_uuid, &next_peer_pb);
  if (PREDICT_FALSE(!s.ok())) {
    RET_RESPOND_ERROR_NOT_OK(s.CloneAndPrepend(Substitute(
        "unable to proxy to peer {} because it is not in the active config: {}",
        next_uuid,
        SecureShortDebugString(active_config))));
  }
  if (!next_peer_pb->has_last_known_addr()) {
    s = Status::IllegalState("no known address for peer", next_uuid);
    LOG_WITH_PREFIX(ERROR) << s.ToString();
    RET_RESPOND_ERROR_NOT_OK(s);
  }

  // TODO(mpercy): Cache this proxy object (although they are lightweight).
  // We can use a PeerProxyPool, like we do when sending from the leader.
  gscoped_ptr<PeerProxy> next_proxy;
  RET_RESPOND_ERROR_NOT_OK(peer_proxy_factory_->NewProxy(*next_peer_pb, &next_proxy));

  ConsensusResponsePB downstream_response;
  rpc::RpcController controller;
  controller.set_timeout(
      MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));

  // Here, we turn an async API into a blocking one with a CountdownLatch.
  // TODO(mpercy): Use an async approach instead.
  CountDownLatch latch(/*count=*/1);
  rpc::ResponseCallback callback = [&latch] { latch.CountDown(); };
  next_proxy->UpdateAsync(&downstream_request, &downstream_response, &controller, callback);
  latch.Wait();
  if (PREDICT_FALSE(!controller.status().ok())) {
    RET_RESPOND_ERROR_NOT_OK(controller.status().CloneAndPrepend(
        Substitute("Error proxying request from $0 to $1",
                   SecureShortDebugString(local_peer_pb_),
                   SecureShortDebugString(*next_peer_pb))));
  }

  // Proxy the response back to the caller.
  if (downstream_response.has_responder_uuid()) {
    response->set_responder_uuid(downstream_response.responder_uuid());
  }
  if (downstream_response.has_responder_term()) {
    response->set_responder_term(downstream_response.responder_term());
  }
  if (downstream_response.has_status()) {
    *response->mutable_status() = downstream_response.status();
  }
  if (downstream_response.has_error()) {
    *response->mutable_error() = downstream_response.error();
  }

  context->RespondSuccess();
}

////////////////////////////////////////////////////////////////////////
// ConsensusBootstrapInfo
////////////////////////////////////////////////////////////////////////

ConsensusBootstrapInfo::ConsensusBootstrapInfo()
  : last_id(MinimumOpId()),
    last_committed_id(MinimumOpId()) {
}

ConsensusBootstrapInfo::~ConsensusBootstrapInfo() {
  STLDeleteElements(&orphaned_replicates);
}

////////////////////////////////////////////////////////////////////////
// ConsensusRound
////////////////////////////////////////////////////////////////////////

ConsensusRound::ConsensusRound(RaftConsensus* consensus,
                               gscoped_ptr<ReplicateMsg> replicate_msg,
                               ConsensusReplicatedCallback replicated_cb)
    : consensus_(consensus),
      replicate_msg_(new RefCountedReplicate(replicate_msg.release())),
      replicated_cb_(std::move(replicated_cb)),
      bound_term_(-1) {}

ConsensusRound::ConsensusRound(RaftConsensus* consensus,
                               ReplicateRefPtr replicate_msg)
    : consensus_(consensus),
      replicate_msg_(std::move(replicate_msg)),
      bound_term_(-1) {
  DCHECK(replicate_msg_);
}

void ConsensusRound::NotifyReplicationFinished(const Status& status) {
  if (PREDICT_FALSE(!replicated_cb_)) return;
  replicated_cb_(status);
}

Status ConsensusRound::CheckBoundTerm(int64_t current_term) const {
  if (PREDICT_FALSE(bound_term_ != -1 &&
                    bound_term_ != current_term)) {
    return Status::Aborted(
      strings::Substitute(
        "Transaction submitted in term $0 cannot be replicated in term $1",
        bound_term_, current_term));
  }
  return Status::OK();
}

}  // namespace consensus
}  // namespace kudu
