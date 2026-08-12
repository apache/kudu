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

#include "kudu/master/auto_rebalancer.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/rebalance/placement_policy_util.h"
#include "kudu/rebalance/rebalance_algo.h"
#include "kudu/rebalance/rebalancer.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/init.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using kudu::cluster_summary::HealthCheckResult;
using kudu::cluster_summary::ReplicaSummary;
using kudu::cluster_summary::ServerHealth;
using kudu::cluster_summary::ServerHealthSummary;
using kudu::cluster_summary::TableSummary;
using kudu::cluster_summary::TabletSummary;
using kudu::consensus::ADD_PEER;
using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigResponsePB;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::GetConsensusStateRequestPB;
using kudu::consensus::GetConsensusStateResponsePB;
using kudu::consensus::LeaderStepDownMode;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::MODIFY_PEER;
using kudu::consensus::RaftPeerPB;
using kudu::master::TSManager;
using kudu::pb_util::SecureShortDebugString;
using kudu::rebalance::BuildTabletExtraInfoMap;
using kudu::rebalance::ClusterInfo;
using kudu::rebalance::ClusterLocalityInfo;
using kudu::rebalance::ClusterRawInfo;
using kudu::rebalance::PlacementPolicyViolationInfo;
using kudu::rebalance::Rebalancer;
using kudu::rebalance::SelectReplicaToMove;
using kudu::rebalance::TableReplicaMove;
using kudu::rebalance::TabletExtraInfo;
using kudu::rebalance::TabletsPlacementInfo;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using strings::Substitute;

using std::nullopt;
using std::optional;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

DEFINE_double(auto_rebalancing_load_imbalance_threshold,
              kudu::rebalance::Rebalancer::Config::kLoadImbalanceThreshold,
              "The threshold for the per-table location load imbalance. "
              "The threshold is used during the cross-location rebalancing "
              "phase. If the measured cross-location load imbalance for a "
              "table is greater than the specified threshold, the rebalancer "
              "tries to move table's replicas to reduce the imbalance. "
              "The recommended range for the threshold is [0.5, ...) with the "
              "default value of 1.0. The threshold represents a policy "
              "wrt what to prefer: either ideal balance of the cross-location "
              "load on per-table basis (lower threshold value) or minimum "
              "number of replica movements between locations "
              "(greater threshold value). The default value is empirically "
              "proven to be a good choice between 'ideal' and 'good enough' "
              "replica distributions.");

DEFINE_uint32(auto_rebalancing_interval_seconds, 30,
              "How long to sleep in between rebalancing cycles, before checking "
              "the cluster again to see if there is skew and rebalancing to be done.");

DEFINE_uint32(auto_rebalancing_max_moves_per_server, 1,
              "Maximum number of replica moves to perform concurrently on one "
              "tablet server: 'move from' and 'move to' are counted "
              "as separate move operations.");

DEFINE_uint32(auto_rebalancing_rpc_timeout_seconds, 60,
              "RPC timeout in seconds when making RPCs to request moving tablet replicas "
              "or to check if the replica movement has completed.");

DEFINE_uint32(auto_rebalancing_wait_for_replica_moves_seconds, 1,
              "How long to wait before checking to see if the scheduled replica movement "
              "in this iteration of auto-rebalancing has completed.");

DEFINE_bool(auto_rebalancing_fail_moves_for_test, false,
            "All CheckMoveCompleted will fail with IllegalState if this flag is true. "
            "This is only used for test.");
TAG_FLAG(auto_rebalancing_fail_moves_for_test, unsafe);

DEFINE_bool(auto_rebalancing_prefer_follower_replica_moves, true,
            "When true, among equally imbalanced table/move candidates the "
            "auto-rebalancer prefers replica moves whose source tablet server "
            "hosts a non-leader replica for that table, when that information is "
            "available from the cluster health report. When false, no such "
            "preference is applied. Moving a leader replica may still be chosen "
            "when necessary or when follower availability is unknown.");
TAG_FLAG(auto_rebalancing_prefer_follower_replica_moves, advanced);
TAG_FLAG(auto_rebalancing_prefer_follower_replica_moves, runtime);

DEFINE_bool(auto_rebalancing_enable_range_rebalancing, false,
            "Whether to rebalance each range partition independently. "
            "When enabled, the auto-rebalancer treats each range partition "
            "as a separate entity for balancing purposes, allowing finer-grained "
            "control over replica distribution across tablet servers.");
TAG_FLAG(auto_rebalancing_enable_range_rebalancing, advanced);
TAG_FLAG(auto_rebalancing_enable_range_rebalancing, runtime);

DECLARE_bool(auto_rebalancing_enabled);

METRIC_DEFINE_counter(server, auto_rebalancer_leader_moves_scheduled,
                      "Auto-Rebalancer Leader Moves Scheduled",
                      kudu::MetricUnit::kTablets,
                      "Number of replica moves scheduled by the auto-rebalancer "
                      "where the source replica was the Raft leader (follower "
                      "candidates were unavailable on the source tablet server).",
                      kudu::MetricLevel::kInfo);

METRIC_DEFINE_counter(server, auto_rebalancer_follower_moves_scheduled,
                      "Auto-Rebalancer Follower Moves Scheduled",
                      kudu::MetricUnit::kTablets,
                      "Number of replica moves scheduled by the auto-rebalancer "
                      "where the source replica was a non-leader follower.",
                      kudu::MetricLevel::kInfo);

METRIC_DEFINE_counter(server, auto_rebalancer_rounds_completed,
                      "Auto-Rebalancer Rounds Completed",
                      kudu::MetricUnit::kUnits,
                      "Number of full rebalancing cycles completed by the "
                      "auto-rebalancer.",
                      kudu::MetricLevel::kInfo);

namespace kudu {

namespace master {

AutoRebalancerTask::AutoRebalancerTask(CatalogManager* catalog_manager,
                                       TSManager* ts_manager,
                                       const scoped_refptr<MetricEntity>& metric_entity)
    : catalog_manager_(catalog_manager),
      ts_manager_(ts_manager),
      shutdown_(1),
      rebalancer_(Rebalancer(Rebalancer::Config(
      /*ignored_tservers*/{},
      /*master_addresses*/{},
      /*table_filters*/{},
      FLAGS_auto_rebalancing_max_moves_per_server,
      /*max_staleness_interval_sec*/300,
      /*max_run_time_sec*/0,
      /*move_replicas_from_ignored_tservers*/false,
      /*move_rf1_replicas*/false,
      /*output_replica_distribution_details*/false,
      /*run_policy_fixer*/true,
      /*run_cross_location_rebalancing*/true,
      /*run_intra_location_rebalancing*/true,
      FLAGS_auto_rebalancing_load_imbalance_threshold,
      /*force_rebalance_replicas_on_maintenance_tservers*/false,
      /*intra_location_rebalancing_concurrency*/0,
      FLAGS_auto_rebalancing_enable_range_rebalancing))),
      random_generator_(random_device_()),
      leader_moves_scheduled_(METRIC_auto_rebalancer_leader_moves_scheduled.Instantiate(
          metric_entity)),
      follower_moves_scheduled_(METRIC_auto_rebalancer_follower_moves_scheduled.Instantiate(
          metric_entity)),
      rounds_completed_(METRIC_auto_rebalancer_rounds_completed.Instantiate(metric_entity)),
      number_of_loop_iterations_for_test_(0),
      moves_attempted_this_round_for_test_(0),
      moves_scheduled_this_round_for_test_(0) {
}

AutoRebalancerTask::~AutoRebalancerTask() {
  if (thread_) {
    Shutdown();
  }
}

Status AutoRebalancerTask::Init() {
  DCHECK(!thread_) << "AutoRebalancerTask is already initialized";
  MessengerBuilder builder("auto-rebalancer");
  if (auto username = kudu::security::GetLoggedInUsernameFromKeytab()) {
    builder.set_sasl_proto_name(username.value());
  }
  RETURN_NOT_OK(std::move(builder).Build(&messenger_));
  return Thread::Create("catalog manager", "auto-rebalancer",
                        [this]() { this->RunLoop(); }, &thread_);
}

void AutoRebalancerTask::Shutdown() {
  CHECK(thread_) << "AutoRebalancerTask is not initialized";
  if (!shutdown_.CountDown()) {
    return;
  }
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
  thread_.reset();
}

void AutoRebalancerTask::RunLoop() {
  vector<Rebalancer::ReplicaMove> replica_moves;
  while (!shutdown_.WaitFor(
      MonoDelta::FromSeconds(FLAGS_auto_rebalancing_interval_seconds))) {
    // Retry any stuck replace-marker cleanups first, even when rebalancing is
    // disabled: it may have been turned off right after a round failed, and
    // those markers still need to be cleared.
    ProcessPendingReplaceClears();
    if (!FLAGS_auto_rebalancing_enabled) {
      // Toggling the auto-rebalancer on/off by changing FLAGS_auto_rebalancing_enabled,
      // will take effect in the next loop. Already scheduled/running replica moves will
      // be unaffected.
      continue;
    }
    // If catalog manager isn't initialized or isn't the leader, don't do rebalancing.
    // Putting the auto-rebalancer to sleep shouldn't affect the master's ability
    // to become the leader. When the thread wakes up and discovers it is now
    // the leader, then it can begin auto-rebalancing.
    {
      CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
      if (!l.first_failed_status().ok()) {
        moves_attempted_this_round_for_test_ = 0;
        moves_scheduled_this_round_for_test_ = 0;
        continue;
      }
    }

    number_of_loop_iterations_for_test_++;
    // Reset the per-round counters at the start of each iteration. Otherwise a
    // round that gets skipped later (say, BuildClusterRawInfo failing during
    // recovery) would leave tests reading a stale count from the previous
    // round.
    moves_attempted_this_round_for_test_ = 0;
    moves_scheduled_this_round_for_test_ = 0;

    // Structs to hold information about the cluster's status.
    ClusterRawInfo raw_info;
    ClusterInfo cluster_info;
    TabletsPlacementInfo placement_info;
    Status s = BuildClusterRawInfo(/*location*/nullopt, &raw_info);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Could not retrieve cluster info: $0", s.ToString());
      continue;
    }

    // NOTE: There should be no moves in progress, because this loop waits for
    // scheduled moves to complete before continuing to the next iteration.
    s = rebalancer_.BuildClusterInfo(raw_info, Rebalancer::MovesInProgress(), &cluster_info);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Could not build cluster info: $0", s.ToString());
      continue;
    }
    if (config_.run_policy_fixer) {
      s = BuildTabletsPlacementInfo(raw_info, Rebalancer::MovesInProgress(), &placement_info);
      if (!s.ok()) {
        LOG(WARNING) << Substitute("Could not build tablet placement info: $0", s.ToString());
        continue;
      }
    }

    DCHECK(replica_moves.empty());
    s = GetMoves(raw_info, cluster_info.locality, placement_info, &replica_moves);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("could not retrieve auto-rebalancing replica moves: $0",
                                 s.ToString());
      continue;
    }
    moves_attempted_this_round_for_test_ = replica_moves.size();
    // Set to -1 as a sentinel while ExecuteMoves() is in progress, so that
    // test assertions reading both counters cannot observe a stale
    // moves_scheduled value from a prior round alongside the new
    // moves_attempted value.
    moves_scheduled_this_round_for_test_ = -1;
    ExecuteMoves(&replica_moves);
    moves_scheduled_this_round_for_test_ = replica_moves.size();

    // Wait for all of the moves from this iteration to complete.
    do {
      if (shutdown_.WaitFor(MonoDelta::FromSeconds(
            FLAGS_auto_rebalancing_wait_for_replica_moves_seconds))) {
        return;
      }
      WARN_NOT_OK(CheckReplicaMovesCompleted(&replica_moves),
                  "scheduled replica move failed to complete");
    } while (!replica_moves.empty());

    // Verify that all move counters are properly cleaned up.
#if DCHECK_IS_ON()
    for (const auto& entry : moves_per_tserver_) {
      DCHECK_EQ(0, entry.second) << "Tserver " << entry.first << " still has " << entry.second
                                 << " moves after all operations completed";
    }
#endif
    // Only rounds that run to completion reach here; early-continue paths
    // (BuildClusterRawInfo, GetMoves failures, etc.) skip this increment.
    rounds_completed_->Increment();
  }
}

Status AutoRebalancerTask::GetMoves(
    const ClusterRawInfo& raw_info,
    const ClusterLocalityInfo& locality,
    const TabletsPlacementInfo& placement_info,
    vector<Rebalancer::ReplicaMove>* replica_moves) {

  const auto& ts_id_by_location = locality.servers_by_location;
  vector<Rebalancer::ReplicaMove> rep_moves;

  // No tservers: no moves to make.
  if (ts_id_by_location.empty()) {
    return Status::OK();
  }

  // One location: use greedy rebalancing algorithm to find moves.
  if (ts_id_by_location.size() == 1) {
    rebalance::TwoDimensionalGreedyAlgo algo(
        rebalance::TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_RANDOM,
        FLAGS_auto_rebalancing_prefer_follower_replica_moves);
    RETURN_NOT_OK(GetMovesUsingRebalancingAlgo(raw_info, &algo, CrossLocations::NO, &rep_moves));
    *replica_moves = std::move(rep_moves);
    return Status::OK();
  }

  // If there are placement policy violations, only find moves to fix them.
  // Set flag to indicate that this round of rebalancing will only fix
  // these violations.
  if (config_.run_policy_fixer) {
    vector<PlacementPolicyViolationInfo> ppvi;
    RETURN_NOT_OK(DetectPlacementPolicyViolations(placement_info, &ppvi));
    // Filter out all reported violations which are already taken care of.
    RETURN_NOT_OK(FindMovesToReimposePlacementPolicy(
        placement_info, locality, ppvi, &rep_moves));
    if (!rep_moves.empty()) {
      *replica_moves = std::move(rep_moves);
      return Status::OK();
    }
  }

  // If no placement policy violations were found, perform load rebalancing.
  // Perform cross-location rebalancing.
  if (config_.run_cross_location_rebalancing) {
    rebalance::LocationBalancingAlgo algo(
        FLAGS_auto_rebalancing_load_imbalance_threshold,
        FLAGS_auto_rebalancing_prefer_follower_replica_moves);
    RETURN_NOT_OK(GetMovesUsingRebalancingAlgo(
        raw_info, &algo, CrossLocations::YES, &rep_moves));
  }

  // Perform intra-location rebalancing.
  if (config_.run_intra_location_rebalancing) {
    rebalance::TwoDimensionalGreedyAlgo algo(
        rebalance::TwoDimensionalGreedyAlgo::EqualSkewOption::PICK_RANDOM,
        FLAGS_auto_rebalancing_prefer_follower_replica_moves);
    for (const auto& elem : ts_id_by_location) {
      const auto& location = elem.first;
      ClusterRawInfo location_raw_info;
      RETURN_NOT_OK(BuildClusterRawInfo(location, &location_raw_info));
      RETURN_NOT_OK(GetMovesUsingRebalancingAlgo(
          location_raw_info, &algo, CrossLocations::NO, &rep_moves));
    }
  }
  *replica_moves = std::move(rep_moves);
  return Status::OK();
}

Status AutoRebalancerTask::GetMovesUsingRebalancingAlgo(
  const ClusterRawInfo& raw_info,
  rebalance::RebalancingAlgo* algo,
  CrossLocations cross_location,
  vector<Rebalancer::ReplicaMove>* replica_moves) {
  // Capture the flag value once to ensure consistency throughout this method
  // and protect against in-flight changes via 'kudu master set_flag'.
  const int max_moves_per_server = FLAGS_auto_rebalancing_max_moves_per_server;

  // Use signed integers to handle the case where replica_moves->size() might exceed
  // the calculated limit, which would cause underflow with unsigned types.
  const int64_t num_tservers = raw_info.tserver_summaries.size();
  int64_t max_moves = max_moves_per_server * num_tservers;
  max_moves -= static_cast<int64_t>(replica_moves->size());
  if (max_moves <= 0) {
    return Status::OK();
  }

  TabletsPlacementInfo tpi;
  if (cross_location == CrossLocations::YES) {
    RETURN_NOT_OK(BuildTabletsPlacementInfo(raw_info, Rebalancer::MovesInProgress(), &tpi));
  }

  unordered_map<string, TabletExtraInfo> extra_info_by_tablet_id;
  BuildTabletExtraInfoMap(raw_info, &extra_info_by_tablet_id);

  vector<TableReplicaMove> moves;
  ClusterInfo cluster_info;
  RETURN_NOT_OK(rebalancer_.BuildClusterInfo(
      raw_info, Rebalancer::MovesInProgress(), &cluster_info));
  RETURN_NOT_OK(algo->GetNextMoves(cluster_info, max_moves, &moves));

  unordered_set<string> tablets_in_move;
  vector<Rebalancer::ReplicaMove> rep_moves;

  for (const auto& move : moves) {
    // Check if this move would exceed the per-tserver limit based on currently
    // in-flight moves. We check against moves_per_tserver_ (the actual ongoing moves)
    // rather than limiting within this batch, since the global max_moves limit
    // already constrains the batch size.
    int src_ongoing = moves_per_tserver_[move.from];
    int dst_ongoing = moves_per_tserver_[move.to];

    if (src_ongoing >= max_moves_per_server || dst_ongoing >= max_moves_per_server) {
      // Skip this move as it would violate per-tserver limits.
      VLOG(1) << Substitute(
          "Skipping move from $0 to $1: per-tserver limit reached "
          "(src=$2, dst=$3, limit=$4)",
          move.from,
          move.to,
          src_ongoing,
          dst_ongoing,
          max_moves_per_server);
      continue;
    }

    vector<string> tablet_ids;
    const bool is_leader_move = rebalancer_.FindReplicas(move, raw_info, &tablet_ids);
    if (cross_location == CrossLocations::YES) {
      // In case of cross-location (a.k.a. inter-location) rebalancing it is
      // necessary to make sure the majority of replicas would not end up
      // at the same location after the move. If so, remove those tablets
      // from the list of candidates.
      RETURN_NOT_OK(rebalancer_.FilterCrossLocationTabletCandidates(
          cluster_info.locality.location_by_ts_id, tpi, move, &tablet_ids));
    }

    RETURN_NOT_OK(SelectReplicaToMove(move, extra_info_by_tablet_id,
                                      &random_generator_, std::move(tablet_ids),
                                      &tablets_in_move, &rep_moves,
                                      is_leader_move));
  }

  *replica_moves = std::move(rep_moves);
  return Status::OK();
}

Status AutoRebalancerTask::GetTabletLeader(
    const string& tablet_id,
    string* leader_uuid,
    HostPort* leader_hp) const {
  TabletLocationsPB locs_pb;
  CatalogManager::TSInfosDict ts_infos_dict;
  // GetTabletLocations() will fail if the catalog manager is not the leader.
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
    RETURN_NOT_OK(l.first_failed_status());
    RETURN_NOT_OK(catalog_manager_->GetTabletLocations(
        tablet_id,
        ReplicaTypeFilter::VOTER_REPLICA,
        /*use_external_addr=*/false,
        &locs_pb,
        &ts_infos_dict,
        nullopt));
  }
  for (const auto& r : locs_pb.interned_replicas()) {
    if (r.role() == RaftPeerPB::LEADER) {
      int index = r.ts_info_idx();
      const TSInfoPB& ts_info = *(ts_infos_dict.ts_info_pbs()[index]);
      *leader_uuid = ts_info.permanent_uuid();
      *leader_hp = HostPortFromPB(ts_info.rpc_addresses(0));
      return Status::OK();
    }
  }
  // No leader at the moment, most likely a transient election. This is worth
  // retrying, so don't use NotFound, which callers read as "the replica is gone".
  return Status::ServiceUnavailable(
      Substitute("Couldn't find leader for tablet $0", tablet_id));
}

void AutoRebalancerTask::ExecuteMoves(
    vector<Rebalancer::ReplicaMove>* replica_moves) {
  vector<int> failed_indices;

  for (int i = 0; i < static_cast<int>(replica_moves->size()); ++i) {
    auto& move_info = (*replica_moves)[i];
    const auto& tablet_id = move_info.tablet_uuid;
    const auto& src_ts_uuid = move_info.ts_uuid_from;
    const auto& dst_ts_uuid = move_info.ts_uuid_to;

    // Attempt to schedule this move. On any failure, log a warning and skip
    // this move so the remaining moves are still attempted.
    Status s = [&]() -> Status {
      // Read the current config opid_index before sending BulkChangeConfig.
      // This is stored on the move so that CheckMoveCompleted can use it as
      // a freshness gate: if CatalogManager still shows the same opid_index
      // after the move is dispatched, the heartbeat carrying the new config
      // hasn't arrived yet and the check should wait rather than act on
      // stale data.
      ConsensusStatePB pre_cstate;
      {
        CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
        RETURN_NOT_OK(l.first_failed_status());
        RETURN_NOT_OK(catalog_manager_->GetTabletConsensusState(tablet_id, &pre_cstate));
      }
      const int64_t pre_opid_index = pre_cstate.committed_config().opid_index();

      string leader_uuid;
      HostPort leader_hp;
      RETURN_NOT_OK(GetTabletLeader(tablet_id, &leader_uuid, &leader_hp));
      shared_ptr<TSDescriptor> leader_desc;
      if (!ts_manager_->LookupTSByUUID(leader_uuid, &leader_desc)) {
        return Status::NotFound(
            Substitute("Couldn't find leader replica's tserver $0", leader_uuid));
      }
      // Mark the replica to be replaced.
      BulkChangeConfigRequestPB req;
      auto* modify_peer = req.add_config_changes();
      modify_peer->set_type(MODIFY_PEER);
      *modify_peer->mutable_peer()->mutable_permanent_uuid() = src_ts_uuid;
      modify_peer->mutable_peer()->mutable_attrs()->set_replace(true);

      // NOTE: 'dst_ts_uuid' is empty if the move was scheduled to fix location
      // policy violations.
      if (!dst_ts_uuid.empty()) {
        shared_ptr<TSDescriptor> dest_desc;
        if (!ts_manager_->LookupTSByUUID(dst_ts_uuid, &dest_desc)) {
          return Status::NotFound("Could not find destination tserver");
        }
        ServerRegistrationPB dest_reg;
        RETURN_NOT_OK(dest_desc->GetRegistration(&dest_reg));

        auto* add_peer_change = req.add_config_changes();
        add_peer_change->set_type(ADD_PEER);
        auto* new_peer = add_peer_change->mutable_peer();
        new_peer->set_permanent_uuid(dst_ts_uuid);
        new_peer->set_member_type(RaftPeerPB::NON_VOTER);
        new_peer->mutable_attrs()->set_promote(true);
        *new_peer->mutable_last_known_addr() = dest_reg.rpc_addresses(0);
      }

      // Send the change config request to the tablet leader.
      // Set the CAS index so the request is rejected if another actor has
      // already modified the config since we read pre_opid_index. This
      // guarantees that if the request succeeds, the new config's opid_index
      // is greater than pre_opid_index.
      //
      // Note: the opid_index read above and this RPC are not atomic. If a
      // heartbeat delivers a config update to CatalogManager between the two
      // calls (e.g. the leader applied an unrelated config change), the
      // leader's committed index will be higher than pre_opid_index and the
      // CAS will fail. The move is then treated as a scheduling failure and
      // retried next cycle.
      ChangeConfigResponsePB resp;
      RpcController rpc;
      rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_rpc_timeout_seconds));
      req.set_dest_uuid(leader_uuid);
      req.set_tablet_id(tablet_id);
      req.set_cas_config_opid_index(pre_opid_index);
      vector<Sockaddr> resolved;
      RETURN_NOT_OK(leader_hp.ResolveAddresses(&resolved));
      ConsensusServiceProxy proxy(messenger_, resolved[0], leader_hp.host());
      RETURN_NOT_OK(proxy.BulkChangeConfig(req, &resp, &rpc));
      if (resp.has_error()) return StatusFromPB(resp.error().status());

      // Record the config opid_index that was current before this move was
      // dispatched. CheckMoveCompleted uses this to determine whether
      // CatalogManager has received the heartbeat reflecting the new config.
      move_info.config_opid_idx = pre_opid_index;

      // Successfully scheduled the move. Increment counters for both source and destination.
      moves_per_tserver_[src_ts_uuid]++;
      if (!dst_ts_uuid.empty()) {
        moves_per_tserver_[dst_ts_uuid]++;
      }
      if (move_info.is_leader_move) {
        leader_moves_scheduled_->Increment();
      } else {
        follower_moves_scheduled_->Increment();
      }
      VLOG(1) << Substitute(
          "Scheduled move: tablet $0 from $1 to $2 "
          "(src_moves=$3, dst_moves=$4)",
          tablet_id,
          src_ts_uuid,
          dst_ts_uuid,
          moves_per_tserver_[src_ts_uuid],
          dst_ts_uuid.empty() ? 0 : moves_per_tserver_[dst_ts_uuid]);
      return Status::OK();
    }();

    if (!s.ok()) {
      LOG(WARNING) << Substitute("Failed to schedule move for tablet $0: $1",
                                 tablet_id, s.ToString());
      failed_indices.push_back(i);
    }
  }

  // Erase failed moves back-to-front to keep indices valid during removal.
  for (int i = static_cast<int>(failed_indices.size()) - 1; i >= 0; --i) {
    replica_moves->erase(replica_moves->begin() + failed_indices[i]);
  }
}

Status AutoRebalancerTask::BuildClusterRawInfo(
    const optional<string>& location,
    ClusterRawInfo* raw_info) const {

  vector<ServerHealthSummary> tserver_summaries;
  unordered_set<string> tserver_uuids;
  vector<TableSummary> table_summaries;
  vector<TabletSummary> tablet_summaries;

  // Avoid making any moves if not all tservers are up, to prevent the possibility
  // of moving tablets, then having to move them again later, when a tserver that
  // was not available before, is available for tablet placement again.
  TSDescriptorVector descriptors;
  ts_manager_->GetDescriptorsAvailableForPlacement(&descriptors);
  if (descriptors.size() != ts_manager_->GetLiveCount()) {
    return Status::IllegalState(Substitute("not all tservers available for tablet placement"));
  }
  tserver_uuids.reserve(descriptors.size());
  tserver_summaries.reserve(descriptors.size());

  // All the tservers are healthy and available for placement.
  // For rebalancing, only need to fill the uuid and location fields.
  for (const auto& ts : descriptors) {
    ServerHealthSummary summary;
    summary.uuid = ts->permanent_uuid();
    if (ts->location()) {
      summary.ts_location = *(ts->location());
    }
    summary.health = ServerHealth::HEALTHY;
    tserver_uuids.insert(summary.uuid);
    tserver_summaries.emplace_back(std::move(summary));
  }

  vector<scoped_refptr<TableInfo>> table_infos;

  {
    CatalogManager::ScopedLeaderSharedLock leader_lock(catalog_manager_);
    RETURN_NOT_OK(leader_lock.first_failed_status());
    catalog_manager_->GetAllTables(&table_infos);
  }

  table_summaries.reserve(table_infos.size());

  for (const auto& table : table_infos) {
    TableMetadataLock table_l(table.get(), LockMode::READ);

    const SysTablesEntryPB& table_data = table->metadata().state().pb;
    if (table_data.state() == SysTablesEntryPB::REMOVED) {
      // Don't worry about rebalancing replicas that belong to deleted tables.
      continue;
    }
    TableSummary table_summary;
    table_summary.id = table->id();
    table_summary.name = table_data.name();
    table_summary.replication_factor = table_data.num_replicas();

    vector<scoped_refptr<TabletInfo>> tablet_infos;
    table->GetAllTablets(&tablet_infos);
    tablet_summaries.reserve(tablet_summaries.size() + tablet_infos.size());

    for (const auto& tablet : tablet_infos) {
      TabletMetadataLock tablet_l(tablet.get(), LockMode::READ);

      TabletSummary tablet_summary;
      tablet_summary.id = tablet->id();
      tablet_summary.table_id = table_summary.id;
      tablet_summary.table_name = table_summary.name;

      // Extract range partition key for range-aware rebalancing
      if (FLAGS_auto_rebalancing_enable_range_rebalancing) {
        const auto& tablet_pb = tablet_l.data().pb;
        if (tablet_pb.has_partition()) {
          Partition partition;
          Partition::FromPB(tablet_pb.partition(), &partition);
          const auto& range_key_begin = partition.begin().range_key();

          // Format as hex string for consistency with ksck.
          tablet_summary.range_key_begin =
              HexEncodeToString(Slice(range_key_begin));
          VLOG(2) << "Tablet " << tablet_summary.id
                  << " range_key_begin: " << tablet_summary.range_key_begin;
        }
      }

      // Retrieve all replicas of the tablet.
      vector<ReplicaSummary> replicas;
      TabletLocationsPB locs_pb;
      CatalogManager::TSInfosDict ts_infos_dict;
      // GetTabletLocations() will fail if the catalog manager is not the leader.
      {
        CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager_);
        RETURN_NOT_OK(leaderlock.first_failed_status());
        // This will only return tablet replicas in the RUNNING state, and filter
        // to only retrieve voter replicas.
        RETURN_NOT_OK(catalog_manager_->GetTabletLocations(
            tablet_summary.id,
            ReplicaTypeFilter::VOTER_REPLICA,
            /*use_external_addr=*/false,
            &locs_pb,
            &ts_infos_dict,
            nullopt));
      }

      // Consensus state information is the same for all replicas of this tablet.
      const ConsensusStatePB& cstatepb = tablet_l.data().pb.consensus_state();
      vector<string> voters;
      vector<string> non_voters;
      for (const auto& peer : cstatepb.committed_config().peers()) {
        if (peer.member_type() == RaftPeerPB::VOTER) {
          voters.emplace_back(peer.permanent_uuid());
        } else if (peer.member_type() == RaftPeerPB::NON_VOTER) {
          non_voters.emplace_back(peer.permanent_uuid());
        }
      }

      int leaders_count = 0;

      // Build a summary for each replica of the tablet.
      // Make sure that the tserver the tablet is on is registered with the master
      // and is available for replica placement.
      // If not, return an error.
      for (const auto& r : locs_pb.interned_replicas()) {
        int index = r.ts_info_idx();
        const TSInfoPB& ts_info = *(ts_infos_dict.ts_info_pbs()[index]);
        ReplicaSummary rep;
        rep.ts_uuid = ts_info.permanent_uuid();
        if (!ContainsKey(tserver_uuids, rep.ts_uuid)) {
          return Status::NotFound(Substitute("tserver $0 not available for placement",
                                             rep.ts_uuid));
        }
        const auto& addr = ts_info.rpc_addresses(0);
        rep.ts_address = Substitute("$0:$1", addr.host(), addr.port());
        rep.is_leader = r.role() == RaftPeerPB::LEADER;
        if (rep.is_leader) {
          leaders_count++;
        }
        rep.is_voter = true;
        rep.ts_healthy = true;
        replicas.emplace_back(std::move(rep));
      }
      tablet_summary.replicas = std::move(replicas);

      // Determine if tablet is healthy enough for rebalancing.
      if (voters.size() < table_summary.replication_factor) {
        tablet_summary.result = HealthCheckResult::UNDER_REPLICATED;
      } else if (leaders_count != 1) {
        tablet_summary.result = HealthCheckResult::UNAVAILABLE;
      } else {
        tablet_summary.result = HealthCheckResult::HEALTHY;
      }
      tablet_summaries.emplace_back(std::move(tablet_summary));
    }
    table_summaries.emplace_back(std::move(table_summary));
  }

  if (!location) {
    // Information on the whole cluster.
    raw_info->tserver_summaries = std::move(tserver_summaries);
    raw_info->tablet_summaries = std::move(tablet_summaries);
    raw_info->table_summaries = std::move(table_summaries);
    return Status::OK();
  }

  // Information on the specified location only: filter out non-relevant info.
  const auto& location_str = *location;
  unordered_set<string> ts_ids_at_location;
  for (const auto& summary : tserver_summaries) {
    if (summary.ts_location == location_str) {
      raw_info->tserver_summaries.emplace_back(summary);
      InsertOrDie(&ts_ids_at_location, summary.uuid);
    }
  }
  unordered_set<string> table_ids_at_location;
  for (const auto& summary : tablet_summaries) {
    const auto& replicas = summary.replicas;
    vector<ReplicaSummary> replicas_at_location;
    replicas_at_location.reserve(replicas.size());
    for (const auto& replica : replicas) {
      if (ContainsKey(ts_ids_at_location, replica.ts_uuid)) {
        replicas_at_location.emplace_back(replica);
      }
    }
    if (!replicas_at_location.empty()) {
      table_ids_at_location.insert(summary.table_id);
      raw_info->tablet_summaries.emplace_back(summary);
      raw_info->tablet_summaries.back().replicas = std::move(replicas_at_location);
    }
  }
  for (const auto& summary : table_summaries) {
    if (ContainsKey(table_ids_at_location, summary.id)) {
      raw_info->table_summaries.emplace_back(summary);
    }
  }
  return Status::OK();
}

Status AutoRebalancerTask::CheckReplicaMovesCompleted(
    vector<rebalance::Rebalancer::ReplicaMove>* replica_moves) {

  bool move_is_complete = false;
  vector<int> indexes_to_remove;

  for (int i = 0; i < replica_moves->size(); ++i) {
    const rebalance::Rebalancer::ReplicaMove& move = (*replica_moves)[i];

    // Check if there was an error in checking move completion. If so, remove
    // the problematic one from 'replica_moves'.
    Status s = CheckMoveCompleted(move, &move_is_complete);
    if (!s.ok()) {
      // Move failed. Decrement the per-tserver counters.
      const auto& src_ts_uuid = move.ts_uuid_from;
      const auto& dst_ts_uuid = move.ts_uuid_to;

      if (moves_per_tserver_[src_ts_uuid] > 0) {
        moves_per_tserver_[src_ts_uuid]--;
      }
      if (!dst_ts_uuid.empty() && moves_per_tserver_[dst_ts_uuid] > 0) {
        moves_per_tserver_[dst_ts_uuid]--;
      }

      // Clear the replace marker so the master doesn't keep trying to replace
      // the replica (especially painful for leaders). Right after we scheduled
      // the move the leader may still be busy with its own config changes
      // (promoting the new NON_VOTER, stepping down because the source is
      // marked for replacement, or handing off leadership), so this cleanup can
      // race with that work and get rejected. Retry transient failures with a
      // short backoff. We can't count on the catalog manager's auto-replacement
      // to clean this up instead, since that needs a NON_VOTER promotion which
      // may never happen once the move is treated as failed.
      constexpr int kMaxClearAttempts = 3;
      Status clear_replace_status;
      for (int attempt = 1; attempt <= kMaxClearAttempts; ++attempt) {
        clear_replace_status = TryClearReplaceMarker(move);
        // OK includes the case where the marker was already cleared.
        if (clear_replace_status.ok()) break;
        // Nothing left to clear: the replica is gone from the config (NotFound)
        // or the marker was already cleared (InvalidArgument).
        if (clear_replace_status.IsNotFound() ||
            clear_replace_status.IsInvalidArgument()) {
          clear_replace_status = Status::OK();
          break;
        }
        if (attempt == kMaxClearAttempts) break;
        // Short inline backoff (100ms, then 200ms). Anything still failing
        // after that goes onto pending_replace_clears_ and is retried on the
        // next loop iteration; leader transfers and pending config changes
        // usually settle within one rebalancer interval, and spinning here
        // any longer would just hold up ExecuteMoves.
        const auto delay = MonoDelta::FromMilliseconds(100 * (1 << (attempt - 1)));
        if (shutdown_.WaitFor(delay)) {
          // Shutdown requested; bail without erasing so a rerun (if any)
          // sees the same state.
          return s;
        }
      }
      if (!clear_replace_status.ok()) {
        LOG(WARNING) << Substitute(
            "Removing replace marker failed after $0 inline attempts; "
            "will retry on next rebalancer loop iteration: $1",
            kMaxClearAttempts, clear_replace_status.message().ToString());
        pending_replace_clears_.emplace_back(move);
      }

      // Drop the failed move so rebalancing can make progress.
      replica_moves->erase(replica_moves->begin() + i);
      LOG(WARNING) << Substitute("Could not move replica: $0", s.ToString());
      return s;
    }
    // If the move was completed, remove it from 'replica_moves'.
    if (move_is_complete) {
      indexes_to_remove.emplace_back(i);
    }
  }

  // For all completed moves, decrement the per-tserver counters and remove from list.
  int num_indexes = static_cast<int>(indexes_to_remove.size());
  for (int j = num_indexes - 1; j >= 0; --j) {
    const auto& move = (*replica_moves)[indexes_to_remove[j]];
    const auto& src_ts_uuid = move.ts_uuid_from;
    const auto& dst_ts_uuid = move.ts_uuid_to;

    if (moves_per_tserver_[src_ts_uuid] > 0) {
      moves_per_tserver_[src_ts_uuid]--;
    }
    if (!dst_ts_uuid.empty() && moves_per_tserver_[dst_ts_uuid] > 0) {
      moves_per_tserver_[dst_ts_uuid]--;
    }

    VLOG(1) << Substitute(
        "Move completed: tablet $0 from $1 to $2 "
        "(src_moves=$3, dst_moves=$4)",
        move.tablet_uuid,
        src_ts_uuid,
        dst_ts_uuid,
        moves_per_tserver_[src_ts_uuid],
        dst_ts_uuid.empty() ? 0 : moves_per_tserver_[dst_ts_uuid]);

    replica_moves->erase(replica_moves->begin() + indexes_to_remove[j]);
  }

  return Status::OK();
}

Status AutoRebalancerTask::CheckMoveCompleted(
    const rebalance::Rebalancer::ReplicaMove& replica_move,
    bool* is_complete) {

  if (PREDICT_FALSE(FLAGS_auto_rebalancing_fail_moves_for_test)) {
    return Status::IllegalState("Injected artificial test failure.");
  }

  DCHECK(is_complete);
  *is_complete = false;

  const auto& tablet_uuid = replica_move.tablet_uuid;
  const auto& from_ts_uuid = replica_move.ts_uuid_from;
  const auto& to_ts_uuid = replica_move.ts_uuid_to;

  // Read consensus state from CatalogManager. CatalogManager's copy is
  // populated via tserver-to-master heartbeats and may lag the tserver's
  // actual Raft state by up to one heartbeat interval. We use the config
  // opid_index recorded before BulkChangeConfig was sent (stored in
  // replica_move.config_opid_idx) as a freshness gate: if CatalogManager
  // still shows the same opid_index, the heartbeat carrying the new config
  // hasn't arrived yet and we wait rather than act on stale data.
  ConsensusStatePB cstate;
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
    RETURN_NOT_OK(l.first_failed_status());
    RETURN_NOT_OK(catalog_manager_->GetTabletConsensusState(tablet_uuid, &cstate));
  }

  // config_opid_idx is always set for moves that reach this point: it is
  // populated in ExecuteMoves immediately after BulkChangeConfig succeeds,
  // and only successfully dispatched moves remain in replica_moves.
  DCHECK(replica_move.config_opid_idx);

  // If the opid_index hasn't advanced past what we saw before scheduling,
  // CatalogManager hasn't yet processed the heartbeat for the new config.
  // Return without acting so we retry on the next polling cycle.
  if (cstate.committed_config().opid_index() <= *replica_move.config_opid_idx) {
    return Status::OK();  // is_complete remains false; catalog not yet fresh
  }

  bool to_ts_uuid_in_config = false;
  bool to_ts_uuid_is_a_voter = false;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == to_ts_uuid) {
      to_ts_uuid_in_config = true;
      if (peer.member_type() == RaftPeerPB::VOTER) {
        to_ts_uuid_is_a_voter = true;
      }
      break;
    }
  }

  // Failure case: newly added replica is no longer in the config.
  if (!to_ts_uuid.empty() && !to_ts_uuid_in_config) {
    return Status::Incomplete(Substitute(
        "tablet $0, TS $1 -> TS $2 move failed, destination replica "
        "disappeared from tablet's Raft config: $3",
        tablet_uuid, from_ts_uuid, to_ts_uuid,
        SecureShortDebugString(cstate.committed_config())));
  }

  // Check if replica slated for removal is still in the config.
  bool from_ts_uuid_in_config = false;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == from_ts_uuid) {
      // Source replica must have the REPLACE attribute set.
      if (!peer.attrs().replace()) {
        return Status::IllegalState(Substitute(
            "$0: source replica $1 does not have REPLACE attribute set",
            tablet_uuid, from_ts_uuid));
      }
      // If the source is the current leader, step it down so the Raft group
      // can evict it once the destination is promoted to voter. It doesn't
      // make sense to step down before that promotion, since doing so only
      // delays the process and the stepped-down leader will not be evicted
      // until the newly added replica is promoted to voter.
      if (from_ts_uuid == cstate.leader_uuid()) {
        // Re-read the leader host/port from the catalog for the step-down RPC.
        string leader_uuid;
        HostPort leader_hp;
        RETURN_NOT_OK(GetTabletLeader(tablet_uuid, &leader_uuid, &leader_hp));
        // Only proceed if the catalog-reported leader still matches.
        if (leader_uuid == from_ts_uuid) {
          shared_ptr<TSDescriptor> desc;
          if (!ts_manager_->LookupTSByUUID(leader_uuid, &desc)) {
            return Status::NotFound("Could not find leader replica's tserver");
          }
          shared_ptr<ConsensusServiceProxy> proxy;
          RETURN_NOT_OK(desc->GetConsensusProxy(messenger_, &proxy));
          LeaderStepDownRequestPB req;
          LeaderStepDownResponsePB resp;
          RpcController rpc;
          req.set_dest_uuid(from_ts_uuid);
          req.set_tablet_id(tablet_uuid);
          req.set_mode(LeaderStepDownMode::GRACEFUL);
          rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_rpc_timeout_seconds));
          RETURN_NOT_OK(proxy->LeaderStepDown(req, &resp, &rpc));
          if (resp.has_error()) {
            return StatusFromPB(resp.error().status());
          }
        }
      }

      from_ts_uuid_in_config = true;
      break;
    }
  }

  if (!from_ts_uuid_in_config &&
      (to_ts_uuid_is_a_voter || to_ts_uuid.empty())) {
    *is_complete = true;
  }

  return Status::OK();
}

Status AutoRebalancerTask::TryClearReplaceMarker(
    const Rebalancer::ReplicaMove& move) {
  string leader_uuid;
  HostPort leader_hp;
  // Resolving the leader also re-checks our own catalog leadership (it takes the
  // leader lock), so if we've lost it here, or there's no tablet leader yet, we
  // bail out and the caller leaves the marker pending.
  RETURN_NOT_OK(GetTabletLeader(move.tablet_uuid, &leader_uuid, &leader_hp));
  vector<Sockaddr> resolved;
  RETURN_NOT_OK(leader_hp.ResolveAddresses(&resolved));
  ConsensusServiceProxy proxy(messenger_, resolved[0], leader_hp.host());

  // Try to clear only the marker our own move set, and only if nobody has
  // touched the config since. Other actors (an operator running
  // `kudu cluster rebalance`, or the catalog re-replicating an under-replicated
  // tablet) can set 'replace' on the same replica for good reasons, and clearing
  // that out from under them would interfere with their work and move replicas
  // around for no reason. So read the current config and CAS the clear against
  // its opid_index below. We read from the leader rather than the catalog, whose
  // view lags by a heartbeat and would give a stale opid.
  ConsensusStatePB cstate;
  {
    GetConsensusStateRequestPB req;
    GetConsensusStateResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_rpc_timeout_seconds));
    req.set_dest_uuid(leader_uuid);
    req.add_tablet_ids(move.tablet_uuid);
    RETURN_NOT_OK(proxy.GetConsensusState(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    if (resp.tablets_size() != 1 || !resp.tablets(0).has_cstate()) {
      // Leader didn't return this tablet (yet); treat as transient and retry.
      return Status::ServiceUnavailable(Substitute(
          "leader $0 did not report consensus state for tablet $1",
          leader_uuid, move.tablet_uuid));
    }
    cstate = resp.tablets(0).cstate();
  }

  const RaftPeerPB* src_peer = nullptr;
  for (const auto& peer : cstate.committed_config().peers()) {
    if (peer.permanent_uuid() == move.ts_uuid_from) {
      src_peer = &peer;
      break;
    }
  }
  // Replica already gone from the config; nothing to clear.
  if (!src_peer) {
    return Status::NotFound(Substitute(
        "replica $0 is no longer in tablet $1's config",
        move.ts_uuid_from, move.tablet_uuid));
  }
  // Marker already cleared, so skip the no-op change and leave the config alone.
  if (!src_peer->attrs().replace()) {
    return Status::OK();
  }

  BulkChangeConfigRequestPB req;
  auto* modify_peer = req.add_config_changes();
  modify_peer->set_type(MODIFY_PEER);
  *modify_peer->mutable_peer()->mutable_permanent_uuid() = move.ts_uuid_from;
  modify_peer->mutable_peer()->mutable_attrs()->set_replace(false);
  req.set_dest_uuid(leader_uuid);
  req.set_tablet_id(move.tablet_uuid);
  // CAS against the config we just read: if anyone changes it before this lands,
  // the leader rejects us (CAS_FAILED) and we retry fresh instead of clobbering
  // their change.
  //
  // The CAS narrows this window but does not fully close it. It proves the
  // config did not change between the read above and this request, but it
  // cannot prove the 'replace' marker we clear is the one our failed move set:
  // another actor could have set 'replace' on this same replica at the same
  // opid. That is more likely when we reach here from ProcessPendingReplaceClears()
  // on a later RunLoop iteration, where more time has passed since the move
  // failed. We accept this: a spurious clear cannot leave the tablet
  // under-replicated forever, since the catalog re-replicates genuinely
  // under-replicated tablets on its own, independent of the 'replace' attribute.
  // At worst it delays a proactive replacement, which is retried.
  req.set_cas_config_opid_index(cstate.committed_config().opid_index());

  ChangeConfigResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_rpc_timeout_seconds));
  RETURN_NOT_OK(proxy.BulkChangeConfig(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

void AutoRebalancerTask::ProcessPendingReplaceClears() {
  if (pending_replace_clears_.empty()) {
    return;
  }
  // Without catalog leadership we can't resolve tablet leaders, and any
  // marker we left behind is now the new leader's responsibility.
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
    if (!l.first_failed_status().ok()) {
      return;
    }
  }
  vector<Rebalancer::ReplicaMove> still_pending;
  still_pending.reserve(pending_replace_clears_.size());
  for (const auto& move : pending_replace_clears_) {
    Status s = TryClearReplaceMarker(move);
    if (s.ok() || s.IsNotFound() || s.IsInvalidArgument()) {
      // Nothing left to do: cleared (OK, including the already-clear case) or
      // the replica is gone from the config (NotFound).
      continue;
    }
    still_pending.emplace_back(move);
  }
  pending_replace_clears_ = std::move(still_pending);
}

} // namespace master
} // namespace kudu
