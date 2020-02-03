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
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
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
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
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

namespace kudu {

namespace master {

AutoRebalancerTask::AutoRebalancerTask(CatalogManager* catalog_manager,
                                       TSManager* ts_manager)
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
      FLAGS_auto_rebalancing_load_imbalance_threshold))),
      random_generator_(random_device_()),
      number_of_loop_iterations_for_test_(0),
      moves_scheduled_this_round_for_test_(0) {
}

AutoRebalancerTask::~AutoRebalancerTask() {
  if (thread_) {
    Shutdown();
  }
}

Status AutoRebalancerTask::Init() {
  DCHECK(!thread_) << "AutoRebalancerTask is already initialized";
  RETURN_NOT_OK(MessengerBuilder("auto-rebalancer").Build(&messenger_));
  return kudu::Thread::Create("catalog manager", "auto-rebalancer",
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

    // If catalog manager isn't initialized or isn't the leader, don't do rebalancing.
    // Putting the auto-rebalancer to sleep shouldn't affect the master's ability
    // to become the leader. When the thread wakes up and discovers it is now
    // the leader, then it can begin auto-rebalancing.
    {
      CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
      if (!l.first_failed_status().ok()) {
        moves_scheduled_this_round_for_test_ = 0;
        continue;
      }
    }

    number_of_loop_iterations_for_test_++;

    // Structs to hold information about the cluster's status.
    ClusterRawInfo raw_info;
    ClusterInfo cluster_info;
    TabletsPlacementInfo placement_info;

    Status s = BuildClusterRawInfo(/*location*/boost::none, &raw_info);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Could not retrieve cluster info: $0", s.ToString());
      continue;
    }

    // There should be no in-flight moves in progress, because this loop waits
    // for scheduled moves to complete before continuing to the next iteration.
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

    // With the current synchronous implementation, verify that any moves
    // scheduled in the previous iteration completed.
    // The container 'replica_moves' should be empty.
    DCHECK_EQ(0, replica_moves.size());

    // For simplicity, if there are policy violations, only move replicas
    // to satisfy placement policy in this loop iteration.
    s = GetMoves(raw_info, cluster_info.locality, placement_info, &replica_moves);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("could not retrieve auto-rebalancing replica moves: $0",
                                 s.ToString());
      continue;
    }

    WARN_NOT_OK(ExecuteMoves(replica_moves),
                "failed to send replica move request");

    moves_scheduled_this_round_for_test_ = replica_moves.size();

    do {
      if (shutdown_.WaitFor(MonoDelta::FromSeconds(
            FLAGS_auto_rebalancing_wait_for_replica_moves_seconds))) {
        return;
      }
      WARN_NOT_OK(CheckReplicaMovesCompleted(&replica_moves),
                  "scheduled replica move failed to complete");
    } while (!replica_moves.empty());
  } // end while
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
    rebalance::TwoDimensionalGreedyAlgo algo;
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
    rebalance::LocationBalancingAlgo algo(FLAGS_auto_rebalancing_load_imbalance_threshold);
    RETURN_NOT_OK(GetMovesUsingRebalancingAlgo(
        raw_info, &algo, CrossLocations::YES, &rep_moves));
  }

  // Perform intra-location rebalancing.
  if (config_.run_intra_location_rebalancing) {
    rebalance::TwoDimensionalGreedyAlgo algo;
    for (const auto& elem : ts_id_by_location) {
      const auto& location = elem.first;
      ClusterRawInfo location_raw_info;
      BuildClusterRawInfo(location, &location_raw_info);
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

  auto num_tservers = raw_info.tserver_summaries.size();
  auto max_moves = FLAGS_auto_rebalancing_max_moves_per_server * num_tservers;
  max_moves -= replica_moves->size();
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
    vector<string> tablet_ids;
    Rebalancer::FindReplicas(move, raw_info, &tablet_ids);
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
                                      &tablets_in_move, &rep_moves));
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
        &locs_pb,
        &ts_infos_dict,
        boost::none));
  }
  for (const auto& r : locs_pb.interned_replicas()) {
    if (r.role() == RaftPeerPB::LEADER) {
      int index = r.ts_info_idx();
      const TSInfoPB ts_info = *(ts_infos_dict.ts_info_pbs[index]);
      *leader_uuid = ts_info.permanent_uuid();
      const auto& addr = ts_info.rpc_addresses(0);
      leader_hp->set_host(addr.host());
      leader_hp->set_port(addr.port());
      return Status::OK();
    }
  }
  return Status::NotFound(Substitute("Couldn't find leader for tablet $0", tablet_id));
}

// TODO(hannah.nguyen): remove moves that fail to be scheduled from 'replica_moves'.
Status AutoRebalancerTask::ExecuteMoves(
    const vector<Rebalancer::ReplicaMove>& replica_moves) {

  for (const auto& move_info : replica_moves) {
    const auto& tablet_id = move_info.tablet_uuid;
    const auto& src_ts_uuid = move_info.ts_uuid_from;
    const auto& dst_ts_uuid = move_info.ts_uuid_to;

    string leader_uuid;
    HostPort leader_hp;
    RETURN_NOT_OK(GetTabletLeader(tablet_id, &leader_uuid, &leader_hp));

    // Mark the replica to be replaced.
    BulkChangeConfigRequestPB req;
    auto* replace = req.add_config_changes();
    replace->set_type(MODIFY_PEER);
    *replace->mutable_peer()->mutable_permanent_uuid() = src_ts_uuid;
    replace->mutable_peer()->mutable_attrs()->set_replace(true);

    shared_ptr<TSDescriptor> desc;
    if (!ts_manager_->LookupTSByUUID(leader_uuid, &desc)) {
      return Status::NotFound(
          Substitute("Couldn't find leader replica's tserver $0", leader_uuid));
    }

    // Set up the proxy to communicate with the leader replica's tserver.
    shared_ptr<ConsensusServiceProxy> proxy;
    HostPort hp;
    RETURN_NOT_OK(hp.ParseString(leader_uuid, leader_hp.port()));
    vector<Sockaddr> resolved;
    RETURN_NOT_OK(hp.ResolveAddresses(&resolved));
    proxy.reset(new ConsensusServiceProxy(messenger_, resolved[0], hp.host()));

    // Find the specified destination tserver, if load rebalancing.
    // Otherwise, replica moves to fix placement policy violations do not have
    // destination tservers specified, i.e. dst_ts_uuid will be empty if
    // there are placement policy violations.
    if (!dst_ts_uuid.empty()) {
      // Verify that the destination tserver exists.
      shared_ptr<TSDescriptor> dest_desc;
      if (!ts_manager_->LookupTSByUUID(dst_ts_uuid, &dest_desc)) {
        return Status::NotFound("Could not find destination tserver");
      }

      auto* change = req.add_config_changes();
      change->set_type(ADD_PEER);
      *change->mutable_peer()->mutable_permanent_uuid() = dst_ts_uuid;
      change->mutable_peer()->set_member_type(RaftPeerPB::NON_VOTER);
      change->mutable_peer()->mutable_attrs()->set_promote(true);
      RETURN_NOT_OK(
          HostPortToPB(hp, change->mutable_peer()->mutable_last_known_addr()));
    }

    // Request movement or replacement of the replica.
    ChangeConfigResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_rpc_timeout_seconds));
    req.set_dest_uuid(leader_uuid);
    req.set_tablet_id(tablet_id);

    RETURN_NOT_OK(proxy->BulkChangeConfig(req, &resp, &rpc));
    if (resp.has_error()) return StatusFromPB(resp.error().status());
  }

  return Status::OK();
}

Status AutoRebalancerTask::BuildClusterRawInfo(
    const boost::optional<string>& location,
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
    tserver_summaries.push_back(std::move(summary));
  }

  vector<scoped_refptr<TableInfo>> table_infos;

  {
    CatalogManager::ScopedLeaderSharedLock leader_lock(catalog_manager_);
    RETURN_NOT_OK(leader_lock.first_failed_status());
    RETURN_NOT_OK(catalog_manager_->GetAllTables(&table_infos));
  }

  table_summaries.reserve(table_infos.size());

  for (const auto& table : table_infos) {
    TableMetadataLock table_l(table.get(), LockMode::READ);

    TableSummary table_summary;
    table_summary.id = table->id();
    const SysTablesEntryPB& table_data = table->metadata().state().pb;
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
            &locs_pb,
            &ts_infos_dict,
            boost::none));
      }

      // Consensus state information is the same for all replicas of this tablet.
      const ConsensusStatePB& cstatepb = tablet_l.data().pb.consensus_state();
      vector<string> voters;
      vector<string> non_voters;
      for (const auto& peer : cstatepb.committed_config().peers()) {
        if (peer.member_type() == RaftPeerPB::VOTER) {
          voters.push_back(peer.permanent_uuid());
        } else if (peer.member_type() == RaftPeerPB::NON_VOTER) {
          non_voters.push_back(peer.permanent_uuid());
        }
      }

      int leaders_count = 0;

      // Build a summary for each replica of the tablet.
      // Make sure that the tserver the tablet is on is registered with the master
      // and is available for replica placement.
      // If not, return an error.
      for (const auto& r : locs_pb.interned_replicas()) {
        int index = r.ts_info_idx();
        const TSInfoPB& ts_info = *(ts_infos_dict.ts_info_pbs[index]);
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
        replicas.push_back(rep);
      }

      tablet_summary.replicas.swap(replicas);

      // Determine if tablet is healthy enough for rebalancing.
      if (voters.size() < table_summary.replication_factor) {
        tablet_summary.result = HealthCheckResult::UNDER_REPLICATED;
      } else if (leaders_count != 1) {
        tablet_summary.result = HealthCheckResult::UNAVAILABLE;
      } else {
        tablet_summary.result = HealthCheckResult::HEALTHY;
      }

      tablet_summaries.push_back(std::move(tablet_summary));
    }

    table_summaries.push_back(std::move(table_summary));
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
      raw_info->tserver_summaries.push_back(summary);
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
        replicas_at_location.push_back(replica);
      }
    }
    if (!replicas_at_location.empty()) {
      table_ids_at_location.insert(summary.table_id);
      raw_info->tablet_summaries.push_back(summary);
      raw_info->tablet_summaries.back().replicas = std::move(replicas_at_location);
    }
  }

  for (const auto& summary : table_summaries) {
    if (ContainsKey(table_ids_at_location, summary.id)) {
      raw_info->table_summaries.push_back(summary);
    }
  }

  return Status::OK();
}

Status AutoRebalancerTask::CheckReplicaMovesCompleted(
    vector<rebalance::Rebalancer::ReplicaMove>* replica_moves) {

  bool move_is_complete;
  vector<int> indexes_to_remove;

  for (int i = 0; i < replica_moves->size(); ++i) {
    const rebalance::Rebalancer::ReplicaMove& move = (*replica_moves)[i];

    // Check if there was an error in checking move completion.
    // If so, moves are erased from 'replica_moves' other than this problematic one.
    Status s = CheckMoveCompleted(move, &move_is_complete);
    if (!s.ok()) {
      replica_moves->erase(replica_moves->begin() + i);
      LOG(WARNING) << Substitute("Could not move replica: $0", s.ToString());
      return s;
    }

    // If move was completed, remove it from 'replica_moves'.
    if (move_is_complete) {
      indexes_to_remove.push_back(i);
    }
  }

  int num_indexes = static_cast<int>(indexes_to_remove.size());
  for (int j = num_indexes - 1; j >= 0; --j) {
    replica_moves->erase(replica_moves->begin() + indexes_to_remove[j]);
  }

  return Status::OK();
}

// TODO(hannah.nguyen): Retrieve consensus state information from the CatalogManager instead.
// Currently, this implementation mirrors CheckCompleteMove() in tools/tool_replica_util.
Status AutoRebalancerTask::CheckMoveCompleted(
    const rebalance::Rebalancer::ReplicaMove& replica_move,
    bool* is_complete) {

  DCHECK(is_complete);
  *is_complete = false;

  const auto& tablet_uuid = replica_move.tablet_uuid;
  const auto& from_ts_uuid = replica_move.ts_uuid_from;
  const auto& to_ts_uuid = replica_move.ts_uuid_to;

  // Get the latest leader info. This may change later.
  string orig_leader_uuid;
  HostPort orig_leader_hp;
  RETURN_NOT_OK(GetTabletLeader(tablet_uuid, &orig_leader_uuid, &orig_leader_hp));
  shared_ptr<TSDescriptor> desc;
  if (!ts_manager_->LookupTSByUUID(orig_leader_uuid, &desc)) {
    return Status::NotFound("Could not find leader replica's tserver");
  }
  shared_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(desc->GetConsensusProxy(messenger_, &proxy));

  // Check if replica at 'to_ts_uuid' is in the config, and if it has been
  // promoted to voter.
  ConsensusStatePB cstate;
  GetConsensusStateRequestPB req;
  GetConsensusStateResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_rpc_timeout_seconds));
  req.set_dest_uuid(orig_leader_uuid);
  req.add_tablet_ids(tablet_uuid);
  RETURN_NOT_OK(proxy->GetConsensusState(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.tablets_size() == 0) {
    return Status::NotFound("tablet not found:", tablet_uuid);
  }
  DCHECK_EQ(1, resp.tablets_size());
  cstate = resp.tablets(0).cstate();

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
        "tablet $0, TS $1 -> TS $2 move failed, target replica disappeared",
        tablet_uuid, from_ts_uuid, to_ts_uuid));
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
      // Replica to be removed is the leader.
      // - It's possible that leadership changed and 'orig_leader_uuid' is not
      //   the leader's UUID by the time 'cstate' was collected. Let's
      //   cross-reference the two sources and only act if they agree.
      // - It doesn't make sense to have the leader step down if the newly-added
      //   replica hasn't been promoted to a voter yet, since changing
      //   leadership can only delay that process and the stepped-down leader
      //   replica will not be evicted until the newly added replica is promoted
      //   to voter.
      if (orig_leader_uuid == from_ts_uuid && orig_leader_uuid == cstate.leader_uuid()) {
        LeaderStepDownRequestPB req;
        LeaderStepDownResponsePB resp;
        RpcController rpc;
        req.set_dest_uuid(orig_leader_uuid);
        req.set_tablet_id(tablet_uuid);
        req.set_mode(LeaderStepDownMode::GRACEFUL);
        rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_rebalancing_rpc_timeout_seconds));
        RETURN_NOT_OK(proxy->LeaderStepDown(req, &resp, &rpc));
        if (resp.has_error()) {
          return StatusFromPB(resp.error().status());
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

} // namespace master
} // namespace kudu
