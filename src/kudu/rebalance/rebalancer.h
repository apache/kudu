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
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/rebalance/cluster_status.h"
#include "kudu/util/status.h"


namespace kudu {
namespace rebalance {

struct ClusterInfo;
struct TableReplicaMove;
struct TabletsPlacementInfo;

// Sub-set of fields from ClusterResult which are relevant to the rebalancing.
struct ClusterRawInfo {
  std::vector<cluster_summary::ServerHealthSummary> tserver_summaries;
  std::vector<cluster_summary::TableSummary> table_summaries;
  std::vector<cluster_summary::TabletSummary> tablet_summaries;
};

// A class implementing logic for Kudu cluster rebalancing.
// A Rebalancer object encapsulates structs with cluster information
// and functions needed to calculate and implement replica moves.
class Rebalancer {
 public:
  // Configuration parameters for the rebalancer aggregated into a struct.
  struct Config {
    static constexpr double kLoadImbalanceThreshold = 1.0;

    // NOLINTNEXTLINE(google-explicit-constructor)
    Config(std::vector<std::string> ignored_tservers_param = {},
           std::vector<std::string> master_addresses = {},
           std::vector<std::string> table_filters = {},
           size_t max_moves_per_server = 5,
           size_t max_staleness_interval_sec = 300,
           int64_t max_run_time_sec = 0,
           bool move_rf1_replicas = false,
           bool output_replica_distribution_details = false,
           bool run_policy_fixer = true,
           bool run_cross_location_rebalancing = true,
           bool run_intra_location_rebalancing = true,
           double load_imbalance_threshold = kLoadImbalanceThreshold);

    // UUIDs of ignored servers. If empty, allow to run the
    // rebalancing only when all tablet servers in cluster are healthy.
    // If not empty, allow to run the rebalancing when servers in
    // ignored_tservers are unhealthy.
    std::unordered_set<std::string> ignored_tservers;

    // Kudu masters' RPC endpoints.
    std::vector<std::string> master_addresses;

    // Names of tables to balance. If empty, every table and the whole cluster
    // will be balanced.
    std::vector<std::string> table_filters;

    // Maximum number of move operations to run concurrently on one server.
    // An 'operation on a server' means a move operation where either source or
    // destination replica is located on the specified server.
    size_t max_moves_per_server;

    // Maximum duration of the 'staleness' interval, when the rebalancer cannot
    // make any progress in scheduling new moves and no prior scheduled moves
    // are left, even if re-synchronizing against the cluster's state again and
    // again. Such a staleness usually happens in case of a persistent problem
    // with the cluster or when some unexpected concurrent activity is present
    // (such as automatic recovery of failed replicas, etc.).
    size_t max_staleness_interval_sec;

    // Maximum run time, in seconds.
    int64_t max_run_time_sec;

    // Whether to move replicas of tablets with replication factor of one.
    bool move_rf1_replicas;

    // Whether Rebalancer::PrintStats() should output per-table and per-server
    // replica distribution details.
    bool output_replica_distribution_details;

    // In case of multi-location cluster, whether to detect and fix placement
    // policy violations. Fixing placement policy violations involves moving
    // tablet replicas across different locations in the cluster.
    // This setting is applicable to multi-location clusters only.
    bool run_policy_fixer;

    // In case of multi-location cluster, whether to move tablet replicas
    // between locations in attempt to spread tablet replicas among location
    // evenly (equalizing loads of locations throughout the cluster).
    // This setting is applicable to multi-location clusters only.
    bool run_cross_location_rebalancing;

    // In case of multi-location cluster, whether to rebalance tablet replica
    // distribution within each location.
    // This setting is applicable to multi-location clusters only.
    bool run_intra_location_rebalancing;

    // The per-table location load imbalance threshold for the cross-location
    // balancing algorithm.
    double load_imbalance_threshold;
  };

  // Represents a concrete move of a replica from one tablet server to another.
  // Formed logically from a TableReplicaMove by specifying a tablet for the move.
  // Originally from "tools/rebalancer.h"
  struct ReplicaMove {
    std::string tablet_uuid;
    std::string ts_uuid_from;
    std::string ts_uuid_to;
    boost::optional<int64_t> config_opid_idx; // for CAS-enabled Raft changes
  };

  enum class RunStatus {
    UNKNOWN,
    CLUSTER_IS_BALANCED,
    TIMED_OUT,
  };

  // A helper type: key is tablet UUID which corresponds to value.tablet_uuid.
  // Originally from "tools/rebalancer.h"
  typedef std::unordered_map<std::string, ReplicaMove> MovesInProgress;

  explicit Rebalancer(Config config);

 protected:
  // Helper class to find and schedule next available rebalancing move operation
  // and track already scheduled ones.
  class Runner {
   public:
    virtual ~Runner() = default;

    // Initialize instance of Runner so it can run against Kudu cluster with
    // the 'master_addresses' RPC endpoints.
    virtual Status Init(std::vector<std::string> master_addresses) = 0;

    // Load information on the prescribed replica movement operations. Also,
    // populate helper containers and other auxiliary run-time structures
    // used by ScheduleNextMove(). This method is called with every batch
    // of move operations output by the rebalancing algorithm once previously
    // loaded moves have been scheduled.
    virtual void LoadMoves(std::vector<Rebalancer::ReplicaMove> replica_moves) = 0;

    // Schedule next replica move. Returns 'true' if replica move operation
    // has been scheduled successfully; otherwise returns 'false' and sets
    // the 'has_errors' and 'timed_out' parameters accordingly.
    virtual bool ScheduleNextMove(bool* has_errors, bool* timed_out) = 0;

    // Update statuses and auxiliary information on in-progress replica move
    // operations. The 'timed_out' parameter is set to 'true' if not all
    // in-progress operations were processed by the deadline specified by
    // the 'deadline_' member field. The method returns 'true' if it's necessary
    // to clear the state of the in-progress operations, i.e. 'forget'
    // those, starting from a clean state.
    virtual bool UpdateMovesInProgressStatus(bool* has_errors, bool* timed_out) = 0;

    virtual Status GetNextMoves(bool* has_moves) = 0;

    virtual uint32_t moves_count() const = 0;
  }; // class Runner

  friend class KsckResultsToClusterBalanceInfoTest;

  // Given high-level move-some-tablet-replica-for-a-table information from the
  // rebalancing algorithm, find appropriate tablet replicas to move between the
  // specified tablet servers. The set of result tablet UUIDs is output
  // into the 'tablet_ids' container (note: the container is first cleared).
  // The source and destination replicas are determined by the elements of the
  // 'tablet_ids' container and tablet server UUIDs TableReplicaMove::from and
  // TableReplica::to correspondingly. If no suitable tablet replicas are found,
  // 'tablet_ids' will be empty with the result status of Status::OK().
  static Status FindReplicas(const TableReplicaMove& move,
                             const ClusterRawInfo& raw_info,
                             std::vector<std::string>* tablet_ids);

  // Filter move operations in 'replica_moves': remove all operations that would
  // involve moving replicas of tablets which are in 'scheduled_moves'. The
  // 'replica_moves' cannot be null.
  static void FilterMoves(const MovesInProgress& scheduled_moves,
                          std::vector<ReplicaMove>* replica_moves);

  // Filter the list of candidate tablets to make sure the location
  // of the destination server would not contain the majority of replicas
  // after the move. The 'tablet_ids' is an in-out parameter.
  static Status FilterCrossLocationTabletCandidates(
      const std::unordered_map<std::string, std::string>& location_by_ts_id,
      const TabletsPlacementInfo& placement_info,
      const TableReplicaMove& move,
      std::vector<std::string>* tablet_ids);

  // Convert the 'raw' information about the cluster into information suitable
  // for the input of the high-level rebalancing algorithm.
  // The 'moves_in_progress' parameter contains information on the replica moves
  // which have been scheduled by a caller and still in progress: those are
  // considered as successfully completed and applied to the 'raw_info' when
  // building ClusterBalanceInfo for the specified 'raw_info' input. The idea
  // is to prevent the algorithm outputting the same moves again while some
  // of the moves recommended at prior steps are still in progress.
  // The result cluster balance information is output into the 'info' parameter.
  // The 'info' output parameter cannot be null.
  Status BuildClusterInfo(const ClusterRawInfo& raw_info,
                          const MovesInProgress& moves_in_progress,
                          ClusterInfo* info) const;

  // Configuration for the rebalancer.
  const Config config_;
};

} // namespace rebalance
} // namespace kudu
