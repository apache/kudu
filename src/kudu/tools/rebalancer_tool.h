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
#include <iosfwd>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/client/shared_ptr.h"
#include "kudu/rebalance/rebalance_algo.h"
#include "kudu/rebalance/rebalancer.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

namespace client {
class KuduClient;
}

namespace tools {

class Ksck;
struct KsckResults;

// A class implementing logic for Kudu cluster rebalancing.
// This class inherits from rebalance::Rebalancer but also
// implements additional functions to print cluster balance
// information.
class RebalancerTool : public rebalance::Rebalancer {
 public:

  // Create Rebalancer object with the specified configuration.
  explicit RebalancerTool(const Config& config);

  // Print the stats on the cluster balance information into the 'out' stream.
  Status PrintStats(std::ostream& out);

  // Run the rebalancing: start the process and return once the balancing
  // criteria are satisfied or if an error occurs. The number of attempted
  // moves is output into the 'moves_count' parameter (if the parameter is
  // not null). The 'result_status' output parameter cannot be null.
  Status Run(RunStatus* result_status, size_t* moves_count = nullptr);

 private:
  // Common base for a few Runner implementations.
  class BaseRunner : public Runner {
   public:
    BaseRunner(RebalancerTool* rebalancer,
               std::unordered_set<std::string> ignored_tservers,
               size_t max_moves_per_server,
               boost::optional<MonoTime> deadline);

    Status Init(std::vector<std::string> master_addresses) override;

    Status GetNextMoves(bool* has_moves) override;

    uint32_t moves_count() const override {
      return moves_count_;
    }

   protected:
    // Get next batch of replica moves from the rebalancing algorithm.
    // Essentially, it runs ksck against the cluster and feeds the data into the
    // rebalancing algorithm along with the information on currently pending
    // replica movement operations. The information returned by the high-level
    // rebalancing algorithm is translated into particular replica movement
    // instructions, which are used to populate the 'replica_moves' parameter
    // (the container is cleared first).
    virtual Status GetNextMovesImpl(std::vector<Rebalancer::ReplicaMove>* moves) = 0;

    // Update the helper containers once a scheduled operation is complete
    // (i.e. succeeded or failed).
    void UpdateOnMoveCompleted(const std::string& ts_uuid);

    // A pointer to the Rebalancer object.
    RebalancerTool* rebalancer_;

    // A set of ignored tablet server UUIDs.
    const std::unordered_set<std::string> ignored_tservers_;

    // Maximum allowed number of move operations per server. For a move
    // operation, a source replica adds +1 at the source server and the target
    // replica adds +1 at the destination server.
    const size_t max_moves_per_server_;

    // Deadline for the activity performed by the Runner class in
    // ScheduleNextMoves() and UpdateMovesInProgressStatus() methods.
    const boost::optional<MonoTime> deadline_;

    // Client object to make queries to Kudu masters for various auxiliary info
    // while scheduling move operations and monitoring their status.
    client::sp::shared_ptr<client::KuduClient> client_;

    // Information on scheduled replica movement operations; keys are
    // tablet UUIDs, values are ReplicaMove structures.
    Rebalancer::MovesInProgress scheduled_moves_;

    // Number of successfully completed replica moves operations.
    uint32_t moves_count_;

    // Kudu cluster RPC end-points.
    std::vector<std::string> master_addresses_;

    // Mapping 'tserver UUID' --> 'scheduled move operations count'.
    std::unordered_map<std::string, int32_t> op_count_per_ts_;

    // Mapping 'scheduled move operations count' --> 'tserver UUID'. That's
    // just reversed 'op_count_per_ts_'.
    std::multimap<int32_t, std::string> ts_per_op_count_;
  }; // class BaseRunner

  // Runner that leverages RebalancingAlgo interface for rebalancing.
  class AlgoBasedRunner : public BaseRunner {
   public:
    // The 'ignored_tservers' specifies tablet servers that could be
    // ignored by rebalancer.
    // The 'max_moves_per_server' specifies the maximum number of operations
    // per tablet server (both the source and the destination are counted in).
    // The 'deadline' specifies the deadline for the run, 'boost::none'
    // if no timeout is set. If 'location' is boost::none, rebalance across
    // locations.
    AlgoBasedRunner(RebalancerTool* rebalancer,
                    std::unordered_set<std::string> ignored_tservers,
                    size_t max_moves_per_server,
                    boost::optional<MonoTime> deadline);

    Status Init(std::vector<std::string> master_addresses) override;

    void LoadMoves(std::vector<Rebalancer::ReplicaMove> replica_moves) override;

    bool ScheduleNextMove(bool* has_errors, bool* timed_out) override;

    bool UpdateMovesInProgressStatus(bool* has_errors,
                                     bool* timed_out,
                                     bool* has_pending_moves) override;

    // Get the cluster location the runner is slated to run/running at.
    // 'boost::none' means all the cluster.
    virtual const boost::optional<std::string>& location() const = 0;

    // Rebalancing algorithm that running uses to find replica moves.
    virtual rebalance::RebalancingAlgo* algorithm() = 0;

   protected:
    Status GetNextMovesImpl(std::vector<Rebalancer::ReplicaMove>* replica_moves) override;

    // Using the helper containers src_op_indices_ and dst_op_indices_,
    // find the index of the most optimal replica movement operation
    // and output the index into the 'op_idx' parameter.
    bool FindNextMove(size_t* op_idx);

    // Update the helper containers once a move operation has been scheduled.
    void UpdateOnMoveScheduled(size_t idx,
                               const std::string& tablet_uuid,
                               const std::string& src_ts_uuid,
                               const std::string& dst_ts_uuid,
                               bool is_success);

    // Auxiliary method used by UpdateOnMoveScheduled() implementation.
    void UpdateOnMoveScheduledImpl(
        size_t idx,
        const std::string& ts_uuid,
        bool is_success,
        std::unordered_map<std::string, std::set<size_t>>* op_indices);

    // The moves to schedule.
    std::vector<Rebalancer::ReplicaMove> replica_moves_;

    // Mapping 'tserver UUID' --> 'indices of move operations having the
    // tserver UUID (i.e. the key) as the source of the move operation'.
    std::unordered_map<std::string, std::set<size_t>> src_op_indices_;

    // Mapping 'tserver UUID' --> 'indices of move operations having the
    // tserver UUID (i.e. the key) as the destination of the move operation'.
    std::unordered_map<std::string, std::set<size_t>> dst_op_indices_;

    // Random device and generator for selecting among multiple choices, when
    // appropriate.
    std::random_device random_device_;
    std::mt19937 random_generator_;
  }; // class AlgoBasedRunner

  class IntraLocationRunner : public AlgoBasedRunner {
   public:
    // The 'ignored_tservers' specifies tablet servers that could be
    // ignored by rebalancer.
    // The 'max_moves_per_server' specifies the maximum number of operations
    // per tablet server (both the source and the destination are counted in).
    // The 'deadline' specifies the deadline for the run, 'boost::none'
    // if no timeout is set. In case of non-location aware cluster or if there
    // is just one location defined in the whole cluster, the whole cluster will
    // be rebalanced.
    IntraLocationRunner(RebalancerTool* rebalancer,
                        std::unordered_set<std::string> ignored_tservers,
                        size_t max_moves_per_server,
                        boost::optional<MonoTime> deadline,
                        std::string location);

    rebalance::RebalancingAlgo* algorithm() override {
      return &algorithm_;
    }

    const boost::optional<std::string>& location() const override {
      return location_;
    }

   private:
    const boost::optional<std::string> location_;

    // An instance of the balancing algorithm.
    rebalance::TwoDimensionalGreedyAlgo algorithm_;
  };

  class CrossLocationRunner : public AlgoBasedRunner {
   public:
    // The 'ignored_tservers' specifies tablet servers that could be
    // ignored by rebalancer.
    // The 'max_moves_per_server' specifies the maximum number of operations
    // per tablet server (both the source and the destination are counted in).
    // The 'load_imbalance_threshold' specified the threshold for the
    // balancing algorithm used for finding the most optimal replica movements.
    // The 'deadline' specifies the deadline for the run, 'boost::none'
    // if no timeout is set.
    CrossLocationRunner(RebalancerTool* rebalancer,
                        std::unordered_set<std::string> ignored_tservers,
                        size_t max_moves_per_server,
                        double load_imbalance_threshold,
                        boost::optional<MonoTime> deadline);

    rebalance::RebalancingAlgo* algorithm() override {
      return &algorithm_;
    }

    const boost::optional<std::string>& location() const override {
      return location_;
    }

   private:
    const boost::optional<std::string> location_ = boost::none;

    // An instance of the balancing algorithm.
    rebalance::LocationBalancingAlgo algorithm_;
  };

  // Runner that leverages 'SetReplace' method to move replicas.
  class ReplaceBasedRunner : public BaseRunner {
   public:
    // The 'ignored_tservers' specifies tablet servers that could be
    // ignored by rebalancer.
    // The 'max_moves_per_server' specifies the maximum number of operations
    // per tablet server (both the source and the destination are counted in).
    // The 'load_imbalance_threshold' specified the threshold for the
    // balancing algorithm used for finding the most optimal replica movements.
    // The 'deadline' specifies the deadline for the run, 'boost::none'
    // if no timeout is set.
    ReplaceBasedRunner(RebalancerTool* rebalancer,
                       std::unordered_set<std::string> ignored_tservers,
                       size_t max_moves_per_server,
                       boost::optional<MonoTime> deadline);

    Status Init(std::vector<std::string> master_addresses) override;

    void LoadMoves(std::vector<Rebalancer::ReplicaMove> replica_moves) override;

    bool ScheduleNextMove(bool* has_errors, bool* timed_out) override;

    bool UpdateMovesInProgressStatus(bool* has_errors,
                                     bool* timed_out,
                                     bool* has_pending_moves) override;

   protected:
    // Key is tserver UUID which corresponds to value.ts_uuid_from.
    typedef std::unordered_multimap<std::string, Rebalancer::ReplicaMove> MovesToSchedule;

    Status GetNextMovesImpl(std::vector<Rebalancer::ReplicaMove>* replica_moves) override;

    virtual Status GetReplaceMoves(const rebalance::ClusterInfo& ci,
                                   const rebalance::ClusterRawInfo& raw_info,
                                   std::vector<Rebalancer::ReplicaMove>* replica_moves) = 0;

    bool FindNextMove(Rebalancer::ReplicaMove* move);

    // Update the helper containers once a move operation has been scheduled.
    void UpdateOnMoveScheduled(Rebalancer::ReplicaMove move);

    // Moves yet to schedule.
    MovesToSchedule moves_to_schedule_;
  };

  class PolicyFixer : public ReplaceBasedRunner {
   public:
    PolicyFixer(RebalancerTool* rebalancer,
                std::unordered_set<std::string> ignored_tservers,
                size_t max_moves_per_server,
                boost::optional<MonoTime> deadline);
   private:
   // Get replica moves to restore the placement policy restrictions.
   // If returns Status::OK() with replica_moves empty, the distribution
   // of tablet relicas is considered conform the main constraint of the
   // placement policy.
    Status GetReplaceMoves(const rebalance::ClusterInfo& ci,
                           const rebalance::ClusterRawInfo& raw_info,
                           std::vector<Rebalancer::ReplicaMove>* replica_moves) override;
  };

  class IgnoredTserversRunner : public ReplaceBasedRunner {
   public:
    IgnoredTserversRunner(RebalancerTool* rebalancer,
                          std::unordered_set<std::string> ignored_tservers,
                          size_t max_moves_per_server,
                          boost::optional<MonoTime> deadline);

   private:
    // Key is tserver UUID which corresponds to value.ts_uuid_from.
    typedef std::unordered_multimap<std::string, Rebalancer::ReplicaMove> MovesToSchedule;

    struct TabletInfo {
      std::string tablet_id;
      boost::optional<int64_t> config_idx;  // For CAS-like change of Raft configs.
    };

    // Mapping tserver UUID to tablets on it.
    typedef std::unordered_map<std::string, std::vector<TabletInfo>> IgnoredTserversInfo;

    // Get replica moves to move replicas from healthy ignored tservers.
    // If returns Status::OK() with replica_moves empty, there would be
    // no replica on the healthy ignored tservers.
    Status GetReplaceMoves(const rebalance::ClusterInfo& ci,
                           const rebalance::ClusterRawInfo& raw_info,
                           std::vector<Rebalancer::ReplicaMove>* replica_moves) override;

    // Check the state of ignored tservers.
    // Return Status::OK() only when all the ignored tservers are in maintenance mode.
    Status CheckIgnoredTserversState(const rebalance::ClusterInfo& ci);

    void GetMovesFromIgnoredTservers(const IgnoredTserversInfo& ignored_tservers_info,
                                     std::vector<Rebalancer::ReplicaMove>* replica_moves);

    // Random device and generator for selecting among multiple choices, when appropriate.
    std::random_device random_device_;
    std::mt19937 random_generator_;
  };

  // Convert ksck results into information relevant to rebalancing the cluster
  // at the location specified by 'location' parameter ('boost::none' for
  // 'location' means that's about cross-location rebalancing). Basically,
  // 'raw' information is just a sub-set of relevant fields of the KsckResults
  // structure filtered to contain information only for the specified location.
  static Status KsckResultsToClusterRawInfo(
      const boost::optional<std::string>& location,
      const KsckResults& ksck_info,
      rebalance::ClusterRawInfo* raw_info);

  // Print replica count infomation on ClusterInfo::tservers_to_empty.
  Status PrintIgnoredTserversStats(const rebalance::ClusterInfo& ci,
                                   std::ostream& out) const;

  // Print information on the cross-location balance.
  Status PrintCrossLocationBalanceStats(const rebalance::ClusterInfo& ci,
                                        std::ostream& out) const;

  // Print statistics for the specified location. If 'location' is an empty
  // string, that's about printing the cluster-wide stats for a cluster that
  // doesn't have any locations defined.
  Status PrintLocationBalanceStats(const std::string& location,
                                   const rebalance::ClusterRawInfo& raw_info,
                                   const rebalance::ClusterInfo& ci,
                                   std::ostream& out) const;

  Status PrintPolicyViolationInfo(const rebalance::ClusterRawInfo& raw_info,
                                  std::ostream& out) const;

  // Check whether it is safe to move all replicas from the ignored to other servers.
  Status CheckIgnoredServers(const rebalance::ClusterRawInfo& raw_info,
                             const rebalance::ClusterInfo& cluster_info);

  // Run rebalancing using the specified runner.
  Status RunWith(Runner* runner, RunStatus* result_status);

  // Refresh the information on the cluster for the specified location
  // (involves running ksck).
  Status GetClusterRawInfo(const boost::optional<std::string>& location,
                           rebalance::ClusterRawInfo* raw_info);

  // Reset ksck-related fields and run ksck against the cluster.
  Status RefreshKsckResults();

  // Auxiliary Ksck object to get information on the cluster.
  std::shared_ptr<Ksck> ksck_;
};

} // namespace tools
} // namespace kudu
