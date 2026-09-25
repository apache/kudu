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

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class Counter;
class MetricEntity;
class Thread;

namespace rpc {
class Messenger;
}  // namespace rpc

namespace master {

class CatalogManager;
class TSManager;
class TableInfo;

// A CatalogManager background task which auto-rebalances tablets' leaders distribution
// by transferring leadership between tablet replicas.
//
// As a background task, the lifetime of an instance of this class must be less
// than the catalog manager it belongs to.
//
// The auto-rebalancing task continuously wakes up according to its
// configured poll period. It performs no work when the master is a follower.
class AutoLeaderRebalancerTask {
 public:
  AutoLeaderRebalancerTask(CatalogManager* catalog_manager,
                           TSManager* ts_manager,
                           const scoped_refptr<MetricEntity>& metric_entity);

  ~AutoLeaderRebalancerTask();

  Status Init();

  void Shutdown();

  // Returns Status::OK() without doing anything if the catalog manager isn't
  // running or this master isn't leader-ready yet. 'ran', when provided, is set
  // to whether a pass actually took place, distinguishing that no-op from a
  // completed round.
  Status RunLeaderRebalancer(bool* ran = nullptr);

  enum class ExecuteMode { NORMAL, TEST };

 private:
  friend class AutoRebalancerTest;
  friend class LeaderRebalancerTest;

  // Runs the main loop of the auto-leader-rebalancing thread.
  void RunLoop();

  // Run Leader Rebalance for a table.
  // global_leader_count, when provided, is read as a tiebreaker during destination scoring and
  // updated with every planned move so that successive table calls share a consistent view of
  // in-round leader distribution.
  // num_scheduled_moves, when provided, is incremented by the number of leader transfers planned
  // for this table, letting the caller tell whether per-table balancing still has work to do.
  Status RunLeaderRebalanceForTable(
      const scoped_refptr<TableInfo>& table_info,
      const std::vector<std::string>& tserver_uuids,
      const std::unordered_set<std::string>& exclude_dest_uuids,
      std::unordered_map<std::string, int>* global_leader_count = nullptr,
      AutoLeaderRebalancerTask::ExecuteMode mode = AutoLeaderRebalancerTask::ExecuteMode::NORMAL,
      int* num_scheduled_moves = nullptr);

  // Runs a global corrective pass once every table has been balanced on its
  // own by RunLeaderRebalanceForTable. Per-table balancing keeps each table
  // within floor/ceil of its own average, but it can still leave the cluster
  // globally skewed when the same tserver repeatedly receives the ceiling
  // (the extra '+1') allocation across many tables, e.g. lots of single-tablet
  // or otherwise indivisible tables whose lone leader keeps landing on it.
  //
  // 'global_leader_count' is the in-round leader distribution accumulated by
  // the per-table calls. Tservers above the global ceiling are treated as move
  // sources and tservers below the global ceiling as destinations. A leader is
  // only transferred when the destination holds strictly fewer of that table's
  // leaders than the source, so the move improves global balance without
  // pushing the affected table out of its own floor/ceil range. The map is
  // updated for every planned move so successive tables share a consistent
  // view.
  Status RunGlobalLeaderRebalance(
      const std::vector<scoped_refptr<TableInfo>>& table_infos,
      const std::vector<std::string>& tserver_uuids,
      const std::unordered_set<std::string>& exclude_dest_uuids,
      std::unordered_map<std::string, int>* global_leader_count,
      AutoLeaderRebalancerTask::ExecuteMode mode = AutoLeaderRebalancerTask::ExecuteMode::NORMAL);

  // Only one task can be scheduled at a time.
  Mutex running_mutex_;

  // The associated catalog manager.
  CatalogManager* catalog_manager_;

  // The associated TS manager.
  TSManager* ts_manager_;

  // The auto-rebalancing thread.
  scoped_refptr<kudu::Thread> thread_;

  // latch used to indicate that the thread is shutting down.
  CountDownLatch shutdown_;

  // send rpc messages for 'LeaderStepDown'.
  std::shared_ptr<rpc::Messenger> messenger_;

  // Random device and generator for selecting among multiple choices.
  std::random_device random_device_;
  std::mt19937 random_generator_;

  // Counters exposing what the auto-leader-rebalancer is doing across rounds.
  // See the METRIC_DEFINE_counter blocks in auto_leader_rebalancer.cc for the
  // per-metric descriptions. The three move counters follow the invariant
  //     moves_scheduled_ = moves_completed_ + moves_failed_
  // over the set of transfers that made it past the pre-flight applicability
  // checks in the per-table and global passes.
  scoped_refptr<Counter> moves_scheduled_;
  scoped_refptr<Counter> moves_completed_;
  scoped_refptr<Counter> moves_failed_;
  scoped_refptr<Counter> rounds_completed_;
  scoped_refptr<Counter> global_pass_skipped_;
  // Warn-severity counter, meant as the main signal for alerting. Bumped
  // once per round that returned an unexpected error.
  scoped_refptr<Counter> task_errors_;

  // Variables for testing.
  std::atomic<int> number_of_loop_iterations_for_test_;
  std::atomic<int> moves_scheduled_this_round_for_test_;
};

}  // namespace master
}  // namespace kudu
