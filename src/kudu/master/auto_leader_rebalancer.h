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
#include <unordered_set>
#include <vector>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

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
  AutoLeaderRebalancerTask(CatalogManager* catalog_manager, TSManager* ts_manager);

  ~AutoLeaderRebalancerTask();

  Status Init();

  void Shutdown();

  Status RunLeaderRebalancer();

  enum class ExecuteMode { NORMAL, TEST };

 private:
  friend class AutoRebalancerTest;
  friend class LeaderRebalancerTest;

  // Runs the main loop of the auto-leader-rebalancing thread.
  void RunLoop();

  // Run Leader Rebalance for a table
  Status RunLeaderRebalanceForTable(
      const scoped_refptr<TableInfo>& table_info,
      const std::vector<std::string>& tserver_uuids,
      const std::unordered_set<std::string>& exclude_dest_uuids,
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

  // Variables for testing.
  std::atomic<int> number_of_loop_iterations_for_test_;
  std::atomic<int> moves_scheduled_this_round_for_test_;
};

}  // namespace master
}  // namespace kudu
