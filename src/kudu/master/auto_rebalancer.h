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
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rebalance/rebalancer.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class Thread;

namespace rebalance {
class RebalancingAlgo;
struct ClusterLocalityInfo;
struct TabletsPlacementInfo;
} // namespace rebalance

namespace rpc {
class Messenger;
} // namespace rpc

namespace master {

class CatalogManager;
class TSManager;

enum CrossLocations {
  YES,
  NO
};

// A CatalogManager background task which auto-rebalances tablet replica distribution.
//
// As a background task, the lifetime of an instance of this class must be less
// than the catalog manager it belongs to.
//
// The auto-rebalancing task continuously wakes up according to its
// configured poll period. It performs no work when the master is a follower.
class AutoRebalancerTask {
 public:

  AutoRebalancerTask(CatalogManager* catalog_manager, TSManager* ts_manager);
  ~AutoRebalancerTask();

  // Initializes the auto-rebalancer.
  Status Init() WARN_UNUSED_RESULT;

  // Shuts down the auto-rebalancer. This must be called
  // before shutting down the catalog manager.
  void Shutdown();

 private:

  friend class AutoRebalancerTest;

  // Runs the main loop of the auto-rebalancing thread.
  void RunLoop();

  // Collects information about the cluster at the location specified by the
  // 'location' parameter. If there is no location specified (and the parameter
  // is set to 'boost::none'), information is being collected about the cluster
  // to do cross-location rebalancing.
  Status BuildClusterRawInfo(
      const boost::optional<std::string>& location,
      rebalance::ClusterRawInfo* raw_info) const;

  // Gets a set of replica moves using the specified rebalancing algorithm and
  // the information in 'raw_info.' The 'cross_location' parameter specifies
  // whether or not the replica moves should be for cross-location rebalancing.
  // The function adds to the 'replica_moves' parameter, which may or may not
  // be empty.
  Status GetMovesUsingRebalancingAlgo(
      const rebalance::ClusterRawInfo& raw_info,
      rebalance::RebalancingAlgo* algo,
      CrossLocations cross_location,
      std::vector<rebalance::Rebalancer::ReplicaMove>* replica_moves);

  // Gets next set of replica moves for the auto-rebalancer task. The number of
  // moves that will be put into the parameter 'replica_moves' is limited by a
  // gflag that sets the maximum number of replica moves per server.
  // The function first checks for violations of the placement policy. If so,
  // 'replica_moves' will only be populated with moves to reinstate the policy.
  // Otherwise, the function will find and add as many replica moves as possible
  // to perform load rebalancing.
  // If there are multiple locations, the moves will be first to perform cross-location
  // rebalancing, then intra-location rebalancing.
  // Returns a non-OK status if information about the cluster or its locations
  // cannot be constructed, or there is no way to reimpose the placement policy.
  // No changes are made to 'replica_moves' if a non-OK status is returned.
  // If no moves are necessary to rebalance the cluster, the function returns
  // Status::OK() and 'replica_moves' remains empty.
  Status GetMoves(
      const rebalance::ClusterRawInfo& raw_info,
      const rebalance::ClusterLocalityInfo& locality,
      const rebalance::TabletsPlacementInfo& placement_info,
      std::vector<rebalance::Rebalancer::ReplicaMove>* replica_moves);

  // Gets information on the current leader replica for the specified tablet and
  // populates the 'leader_uuid' and 'leader_hp' output parameters. The
  // function returns Status::NotFound() if no replica is a leader for the tablet.
  Status GetTabletLeader(
      const std::string& tablet_id,
      std::string* leader_uuid,
      HostPort* leader_hp) const;

  // Finds replicas that are specified in 'replica_moves' and make requests
  // to have them moved in order to rebalance the cluster.
  // Returns a non-OK status if the replica or the replica's tserver
  // cannot be found, or the request to move the replica cannot be completed.
  Status ExecuteMoves(
      const std::vector<rebalance::Rebalancer::ReplicaMove>& replica_moves);

  // Given a set of replica moves, return Status::OK() if checking completion
  // progress does not encounter an error. Otherwise, return the first error
  // encountered when checking the status of the moves.
  //
  // If no error was encountered, moves that have been completed are
  // removed from 'replica_moves'. Otherwise, only the move that caused an error
  // is removed from 'replica_moves'.
  Status CheckReplicaMovesCompleted(
      std::vector<rebalance::Rebalancer::ReplicaMove>* replica_moves);

  // Given a replica move, determine whether the operation has completed.
  // The 'is_complete' output parameter cannot be null.
  // If the replica move is completed and no errors were encountered, 'is_complete'
  // is set to true and the function returns Status::OK(). If the replica move
  // is not complete yet, but no errors were encountered, 'is_complete' is set
  // to false and the function returns Status::OK().
  //
  // Otherwise, it returns the first non-OK status encountered while trying to
  // get the status of the replica movement.
  //
  // If the source replica is a leader, this function asks it to step down.
  Status CheckMoveCompleted(
      const rebalance::Rebalancer::ReplicaMove& replica_move,
      bool* is_complete);

  // The associated catalog manager.
  CatalogManager* catalog_manager_;

  // The associated TS manager.
  TSManager* ts_manager_;

  // The auto-rebalancing thread.
  scoped_refptr<kudu::Thread> thread_;

  // Latch used to indicate that the thread is shutting down.
  CountDownLatch shutdown_;

  // The internal Rebalancer object.
  rebalance::Rebalancer rebalancer_;

  // The Config struct to initialize the Rebalancer object.
  rebalance::Rebalancer::Config config_;

  std::shared_ptr<rpc::Messenger> messenger_;

  // Random device and generator for selecting among multiple choices.
  std::random_device random_device_;
  std::mt19937 random_generator_;

  // Variables for testing.
  std::atomic<int> number_of_loop_iterations_for_test_;
  std::atomic<int> moves_scheduled_this_round_for_test_;
};

} // namespace master
} // namespace kudu
