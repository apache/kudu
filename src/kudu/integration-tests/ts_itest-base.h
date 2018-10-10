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

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoDelta;

namespace client {
class KuduClient;
class KuduTable;
} // namespace client

namespace tserver {

// A base for tablet server integration tests.
class TabletServerIntegrationTestBase : public TabletServerTestBase {
 public:
  TabletServerIntegrationTestBase();

  void SetUp() override;

  void AddExtraFlags(const std::string& flags_str,
                     std::vector<std::string>* flags);

  void CreateCluster(const std::string& data_root_path,
                     std::vector<std::string> non_default_ts_flags = {},
                     std::vector<std::string> non_default_master_flags = {},
                     cluster::LocationInfo location_info = {});

  // Creates TSServerDetails instance for each TabletServer and stores them
  // in 'tablet_servers_'.
  void CreateTSProxies();

  // Waits that all replicas for a all tablets of 'table_id' table are online
  // and creates the tablet_replicas_ map.
  void WaitForReplicasAndUpdateLocations(const std::string& table_id = kTableId);

  // Returns the last committed leader of the consensus configuration. Tries to get it from master
  // but then actually tries to the get the committed consensus configuration to make sure.
  itest::TServerDetails* GetLeaderReplicaOrNull(const std::string& tablet_id);

  // For the last committed consensus configuration, return the last committed
  // leader of the consensus configuration and its followers.
  Status GetTabletLeaderAndFollowers(const std::string& tablet_id,
                                     itest::TServerDetails** leader,
                                     std::vector<itest::TServerDetails*>* followers);

  Status GetLeaderReplicaWithRetries(const std::string& tablet_id,
                                     itest::TServerDetails** leader,
                                     int max_attempts = 100);

  Status GetTabletLeaderUUIDFromMaster(const std::string& tablet_id,
                                       std::string* leader_uuid);

  itest::TServerDetails* GetReplicaWithUuidOrNull(const std::string& tablet_id,
                                                  const std::string& uuid);

  // Gets the the locations of the consensus configuration and waits until all replicas
  // are available for all tablets.
  void WaitForTSAndReplicas(const std::string& table_id = kTableId);

  // Removes a set of servers from the replicas_ list.
  // Handy for controlling who to validate against after killing servers.
  void PruneFromReplicas(const std::unordered_set<std::string>& uuids);

  void GetOnlyLiveFollowerReplicas(const std::string& tablet_id,
                                   std::vector<itest::TServerDetails*>* followers);

  // Return the index within 'replicas' for the replica which is farthest ahead.
  int64_t GetFurthestAheadReplicaIdx(const std::string& tablet_id,
                                     const std::vector<itest::TServerDetails*>& replicas);

  Status ShutdownServerWithUUID(const std::string& uuid);

  Status RestartServerWithUUID(const std::string& uuid);

  // Since we're fault-tolerant we might mask when a tablet server is
  // dead. This returns Status::IllegalState() if fewer than 'num_tablet_servers'
  // are alive.
  Status CheckTabletServersAreAlive(int num_tablet_servers);

  void TearDown() override;

  void CreateClient(client::sp::shared_ptr<client::KuduClient>* client);

  // Create a table with a single tablet, with 'num_replicas'.
  void CreateTable(const std::string& table_id = kTableId);

  // Starts an external cluster with a single tablet and a number of replicas
  // equal to 'FLAGS_num_replicas'. The caller can pass 'ts_flags' and
  // 'master_flags' to specify non-default flags to pass to the tablet servers
  // and masters respectively. For location-aware tests scenarios, location
  // mapping rules can be passed using the 'location_info' parameter.
  void BuildAndStart(std::vector<std::string> ts_flags = {},
                     std::vector<std::string> master_flags = {},
                     cluster::LocationInfo location_info = {});

  void AssertAllReplicasAgree(int expected_result_count);

  // Check for and restart any TS that have crashed.
  // Returns the number of servers restarted.
  int RestartAnyCrashedTabletServers();

  // Assert that no tablet servers have crashed.
  // Tablet servers that have been manually Shutdown() are allowed.
  void AssertNoTabletServersCrashed();

  // Find the tablet leader replica and wait for at least one operation
  // committed in current term. This is useful when finding a leader replica
  // to commence a Raft configuration change. Otherwise, any Raft configuration
  // change attempt ends up with error:
  // 'Illegal state: Leader has not yet committed an operation in its own term'.
  Status WaitForLeaderWithCommittedOp(const std::string& tablet_id,
                                      const MonoDelta& timeout,
                                      itest::TServerDetails** leader);

  // Get UUIDs of tablet servers that have a replica of the tablet identified
  // by the 'tablet_id' parameter. The result is sorted in ascending order.
  std::vector<std::string> GetServersWithReplica(const std::string& tablet_id) const;

  // Get UUIDs of tablet servers that do not have replicas of the tablet
  // identified by the 'tablet_id' parameter. The result is sorted in ascending order.
  std::vector<std::string> GetServersWithoutReplica(const std::string& tablet_id) const;

 protected:
  gscoped_ptr<cluster::ExternalMiniCluster> cluster_;
  gscoped_ptr<itest::MiniClusterFsInspector> inspect_;

  // Maps server uuid to TServerDetails
  itest::TabletServerMap tablet_servers_;
  // Maps tablet to all replicas.
  itest::TabletReplicaMap tablet_replicas_;

  client::sp::shared_ptr<client::KuduClient> client_;
  client::sp::shared_ptr<client::KuduTable> table_;
  std::string tablet_id_;

  ThreadSafeRandom random_;
};

}  // namespace tserver
}  // namespace kudu
