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

#include <memory>
#include <string>
#include <vector>

#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/integration-tests/mini_cluster_base.h"
#include "kudu/util/env.h"

namespace kudu {

namespace client {
class KuduClient;
class KuduClientBuilder;
}

namespace master {
class MiniMaster;
class TSDescriptor;
class TabletLocationsPB;
}

namespace tserver {
class MiniTabletServer;
}

struct MiniClusterOptions {
  MiniClusterOptions();

  // Number of master servers.
  // Default: 1
  int num_masters;

  // Number of TS to start.
  // Default: 1
  int num_tablet_servers;

  // Directory in which to store data.
  // Default: "", which auto-generates a unique path for this cluster.
  // The default may only be used from a gtest unit test.
  std::string data_root;

  // List of RPC ports for the master to run on.
  // Defaults to a list 0 (ephemeral ports).
  std::vector<uint16_t> master_rpc_ports;

  // List of RPC ports for the tservers to run on.
  // Defaults to a list of 0 (ephemeral ports).
  std::vector<uint16_t> tserver_rpc_ports;
};

// An in-process cluster with a MiniMaster and a configurable
// number of MiniTabletServers for use in tests.
class InternalMiniCluster : public MiniClusterBase {
 public:
  InternalMiniCluster(Env* env, const MiniClusterOptions& options);
  virtual ~InternalMiniCluster();

  // Start a cluster with a Master and 'num_tablet_servers' TabletServers.
  // All servers run on the loopback interface with ephemeral ports.
  Status Start() override;

  // Like the previous method but performs initialization synchronously, i.e.
  // this will wait for all TS's to be started and initialized. Tests should
  // use this if they interact with tablets immediately after Start();
  Status StartSync();

  void ShutdownNodes(ClusterNodes nodes) override;

  // Setup a consensus configuration of distributed masters, with count specified in
  // 'options'. Requires that a reserve RPC port is specified in
  // 'options' for each master.
  Status StartDistributedMasters();

  // Add a new standalone master to the cluster. The new master is started.
  Status StartSingleMaster();

  // Add a new TS to the cluster. The new TS is started.
  // Requires that the master is already running.
  Status AddTabletServer();

  // If this cluster is configured for a single non-distributed
  // master, return the single master. Exits with a CHECK failure if
  // there are multiple masters.
  master::MiniMaster* mini_master() const {
    CHECK_EQ(mini_masters_.size(), 1);
    return mini_master(0);
  }

  // Returns the Master at index 'idx' for this InternalMiniCluster.
  master::MiniMaster* mini_master(int idx) const;

  // Return number of mini masters.
  int num_masters() const override {
    return mini_masters_.size();
  }

  // Returns the TabletServer at index 'idx' of this InternalMiniCluster.
  // 'idx' must be between 0 and 'num_tablet_servers' -1.
  tserver::MiniTabletServer* mini_tablet_server(int idx) const;

  int num_tablet_servers() const override {
    return mini_tablet_servers_.size();
  }

  std::string GetMasterFsRoot(int idx) const;

  std::string GetTabletServerFsRoot(int idx) const;

  // Wait until the number of registered tablet servers reaches the given
  // count on all masters. Returns Status::TimedOut if the desired count is not
  // achieved within kRegistrationWaitTimeSeconds.
  enum class MatchMode {
    // Ensure that the tservers retrieved from each master match up against the
    // tservers defined in this cluster. The matching is done via
    // NodeInstancePBs comparisons. If even one match fails, the retrieved
    // response is considered to be malformed and is retried.
    //
    // Note: tservers participate in matching even if they are shut down.
    MATCH_TSERVERS,

    // Do not perform any matching on the retrieved tservers.
    DO_NOT_MATCH_TSERVERS,
  };
  Status WaitForTabletServerCount(int count) const;
  Status WaitForTabletServerCount(int count, MatchMode mode,
                                  std::vector<std::shared_ptr<master::TSDescriptor>>* descs) const;

  Status CreateClient(client::KuduClientBuilder* builder,
                      client::sp::shared_ptr<client::KuduClient>* client) const override;

  // Determine the leader master of the cluster. Upon successful completion,
  // sets 'idx' to the leader master's index. The result index index can be used
  // as an argument for calls to mini_master().
  //
  // It's possible to use 'nullptr' instead of providing a valid placeholder
  // for the result master index. That's for use cases when it's enough
  // to determine if the cluster has established leader master
  // without intent to get the actual index.
  //
  // Note: if a leader election occurs after this method is executed, the
  // last result may not be valid.
  Status GetLeaderMasterIndex(int* idx) const;

  std::shared_ptr<rpc::Messenger> messenger() const override;
  std::shared_ptr<master::MasterServiceProxy> master_proxy() const override;
  std::shared_ptr<master::MasterServiceProxy> master_proxy(int idx) const override;

 private:
  enum {
    kRegistrationWaitTimeSeconds = 15,
    kMasterStartupWaitTimeSeconds = 30,
  };

  bool running_;

  Env* const env_;
  const std::string fs_root_;
  const int num_masters_initial_;
  const int num_ts_initial_;

  const std::vector<uint16_t> master_rpc_ports_;
  const std::vector<uint16_t> tserver_rpc_ports_;

  std::vector<std::shared_ptr<master::MiniMaster> > mini_masters_;
  std::vector<std::shared_ptr<tserver::MiniTabletServer> > mini_tablet_servers_;

  std::shared_ptr<rpc::Messenger> messenger_;

  DISALLOW_COPY_AND_ASSIGN(InternalMiniCluster);
};

} // namespace kudu
