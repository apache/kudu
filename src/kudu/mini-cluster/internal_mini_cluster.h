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
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/mini-cluster/mini_cluster.h"

namespace kudu {

class Env;
class HostPort;
class Status;

namespace client {
class KuduClient;
class KuduClientBuilder;
}

namespace master {
class MasterServiceProxy;
class MiniMaster;
class TSDescriptor;
}

namespace rpc {
class Messenger;
}

namespace tserver {
class MiniTabletServer;
}

namespace cluster {

struct InternalMiniClusterOptions {
  InternalMiniClusterOptions();

  // Number of master servers.
  // Default: 1
  int num_masters;

  // Number of TS to start.
  // Default: 1
  int num_tablet_servers;

  // Number of data dirs for each daemon.
  // Default: 1 (this will place the wals in the same dir)
  int num_data_dirs;

  // Directory in which to store the cluster's data.
  // Default: "", which auto-generates a unique path for this cluster.
  // The default may only be used from a gtest unit test.
  std::string cluster_root;

  MiniCluster::BindMode bind_mode;

  // List of RPC ports for the master to run on.
  // Defaults to an empty list.
  // In single-master mode, an empty list implies port 0 (transient port).
  // In multi-master mode, an empty list is illegal and will result in a CHECK failure.
  std::vector<uint16_t> master_rpc_ports;

  // List of RPC ports for the tservers to run on.
  // Defaults to an empty list.
  // When adding a tablet server to the cluster via AddTabletServer(), if the
  // index of that tablet server in the cluster is greater than the number of
  // elements in this list, a transient port (port 0) will be used.
  std::vector<uint16_t> tserver_rpc_ports;
};

// An in-process cluster with a MiniMaster and a configurable
// number of MiniTabletServers for use in tests.
class InternalMiniCluster : public MiniCluster {
 public:
  InternalMiniCluster(Env* env, InternalMiniClusterOptions options);
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

  // Returns the TabletServer with uuid 'uuid', or nullptr if not found.
  tserver::MiniTabletServer* mini_tablet_server_by_uuid(const std::string& uuid) const;

  // Return the index of the tablet server that has the given 'uuid', or
  // -1 if no such UUID can be found.
  int tablet_server_index_by_uuid(const std::string& uuid) const;

  int num_tablet_servers() const override {
    return mini_tablet_servers_.size();
  }

  // Returns the WALs root directory for the tablet server 'ts_idx'.
  std::string WalRootForTS(int ts_idx) const override;

  // Returns the UUID for the tablet server 'ts_idx'.
  std::string UuidForTS(int ts_idx) const override;

  // Returns the Env on which the cluster operates.
  Env* env() const override {
    return env_;
  }

  BindMode bind_mode() const override {
    return opts_.bind_mode;
  }

  std::vector<uint16_t> master_rpc_ports() const override {
    return opts_.master_rpc_ports;
  }

  std::vector<HostPort> master_rpc_addrs() const override;

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

  Env* const env_;

  InternalMiniClusterOptions opts_;

  bool running_;

  std::vector<std::shared_ptr<master::MiniMaster> > mini_masters_;
  std::vector<std::shared_ptr<tserver::MiniTabletServer> > mini_tablet_servers_;

  std::shared_ptr<rpc::Messenger> messenger_;

  DISALLOW_COPY_AND_ASSIGN(InternalMiniCluster);
};

} // namespace cluster
} // namespace kudu
