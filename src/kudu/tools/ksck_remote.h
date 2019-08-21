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

#ifndef KUDU_TOOLS_KSCK_REMOTE_H
#define KUDU_TOOLS_KSCK_REMOTE_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/client/shared_ptr.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/tools/ksck.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;

namespace client {
class KuduClient;
}

namespace cluster_summary {
enum class ServerHealth;
}

namespace consensus {
class ConsensusServiceProxy;
}

namespace rpc {
class Messenger;
}

namespace server {
class GenericServiceProxy;
}

namespace tserver {
class TabletServerServiceProxy;
}

namespace tools {

class KsckChecksumManager;
struct KsckChecksumOptions;

// This implementation connects to a master via RPC.
class RemoteKsckMaster : public KsckMaster {
 public:
  RemoteKsckMaster(const std::string& address,
                   std::shared_ptr<rpc::Messenger> messenger)
      : KsckMaster(address),
        messenger_(std::move(messenger)) {
  }

  // Resolves the host/port and sets up proxies.
  // Must be called before FetchInfo() or FetchConsensusState();
  Status Init() override;

  Status FetchInfo() override;

  // Gathers consensus state for the master tablet.
  Status FetchConsensusState() override;

  Status FetchUnusualFlags() override;

 private:
  std::shared_ptr<rpc::Messenger> messenger_;
  std::shared_ptr<server::GenericServiceProxy> generic_proxy_;
  std::shared_ptr<consensus::ConsensusServiceProxy> consensus_proxy_;
};

// This implementation connects to a tablet server via RPC.
class RemoteKsckTabletServer : public KsckTabletServer,
                               public std::enable_shared_from_this<RemoteKsckTabletServer> {
 public:
  explicit RemoteKsckTabletServer(const std::string& id,
                                  HostPort host_port,
                                  std::shared_ptr<rpc::Messenger> messenger,
                                  const std::string& location = "")
      : KsckTabletServer(id, location),
        host_port_(std::move(host_port)),
        messenger_(std::move(messenger)) {
  }

  // Resolves the host/port and sets up proxies.
  // Must be called after constructing.
  Status Init();

  Status FetchInfo(cluster_summary::ServerHealth* health) override;

  Status FetchConsensusState(cluster_summary::ServerHealth* health) override;

  Status FetchUnusualFlags() override;

  void FetchCurrentTimestampAsync() override;
  Status FetchCurrentTimestamp() override;

  void RunTabletChecksumScanAsync(
      const std::string& tablet_id,
      const Schema& schema,
      const KsckChecksumOptions& options,
      std::shared_ptr<KsckChecksumManager> manager) override;

  virtual std::string address() const override {
    return host_port_.ToString();
  }

 private:
  // A callback to update the timestamp from the remote server.
  struct ServerClockResponseCallback {
   public:
    explicit ServerClockResponseCallback(std::shared_ptr<RemoteKsckTabletServer> ts)
        : ts(std::move(ts)) {}

    void Run();

    std::shared_ptr<RemoteKsckTabletServer> ts;
    server::ServerClockRequestPB req;
    server::ServerClockResponsePB resp;
    rpc::RpcController rpc;

   private:
    // Prevent instances of this class from being allocated on the stack.
    ~ServerClockResponseCallback() = default;
  };

  const HostPort host_port_;
  const std::shared_ptr<rpc::Messenger> messenger_;
  std::shared_ptr<server::GenericServiceProxy> generic_proxy_;
  std::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
  std::shared_ptr<consensus::ConsensusServiceProxy> consensus_proxy_;
};

// A KsckCluster that connects to a cluster via RPC.
class RemoteKsckCluster : public KsckCluster {
 public:
  static Status Build(const std::vector<std::string>& master_addresses,
                      std::shared_ptr<KsckCluster>* cluster);

  virtual Status Connect() override;

  virtual Status RetrieveTabletServers() override;

  virtual Status RetrieveTablesList() override;

  virtual Status RetrieveAllTablets() override;

  virtual Status RetrieveTabletsList(const std::shared_ptr<KsckTable>& table) override;

  std::shared_ptr<rpc::Messenger> messenger() const override {
    return messenger_;
  }

 private:
  RemoteKsckCluster(std::vector<std::string> master_addresses,
                    std::shared_ptr<rpc::Messenger> messenger);

  const std::vector<std::string> master_addresses_;
  const std::shared_ptr<rpc::Messenger> messenger_;
  client::sp::shared_ptr<client::KuduClient> client_;
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_KSCK_REMOTE_H
