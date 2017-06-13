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

#include "kudu/client/client.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/server/server_base.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tools/ksck.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/net/sockaddr.h"

namespace kudu {

class Schema;

namespace tools {

// This implementation connects to a Tablet Server via RPC.
class RemoteKsckTabletServer : public KsckTabletServer {
 public:
  explicit RemoteKsckTabletServer(const std::string& id,
                                  const HostPort host_port,
                                  std::shared_ptr<rpc::Messenger> messenger)
      : KsckTabletServer(id),
        host_port_(host_port),
        messenger_(std::move(messenger)) {
  }

  // Resolves the host/port and sets up proxies.
  // Must be called after constructing.
  Status Init();

  Status FetchInfo() override;

  Status FetchConsensusState() override;

  void RunTabletChecksumScanAsync(
      const std::string& tablet_id,
      const Schema& schema,
      const ChecksumOptions& options,
      ChecksumProgressCallbacks* callbacks) override;

  virtual std::string address() const OVERRIDE {
    return host_port_.ToString();
  }

 private:
  const HostPort host_port_;
  const std::shared_ptr<rpc::Messenger> messenger_;
  std::shared_ptr<server::GenericServiceProxy> generic_proxy_;
  std::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
  std::shared_ptr<consensus::ConsensusServiceProxy> consensus_proxy_;
};

// This implementation connects to a Master via RPC.
class RemoteKsckMaster : public KsckMaster {
 public:

  static Status Build(const std::vector<std::string>& master_addresses,
                      std::shared_ptr<KsckMaster>* master);

  virtual ~RemoteKsckMaster() { }

  virtual Status Connect() OVERRIDE;

  virtual Status RetrieveTabletServers(TSMap* tablet_servers) OVERRIDE;

  virtual Status RetrieveTablesList(std::vector<std::shared_ptr<KsckTable> >* tables) OVERRIDE;

  virtual Status RetrieveTabletsList(const std::shared_ptr<KsckTable>& table) OVERRIDE;

 private:

  RemoteKsckMaster(std::vector<std::string> master_addresses,
                   std::shared_ptr<rpc::Messenger> messenger)
      : master_addresses_(std::move(master_addresses)),
        messenger_(std::move(messenger)) {
  }

  const std::vector<std::string> master_addresses_;
  const std::shared_ptr<rpc::Messenger> messenger_;

  client::sp::shared_ptr<client::KuduClient> client_;
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_KSCK_REMOTE_H
