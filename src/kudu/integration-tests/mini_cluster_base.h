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

#include "kudu/client/shared_ptr.h"

namespace kudu {
class Status;

namespace client {
class KuduClient;
class KuduClientBuilder;
} // namespace client

namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
} // namespace rpc

// Mode to which node types a certain action (like Shutdown()) should apply.
enum class ClusterNodes {
  ALL,
  MASTERS_ONLY,
  TS_ONLY
};

// Base class for MiniCluster implementations. Provides some commonly-used
// virtual methods that allow for abstracting the details of whether the
// mini-cluster implementation is in-process or out-of-process.
class MiniClusterBase {
 public:
  MiniClusterBase() {}
  virtual ~MiniClusterBase() {}

  // Start the cluster.
  virtual Status Start() = 0;

  // Convenience method for shutting down the whole cluster.
  virtual void Shutdown() final {
    ShutdownNodes(ClusterNodes::ALL);
  }

  // Shuts down the whole cluster or part of it, depending on the selected
  // 'nodes'.
  virtual void ShutdownNodes(ClusterNodes nodes) = 0;

  virtual int num_masters() const = 0;

  virtual int num_tablet_servers() const = 0;

  // Create a client configured to talk to this cluster. 'builder' may contain
  // override options for the client. The master address will be overridden to
  // talk to the running master(s). If 'builder' is a nullptr, default options
  // will be used.
  //
  // REQUIRES: the cluster must have already been Start()ed.
  virtual Status CreateClient(client::KuduClientBuilder* builder,
                              client::sp::shared_ptr<client::KuduClient>* client) const = 0;

  // Return a messenger for use by clients.
  virtual std::shared_ptr<rpc::Messenger> messenger() const = 0;

  // If the cluster is configured for a single non-distributed master,
  // return a proxy to that master. Requires that the single master is
  // running.
  virtual std::shared_ptr<master::MasterServiceProxy> master_proxy() const = 0;

  // Returns an RPC proxy to the master at 'idx'. Requires that the
  // master at 'idx' is running.
  virtual std::shared_ptr<master::MasterServiceProxy> master_proxy(int idx) const = 0;
};

} // namespace kudu
