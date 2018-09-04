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
#include "kudu/util/status.h"

namespace kudu {

class Env;
class HostPort;
class Socket;

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

namespace tserver {
class TabletServerServiceProxy;
} // namespace tserver

namespace cluster {

// Mode to which node types a certain action (like Shutdown()) should apply.
enum class ClusterNodes {
  ALL,
  MASTERS_ONLY,
  TS_ONLY
};

// Base class for MiniCluster implementations. Provides some commonly-used
// virtual methods that allow for abstracting the details of whether the
// mini-cluster implementation is in-process or out-of-process.
class MiniCluster {
 public:
  // BindMode lets you specify the socket binding mode for RPC and/or HTTP server.
  // A) LOOPBACK binds each server to loopback ip address "127.0.0.1".
  //
  // B) WILDCARD specifies "0.0.0.0" as the ip to bind to, which means sockets
  // can be bound to any interface on the local host.
  // For example, if a host has two interfaces with addresses
  // 192.168.0.10 and 192.168.0.11, the server process can accept connection
  // requests addressed to 192.168.0.10 or 192.168.0.11.
  //
  // C) UNIQUE_LOOPBACK binds each tablet server to a different loopback address.
  // This affects the server's RPC server, and also forces the server to
  // only use this IP address for outgoing socket connections as well.
  // This allows the use of iptables on the localhost to simulate network
  // partitions.
  //
  // The addresses used are 127.<A>.<B>.<C> where:
  // - <A,B> are the high and low bytes of the pid of the process running the
  //   minicluster (not the daemon itself).
  // - <C> is the index of the server within this minicluster.
  //
  // This requires that the system is set up such that processes may bind
  // to any IP address in the localhost netblock (127.0.0.0/8). This seems
  // to be the case on common Linux distributions. You can verify by running
  // 'ip addr | grep 127.0.0.1' and checking that the address is listed as
  // '127.0.0.1/8'.
  //
  // Note: UNIQUE_LOOPBACK is not supported on macOS.
  //
  // Default: UNIQUE_LOOPBACK on Linux, LOOPBACK on macOS.
  enum BindMode {
    UNIQUE_LOOPBACK,
    WILDCARD,
    LOOPBACK
  };

#if defined(__APPLE__)
  static constexpr const BindMode kDefaultBindMode = BindMode::LOOPBACK;
#else
  static constexpr const BindMode kDefaultBindMode = BindMode::UNIQUE_LOOPBACK;
#endif

  enum DaemonType {
    MASTER,
    TSERVER
  };

  static constexpr const char* const kWildcardIpAddr = "0.0.0.0";
  static constexpr const char* const kLoopbackIpAddr = "127.0.0.1";

  MiniCluster() {}

  virtual ~MiniCluster() = default;

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

  virtual BindMode bind_mode() const = 0;

  /// Returns the RPC addresses of all Master nodes in the cluster.
  ///
  /// REQUIRES: the cluster must have already been Start()ed.
  virtual std::vector<HostPort> master_rpc_addrs() const = 0;

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

  // Returns an RPC proxy to the tserver at 'idx'. Requires that the tserver at
  // 'idx' is running.
  virtual std::shared_ptr<tserver::TabletServerServiceProxy> tserver_proxy(int idx) const = 0;

  // Returns the UUID for the tablet server 'ts_idx'
  virtual std::string UuidForTS(int ts_idx) const = 0;

  // Returns the WALs root directory for the tablet server 'ts_idx'.
  virtual std::string WalRootForTS(int ts_idx) const = 0;

  // Returns the Env on which the cluster operates.
  virtual Env* env() const = 0;

  /// Reserves a unique socket address for a mini-cluster daemon. The address
  /// can be ascertained through the returned socket, and will remain reserved
  /// for the life of the socket. The daemon must use the SO_REUSEPORT socket
  /// option when binding to the address.
  static Status ReserveDaemonSocket(DaemonType type,
                                    int index,
                                    BindMode bind_mode,
                                    std::unique_ptr<Socket>* socket);

 protected:
  // Return the IP address that the daemon with the given index will bind to.
  // If bind_mode is LOOPBACK, this will be 127.0.0.1 and if it is WILDCARD it
  // will be 0.0.0.0. Otherwise, it is another IP in the local netblock.
  static std::string GetBindIpForDaemon(DaemonType type, int index, BindMode bind_mode);
};

} // namespace cluster
} // namespace kudu
