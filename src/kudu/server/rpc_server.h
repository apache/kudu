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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

template <class T> class scoped_refptr;

namespace kudu {

namespace rpc {
class AcceptorPool;
class Messenger;
class ServiceIf;
class ServicePool;
} // namespace rpc

struct RpcServerOptions {
  RpcServerOptions();

  std::string rpc_bind_addresses;
  std::string rpc_advertised_addresses;
  uint32_t num_acceptors_per_address;
  uint32_t num_service_threads;
  uint16_t default_port;
  size_t service_queue_length;
};

class RpcServer {
 public:
  explicit RpcServer(RpcServerOptions opts);
  ~RpcServer();

  // Set a hook which will be called by any registered service when
  // its queue overflows. The service pool itself will be passed
  // as a parameter.
  //
  // REQUIRES: must be set before the server is started.
  void set_too_busy_hook(std::function<void(rpc::ServicePool*)> hook) {
    CHECK_NE(server_state_, STARTED);
    too_busy_hook_ = std::move(hook);
  }

  Status Init(const std::shared_ptr<rpc::Messenger>& messenger) WARN_UNUSED_RESULT;
  // Services need to be registered after Init'ing, but before Start'ing.
  // The service's ownership will be given to a ServicePool.
  Status RegisterService(gscoped_ptr<rpc::ServiceIf> service) WARN_UNUSED_RESULT;
  Status Bind() WARN_UNUSED_RESULT;
  Status Start() WARN_UNUSED_RESULT;
  void Shutdown();

  std::string ToString() const;

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been Start()ed.
  Status GetBoundAddresses(std::vector<Sockaddr>* addresses) const WARN_UNUSED_RESULT;

  // Return the addresses that this server is advertising externally
  // to the world. Requires that the server has been Start()ed.
  Status GetAdvertisedAddresses(std::vector<Sockaddr>* addresses) const WARN_UNUSED_RESULT;

  const rpc::ServicePool* service_pool(const std::string& service_name) const;

  // Return all of the currently-registered service pools.
  //
  // This is not thread-safe against concurrent calls to RegisterService().
  std::vector<scoped_refptr<rpc::ServicePool>> service_pools() const;

 private:
  enum ServerState {
    // Default state when the rpc server is constructed.
    UNINITIALIZED,
    // State after Init() was called.
    INITIALIZED,
    // State after Bind().
    BOUND,
    // State after Start() was called.
    STARTED
  };
  ServerState server_state_;

  const RpcServerOptions options_;
  std::shared_ptr<rpc::Messenger> messenger_;

  // Parsed addresses to bind RPC to. Set by Init()
  std::vector<Sockaddr> rpc_bind_addresses_;

  // Parsed addresses to advertise. Set by Init(). Empty if rpc_bind_addresses_
  // should be advertised.
  std::vector<Sockaddr> rpc_advertised_addresses_;

  std::vector<std::shared_ptr<rpc::AcceptorPool> > acceptor_pools_;

  // Function called when one of this server's pools rejects an RPC due to queue overflow.
  std::function<void(rpc::ServicePool*)> too_busy_hook_;

  DISALLOW_COPY_AND_ASSIGN(RpcServer);
};

} // namespace kudu
