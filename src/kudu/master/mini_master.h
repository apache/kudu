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

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/env.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace kudu {

namespace master {

class Master;
struct MasterOptions;

// An in-process Master meant for use in test cases.
//
// TODO: Store the distributed cluster configuration in the object, to avoid
// having multiple Start methods.
class MiniMaster {
 public:
  MiniMaster(std::string fs_root, HostPort rpc_bind_addr);
  ~MiniMaster();

  // Start a master running on the address specified to the constructor.
  // To determine the address that the server bound to, call
  // MiniMaster::bound_addr()
  Status Start();

  Status StartDistributedMaster(std::vector<HostPort> peer_addrs);

  // Restart the master on the same ports as it was previously bound.
  // Requires that the master is currently started.
  Status Restart();

  void Shutdown();

  Status WaitForCatalogManagerInit() const;

  bool is_started() const { return master_ ? true : false; }

  const Sockaddr bound_rpc_addr() const;
  const Sockaddr bound_http_addr() const;

  const Master* master() const { return master_.get(); }
  Master* master() { return master_.get(); }

  // Return UUID of this mini master.
  std::string permanent_uuid() const;

  std::string bound_rpc_addr_str() const;

 private:
  Status StartOnAddrs(const HostPort& rpc_bind_addr,
                      const HostPort& web_bind_addr,
                      MasterOptions* opts);

  Status StartDistributedMasterOnAddrs(const HostPort& rpc_bind_addr,
                                       const HostPort& web_bind_addr,
                                       std::vector<HostPort> peer_addrs);

  const HostPort rpc_bind_addr_;
  const std::string fs_root_;
  Sockaddr bound_rpc_;
  Sockaddr bound_http_;
  std::unique_ptr<Master> master_;
};

} // namespace master
} // namespace kudu
