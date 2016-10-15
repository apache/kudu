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

#include "kudu/master/master_options.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace kudu {
namespace master {

class Master;

// An in-process Master meant for use in test cases.
class MiniMaster {
 public:
  MiniMaster(std::string fs_root, HostPort rpc_bind_addr, int num_data_dirs = 1);
  ~MiniMaster();

  // Start a master running on the address specified to the constructor.
  // To determine the address that the server bound to, call
  // MiniMaster::bound_addr()
  Status Start();

  // Restart the master on the same ports as it was previously bound.
  // Requires that Shutdown() has already been called.
  Status Restart();

  // Set the the addresses for distributed masters.
  void SetMasterAddresses(std::vector<HostPort> master_addrs);

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
  const std::string fs_root_;
  const HostPort rpc_bind_addr_;
  Sockaddr bound_rpc_;
  Sockaddr bound_http_;
  MasterOptions opts_;
  std::unique_ptr<Master> master_;
};

} // namespace master
} // namespace kudu
