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

#include "kudu/master/mini_master.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master_options.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(enable_minidumps);
DECLARE_bool(rpc_server_allow_ephemeral_ports);

namespace kudu {
namespace master {

MiniMaster::MiniMaster(string fs_root, HostPort rpc_bind_addr, int num_data_dirs)
    : fs_root_(std::move(fs_root)),
      rpc_bind_addr_(std::move(rpc_bind_addr)) {
  // Disable minidump handler (we allow only one per process).
  FLAGS_enable_minidumps = false;
  HostPort web_bind_addr(rpc_bind_addr_.host(), /*port=*/ 0);
  opts_.rpc_opts.rpc_bind_addresses = rpc_bind_addr_.ToString();
  opts_.webserver_opts.bind_interface = web_bind_addr.host();
  opts_.webserver_opts.port = web_bind_addr.port();
  if (num_data_dirs == 1) {
    opts_.fs_opts.wal_path = fs_root_;
    opts_.fs_opts.data_paths = { fs_root_ };
  } else {
    vector<string> fs_data_dirs;
    for (int dir = 0; dir < num_data_dirs; dir++) {
      fs_data_dirs.emplace_back(JoinPathSegments(fs_root_, Substitute("data-$0", dir)));
    }
    opts_.fs_opts.wal_path = JoinPathSegments(fs_root_, "wal");
    opts_.fs_opts.data_paths = fs_data_dirs;
  }
}

MiniMaster::~MiniMaster() {
  Shutdown();
}

void MiniMaster::SetMasterAddresses(vector<HostPort> master_addrs) {
  CHECK(!master_);
  opts_.master_addresses = std::move(master_addrs);
}

Status MiniMaster::Start() {
  CHECK(!master_);
  if (opts_.master_addresses.empty()) {
    FLAGS_rpc_server_allow_ephemeral_ports = true;
  }
  // In case the wal dir and data dirs are subdirectories of the root directory,
  // ensure the root directory exists.
  RETURN_NOT_OK(env_util::CreateDirIfMissing(Env::Default(), fs_root_));
  unique_ptr<Master> master(new Master(opts_));
  RETURN_NOT_OK(master->Init());
  RETURN_NOT_OK(master->StartAsync());
  master_.swap(master);

  // Wait for the catalog manager to be ready if we only have a single master.
  if (opts_.master_addresses.empty()) {
    return master_->WaitForCatalogManagerInit();
  }
  return Status::OK();
}

Status MiniMaster::Restart() {
  CHECK(!master_);
  opts_.rpc_opts.rpc_bind_addresses = bound_rpc_.ToString();
  opts_.webserver_opts.bind_interface = bound_http_.host();
  opts_.webserver_opts.port = bound_http_.port();
  Shutdown();
  return Start();
}

void MiniMaster::Shutdown() {
  if (master_) {
    bound_rpc_ = bound_rpc_addr();
    bound_http_ = bound_http_addr();
    master_->Shutdown();
    master_.reset();
  }
}

Status MiniMaster::WaitForCatalogManagerInit() const {
  return master_->WaitForCatalogManagerInit();
}

const Sockaddr MiniMaster::bound_rpc_addr() const {
  return master_->first_rpc_address();
}

const Sockaddr MiniMaster::bound_http_addr() const {
  return master_->first_http_address();
}

std::string MiniMaster::permanent_uuid() const {
  return DCHECK_NOTNULL(master_->fs_manager())->uuid();
}

std::string MiniMaster::bound_rpc_addr_str() const {
  return bound_rpc_addr().ToString();
}

} // namespace master
} // namespace kudu
