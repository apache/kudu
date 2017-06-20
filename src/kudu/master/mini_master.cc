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

#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/webserver.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(enable_minidumps);
DECLARE_bool(rpc_server_allow_ephemeral_ports);

namespace kudu {
namespace master {

MiniMaster::MiniMaster(string fs_root, HostPort rpc_bind_addr)
    : rpc_bind_addr_(std::move(rpc_bind_addr)),
      fs_root_(std::move(fs_root)) {
  // Disable minidump handler (we allow only one per process).
  FLAGS_enable_minidumps = false;
}

MiniMaster::~MiniMaster() {
  Shutdown();
}

Status MiniMaster::Start() {
  FLAGS_rpc_server_allow_ephemeral_ports = true;
  MasterOptions opts;
  HostPort web_bind_addr(rpc_bind_addr_.host(), /*port=*/ 0);
  RETURN_NOT_OK(StartOnAddrs(rpc_bind_addr_, web_bind_addr, &opts));
  return master_->WaitForCatalogManagerInit();
}

Status MiniMaster::StartDistributedMaster(vector<HostPort> peer_addrs) {
  HostPort web_bind_addr(rpc_bind_addr_.host(), /*port=*/ 0);
  return StartDistributedMasterOnAddrs(rpc_bind_addr_, web_bind_addr,
                                       std::move(peer_addrs));
}

Status MiniMaster::Restart() {
  MasterOptions opts;
  RETURN_NOT_OK(StartOnAddrs(HostPort(bound_rpc_), HostPort(bound_http_), &opts));
  return WaitForCatalogManagerInit();
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

Status MiniMaster::StartOnAddrs(const HostPort& rpc_bind_addr,
                                const HostPort& web_bind_addr,
                                MasterOptions* opts) {
  CHECK(!master_);

  opts->rpc_opts.rpc_bind_addresses = rpc_bind_addr.ToString();
  opts->webserver_opts.bind_interface = web_bind_addr.host();
  opts->webserver_opts.port = web_bind_addr.port();
  opts->fs_opts.wal_path = fs_root_;
  opts->fs_opts.data_paths = { fs_root_ };

  unique_ptr<Master> server(new Master(*opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->StartAsync());
  master_.swap(server);

  return Status::OK();
}

Status MiniMaster::StartDistributedMasterOnAddrs(const HostPort& rpc_bind_addr,
                                                 const HostPort& web_bind_addr,
                                                 std::vector<HostPort> peer_addrs) {
  MasterOptions opts;
  opts.master_addresses = std::move(peer_addrs);
  return StartOnAddrs(rpc_bind_addr, web_bind_addr, &opts);
}

} // namespace master
} // namespace kudu
