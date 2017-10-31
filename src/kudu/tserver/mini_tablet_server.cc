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

#include "kudu/tserver/mini_tablet_server.h"

#include <ostream>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/webserver_options.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

DECLARE_bool(enable_minidumps);
DECLARE_bool(rpc_server_allow_ephemeral_ports);

using kudu::tablet::TabletReplica;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::RaftConfigPB;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

MiniTabletServer::MiniTabletServer(string fs_root,
                                   const HostPort& rpc_bind_addr,
                                   int num_data_dirs)
    : fs_root_(std::move(fs_root)) {
  // Disable minidump handler (we allow only one per process).
  FLAGS_enable_minidumps = false;
  // Start RPC server on loopback.
  FLAGS_rpc_server_allow_ephemeral_ports = true;
  opts_.rpc_opts.rpc_bind_addresses = rpc_bind_addr.ToString();
  opts_.webserver_opts.bind_interface = rpc_bind_addr.host();
  opts_.webserver_opts.port = 0;
  if (num_data_dirs == 1) {
    opts_.fs_opts.wal_root = fs_root_;
    opts_.fs_opts.data_roots = { fs_root_ };
  } else {
    vector<string> fs_data_dirs;
    for (int dir = 0; dir < num_data_dirs; dir++) {
      fs_data_dirs.emplace_back(JoinPathSegments(fs_root_, Substitute("data-$0", dir)));
    }
    opts_.fs_opts.wal_root = JoinPathSegments(fs_root_, "wal");
    opts_.fs_opts.data_roots = fs_data_dirs;
  }
}

MiniTabletServer::~MiniTabletServer() {
  Shutdown();
}

Status MiniTabletServer::Start() {
  CHECK(!server_);
  // In case the wal dir and data dirs are subdirectories of the root directory,
  // ensure the root directory exists.
  RETURN_NOT_OK(env_util::CreateDirIfMissing(Env::Default(), fs_root_));
  unique_ptr<TabletServer> server(new TabletServer(opts_));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->Start());
  server_.swap(server);

  return Status::OK();
}

Status MiniTabletServer::WaitStarted() {
  return server_->WaitInited();
}

void MiniTabletServer::Shutdown() {
  if (server_) {
    // Save the bound addrs back into the options structure so that, if we restart the
    // server, it will come back on the same address. This is necessary since we don't
    // currently support tablet servers re-registering on different ports (KUDU-418).
    opts_.rpc_opts.rpc_bind_addresses = bound_rpc_addr().ToString();
    opts_.webserver_opts.port = bound_http_addr().port();
    server_->Shutdown();
    server_.reset();
  }
}

Status MiniTabletServer::Restart() {
  return Start();
}

RaftConfigPB MiniTabletServer::CreateLocalConfig() const {
  CHECK(server_) << "must call Start() first";
  RaftConfigPB config;
  RaftPeerPB* peer = config.add_peers();
  peer->set_permanent_uuid(server_->instance_pb().permanent_uuid());
  peer->set_member_type(RaftPeerPB::VOTER);
  peer->mutable_last_known_addr()->set_host(bound_rpc_addr().host());
  peer->mutable_last_known_addr()->set_port(bound_rpc_addr().port());
  return config;
}

Status MiniTabletServer::AddTestTablet(const std::string& table_id,
                                       const std::string& tablet_id,
                                       const Schema& schema) {
  return AddTestTablet(table_id, tablet_id, schema, CreateLocalConfig());
}

Status MiniTabletServer::AddTestTablet(const std::string& table_id,
                                       const std::string& tablet_id,
                                       const Schema& schema,
                                       const RaftConfigPB& config) {
  Schema schema_with_ids = SchemaBuilder(schema).Build();
  pair<PartitionSchema, Partition> partition = tablet::CreateDefaultPartition(schema_with_ids);

  return server_->tablet_manager()->CreateNewTablet(
      table_id, tablet_id, partition.second, table_id,
      schema_with_ids, partition.first, config, nullptr);
}

vector<string> MiniTabletServer::ListTablets() const {
  vector<string> tablet_ids;
  vector<scoped_refptr<TabletReplica>> replicas;
  server_->tablet_manager()->GetTabletReplicas(&replicas);
  for (const auto& replica : replicas) {
    tablet_ids.push_back(replica->tablet_id());
  }
  return tablet_ids;
}

void MiniTabletServer::FailHeartbeats() {
  server_->set_fail_heartbeats_for_tests(true);
}

const Sockaddr MiniTabletServer::bound_rpc_addr() const {
  return server_->first_rpc_address();
}

const Sockaddr MiniTabletServer::bound_http_addr() const {
  return server_->first_http_address();
}

const string& MiniTabletServer::uuid() const {
  return server_->fs_manager()->uuid();
}

} // namespace tserver
} // namespace kudu
