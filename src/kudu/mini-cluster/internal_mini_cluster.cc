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

#include "kudu/mini-cluster/internal_mini_cluster.h"

#include <cstdint>
#include <memory>
#include <ostream>
#include <unordered_set>
#include <utility>

#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using std::unique_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cluster {

using client::KuduClient;
using client::KuduClientBuilder;
using master::CatalogManager;
using master::MasterServiceProxy;
using master::MiniMaster;
using master::TSDescriptor;
using std::shared_ptr;
using tserver::MiniTabletServer;
using tserver::TabletServer;
using tserver::TabletServerServiceProxy;

InternalMiniClusterOptions::InternalMiniClusterOptions()
  : num_masters(1),
    num_tablet_servers(1),
    num_data_dirs(1),
    bind_mode(MiniCluster::kDefaultBindMode) {
}

InternalMiniCluster::InternalMiniCluster(Env* env, InternalMiniClusterOptions options)
  : env_(env),
    opts_(std::move(options)),
    running_(false) {
  if (opts_.cluster_root.empty()) {
    opts_.cluster_root = JoinPathSegments(GetTestDataDirectory(), "minicluster-data");
  }
}

InternalMiniCluster::~InternalMiniCluster() {
  Shutdown();
}

Status InternalMiniCluster::Start() {
  CHECK(!opts_.cluster_root.empty()) << "No cluster root was provided";
  CHECK(!running_);

  if (!env_->FileExists(opts_.cluster_root)) {
    RETURN_NOT_OK(env_->CreateDir(opts_.cluster_root));
  }

  RETURN_NOT_OK_PREPEND(StartMasters(), "Couldn't start masters");

  for (int i = 0; i < opts_.num_tablet_servers; i++) {
    RETURN_NOT_OK_PREPEND(AddTabletServer(),
                          Substitute("Error adding TS $0", i));
  }

  RETURN_NOT_OK_PREPEND(WaitForTabletServerCount(opts_.num_tablet_servers),
                        "Waiting for tablet servers to start");

  RETURN_NOT_OK_PREPEND(rpc::MessengerBuilder("minicluster-messenger")
                        .set_num_reactors(1)
                        .set_max_negotiation_threads(1)
                        .Build(&messenger_),
                        "Failed to start Messenger for minicluster");

  running_ = true;
  return Status::OK();
}

Status InternalMiniCluster::StartMasters() {
  int num_masters = opts_.num_masters;

  // Collect and keep alive the set of master sockets bound with SO_REUSEPORT
  // until all master proccesses are started. This allows the mini-cluster to
  // reserve a set of ports up front, then later start the set of masters, each
  // configured with the full set of ports.
  //
  // TODO(dan): re-bind the ports between node restarts in order to prevent other
  // processess from binding to them in the interim.
  vector<unique_ptr<Socket>> reserved_sockets;

  if (mini_masters_.empty()) {
    vector<HostPort> master_rpc_addrs;
    for (int i = 0; i < num_masters; i++) {
      unique_ptr<Socket> reserved_socket;
      RETURN_NOT_OK_PREPEND(ReserveDaemonSocket(MiniCluster::MASTER, i, opts_.bind_mode,
                                                &reserved_socket),
                            "failed to reserve master socket address");

      Sockaddr addr;
      RETURN_NOT_OK(reserved_socket->GetSocketAddress(&addr));

      master_rpc_addrs.emplace_back(addr.host(), addr.port());
      reserved_sockets.emplace_back(std::move(reserved_socket));
    }

    LOG(INFO) << "Creating distributed mini masters. Addrs: "
              << HostPort::ToCommaSeparatedString(master_rpc_addrs);

    for (int i = 0; i < num_masters; i++) {
      shared_ptr<MiniMaster> mini_master(new MiniMaster(GetMasterFsRoot(i), master_rpc_addrs[i]));
      if (num_masters > 1) {
        mini_master->SetMasterAddresses(master_rpc_addrs);
      }
      mini_masters_.emplace_back(std::move(mini_master));
    }
  }

  CHECK_EQ(num_masters, mini_masters_.size());
  for (int i = 0; i < num_masters; i++) {
    RETURN_NOT_OK_PREPEND(mini_masters_[i]->Start(), Substitute("failed to start master $0", i));
    VLOG(1) << "Started MiniMaster with UUID " << mini_masters_[i]->permanent_uuid()
            << " at index " << i;
  }

  for (int i = 0; i < num_masters; i++) {
    LOG(INFO) << "Waiting to initialize catalog manager on master " << i;
    RETURN_NOT_OK_PREPEND(mini_masters_[i]->WaitForCatalogManagerInit(),
                          Substitute("Could not initialize catalog manager on master $0", i));
  }

  if (num_masters == 1) {
    RETURN_NOT_OK(mini_masters_[0]->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
          MonoDelta::FromSeconds(kMasterStartupWaitTimeSeconds)));
  }

  return Status::OK();
}

Status InternalMiniCluster::StartSync() {
  RETURN_NOT_OK(Start());
  int count = 0;
  for (const shared_ptr<MiniTabletServer>& tablet_server : mini_tablet_servers_) {
    RETURN_NOT_OK_PREPEND(tablet_server->WaitStarted(),
                          Substitute("TabletServer $0 failed to start.", count));
    count++;
  }
  return Status::OK();
}

Status InternalMiniCluster::AddTabletServer() {
  if (mini_masters_.empty()) {
    return Status::IllegalState("Master not yet initialized");
  }
  int new_idx = mini_tablet_servers_.size();

  uint16_t ts_rpc_port = 0;
  if (opts_.tserver_rpc_ports.size() > new_idx) {
    ts_rpc_port = opts_.tserver_rpc_ports[new_idx];
  }

  string bind_ip = GetBindIpForDaemon(MiniCluster::TSERVER, new_idx, opts_.bind_mode);
  gscoped_ptr<MiniTabletServer> tablet_server(new MiniTabletServer(GetTabletServerFsRoot(new_idx),
                                                                   HostPort(bind_ip, ts_rpc_port),
                                                                   opts_.num_data_dirs));

  // set the master addresses
  tablet_server->options()->master_addresses = master_rpc_addrs();
  RETURN_NOT_OK(tablet_server->Start())
  mini_tablet_servers_.push_back(shared_ptr<MiniTabletServer>(tablet_server.release()));
  return Status::OK();
}

void InternalMiniCluster::ShutdownNodes(ClusterNodes nodes) {
  if (nodes == ClusterNodes::ALL || nodes == ClusterNodes::TS_ONLY) {
    for (const shared_ptr<MiniTabletServer>& tablet_server : mini_tablet_servers_) {
      tablet_server->Shutdown();
    }
    mini_tablet_servers_.clear();
  }
  if (nodes == ClusterNodes::ALL || nodes == ClusterNodes::MASTERS_ONLY) {
    for (const shared_ptr<MiniMaster>& master_server : mini_masters_) {
      master_server->Shutdown();
    }
  }
  running_ = false;
}

MiniMaster* InternalMiniCluster::mini_master(int idx) const {
  CHECK_GE(idx, 0) << "Master idx must be >= 0";
  CHECK_LT(idx, mini_masters_.size()) << "Master idx must be < num masters started";
  return mini_masters_[idx].get();
}

MiniTabletServer* InternalMiniCluster::mini_tablet_server(int idx) const {
  CHECK_GE(idx, 0) << "TabletServer idx must be >= 0";
  CHECK_LT(idx, mini_tablet_servers_.size()) << "TabletServer idx must be < 'num_ts_started_'";
  return mini_tablet_servers_[idx].get();
}

MiniTabletServer* InternalMiniCluster::mini_tablet_server_by_uuid(const string& uuid) const {
  for (const auto& ts : mini_tablet_servers_) {
    if (ts->uuid() == uuid) {
      return ts.get();
    }
  }
  return nullptr;
}

int InternalMiniCluster::tablet_server_index_by_uuid(const std::string& uuid) const {
  for (int i = 0; i < mini_tablet_servers_.size(); i++) {
    if (mini_tablet_servers_[i]->uuid() == uuid) {
      return i;
    }
  }
  return -1;
}

vector<HostPort> InternalMiniCluster::master_rpc_addrs() const {
  vector<HostPort> master_hostports;
  for (const auto& master : mini_masters_) {
      Sockaddr add = master->bound_rpc_addr();
      master_hostports.emplace_back(add.host(), add.port());
  }
  return master_hostports;
}

string InternalMiniCluster::GetMasterFsRoot(int idx) const {
  return JoinPathSegments(opts_.cluster_root, Substitute("master-$0-root", idx));
}

string InternalMiniCluster::GetTabletServerFsRoot(int idx) const {
  return JoinPathSegments(opts_.cluster_root, Substitute("ts-$0-root", idx));
}

Status InternalMiniCluster::WaitForTabletServerCount(int count) const {
  vector<shared_ptr<master::TSDescriptor>> descs;
  return WaitForTabletServerCount(count, MatchMode::MATCH_TSERVERS, &descs);
}

Status InternalMiniCluster::WaitForTabletServerCount(int count,
                                             MatchMode mode,
                                             vector<shared_ptr<TSDescriptor>>* descs) const {
  std::unordered_set<int> masters_to_search;
  for (int i = 0; i < num_masters(); i++) {
    if (!mini_master(i)->master()->IsShutdown()) {
      masters_to_search.insert(i);
    }
  }

  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall_seconds() < kRegistrationWaitTimeSeconds) {
    for (auto iter = masters_to_search.begin(); iter != masters_to_search.end();) {
      mini_master(*iter)->master()->ts_manager()->GetAllDescriptors(descs);
      int match_count = 0;
      switch (mode) {
        case MatchMode::MATCH_TSERVERS:
          // GetAllDescriptors() may return servers that are no longer online.
          // Do a second step of verification to verify that the descs that we got
          // are aligned (same uuid/seqno) with the TSs that we have in the cluster.
          for (const shared_ptr<TSDescriptor>& desc : *descs) {
            for (const auto& mini_tablet_server : mini_tablet_servers_) {
              const TabletServer* ts = mini_tablet_server->server();
              if (ts->instance_pb().permanent_uuid() == desc->permanent_uuid() &&
                  ts->instance_pb().instance_seqno() == desc->latest_seqno()) {
                match_count++;
                break;
              }
            }
          }
          break;
        case MatchMode::DO_NOT_MATCH_TSERVERS:
          match_count = descs->size();
          break;
        default:
          LOG(FATAL) << "Invalid match mode";
      }

      if (match_count == count) {
        // This master has returned the correct set of tservers.
        iter = masters_to_search.erase(iter);
      } else {
        iter++;
      }
    }
    if (masters_to_search.empty()) {
      // All masters have returned the correct set of tservers.
      LOG(INFO) << Substitute("$0 TS(s) registered with all masters after $1s",
                              count, sw.elapsed().wall_seconds());
      return Status::OK();
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
  return Status::TimedOut(Substitute(
      "Timed out waiting for $0 TS(s) to register with all masters", count));
}

Status InternalMiniCluster::CreateClient(KuduClientBuilder* builder,
                                         client::sp::shared_ptr<KuduClient>* client) const {
  client::KuduClientBuilder defaults;
  if (builder == nullptr) {
    builder = &defaults;
  }

  builder->clear_master_server_addrs();
  for (const shared_ptr<MiniMaster>& master : mini_masters_) {
    CHECK(master);
    builder->add_master_server_addr(master->bound_rpc_addr_str());
  }
  return builder->Build(client);
}

Status InternalMiniCluster::GetLeaderMasterIndex(int* idx) const {
  const MonoTime deadline = MonoTime::Now() +
      MonoDelta::FromSeconds(kMasterStartupWaitTimeSeconds);

  int leader_idx = -1;
  while (MonoTime::Now() < deadline) {
    for (int i = 0; i < num_masters(); i++) {
      master::MiniMaster* mm = mini_master(i);
      if (!mm->is_started() || mm->master()->IsShutdown()) {
        continue;
      }
      master::CatalogManager* catalog = mm->master()->catalog_manager();
      master::CatalogManager::ScopedLeaderSharedLock l(catalog);
      if (l.first_failed_status().ok()) {
        leader_idx = i;
        break;
      }
    }
    if (leader_idx != -1) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  if (leader_idx == -1) {
    return Status::NotFound("Leader master was not found within deadline");
  }

  if (idx) {
    *idx = leader_idx;
  }
  return Status::OK();
}

std::shared_ptr<rpc::Messenger> InternalMiniCluster::messenger() const {
  return messenger_;
}

std::shared_ptr<MasterServiceProxy> InternalMiniCluster::master_proxy() const {
  CHECK_EQ(1, mini_masters_.size());
  return master_proxy(0);
}

std::shared_ptr<MasterServiceProxy> InternalMiniCluster::master_proxy(int idx) const {
  const auto& addr = CHECK_NOTNULL(mini_master(idx))->bound_rpc_addr();
  return std::make_shared<MasterServiceProxy>(messenger_, addr, addr.host());
}

std::shared_ptr<TabletServerServiceProxy> InternalMiniCluster::tserver_proxy(int idx) const {
  const auto& addr = CHECK_NOTNULL(mini_tablet_server(idx))->bound_rpc_addr();
  return std::make_shared<TabletServerServiceProxy>(messenger_, addr, addr.host());
}

string InternalMiniCluster::WalRootForTS(int ts_idx) const {
  return mini_tablet_server(ts_idx)->options()->fs_opts.wal_root;
}

string InternalMiniCluster::UuidForTS(int ts_idx) const {
  return mini_tablet_server(ts_idx)->uuid();
}

} // namespace cluster
} // namespace kudu
