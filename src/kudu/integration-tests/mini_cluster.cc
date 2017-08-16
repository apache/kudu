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

#include "kudu/integration-tests/mini_cluster.h"


#include "kudu/client/client.h"
#include "kudu/gutil/strings/join.h"
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
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using strings::Substitute;

namespace kudu {

using client::KuduClient;
using client::KuduClientBuilder;
using master::CatalogManager;
using master::MasterServiceProxy;
using master::MiniMaster;
using master::TSDescriptor;
using std::shared_ptr;
using tserver::MiniTabletServer;
using tserver::TabletServer;

MiniClusterOptions::MiniClusterOptions()
 :  num_masters(1),
    num_tablet_servers(1) {
}

MiniCluster::MiniCluster(Env* env, const MiniClusterOptions& options)
  : running_(false),
    env_(env),
    fs_root_(!options.data_root.empty() ? options.data_root :
                JoinPathSegments(GetTestDataDirectory(), "minicluster-data")),
    num_masters_initial_(options.num_masters),
    num_ts_initial_(options.num_tablet_servers),
    master_rpc_ports_(options.master_rpc_ports),
    tserver_rpc_ports_(options.tserver_rpc_ports) {
}

MiniCluster::~MiniCluster() {
  Shutdown();
}

Status MiniCluster::Start() {
  CHECK(!fs_root_.empty()) << "No Fs root was provided";
  CHECK(!running_);

  if (num_masters_initial_ > 1) {
    CHECK_GE(master_rpc_ports_.size(), num_masters_initial_);
  }

  if (!env_->FileExists(fs_root_)) {
    RETURN_NOT_OK(env_->CreateDir(fs_root_));
  }

  // start the masters
  if (num_masters_initial_ > 1) {
    RETURN_NOT_OK_PREPEND(StartDistributedMasters(),
                          "Couldn't start distributed masters");
  } else {
    RETURN_NOT_OK_PREPEND(StartSingleMaster(), "Couldn't start the single master");
  }

  for (int i = 0; i < num_ts_initial_; i++) {
    RETURN_NOT_OK_PREPEND(AddTabletServer(),
                          Substitute("Error adding TS $0", i));
  }

  RETURN_NOT_OK_PREPEND(WaitForTabletServerCount(num_ts_initial_),
                        "Waiting for tablet servers to start");

  RETURN_NOT_OK_PREPEND(rpc::MessengerBuilder("minicluster-messenger")
                        .set_num_reactors(1)
                        .set_max_negotiation_threads(1)
                        .Build(&messenger_),
                        "Failed to start Messenger for minicluster");

  running_ = true;
  return Status::OK();
}

Status MiniCluster::StartDistributedMasters() {
  CHECK_GE(master_rpc_ports_.size(), num_masters_initial_);
  CHECK_GT(master_rpc_ports_.size(), 1);

  LOG(INFO) << "Creating distributed mini masters. Ports: "
            << JoinInts(master_rpc_ports_, ", ");

  for (int i = 0; i < num_masters_initial_; i++) {
    gscoped_ptr<MiniMaster> mini_master(
        new MiniMaster(env_, GetMasterFsRoot(i), master_rpc_ports_[i]));
    RETURN_NOT_OK_PREPEND(mini_master->StartDistributedMaster(master_rpc_ports_),
                          Substitute("Couldn't start follower $0", i));
    VLOG(1) << "Started MiniMaster with UUID " << mini_master->permanent_uuid()
            << " at index " << i;
    mini_masters_.push_back(shared_ptr<MiniMaster>(mini_master.release()));
  }
  int i = 0;
  for (const shared_ptr<MiniMaster>& master : mini_masters_) {
    LOG(INFO) << "Waiting to initialize catalog manager on master " << i++;
    RETURN_NOT_OK_PREPEND(master->WaitForCatalogManagerInit(),
                          Substitute("Could not initialize catalog manager on master $0", i));
  }
  return Status::OK();
}

Status MiniCluster::StartSync() {
  RETURN_NOT_OK(Start());
  int count = 0;
  for (const shared_ptr<MiniTabletServer>& tablet_server : mini_tablet_servers_) {
    RETURN_NOT_OK_PREPEND(tablet_server->WaitStarted(),
                          Substitute("TabletServer $0 failed to start.", count));
    count++;
  }
  return Status::OK();
}

Status MiniCluster::StartSingleMaster() {
  CHECK_EQ(1, num_masters_initial_);
  CHECK_LE(master_rpc_ports_.size(), 1);
  uint16_t master_rpc_port = 0;
  if (master_rpc_ports_.size() == 1) {
    master_rpc_port = master_rpc_ports_[0];
  }

  // start the master (we need the port to set on the servers).
  gscoped_ptr<MiniMaster> mini_master(
      new MiniMaster(env_, GetMasterFsRoot(0), master_rpc_port));
  RETURN_NOT_OK_PREPEND(mini_master->Start(), "Couldn't start master");
  RETURN_NOT_OK(mini_master->master()->
      WaitUntilCatalogManagerIsLeaderAndReadyForTests(MonoDelta::FromSeconds(5)));
  mini_masters_.push_back(shared_ptr<MiniMaster>(mini_master.release()));
  return Status::OK();
}

Status MiniCluster::AddTabletServer() {
  if (mini_masters_.empty()) {
    return Status::IllegalState("Master not yet initialized");
  }
  int new_idx = mini_tablet_servers_.size();

  uint16_t ts_rpc_port = 0;
  if (tserver_rpc_ports_.size() > new_idx) {
    ts_rpc_port = tserver_rpc_ports_[new_idx];
  }
  gscoped_ptr<MiniTabletServer> tablet_server(
    new MiniTabletServer(GetTabletServerFsRoot(new_idx), ts_rpc_port));

  // set the master addresses
  tablet_server->options()->master_addresses.clear();
  for (const shared_ptr<MiniMaster>& master : mini_masters_) {
    tablet_server->options()->master_addresses.push_back(HostPort(master->bound_rpc_addr()));
  }
  RETURN_NOT_OK(tablet_server->Start())
  mini_tablet_servers_.push_back(shared_ptr<MiniTabletServer>(tablet_server.release()));
  return Status::OK();
}

void MiniCluster::ShutdownNodes(ClusterNodes nodes) {
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
    mini_masters_.clear();
  }
  running_ = false;
}

MiniMaster* MiniCluster::mini_master(int idx) const {
  CHECK_GE(idx, 0) << "Master idx must be >= 0";
  CHECK_LT(idx, mini_masters_.size()) << "Master idx must be < num masters started";
  return mini_masters_[idx].get();
}

MiniTabletServer* MiniCluster::mini_tablet_server(int idx) const {
  CHECK_GE(idx, 0) << "TabletServer idx must be >= 0";
  CHECK_LT(idx, mini_tablet_servers_.size()) << "TabletServer idx must be < 'num_ts_started_'";
  return mini_tablet_servers_[idx].get();
}

string MiniCluster::GetMasterFsRoot(int idx) const {
  return JoinPathSegments(fs_root_, Substitute("master-$0-root", idx));
}

string MiniCluster::GetTabletServerFsRoot(int idx) const {
  return JoinPathSegments(fs_root_, Substitute("ts-$0-root", idx));
}

Status MiniCluster::WaitForTabletServerCount(int count) const {
  vector<shared_ptr<master::TSDescriptor>> descs;
  return WaitForTabletServerCount(count, MatchMode::MATCH_TSERVERS, &descs);
}

Status MiniCluster::WaitForTabletServerCount(int count,
                                             MatchMode mode,
                                             vector<shared_ptr<TSDescriptor>>* descs) const {
  unordered_set<int> masters_to_search;
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
            for (auto mini_tablet_server : mini_tablet_servers_) {
              auto ts = mini_tablet_server->server();
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

Status MiniCluster::CreateClient(KuduClientBuilder* builder,
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

Status MiniCluster::GetLeaderMasterIndex(int* idx) const {
  const MonoTime deadline = MonoTime::Now() +
      MonoDelta::FromSeconds(kMasterStartupWaitTimeSeconds);

  int leader_idx = -1;
  while (MonoTime::Now() < deadline) {
    for (int i = 0; i < num_masters(); i++) {
      master::MiniMaster* mm = mini_master(i);
      if (!mm->is_running() || mm->master()->IsShutdown()) {
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

std::shared_ptr<rpc::Messenger> MiniCluster::messenger() const {
  return messenger_;
}

std::shared_ptr<MasterServiceProxy> MiniCluster::master_proxy() const {
  CHECK_EQ(1, mini_masters_.size());
  return master_proxy(0);
}

std::shared_ptr<MasterServiceProxy> MiniCluster::master_proxy(int idx) const {
  const auto& addr = CHECK_NOTNULL(mini_master(idx))->bound_rpc_addr();
  return std::make_shared<MasterServiceProxy>(messenger_, addr, addr.host());
}

} // namespace kudu
