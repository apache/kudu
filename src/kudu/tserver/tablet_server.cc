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

#include "kudu/tserver/tablet_server.h"

#include <ostream>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/cfile/block_cache.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/service_if.h"
#include "kudu/server/rpc_server.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_copy_service.h"
#include "kudu/tserver/tablet_service.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver_path_handlers.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

using std::string;
using std::vector;
using kudu::fs::ErrorHandlerType;
using kudu::rpc::ServiceIf;

namespace kudu {
namespace tserver {

TabletServer::TabletServer(const TabletServerOptions& opts)
  : KuduServer("TabletServer", opts, "kudu.tabletserver"),
    state_(kStopped),
    fail_heartbeats_for_tests_(false),
    opts_(opts),
    tablet_manager_(new TSTabletManager(this)),
    scanner_manager_(new ScannerManager(metric_entity())),
    path_handlers_(new TabletServerPathHandlers(this)) {
}

TabletServer::~TabletServer() {
  Shutdown();
}

Status TabletServer::Init() {
  CHECK_EQ(kStopped, state_);

  cfile::BlockCache::GetSingleton()->StartInstrumentation(metric_entity());

  UnorderedHostPortSet master_addrs;
  for (auto addr : opts_.master_addresses) {
    master_addrs.emplace(std::move(addr));
  }
  // If we deduplicated some masters addresses, log something about it.
  if (master_addrs.size() < opts_.master_addresses.size()) {
    vector<HostPort> addr_list;
    for (const auto& addr : master_addrs) {
      addr_list.emplace_back(addr);
    }
    LOG(INFO) << "deduplicated master addresses: "
              << HostPort::ToCommaSeparatedString(addr_list);
  }
  // Validate that the passed master address actually resolves.
  // We don't validate that we can connect at this point -- it should
  // be allowed to start the TS and the master in whichever order --
  // our heartbeat thread will loop until successfully connecting.
  for (const auto& addr : master_addrs) {
    RETURN_NOT_OK_PREPEND(dns_resolver()->ResolveAddresses(addr, nullptr),
        strings::Substitute("couldn't resolve master service address '$0'",
                            addr.ToString()));
  }

  RETURN_NOT_OK(KuduServer::Init());
  if (web_server_) {
    RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));
  }

  maintenance_manager_ = std::make_shared<MaintenanceManager>(
      MaintenanceManager::kDefaultOptions, fs_manager_->uuid());

  heartbeater_.reset(new Heartbeater(std::move(master_addrs), this));

  RETURN_NOT_OK_PREPEND(tablet_manager_->Init(),
                        "Could not init Tablet Manager");

  RETURN_NOT_OK_PREPEND(scanner_manager_->StartRemovalThread(),
                        "Could not start expired Scanner removal thread");

  state_ = kInitialized;
  return Status::OK();
}

Status TabletServer::WaitInited() {
  return tablet_manager_->WaitForAllBootstrapsToFinish();
}

Status TabletServer::Start() {
  CHECK_EQ(kInitialized, state_);

  fs_manager_->SetErrorNotificationCb(ErrorHandlerType::DISK_ERROR,
      Bind(&TSTabletManager::FailTabletsInDataDir, Unretained(tablet_manager_.get())));
  fs_manager_->SetErrorNotificationCb(ErrorHandlerType::CFILE_CORRUPTION,
      Bind(&TSTabletManager::FailTabletAndScheduleShutdown, Unretained(tablet_manager_.get())));

  gscoped_ptr<ServiceIf> ts_service(new TabletServiceImpl(this));
  gscoped_ptr<ServiceIf> admin_service(new TabletServiceAdminImpl(this));
  gscoped_ptr<ServiceIf> consensus_service(new ConsensusServiceImpl(this, tablet_manager_.get()));
  gscoped_ptr<ServiceIf> tablet_copy_service(new TabletCopyServiceImpl(
      this, tablet_manager_.get()));

  RETURN_NOT_OK(RegisterService(std::move(ts_service)));
  RETURN_NOT_OK(RegisterService(std::move(admin_service)));
  RETURN_NOT_OK(RegisterService(std::move(consensus_service)));
  RETURN_NOT_OK(RegisterService(std::move(tablet_copy_service)));
  RETURN_NOT_OK(KuduServer::Start());

  RETURN_NOT_OK(heartbeater_->Start());
  RETURN_NOT_OK(maintenance_manager_->Start());

  google::FlushLogFiles(google::INFO); // Flush the startup messages.

  state_ = kRunning;
  return Status::OK();
}

void TabletServer::Shutdown() {
  if (kInitialized == state_ || kRunning == state_) {
    const string name = rpc_server_->ToString();
    LOG(INFO) << "TabletServer@" << name << " shutting down...";

    // 1. Stop accepting new RPCs.
    UnregisterAllServices();

    // 2. Shut down the tserver's subsystems.
    maintenance_manager_->Shutdown();
    WARN_NOT_OK(heartbeater_->Stop(), "Failed to stop TS Heartbeat thread");
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::DISK_ERROR);
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::CFILE_CORRUPTION);
    tablet_manager_->Shutdown();

    // 3. Shut down generic subsystems.
    KuduServer::Shutdown();
    LOG(INFO) << "TabletServer@" << name << " shutdown complete.";
  }
  state_ = kStopped;
}

} // namespace tserver
} // namespace kudu
