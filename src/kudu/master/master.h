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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/kserver/kserver.h"
#include "kudu/master/master_options.h"
#include "kudu/util/promise.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class MaintenanceManager;
class MonoDelta;
class ThreadPool;

namespace rpc {
class RpcContext;
}  // namespace rpc

namespace master {
class LocationCache;
}  // namespace master

namespace security {
class TokenSigner;
} // namespace security

namespace transactions {
class TxnManager;
} // namespace transactions

namespace master {

class CatalogManager;
class MasterCertAuthority;
class MasterPathHandlers;
class TSManager;

class Master : public kserver::KuduServer {
 public:
  static const uint16_t kDefaultPort = 7051;
  static const uint16_t kDefaultWebPort = 8051;

  explicit Master(const MasterOptions& opts);
  ~Master();

  Status Init() override;
  Status Start() override;
  void Shutdown() override {
    ShutdownImpl();
  }

  Status StartAsync();
  Status WaitForCatalogManagerInit() const;
  Status WaitForTxnManagerInit(const MonoDelta& timeout = {}) const;

  // Wait until this Master's catalog manager instance is the leader and is ready.
  // This method is intended for use by unit tests.
  // If 'timeout' time is exceeded, returns Status::TimedOut.
  Status WaitUntilCatalogManagerIsLeaderAndReadyForTests(const MonoDelta& timeout)
      WARN_UNUSED_RESULT;

  MasterCertAuthority* cert_authority() { return cert_authority_.get(); }

  security::TokenSigner* token_signer() { return token_signer_.get(); }

  TSManager* ts_manager() { return ts_manager_.get(); }

  CatalogManager* catalog_manager() { return catalog_manager_.get(); }

  transactions::TxnManager* txn_manager() { return txn_manager_.get(); }

  const MasterOptions& opts() { return opts_; }

  LocationCache* location_cache() { return location_cache_.get(); }

  // Get the RPC and HTTP addresses for this master instance.
  Status GetMasterRegistration(ServerRegistrationPB* registration) const;

  // Get node instance, Raft role, RPC and HTTP addresses for all
  // masters.
  //
  // NOTE: this performs a round-trip RPC to all of the masters so
  // should not be used in any performance-critical paths.
  //
  // TODO(todd) move this to a separate class to be re-used in TS and
  // client; cache this information with a TTL (possibly in another
  // SysTable), so that we don't have to perform an RPC call on every
  // request.
  Status ListMasters(std::vector<ServerEntryPB>* masters) const;

  // Gets the HostPorts for all of the VOTER masters in the cluster.
  // This is not as complete as ListMasters() above, but operates just
  // based on local state.
  Status GetMasterHostPorts(std::vector<HostPort>* hostports) const;

  bool IsShutdown() const {
    return state_ == kStopped;
  }

  // Adds the master specified by 'hp' by initiating change config request.
  // RpContext 'rpc' will be used to respond back to the client asynchronously.
  // Returns the status of initiating the master addition request.
  Status AddMaster(const HostPort& hp, rpc::RpcContext* rpc);

  // Removes the master specified by 'hp' and optional 'uuid' by initiating change config request.
  // RpContext 'rpc' will be used to respond back to the client asynchronously.
  // Returns the status of initiating the master removal request.
  Status RemoveMaster(const HostPort& hp, const std::string& uuid, rpc::RpcContext* rpc);

  MaintenanceManager* maintenance_manager() {
    return maintenance_manager_.get();
  }

 private:
  friend class MasterTest;
  friend class CatalogManager;
  friend class transactions::TxnManager;

  void InitCatalogManagerTask();
  Status InitCatalogManager();

  void InitTxnManagerTask();
  Status InitTxnManager();
  Status ScheduleTxnManagerInit();

  // Initialize registration_.
  // Requires that the web server and RPC server have been started.
  Status InitMasterRegistration();

  // A method for internal use in the destructor. Some static code analyzers
  // issue a warning if calling a virtual function from destructor even if it's
  // safe in a particular case.
  void ShutdownImpl();

  enum MasterState {
    kStopped,
    kInitialized,
    kRunning,
    kStopping,
  };

  std::atomic<MasterState> state_;

  std::unique_ptr<MasterCertAuthority> cert_authority_;
  std::unique_ptr<security::TokenSigner> token_signer_;
  std::unique_ptr<CatalogManager> catalog_manager_;
  std::unique_ptr<transactions::TxnManager> txn_manager_;
  std::unique_ptr<MasterPathHandlers> path_handlers_;

  // The status of the catalog manager initialization. This is set
  // by the async initialization task.
  Promise<Status> catalog_manager_init_status_;

  // The status of the TxnManager initialization. This is set by an asynchronous
  // initialization task. The task to initialize TxnManager can be scheduled
  // either by master upon its start or from within TxnManager itself while
  // processing the very first RPC after start.
  Promise<Status> txn_manager_init_status_;

  // For initializing the catalog manager and TxnManager.
  std::unique_ptr<ThreadPool> init_pool_;

  MasterOptions opts_;

  ServerRegistrationPB registration_;
  // True once registration_ has been initialized.
  std::atomic<bool> registration_initialized_;

  // The maintenance manager for this master.
  std::shared_ptr<MaintenanceManager> maintenance_manager_;

  // A simplistic cache to track already assigned locations.
  std::unique_ptr<LocationCache> location_cache_;

  std::unique_ptr<TSManager> ts_manager_;

  DISALLOW_COPY_AND_ASSIGN(Master);
};

} // namespace master
} // namespace kudu
