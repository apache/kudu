// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_MASTER_MASTER_H
#define KUDU_MASTER_MASTER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/master/master_options.h"
#include "kudu/master/master.pb.h"
#include "kudu/server/server_base.h"
#include "kudu/util/metrics.h"
#include "kudu/util/promise.h"
#include "kudu/util/status.h"

namespace kudu {

class MaintenanceManager;
class RpcServer;
struct RpcServerOptions;
class ServerEntryPB;
class ServerRegistrationPB;
class ThreadPool;

namespace rpc {
class Messenger;
class ServicePool;
}

namespace master {

class CatalogManager;
class TSManager;
class MasterPathHandlers;

class Master : public server::ServerBase {
 public:
  static const uint16_t kDefaultPort = 7051;
  static const uint16_t kDefaultWebPort = 8051;

  explicit Master(const MasterOptions& opts);
  ~Master();

  Status Init();
  Status Start();

  Status StartAsync();
  Status WaitForCatalogManagerInit();

  // Wait until this Master's catalog manager instance is the leader and is ready.
  // This method is intended for use by unit tests.
  // If 'timeout' time is exceeded, returns Status::TimedOut.
  Status WaitUntilCatalogManagerIsLeaderAndReadyForTests(const MonoDelta& timeout)
      WARN_UNUSED_RESULT;

  void Shutdown();

  std::string ToString() const;

  TSManager* ts_manager() { return ts_manager_.get(); }

  CatalogManager* catalog_manager() { return catalog_manager_.get(); }

  const MasterOptions& opts() { return opts_; }

  // Get the RPC and HTTP addresses for this master instance.
  Status GetMasterRegistration(ServerRegistrationPB* registration) const;

  // Get node instance, Raft role, RPC and HTTP addresses for all
  // masters.
  //
  // TODO move this to a separate class to be re-used in TS and
  // client; cache this information with a TTL (possibly in another
  // SysTable), so that we don't have to perform an RPC call on every
  // request.
  Status ListMasters(std::vector<ServerEntryPB>* masters) const;

  bool IsShutdown() const {
    return state_ == kStopped;
  }

  MaintenanceManager* maintenance_manager() {
    return maintenance_manager_.get();
  }

 private:
  friend class MasterTest;

  enum MasterState {
    kStopped,
    kInitialized,
    kRunning
  };

  void InitCatalogManagerTask();
  Status InitCatalogManager();

  MasterState state_;

  gscoped_ptr<TSManager> ts_manager_;
  gscoped_ptr<CatalogManager> catalog_manager_;
  gscoped_ptr<MasterPathHandlers> path_handlers_;

  // For initializing the catalog manager.
  gscoped_ptr<ThreadPool> init_pool_;

  // The status of the master initialization. This is set
  // by the async initialization task.
  Promise<Status> init_status_;

  MasterOptions opts_;

  // The maintenance manager for this master.
  std::tr1::shared_ptr<MaintenanceManager> maintenance_manager_;

  DISALLOW_COPY_AND_ASSIGN(Master);
};

} // namespace master
} // namespace kudu
#endif
