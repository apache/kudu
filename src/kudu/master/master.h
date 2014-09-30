// Copyright (c) 2013, Cloudera, inc.
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
#include "kudu/util/status.h"

namespace kudu {

class RpcServer;
class Future;
class TaskExecutor;
struct RpcServerOptions;

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

  void Shutdown();

  std::string ToString() const;

  TSManager* ts_manager() { return ts_manager_.get(); }

  CatalogManager* catalog_manager() { return catalog_manager_.get(); }

  const MasterOptions& opts() { return opts_; }

  // Get the RPC and HTTP addresses for this master instance.
  Status GetMasterRegistration(MasterRegistrationPB* registration) const;

  // Get node instance, quorum role, RPC and HTTP addresses for all
  // masters.
  //
  // TODO move this to a separate class to be re-used in TS and
  // client; cache this information with a TTL (possibly in another
  // SysTable), so that we don't have to perform an RPC call on every
  // request.
  Status ListMasters(std::vector<ListMastersResponsePB::Entry>* masters) const;

 private:
  friend class MasterTest;

  enum MasterState {
    kStopped,
    kInitialized,
    kRunning
  };

  Status InitCatalogManager();

  MasterState state_;

  gscoped_ptr<TSManager> ts_manager_;
  gscoped_ptr<CatalogManager> catalog_manager_;
  gscoped_ptr<MasterPathHandlers> path_handlers_;

  // For initializing the catalog manager.
  gscoped_ptr<TaskExecutor> init_executor_;

  std::tr1::shared_ptr<Future> init_future_;

  MasterOptions opts_;

  DISALLOW_COPY_AND_ASSIGN(Master);
};

} // namespace master
} // namespace kudu
#endif
