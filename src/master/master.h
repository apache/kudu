// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MASTER_H
#define KUDU_MASTER_MASTER_H

#include <string>
#include <tr1/memory>

#include <gtest/gtest.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "master/master_options.h"
#include "server/server_base.h"
#include "util/status.h"

namespace kudu {

class RpcServer;
struct RpcServerOptions;

namespace rpc {
class Messenger;
class ServicePool;
}

namespace master {

class MTabletManager;
class TSManager;

class Master : public server::ServerBase {
 public:
  static const uint16_t kDefaultPort = 7051;
  static const uint16_t kDefaultWebPort = 8051;

  explicit Master(const MasterOptions& opts);
  ~Master();

  Status Init();
  Status Start();
  Status Shutdown();

  string ToString() const;

  TSManager* ts_manager() { return ts_manager_.get(); }

  MTabletManager* tablet_manager() { return tablet_manager_.get(); }

 private:
  friend class MasterTest;

  RpcServer *rpc_server() const { return rpc_server_.get(); }

  bool initted_;

  gscoped_ptr<TSManager> ts_manager_;
  gscoped_ptr<MTabletManager> tablet_manager_;

  DISALLOW_COPY_AND_ASSIGN(Master);
};

} // namespace master
} // namespace kudu
#endif
