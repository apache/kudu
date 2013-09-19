// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_TABLET_SERVER_H
#define KUDU_MASTER_TABLET_SERVER_H

#include <string>
#include <tr1/memory>

#include <gtest/gtest.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "master/master_server_options.h"
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

class TSManager;

class MasterServer : public server::ServerBase {
 public:
  static const uint16_t kDefaultPort = 7051;
  static const uint16_t kDefaultWebPort = 8051;

  explicit MasterServer(const MasterServerOptions& opts);
  ~MasterServer();

  Status Init();
  Status Start();

  string ToString() const;

  TSManager* ts_manager() { return ts_manager_.get(); }

 private:
  friend class MasterServerTest;

  RpcServer *rpc_server() const { return rpc_server_.get(); }

  bool initted_;

  gscoped_ptr<TSManager> ts_manager_;

  DISALLOW_COPY_AND_ASSIGN(MasterServer);
};

} // namespace master
} // namespace kudu
#endif
