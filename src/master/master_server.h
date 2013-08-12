// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_TABLET_SERVER_H
#define KUDU_MASTER_TABLET_SERVER_H

#include <string>
#include <tr1/memory>

#include <gtest/gtest.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "master/master.pb.h"
#include "util/status.h"

namespace kudu {

class RpcServer;
struct RpcServerOptions;

namespace rpc {
class Messenger;
class ServicePool;
}

namespace master {

class MasterServer {
 public:
  static const uint16_t kDefaultPort = 7150;

  explicit MasterServer(const RpcServerOptions& opts);
  ~MasterServer();

  Status Init();
  Status Start();

  string ToString() const;

 private:
  friend class MasterServerTest;

  RpcServer *rpc_server() const { return rpc_server_.get(); }

  bool initted_;

  gscoped_ptr<RpcServer> rpc_server_;

  DISALLOW_COPY_AND_ASSIGN(MasterServer);
};

} // namespace master
} // namespace kudu
#endif
