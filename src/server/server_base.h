// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_SERVER_SERVER_BASE_H
#define KUDU_SERVER_SERVER_BASE_H

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "rpc/service_if.h"
#include "server/rpc_server.h"
#include "server/webserver_options.h"
#include "util/status.h"

namespace kudu {

class Webserver;

namespace rpc {
class ServiceIf;
} // namespace rpc

namespace server {

// Base class for tablet server and master.
// Handles starting and stopping the RPC server and web server,
// and provides a common interface for server-type-agnostic functions.
//
// TODO: this is probably the hook point for things like the metrics registry
// and other shared infrastructure which both the master and the TS will need.
class ServerBase {
 public:
  const RpcServer *rpc_server() const { return rpc_server_.get(); }
  const Webserver *web_server() const { return web_server_.get(); }

 protected:
  ServerBase(const RpcServerOptions& rpc_opts,
             const WebserverOptions& web_opts);
  virtual ~ServerBase();

  Status Init();
  Status Start(gscoped_ptr<rpc::ServiceIf> rpc_impl);

  gscoped_ptr<RpcServer> rpc_server_;
  gscoped_ptr<Webserver> web_server_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_H */
