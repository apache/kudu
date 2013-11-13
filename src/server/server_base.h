// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_SERVER_SERVER_BASE_H
#define KUDU_SERVER_SERVER_BASE_H

#include <string>
#include <tr1/memory>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "rpc/service_if.h"
#include "server/rpc_server.h"
#include "server/webserver_options.h"
#include "util/status.h"

namespace kudu {

class Env;
class FsManager;
class MetricRegistry;
class NodeInstancePB;
class Sockaddr;
class Webserver;

namespace rpc {
class Messenger;
class ServiceIf;
} // namespace rpc

namespace server {

// Base class for tablet server and master.
// Handles starting and stopping the RPC server and web server,
// and provides a common interface for server-type-agnostic functions.
class ServerBase {
 public:
  const RpcServer *rpc_server() const { return rpc_server_.get(); }
  const Webserver *web_server() const { return web_server_.get(); }
  const std::tr1::shared_ptr<rpc::Messenger>& messenger() const { return messenger_; }

  // Return the first RPC address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_rpc_address() const;

  // Return the first HTTP address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_http_address() const;

  FsManager* fs_manager() { return fs_manager_.get(); }

  // Return the instance identifier of this server.
  // This may not be called until after the server is Initted.
  const NodeInstancePB& instance_pb() const;

 protected:
  ServerBase(Env* env, const std::string& base_dir,
             const RpcServerOptions& rpc_opts,
             const WebserverOptions& web_opts);
  virtual ~ServerBase();

  Status Init();
  Status Start(gscoped_ptr<rpc::ServiceIf> rpc_impl);
  Status Shutdown();

  gscoped_ptr<MetricRegistry> metric_registry_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<RpcServer> rpc_server_;
  gscoped_ptr<Webserver> web_server_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;

  // The instance identifier of this server.
  gscoped_ptr<NodeInstancePB> instance_pb_;

 private:
  Status GenerateInstanceID();

  DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_H */
