// Copyright (c) 2013, Cloudera, inc.

#include "server/server_base.h"

#include "server/default-path-handlers.h"
#include "server/rpc_server.h"
#include "server/webserver.h"

namespace kudu {
namespace server {

ServerBase::ServerBase(const RpcServerOptions& rpc_opts,
                       const WebserverOptions& web_opts)
  : rpc_server_(new RpcServer(rpc_opts)),
    web_server_(new Webserver(web_opts)) {
}

ServerBase::~ServerBase() {
  web_server_->Stop();
  rpc_server_->Shutdown();
}

Status ServerBase::Init() {
  RETURN_NOT_OK(rpc_server_->Init());
  return Status::OK();
}

Status ServerBase::Start(gscoped_ptr<rpc::ServiceIf> rpc_impl) {
  RETURN_NOT_OK(rpc_server_->Start(rpc_impl.Pass()));

  AddDefaultPathHandlers(web_server_.get());
  RETURN_NOT_OK(web_server_->Start());
  return Status::OK();
}

} // namespace server
} // namespace kudu
