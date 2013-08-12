// Copyright (c) 2013, Cloudera, inc.

#include "master/master_server.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "server/rpc_server.h"
#include "master/master_service.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;

namespace kudu {
namespace master {

MasterServer::MasterServer(const RpcServerOptions& opts)
  : initted_(false),
    rpc_server_(new RpcServer(opts)) {
}

MasterServer::~MasterServer() {
  rpc_server_->Shutdown();
}

string MasterServer::ToString() const {
  // TODO: include port numbers, etc.
  return "MasterServer";
}

Status MasterServer::Init() {
  CHECK(!initted_);
  RETURN_NOT_OK(rpc_server_->Init(kDefaultPort));
  initted_ = true;
  return Status::OK();
}

Status MasterServer::Start() {
  CHECK(initted_);

  gscoped_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  RETURN_NOT_OK(rpc_server_->Start(impl.Pass()));

  return Status::OK();
}

} // namespace master
} // namespace kudu
