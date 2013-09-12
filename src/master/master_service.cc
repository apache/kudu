// Copyright (c) 2013, Cloudera, inc.

#include "master/master_service.h"
#include "master/master_server.h"
#include "rpc/rpc_context.h"

namespace kudu {
namespace master {

MasterServiceImpl::MasterServiceImpl(MasterServer* server)
  : server_(server) {
}

void MasterServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* context) {
  context->RespondSuccess();
}

} // namespace master
} // namespace kudu
