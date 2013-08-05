// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_service.h"
#include "tserver/tablet_server.h"

namespace kudu {
namespace tserver {

TabletServiceImpl::TabletServiceImpl(TabletServer* server)
  : server_(server) {
}

void TabletServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* context) {
  context->RespondSuccess();
}

} // namespace tserver
} // namespace kudu
