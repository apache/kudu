// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVICE_H
#define KUDU_TSERVER_TABLET_SERVICE_H

#include "tserver/tserver.service.h"

namespace kudu {
namespace tserver {

class TabletServer;

class TabletServiceImpl : public TabletServerServiceIf {
 public:
  explicit TabletServiceImpl(TabletServer* server);

  virtual void Ping(const PingRequestPB* req,
                    PingResponsePB* resp,
                    rpc::RpcContext* context);

 private:
  TabletServer* server_;
};

} // namespace tserver
} // namespace kudu

#endif
