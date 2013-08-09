// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_TABLET_SERVICE_H
#define KUDU_MASTER_TABLET_SERVICE_H

#include "master/master.service.h"

namespace kudu {
namespace master {

class MasterServer;

class MasterServiceImpl : public MasterServerServiceIf {
 public:
  explicit MasterServiceImpl(MasterServer* server);

  virtual void Ping(const PingRequestPB* req,
                    PingResponsePB* resp,
                    rpc::RpcContext* context);

 private:
  MasterServer* server_;
};

} // namespace master
} // namespace kudu

#endif
