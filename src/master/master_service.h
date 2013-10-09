// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MASTER_SERVICE_H
#define KUDU_MASTER_MASTER_SERVICE_H

#include <tr1/memory>

#include "gutil/macros.h"

#include "master/master.service.h"

namespace kudu {

class NodeInstancePB;

namespace master {

class Master;
class TSDescriptor;

// Implementation of the master service. See master.proto for docs
// on each RPC.
class MasterServiceImpl : public MasterServiceIf {
 public:
  explicit MasterServiceImpl(Master* server);

  virtual void Ping(const PingRequestPB* req,
                    PingResponsePB* resp,
                    rpc::RpcContext* rpc);

  virtual void TSHeartbeat(const TSHeartbeatRequestPB* req,
                           TSHeartbeatResponsePB* resp,
                           rpc::RpcContext* rpc);
 private:
  void SetMasterInstancePB(NodeInstancePB* pb) const;

  Master* server_;

  DISALLOW_COPY_AND_ASSIGN(MasterServiceImpl);
};

} // namespace master
} // namespace kudu

#endif
