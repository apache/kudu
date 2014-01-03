// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MASTER_SERVICE_H
#define KUDU_MASTER_MASTER_SERVICE_H

#include <tr1/memory>

#include "gutil/macros.h"
#include "master/master.service.h"
#include "util/metrics.h"

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

  virtual void GetTabletLocations(const GetTabletLocationsRequestPB* req,
                                  GetTabletLocationsResponsePB* resp,
                                  rpc::RpcContext* rpc);

  virtual void CreateTable(const CreateTableRequestPB* req,
                           CreateTableResponsePB* resp,
                           rpc::RpcContext* rpc);
  virtual void DeleteTable(const DeleteTableRequestPB* req,
                           DeleteTableResponsePB* resp,
                           rpc::RpcContext* rpc);
  virtual void ListTables(const ListTablesRequestPB* req,
                          ListTablesResponsePB* resp,
                          rpc::RpcContext* rpc);
  virtual void GetTableLocations(const GetTableLocationsRequestPB* req,
                                 GetTableLocationsResponsePB* resp,
                                 rpc::RpcContext* rpc);

 private:
  Master* server_;

  DISALLOW_COPY_AND_ASSIGN(MasterServiceImpl);
};

} // namespace master
} // namespace kudu

#endif
