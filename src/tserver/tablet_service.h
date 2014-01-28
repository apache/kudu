// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVICE_H
#define KUDU_TSERVER_TABLET_SERVICE_H

#include <string>
#include <vector>

#include "tserver/tserver_service.service.h"

namespace kudu {

namespace tablet { class TransactionContext; }

namespace tserver {

class TabletServer;

class TabletServiceImpl : public TabletServerServiceIf {
 public:
  explicit TabletServiceImpl(TabletServer* server);

  virtual void Ping(const PingRequestPB* req,
                    PingResponsePB* resp,
                    rpc::RpcContext* context);

  virtual void CreateTablet(const CreateTabletRequestPB* req,
                            CreateTabletResponsePB* resp,
                            rpc::RpcContext* context);

  virtual void AlterSchema(const AlterSchemaRequestPB* req,
                           AlterSchemaResponsePB* resp,
                           rpc::RpcContext* context);

  virtual void ChangeConfig(const ChangeConfigRequestPB* req, ChangeConfigResponsePB* resp,
                            rpc::RpcContext* context);

  virtual void Write(const WriteRequestPB* req, WriteResponsePB* resp,
                   rpc::RpcContext* context);

  virtual void Scan(const ScanRequestPB* req,
                    ScanResponsePB* resp,
                    rpc::RpcContext* context);

 private:
  void HandleNewScanRequest(const ScanRequestPB* req,
                            ScanResponsePB* resp,
                            rpc::RpcContext* context);

  void HandleContinueScanRequest(const ScanRequestPB* req,
                                 ScanResponsePB* resp,
                                 rpc::RpcContext* context);

  TabletServer* server_;
};

} // namespace tserver
} // namespace kudu

#endif
