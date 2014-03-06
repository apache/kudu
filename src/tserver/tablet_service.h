// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVICE_H
#define KUDU_TSERVER_TABLET_SERVICE_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "tserver/tserver_service.service.h"

namespace kudu {

namespace tablet {
class TabletPeer;
class TransactionContext;
} // namespace tablet

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

  virtual void DeleteTablet(const DeleteTabletRequestPB* req,
                            DeleteTabletResponsePB* resp,
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
  // Lookup the given tablet, ensuring that it both exists and is RUNNING.
  // If it is not, responds to the RPC associated with 'context' after setting
  // resp->mutable_error() to indicate the failure reason.
  //
  // Returns true if successful.
  template<class RespClass>
  bool LookupTabletOrRespond(const std::string& tablet_id,
                             std::tr1::shared_ptr<tablet::TabletPeer>* peer,
                             RespClass* resp,
                             rpc::RpcContext* context);

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
