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

  virtual void Insert(const InsertRequestPB* req,
                      InsertResponsePB* resp,
                      rpc::RpcContext* context);

  virtual void Scan(const ScanRequestPB* req,
                    ScanResponsePB* resp,
                    rpc::RpcContext* context);

 private:
  void SetupErrorAndRespond(TabletServerErrorPB* error,
                            const Status &s,
                            TabletServerErrorPB::Code code,
                            rpc::RpcContext* context) const;

  // Respond to an error where we don't have a more specific
  // error code in TabletServerErrorPB. This will use the
  // generic UNKNOWN_ERROR code, but also generate a WARNING
  // log on the server so that we can notice and assign a more
  // specific code to this situation.
  void RespondGenericError(const std::string& doing_what,
                           TabletServerErrorPB* error,
                           const Status& s,
                           rpc::RpcContext* context) const;

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
