// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVICE_H
#define KUDU_TSERVER_TABLET_SERVICE_H

#include <string>
#include <vector>

#include "tablet/tablet.h"
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

  virtual void Write(const WriteRequestPB* req, WriteResponsePB* resp,
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

  // Decodes the row block, makes sure the block's schema matches
  // tablet_schema, but the actual rows are decoded with block_schema
  // (in case they are the key projection). The decoded rows are added to
  // row_block.
  bool DecodeRowBlock(RowwiseRowBlockPB* block_pb,
                      WriteResponsePB* resp,
                      rpc::RpcContext* context,
                      const Schema &block_row_schema,
                      const Schema &tablet_schema,
                      vector<const uint8_t*>* row_block);

  void InsertRows(const Schema& client_schema,
                  vector<const uint8_t*> *to_insert,
                  tablet::TransactionContext* tx_ctx,
                  WriteResponsePB *resp,
                  rpc::RpcContext* context,
                  tablet::Tablet *tablet);

  void MutateRows(const Schema& client_schema,
                  vector<const uint8_t*> *to_mutate,
                  vector<const RowChangeList *> *mutations,
                  tablet::TransactionContext* tx_ctx,
                  WriteResponsePB *resp,
                  rpc::RpcContext* context,
                  tablet::Tablet *tablet);

  TabletServer* server_;
};

} // namespace tserver
} // namespace kudu

#endif
