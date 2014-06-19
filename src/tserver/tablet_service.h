// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVICE_H
#define KUDU_TSERVER_TABLET_SERVICE_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "tserver/tserver_service.service.h"

namespace kudu {
class RowwiseIterator;
class Schema;
class Status;

namespace tablet {
class TabletPeer;
class TransactionState;
} // namespace tablet

namespace tserver {

class TabletServer;

class TabletServiceImpl : public TabletServerServiceIf {
 public:
  explicit TabletServiceImpl(TabletServer* server);

  virtual void Ping(const PingRequestPB* req,
                    PingResponsePB* resp,
                    rpc::RpcContext* context) OVERRIDE;

  virtual void CreateTablet(const CreateTabletRequestPB* req,
                            CreateTabletResponsePB* resp,
                            rpc::RpcContext* context) OVERRIDE;

  virtual void DeleteTablet(const DeleteTabletRequestPB* req,
                            DeleteTabletResponsePB* resp,
                            rpc::RpcContext* context) OVERRIDE;

  virtual void AlterSchema(const AlterSchemaRequestPB* req,
                           AlterSchemaResponsePB* resp,
                           rpc::RpcContext* context) OVERRIDE;

  virtual void ChangeConfig(const ChangeConfigRequestPB* req, ChangeConfigResponsePB* resp,
                            rpc::RpcContext* context) OVERRIDE;

  virtual void Write(const WriteRequestPB* req, WriteResponsePB* resp,
                   rpc::RpcContext* context) OVERRIDE;

  virtual void Scan(const ScanRequestPB* req,
                    ScanResponsePB* resp,
                    rpc::RpcContext* context) OVERRIDE;

  virtual void ListTablets(const ListTabletsRequestPB* req,
                           ListTabletsResponsePB* resp,
                           rpc::RpcContext* context) OVERRIDE;

  virtual void UpdateConsensus(const kudu::consensus::ConsensusRequestPB *req,
                               kudu::consensus::ConsensusResponsePB *resp,
                               ::kudu::rpc::RpcContext *context) OVERRIDE;

  virtual void RequestConsensusVote(const kudu::consensus::VoteRequestPB* req,
                                    kudu::consensus::VoteResponsePB* resp,
                                    ::kudu::rpc::RpcContext* context) OVERRIDE;

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

  Status HandleScanAtSnapshot(gscoped_ptr<RowwiseIterator>* iter,
                              ScanResponsePB* resp,
                              const NewScanRequestPB& scan_pb,
                              const Schema& projection,
                              std::tr1::shared_ptr<tablet::TabletPeer> tablet_peer);

  TabletServer* server_;
};

} // namespace tserver
} // namespace kudu

#endif
