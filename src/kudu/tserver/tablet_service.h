// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_TABLET_SERVICE_H
#define KUDU_TSERVER_TABLET_SERVICE_H

#include <string>
#include <vector>

#include "kudu/gutil/ref_counted.h"
#include "kudu/tserver/tserver_service.service.h"
#include "kudu/tserver/tserver_admin.service.h"
#include "kudu/consensus/consensus.service.h"

namespace kudu {
class RowwiseIterator;
class Schema;
class Status;

namespace tablet {
class TabletPeer;
class TransactionState;
} // namespace tablet

namespace tserver {

class RemoteBootstrapServiceIf;
class TabletServer;
class TabletPeerLookupIf;

class TabletServiceImpl : public TabletServerServiceIf {
 public:
  explicit TabletServiceImpl(TabletServer* server);

  virtual void Ping(const PingRequestPB* req,
                    PingResponsePB* resp,
                    rpc::RpcContext* context) OVERRIDE;

  virtual void Write(const WriteRequestPB* req, WriteResponsePB* resp,
                   rpc::RpcContext* context) OVERRIDE;

  virtual void Scan(const ScanRequestPB* req,
                    ScanResponsePB* resp,
                    rpc::RpcContext* context) OVERRIDE;

  virtual void ListTablets(const ListTabletsRequestPB* req,
                           ListTabletsResponsePB* resp,
                           rpc::RpcContext* context) OVERRIDE;

  // TODO: Move this to its own service once we put in service multiplexing support
  // in the RPC protocol.
  virtual void BeginRemoteBootstrapSession(const BeginRemoteBootstrapSessionRequestPB* req,
                                   BeginRemoteBootstrapSessionResponsePB* resp,
                                   rpc::RpcContext* context) OVERRIDE;

  virtual void CheckSessionActive(const CheckRemoteBootstrapSessionActiveRequestPB* req,
                                  CheckRemoteBootstrapSessionActiveResponsePB* resp,
                                  rpc::RpcContext* context) OVERRIDE;

  virtual void FetchData(const FetchDataRequestPB* req,
                         FetchDataResponsePB* resp,
                         rpc::RpcContext* context) OVERRIDE;

  virtual void EndRemoteBootstrapSession(const EndRemoteBootstrapSessionRequestPB* req,
                                 EndRemoteBootstrapSessionResponsePB* resp,
                                 rpc::RpcContext* context) OVERRIDE;

  virtual void Shutdown() OVERRIDE;

 private:
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
                              const scoped_refptr<tablet::TabletPeer>& tablet_peer);

  TabletServer* server_;
  gscoped_ptr<RemoteBootstrapServiceIf> remote_bootstrap_service_;
};

class TabletServiceAdminImpl : public TabletServerAdminServiceIf {
 public:
  explicit TabletServiceAdminImpl(TabletServer* server);
  virtual void CreateTablet(const CreateTabletRequestPB* req,
                            CreateTabletResponsePB* resp,
                            rpc::RpcContext* context) OVERRIDE;

  virtual void DeleteTablet(const DeleteTabletRequestPB* req,
                            DeleteTabletResponsePB* resp,
                            rpc::RpcContext* context) OVERRIDE;

  virtual void AlterSchema(const AlterSchemaRequestPB* req,
                           AlterSchemaResponsePB* resp,
                           rpc::RpcContext* context) OVERRIDE;

 private:
  TabletServer* server_;
};

class ConsensusServiceImpl : public consensus::ConsensusServiceIf {
 public:
  ConsensusServiceImpl(const MetricContext& metric_context,
                       TabletPeerLookupIf* tablet_manager_);

  virtual ~ConsensusServiceImpl();

  virtual void ChangeConfig(const consensus::ChangeConfigRequestPB* req,
                            consensus::ChangeConfigResponsePB* resp,
                            rpc::RpcContext* context) OVERRIDE;

  virtual void UpdateConsensus(const consensus::ConsensusRequestPB *req,
                               consensus::ConsensusResponsePB *resp,
                               rpc::RpcContext *context) OVERRIDE;

  virtual void RequestConsensusVote(const consensus::VoteRequestPB* req,
                                    consensus::VoteResponsePB* resp,
                                    rpc::RpcContext* context) OVERRIDE;

  virtual void GetNodeInstance(const consensus::GetNodeInstanceRequestPB* req,
                               consensus::GetNodeInstanceResponsePB* resp,
                               rpc::RpcContext* context) OVERRIDE;

  virtual void MakePeerLeader(const consensus::MakePeerLeaderRequestPB* req,
                              consensus::MakePeerLeaderResponsePB* resp,
                              rpc::RpcContext* context) OVERRIDE;

 private:
  TabletPeerLookupIf* tablet_manager_;
};

} // namespace tserver
} // namespace kudu

#endif
