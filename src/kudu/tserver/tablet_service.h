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
class Timestamp;

namespace tablet {
class TabletPeer;
class TransactionState;
} // namespace tablet

namespace tserver {

class RemoteBootstrapServiceIf;
class ScanResultCollector;
class TabletPeerLookupIf;
class TabletServer;

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

  virtual void Checksum(const ChecksumRequestPB* req,
                        ChecksumResponsePB* resp,
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
  Status HandleNewScanRequest(tablet::TabletPeer* tablet_peer,
                              const ScanRequestPB* req,
                              const std::string& requestor_string,
                              ScanResultCollector* result_collector,
                              std::string* scanner_id,
                              Timestamp* snap_timestamp,
                              bool* has_more_results,
                              TabletServerErrorPB::Code* error_code);

  Status HandleContinueScanRequest(const ScanRequestPB* req,
                                   ScanResultCollector* result_collector,
                                   bool* has_more_results,
                                   TabletServerErrorPB::Code* error_code);

  Status HandleScanAtSnapshot(gscoped_ptr<RowwiseIterator>* iter,
                              const NewScanRequestPB& scan_pb,
                              const Schema& projection,
                              const scoped_refptr<tablet::TabletPeer>& tablet_peer,
                              Timestamp* snap_timestamp);

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

  virtual void RunLeaderElection(const consensus::RunLeaderElectionRequestPB* req,
                              consensus::RunLeaderElectionResponsePB* resp,
                              rpc::RpcContext* context) OVERRIDE;

  virtual void GetLastOpId(const consensus::GetLastOpIdRequestPB *req,
                           consensus::GetLastOpIdResponsePB *resp,
                           rpc::RpcContext *context) OVERRIDE;

  virtual void GetCommittedQuorum(const consensus::GetCommittedQuorumRequestPB *req,
                                  consensus::GetCommittedQuorumResponsePB *resp,
                                  rpc::RpcContext *context) OVERRIDE;

 private:
  TabletPeerLookupIf* tablet_manager_;
};

} // namespace tserver
} // namespace kudu

#endif
