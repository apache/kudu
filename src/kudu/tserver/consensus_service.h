// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef KUDU_TSERVER_TABLET_SERVICE_H
#define KUDU_TSERVER_TABLET_SERVICE_H

#include <cstdint>
#include <string>

#include "kudu/consensus/consensus.service.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/tserver/tserver_admin.service.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

class Status;

namespace server {
class ServerBase;
} // namespace server

namespace consensus {
class BulkChangeConfigRequestPB;
class ChangeConfigRequestPB;
class ChangeConfigResponsePB;
class ConsensusRequestPB;
class ConsensusResponsePB;
class GetConsensusStateRequestPB;
class GetConsensusStateResponsePB;
class GetLastOpIdRequestPB;
class GetLastOpIdResponsePB;
class GetNodeInstanceRequestPB;
class GetNodeInstanceResponsePB;
class LeaderStepDownRequestPB;
class LeaderStepDownResponsePB;
class RunLeaderElectionRequestPB;
class RunLeaderElectionResponsePB;
class StartTabletCopyRequestPB;
class StartTabletCopyResponsePB;
class TimeManager;
class UnsafeChangeConfigRequestPB;
class UnsafeChangeConfigResponsePB;
class VoteRequestPB;
class VoteResponsePB;
} // namespace consensus

namespace rpc {
class RpcContext;
} // namespace rpc

namespace tserver {

class TSTabletManager;

class ConsensusServiceImpl : public consensus::ConsensusServiceIf {
 public:
  ConsensusServiceImpl(server::ServerBase* server,
                       TSTabletManager* tablet_manager);

  virtual ~ConsensusServiceImpl();

  bool AuthorizeServiceUser(const google::protobuf::Message* req,
                            google::protobuf::Message* resp,
                            rpc::RpcContext* context) override;

  virtual void UpdateConsensus(const consensus::ConsensusRequestPB* req,
                               consensus::ConsensusResponsePB* resp,
                               rpc::RpcContext* context) override;

  virtual void RequestConsensusVote(const consensus::VoteRequestPB* req,
                                    consensus::VoteResponsePB* resp,
                                    rpc::RpcContext* context) override;

  virtual void ChangeConfig(const consensus::ChangeConfigRequestPB* req,
                            consensus::ChangeConfigResponsePB* resp,
                            rpc::RpcContext* context) override;

  virtual void BulkChangeConfig(const consensus::BulkChangeConfigRequestPB* req,
                                consensus::ChangeConfigResponsePB* resp,
                                rpc::RpcContext* context) override;

  virtual void UnsafeChangeConfig(const consensus::UnsafeChangeConfigRequestPB* req,
                                  consensus::UnsafeChangeConfigResponsePB* resp,
                                  rpc::RpcContext* context) override;

  virtual void GetNodeInstance(const consensus::GetNodeInstanceRequestPB* req,
                               consensus::GetNodeInstanceResponsePB* resp,
                               rpc::RpcContext* context) override;

  virtual void RunLeaderElection(const consensus::RunLeaderElectionRequestPB* req,
                                 consensus::RunLeaderElectionResponsePB* resp,
                                 rpc::RpcContext* context) override;

  virtual void LeaderStepDown(const consensus::LeaderStepDownRequestPB* req,
                              consensus::LeaderStepDownResponsePB* resp,
                              rpc::RpcContext* context) override;

  virtual void GetLastOpId(const consensus::GetLastOpIdRequestPB* req,
                           consensus::GetLastOpIdResponsePB* resp,
                           rpc::RpcContext* context) override;

  virtual void GetConsensusState(const consensus::GetConsensusStateRequestPB* req,
                                 consensus::GetConsensusStateResponsePB* resp,
                                 rpc::RpcContext* context) override;

 private:
  server::ServerBase* server_;
  TSTabletManager* tablet_manager_;
};

} // namespace tserver
} // namespace kudu

#endif
