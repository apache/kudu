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

#include "kudu/tserver/consensus_service.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/clock/clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/server/server_base.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/simple_tablet_manager.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/trace.h"
#include "kudu/util/trace_metrics.h"

DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_int32(memory_limit_warn_threshold_percentage);

using google::protobuf::RepeatedPtrField;
using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigRequestPB;
using kudu::consensus::ChangeConfigResponsePB;
using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::consensus::GetLastOpIdRequestPB;
using kudu::consensus::GetNodeInstanceRequestPB;
using kudu::consensus::GetNodeInstanceResponsePB;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::OpId;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RunLeaderElectionRequestPB;
using kudu::consensus::RunLeaderElectionResponsePB;
using kudu::consensus::TimeManager;
using kudu::consensus::UnsafeChangeConfigRequestPB;
using kudu::consensus::UnsafeChangeConfigResponsePB;
using kudu::consensus::VoteRequestPB;
using kudu::consensus::VoteResponsePB;
using kudu::consensus::ServerErrorPB;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcSidecar;
using kudu::server::ServerBase;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace tserver {

static void SetupErrorAndRespond(ServerErrorPB* error,
                                 const Status& s,
                                 ServerErrorPB::Code code,
                                 rpc::RpcContext* context) {
  // Generic "service unavailable" errors will cause the client to retry later.
  if ((code == ServerErrorPB::UNKNOWN_ERROR /*||
       code == TabletServerErrorPB::THROTTLED */) && s.IsServiceUnavailable()) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY, s);
    return;
  }

  StatusToPB(s, error->mutable_status());
  error->set_code(code);
  context->RespondNoCache();
}


namespace {

template<class ReqClass, class RespClass>
bool CheckUuidMatchOrRespondGeneric(TSTabletManager* tablet_manager,
                                    const char* method_name,
                                    const ReqClass* req,
                                    RespClass* resp,
                                    rpc::RpcContext* context) {
  const string& local_uuid = tablet_manager->NodeInstance().permanent_uuid();
  if (PREDICT_FALSE(!req->has_dest_uuid())) {
    // Maintain compat in release mode, but complain.
    string msg = Substitute("$0: Missing destination UUID in request from $1: $2",
                            method_name, context->requestor_string(), SecureShortDebugString(*req));
#ifdef NDEBUG
    KLOG_EVERY_N(ERROR, 100) << msg;
#else
    LOG(DFATAL) << msg;
#endif
    return true;
  }
  if (PREDICT_FALSE(req->dest_uuid() != local_uuid)) {
    Status s = Status::InvalidArgument(Substitute("$0: Wrong destination UUID requested. "
                                                  "Local UUID: $1. Requested UUID: $2",
                                                  method_name, local_uuid, req->dest_uuid()));
    LOG(WARNING) << s.ToString() << ": from " << context->requestor_string()
                 << ": " << SecureShortDebugString(*req);
    SetupErrorAndRespond(resp->mutable_error(), s,
                         ServerErrorPB::WRONG_SERVER_UUID, context);
    return false;
  }
  return true;
}

template<class ReqClass, class RespClass>
bool CheckUuidMatchOrRespond(TSTabletManager* tablet_manager,
                             const char* method_name,
                             const ReqClass* req,
                             RespClass* resp,
                             rpc::RpcContext* context) {
  return CheckUuidMatchOrRespondGeneric(tablet_manager, method_name, req, resp, context);
}

template<>
bool CheckUuidMatchOrRespond(TSTabletManager* tablet_manager,
                             const char* method_name,
                             const ConsensusRequestPB* req,
                             ConsensusResponsePB* resp,
                             rpc::RpcContext* context) {
  const string& local_uuid = tablet_manager->NodeInstance().permanent_uuid();
  if (req->has_proxy_dest_uuid()) {
    if (PREDICT_FALSE(req->proxy_dest_uuid() != local_uuid)) {
      Status s = Status::InvalidArgument(Substitute("$0: Wrong proxy UUID requested. "
                                                    "Local UUID: $1. Requested UUID: $2",
                                                    method_name, local_uuid, req->proxy_dest_uuid()));
      LOG(WARNING) << s.ToString() << ": from " << context->requestor_string()
                  << ": " << SecureShortDebugString(*req);
      SetupErrorAndRespond(resp->mutable_error(), s,
                           ServerErrorPB::WRONG_SERVER_UUID, context);
      return false;
    }
    return true;
  }
  return CheckUuidMatchOrRespondGeneric(tablet_manager, method_name, req, resp, context);
}


template<class RespClass>
bool GetConsensusOrRespond(TSTabletManager* tablet_manager,
                           RespClass* resp,
                           rpc::RpcContext* context,
                           shared_ptr<RaftConsensus>* consensus_out) {
  shared_ptr<RaftConsensus> tmp_consensus = tablet_manager->shared_consensus();
  if (!tmp_consensus) {
    Status s = Status::ServiceUnavailable("Raft Consensus unavailable",
                                          "Tablet replica not initialized");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         ServerErrorPB::CONSENSUS_NOT_RUNNING, context);
    return false;
  }
  *consensus_out = std::move(tmp_consensus);
  return true;
}

template <class RespType>
void HandleUnknownError(const Status& s, RespType* resp, RpcContext* context) {
  resp->Clear();
  SetupErrorAndRespond(resp->mutable_error(), s,
                       ServerErrorPB::UNKNOWN_ERROR,
                       context);
}

template <class ReqType, class RespType>
void HandleResponse(const ReqType* req, RespType* resp,
                    RpcContext* context, const Status& s) {
  if (PREDICT_FALSE(!s.ok())) {
    HandleUnknownError(s, resp, context);
    return;
  }
  context->RespondSuccess();
}

template <class ReqType, class RespType>
static StdStatusCallback BindHandleResponse(
    const ReqType* req,
    RespType* resp,
    RpcContext* context) {
  return std::bind(&HandleResponse<ReqType, RespType>,
                   req,
                   resp,
                   context,
                   std::placeholders::_1);
}

} // namespace

template <class ReqType, class RespType>
void HandleErrorResponse(const ReqType* req, RespType* resp, RpcContext* context,
                         const boost::optional<ServerErrorPB::Code>& error_code,
                         const Status& s) {
  resp->Clear();
  if (error_code) {
    SetupErrorAndRespond(resp->mutable_error(), s, *error_code, context);
  } else {
    HandleUnknownError(s, resp, context);
  }
}

ConsensusServiceImpl::ConsensusServiceImpl(ServerBase* server,
                                           TSTabletManager* tablet_manager)
    : ConsensusServiceIf(server->metric_entity(), server->result_tracker()),
      server_(server),
      tablet_manager_(tablet_manager) {
}

ConsensusServiceImpl::~ConsensusServiceImpl() {
}

bool ConsensusServiceImpl::AuthorizeServiceUser(const google::protobuf::Message* /*req*/,
                                                google::protobuf::Message* /*resp*/,
                                                rpc::RpcContext* rpc) {
  return server_->Authorize(rpc, ServerBase::SUPER_USER | ServerBase::SERVICE_USER);
}

void ConsensusServiceImpl::UpdateConsensus(const ConsensusRequestPB* req,
                                           ConsensusResponsePB* resp,
                                           rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Update RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "UpdateConsensus", req, resp, context)) {
    return;
  }

  // Submit the update directly to the TabletReplica's RaftConsensus instance.
  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) return;

  // Fast path for proxy requests.
  if (consensus->IsProxyRequest(req)) {
    consensus->HandleProxyRequest(req, resp, context);
    return;
  }

  Status s = consensus->Update(req, resp);
  if (PREDICT_FALSE(!s.ok())) {
    // Clear the response first, since a partially-filled response could
    // result in confusing a caller, or in having missing required fields
    // in embedded optional messages.
    resp->Clear();

    SetupErrorAndRespond(resp->mutable_error(), s,
                         ServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::RequestConsensusVote(const VoteRequestPB* req,
                                                VoteResponsePB* resp,
                                                rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Request Vote RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "RequestConsensusVote", req, resp, context)) {
    return;
  }

  boost::optional<OpId> last_logged_opid;
  // Submit the vote request directly to the consensus instance.
  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) return;

  Status s = consensus->RequestVote(req,
                                    consensus::TabletVotingState(std::move(last_logged_opid) /*,
                                                                 data_state*/),
                                    resp);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         ServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::ChangeConfig(const ChangeConfigRequestPB* req,
                                        ChangeConfigResponsePB* resp,
                                        RpcContext* context) {
  VLOG(1) << "Received ChangeConfig RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "ChangeConfig", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) return;
  boost::optional<ServerErrorPB::Code> error_code;
  Status s = consensus->ChangeConfig(*req, BindHandleResponse(req, resp, context), &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    HandleErrorResponse(req, resp, context, error_code, s);
    return;
  }
  // The success case is handled when the callback fires.
}

void ConsensusServiceImpl::BulkChangeConfig(const BulkChangeConfigRequestPB* req,
                                            ChangeConfigResponsePB* resp,
                                            RpcContext* context) {
  VLOG(1) << "Received BulkChangeConfig RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "BulkChangeConfig", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) return;
  boost::optional<ServerErrorPB::Code> error_code;
  Status s = consensus->BulkChangeConfig(*req, BindHandleResponse(req, resp, context), &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    HandleErrorResponse(req, resp, context, error_code, s);
    return;
  }
  // The success case is handled when the callback fires.
}

void ConsensusServiceImpl::UnsafeChangeConfig(const UnsafeChangeConfigRequestPB* req,
                                              UnsafeChangeConfigResponsePB* resp,
                                              RpcContext* context) {
  LOG(INFO) << "Received UnsafeChangeConfig RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!CheckUuidMatchOrRespond(tablet_manager_, "UnsafeChangeConfig", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) {
    return;
  }
  boost::optional<ServerErrorPB::Code> error_code;
  const Status s = consensus->UnsafeChangeConfig(*req, &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    HandleErrorResponse(req, resp, context, error_code, s);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::ChangeProxyTopology(const consensus::ChangeProxyTopologyRequestPB* req,
                                              consensus::ChangeProxyTopologyResponsePB* resp,
                                              rpc::RpcContext* context) {
  LOG(INFO) << "Received ChangeProxyTopology RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!CheckUuidMatchOrRespond(tablet_manager_, "ChangeProxyTopology", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) {
    return;
  }

  HandleResponse(req, resp, context, consensus->ChangeProxyTopology(*req));
}

void ConsensusServiceImpl::GetNodeInstance(const GetNodeInstanceRequestPB* req,
                                           GetNodeInstanceResponsePB* resp,
                                           rpc::RpcContext* context) {
  VLOG(1) << "Received Get Node Instance RPC: " << SecureDebugString(*req);
  resp->mutable_node_instance()->CopyFrom(tablet_manager_->NodeInstance());
  context->RespondSuccess();
}

void ConsensusServiceImpl::RunLeaderElection(const RunLeaderElectionRequestPB* req,
                                             RunLeaderElectionResponsePB* resp,
                                             rpc::RpcContext* context) {
  LOG(INFO) << "Received Run Leader Election RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!CheckUuidMatchOrRespond(tablet_manager_, "RunLeaderElection", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) return;
  Status s = consensus->StartElection(
      consensus::RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE,
      consensus::RaftConsensus::EXTERNAL_REQUEST);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         ServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::LeaderStepDown(const LeaderStepDownRequestPB* req,
                                          LeaderStepDownResponsePB* resp,
                                          RpcContext* context) {
  LOG(INFO) << "Received LeaderStepDown RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!CheckUuidMatchOrRespond(tablet_manager_, "LeaderStepDown", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) return;
  Status s = consensus->StepDown(resp);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         ServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::GetLastOpId(const consensus::GetLastOpIdRequestPB *req,
                                       consensus::GetLastOpIdResponsePB *resp,
                                       rpc::RpcContext *context) {
  DVLOG(3) << "Received GetLastOpId RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "GetLastOpId", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(tablet_manager_, resp, context, &consensus)) return;
  if (PREDICT_FALSE(req->opid_type() == consensus::UNKNOWN_OPID_TYPE)) {
    HandleUnknownError(Status::InvalidArgument("Invalid opid_type specified to GetLastOpId()"),
                       resp, context);
    return;
  }
  boost::optional<OpId> opid = consensus->GetLastOpId(req->opid_type());
  if (!opid) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::IllegalState("Cannot fetch last OpId in WAL"),
                         ServerErrorPB::CONSENSUS_NOT_RUNNING,
                         context);
    return;
  }
  *resp->mutable_opid() = *opid;
  context->RespondSuccess();
}

void ConsensusServiceImpl::GetConsensusState(const consensus::GetConsensusStateRequestPB* req,
                                             consensus::GetConsensusStateResponsePB* resp,
                                             rpc::RpcContext* context) {

#if 0
  DVLOG(3) << "Received GetConsensusState RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "GetConsensusState", req, resp, context)) {
    return;
  }

  unordered_set<string> requested_ids(req->tablet_ids().begin(), req->tablet_ids().end());
  bool all_ids = requested_ids.empty();

  vector<scoped_refptr<TabletReplica>> tablet_replicas;
  tablet_manager_->GetTabletReplicas(&tablet_replicas);
  for (const scoped_refptr<TabletReplica>& replica : tablet_replicas) {
    if (!all_ids && !ContainsKey(requested_ids, replica->tablet_id())) {
      continue;
    }

    shared_ptr<RaftConsensus> consensus(replica->shared_consensus());
    if (!consensus) {
      continue;
    }

    consensus::GetConsensusStateResponsePB_TabletConsensusInfoPB tablet_info;
    Status s = consensus->ConsensusState(tablet_info.mutable_cstate(), req->report_health());
    if (!s.ok()) {
      DCHECK(s.IsIllegalState()) << s.ToString();
      continue;
    }
    tablet_info.set_tablet_id(replica->tablet_id());
    *resp->add_tablets() = std::move(tablet_info);
  }
  const auto scheme = FLAGS_raft_prepare_replacement_before_eviction
      ? consensus::ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
      : consensus::ReplicaManagementInfoPB::EVICT_FIRST;
  resp->mutable_replica_management_info()->set_replacement_scheme(scheme);

#endif

  context->RespondSuccess();
}

} // namespace tserver
} // namespace kudu
