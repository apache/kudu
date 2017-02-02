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

#include "kudu/master/master_service.h"

#include <gflags/gflags.h>
#include <memory>
#include <string>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/master/authn_token_manager.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/outbound_call.h" // for UserCredentials
#include "kudu/server/webserver.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/pb_util.h"


DEFINE_int32(master_inject_latency_on_tablet_lookups_ms, 0,
             "Number of milliseconds that the master will sleep before responding to "
             "requests for tablet locations.");
TAG_FLAG(master_inject_latency_on_tablet_lookups_ms, unsafe);
TAG_FLAG(master_inject_latency_on_tablet_lookups_ms, hidden);

DEFINE_bool(master_support_connect_to_master_rpc, true,
            "Whether to support the ConnectToMaster() RPC. Used for testing "
            "version compatibility fallback in the client.");
TAG_FLAG(master_support_connect_to_master_rpc, unsafe);
TAG_FLAG(master_support_connect_to_master_rpc, hidden);

using kudu::security::SignedTokenPB;

namespace kudu {
namespace master {

using consensus::RaftPeerPB;
using std::string;
using std::vector;
using std::shared_ptr;
using strings::Substitute;

namespace {

// If 's' is not OK and 'resp' has no application specific error set,
// set the error field of 'resp' to match 's' and set the code to
// UNKNOWN_ERROR.
template<class RespClass>
void CheckRespErrorOrSetUnknown(const Status& s, RespClass* resp) {
  if (PREDICT_FALSE(!s.ok() && !resp->has_error())) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
    resp->mutable_error()->set_code(MasterErrorPB::UNKNOWN_ERROR);
  }
}

} // anonymous namespace

MasterServiceImpl::MasterServiceImpl(Master* server)
  : MasterServiceIf(server->metric_entity(), server->result_tracker()),
    server_(server) {
}

void MasterServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* rpc) {
  rpc->RespondSuccess();
}

void MasterServiceImpl::TSHeartbeat(const TSHeartbeatRequestPB* req,
                                    TSHeartbeatResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  // If CatalogManager is not initialized don't even know whether
  // or not we will be a leader (so we can't tell whether or not we can
  // accept tablet reports).
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedOrRespond(resp, rpc)) {
    return;
  }
  bool is_leader_master = l.leader_status().ok();

  // 2. All responses contain this.
  resp->mutable_master_instance()->CopyFrom(server_->instance_pb());
  resp->set_leader_master(is_leader_master);

  // 3. Register or look up the tserver.
  shared_ptr<TSDescriptor> ts_desc;
  if (req->has_registration()) {
    Status s = server_->ts_manager()->RegisterTS(req->common().ts_instance(),
                                                 req->registration(),
                                                 &ts_desc);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Unable to register tserver ($0): $1",
                                 rpc->requestor_string(), s.ToString());
      // TODO: add service-specific errors
      rpc->RespondFailure(s);
      return;
    }
  } else {
    Status s = server_->ts_manager()->LookupTS(req->common().ts_instance(), &ts_desc);
    if (s.IsNotFound()) {
      LOG(INFO) << Substitute("Got heartbeat from unknown tserver ($0) as $1; "
          "Asking this server to re-register.",
          SecureShortDebugString(req->common().ts_instance()), rpc->requestor_string());
      resp->set_needs_reregister(true);

      // Don't bother asking for a full tablet report if we're a follower;
      // it'll just get ignored anyway.
      resp->set_needs_full_tablet_report(is_leader_master);

      rpc->RespondSuccess();
      return;
    } else if (!s.ok()) {
      LOG(WARNING) << Substitute("Unable to look up tserver for heartbeat "
          "request $0 from $1: $2", SecureDebugString(*req),
          rpc->requestor_string(), s.ToString());
      rpc->RespondFailure(s.CloneAndPrepend("Unable to lookup tserver"));
      return;
    }
  }

  // 4. Update tserver soft state based on the heartbeat contents.
  ts_desc->UpdateHeartbeatTime();
  ts_desc->set_num_live_replicas(req->num_live_tablets());

  // 5. Only leaders handle tablet reports.
  if (is_leader_master && req->has_tablet_report()) {
    Status s = server_->catalog_manager()->ProcessTabletReport(
        ts_desc.get(), req->tablet_report(), resp->mutable_tablet_report(), rpc);
    if (!s.ok()) {
      rpc->RespondFailure(s.CloneAndPrepend("Failed to process tablet report"));
      return;
    }
  }

  // 6. Only leaders sign CSR from tablet servers (if present).
  if (is_leader_master && req->has_csr_der()) {
    string cert;
    Status s = server_->cert_authority()->SignServerCSR(req->csr_der(), &cert);
    if (!s.ok()) {
      rpc->RespondFailure(s.CloneAndPrepend("invalid CSR"));
      return;
    }
    LOG(INFO) << "Signed X509 certificate for tserver " << rpc->requestor_string();
    resp->mutable_signed_cert_der()->swap(cert);
  }

  // TODO(aserbin): 7. Send any active CA certs which the TS doesn't have.

  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTabletLocations(const GetTabletLocationsRequestPB* req,
                                           GetTabletLocationsResponsePB* resp,
                                           rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  if (PREDICT_FALSE(FLAGS_master_inject_latency_on_tablet_lookups_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_master_inject_latency_on_tablet_lookups_ms));
  }

  ServerRegistrationPB reg;
  vector<TSDescriptor*> locs;
  for (const string& tablet_id : req->tablet_ids()) {
    // TODO: once we have catalog data. ACL checks would also go here, probably.
    TabletLocationsPB* locs_pb = resp->add_tablet_locations();
    Status s = server_->catalog_manager()->GetTabletLocations(tablet_id, locs_pb);
    if (!s.ok()) {
      resp->mutable_tablet_locations()->RemoveLast();

      GetTabletLocationsResponsePB::Error* err = resp->add_errors();
      err->set_tablet_id(tablet_id);
      StatusToPB(s, err->mutable_status());
    }
  }

  rpc->RespondSuccess();
}

void MasterServiceImpl::CreateTable(const CreateTableRequestPB* req,
                                    CreateTableResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->CreateTable(req, resp, rpc);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                          IsCreateTableDoneResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->IsCreateTableDone(req, resp);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::DeleteTable(const DeleteTableRequestPB* req,
                                    DeleteTableResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->DeleteTable(req, resp, rpc);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::AlterTable(const AlterTableRequestPB* req,
                                   AlterTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->AlterTable(req, resp, rpc);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                                         IsAlterTableDoneResponsePB* resp,
                                         rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->IsAlterTableDone(req, resp, rpc);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListTables(const ListTablesRequestPB* req,
                                   ListTablesResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->ListTables(req, resp);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTableLocations(const GetTableLocationsRequestPB* req,
                                          GetTableLocationsResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  if (PREDICT_FALSE(FLAGS_master_inject_latency_on_tablet_lookups_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_master_inject_latency_on_tablet_lookups_ms));
  }
  Status s = server_->catalog_manager()->GetTableLocations(req, resp);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTableSchema(const GetTableSchemaRequestPB* req,
                                       GetTableSchemaResponsePB* resp,
                                       rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->GetTableSchema(req, resp);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListTabletServers(const ListTabletServersRequestPB* req,
                                          ListTabletServersResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  vector<std::shared_ptr<TSDescriptor> > descs;
  server_->ts_manager()->GetAllDescriptors(&descs);
  for (const std::shared_ptr<TSDescriptor>& desc : descs) {
    ListTabletServersResponsePB::Entry* entry = resp->add_servers();
    desc->GetNodeInstancePB(entry->mutable_instance_id());
    desc->GetRegistration(entry->mutable_registration());
    entry->set_millis_since_heartbeat(desc->TimeSinceHeartbeat().ToMilliseconds());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListMasters(const ListMastersRequestPB* req,
                                    ListMastersResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  vector<ServerEntryPB> masters;
  Status s = server_->ListMasters(&masters);
  if (!s.ok()) {
    StatusToPB(s, resp->mutable_error());
    resp->mutable_error()->set_code(AppStatusPB::UNKNOWN_ERROR);
  } else {
    for (const ServerEntryPB& master : masters) {
      resp->add_masters()->CopyFrom(master);
    }
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::GetMasterRegistration(const GetMasterRegistrationRequestPB* req,
                                              GetMasterRegistrationResponsePB* resp,
                                              rpc::RpcContext* rpc) {
  // instance_id must always be set in order for status pages to be useful.
  resp->mutable_instance_id()->CopyFrom(server_->instance_pb());

  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->GetMasterRegistration(resp->mutable_registration());
  CheckRespErrorOrSetUnknown(s, resp);
  resp->set_role(server_->catalog_manager()->Role());
  rpc->RespondSuccess();
}

void MasterServiceImpl::ConnectToMaster(const ConnectToMasterRequestPB* /*req*/,
                                        ConnectToMasterResponsePB* resp,
                                        rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedOrRespond(resp, rpc)) {
    return;
  }
  auto role = server_->catalog_manager()->Role();
  resp->set_role(role);

  if (l.leader_status().ok()) {
    // TODO(PKI): it seems there is some window when 'role' is LEADER but
    // in fact we aren't done initializing (and we don't have a CA cert).
    // In that case, if we respond with the 'LEADER' role to a client, but
    // don't pass back the CA cert, then the client won't be able to trust
    // anyone... seems like a potential race bug for clients who connect
    // exactly as the leader is changing.
    resp->add_ca_cert_der(server_->cert_authority()->ca_cert_der());

    // Issue an authentication token for the caller.
    // TODO(PKI): we should probably only issue a token if the client is
    // authenticated by kerberos, and not by another token. Otherwise we're
    // essentially allowing unlimited renewal, which is probably not what
    // we want.
    SignedTokenPB authn_token;
    Status s = server_->authn_token_manager()->GenerateToken(
        rpc->user_credentials().real_user(),
        &authn_token);
    if (!s.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 1)
          << "Unable to generate signed token for " << rpc->requestor_string()
          << ": " << s.ToString();
    } else {
      // TODO(todd): this might be a good spot for some auditing code?
      resp->mutable_authn_token()->Swap(&authn_token);
    }
  }
  rpc->RespondSuccess();
}

bool MasterServiceImpl::SupportsFeature(uint32_t feature) const {
  switch (feature) {
    case MasterFeatures::RANGE_PARTITION_BOUNDS:
    case MasterFeatures::ADD_DROP_RANGE_PARTITIONS: return true;
    case MasterFeatures::CONNECT_TO_MASTER:
      return FLAGS_master_support_connect_to_master_rpc;
    default: return false;
  }
}

} // namespace master
} // namespace kudu
