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

#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hms_catalog.h"
#include "kudu/master/authz_provider.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/location_cache.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/server_base.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

DECLARE_bool(hive_metastore_sasl_enabled);
DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_string(hive_metastore_uris);

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

DEFINE_bool(master_non_leader_masters_propagate_tsk, false,
            "Whether a non-leader master sends information about its TSKs in "
            "response to a tablet server's heartbeat. This is intended for "
            "tests scenarios only and should not be used elsewhere.");
TAG_FLAG(master_non_leader_masters_propagate_tsk, hidden);

DEFINE_bool(master_client_location_assignment_enabled, true,
            "Whether masters assign locations to connecting clients. "
            "By default they do if the location assignment command is set, "
            "but setting this flag to 'false' makes masters assign "
            "locations only to tablet servers, not clients.");
TAG_FLAG(master_client_location_assignment_enabled, advanced);
TAG_FLAG(master_client_location_assignment_enabled, runtime);

DEFINE_bool(master_support_authz_tokens, true,
            "Whether the master supports generating authz tokens. Used for "
            "testing version compatibility in the client.");
TAG_FLAG(master_support_authz_tokens, hidden);

using boost::make_optional;
using google::protobuf::Message;
using kudu::consensus::ReplicaManagementInfoPB;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::security::SignedTokenPB;
using kudu::security::TablePrivilegePB;
using kudu::server::ServerBase;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

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

// Sets 'to_state' to the end state of the given 'change' and returns true.
// Returns false if the 'change' isn't supported.
bool StateChangeToTServerState(const TServerStateChangePB::StateChange& change,
                               TServerStatePB* to_state) {
  switch (change) {
    case TServerStateChangePB::ENTER_MAINTENANCE_MODE:
      *to_state = TServerStatePB::MAINTENANCE_MODE;
      return true;
    case TServerStateChangePB::EXIT_MAINTENANCE_MODE:
      *to_state = TServerStatePB::NONE;
      return true;
    default:
      return false;
  }
}

} // anonymous namespace

MasterServiceImpl::MasterServiceImpl(Master* server)
  : MasterServiceIf(server->metric_entity(), server->result_tracker()),
    server_(server) {
}

bool MasterServiceImpl::AuthorizeClient(const Message* /*req*/,
                                        Message* /*resp*/,
                                        rpc::RpcContext* context) {
  return server_->Authorize(context, ServerBase::SUPER_USER | ServerBase::USER);
}

bool MasterServiceImpl::AuthorizeServiceUser(const Message* /*req*/,
                                             Message* /*resp*/,
                                             rpc::RpcContext* context) {
  // We don't allow superusers to pretend to be tablet servers -- there are no
  // operator tools that do anything like this and since we sign requests for
  // tablet servers, we should be extra tight here.
  return server_->Authorize(context, ServerBase::SERVICE_USER);
}

bool MasterServiceImpl::AuthorizeClientOrServiceUser(const Message* /*req*/,
                                                     Message* /*resp*/,
                                                     rpc::RpcContext* context) {
  return server_->Authorize(context, ServerBase::SUPER_USER | ServerBase::USER |
                            ServerBase::SERVICE_USER);
}

bool MasterServiceImpl::AuthorizeSuperUser(const Message* /*req*/,
                                           Message* /*resp*/,
                                           rpc::RpcContext* context) {
  return server_->Authorize(context, ServerBase::SUPER_USER);
}

void MasterServiceImpl::Ping(const PingRequestPB* /*req*/,
                             PingResponsePB* /*resp*/,
                             rpc::RpcContext* rpc) {
  rpc->RespondSuccess();
}

void MasterServiceImpl::ChangeTServerState(const ChangeTServerStateRequestPB* req,
                                           ChangeTServerStateResponsePB* resp,
                                           rpc::RpcContext* rpc) {
  // Do some basic checking on the contents of the request.
  Status s;
  auto respond_error = MakeScopedCleanup([&] {
    if (PREDICT_FALSE(!s.ok())) {
      rpc->RespondFailure(s);
    }
  });
  if (!req->has_change()) {
    s = Status::InvalidArgument("request must contain tserver state change");
    return;
  }
  const auto& ts_state_change = req->change();
  if (!ts_state_change.has_uuid()) {
    s = Status::InvalidArgument("uuid not provided");
    return;
  }
  const auto& ts_uuid = ts_state_change.uuid();
  if (!ts_state_change.has_change()) {
    s = Status::InvalidArgument(Substitute("state change not provided for $0", ts_uuid));
    return;
  }
  const auto& change = ts_state_change.change();
  TServerStatePB to_state;
  if (!StateChangeToTServerState(change, &to_state)) {
    s = Status::InvalidArgument(Substitute("invalid state change: $0", change));
    return;
  }
  respond_error.cancel();

  // Make sure we're the leader.
  CatalogManager* catalog_manager = server_->catalog_manager();
  CatalogManager::ScopedLeaderSharedLock l(catalog_manager);
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  // Set the appropriate state for the given tserver.
  s = server_->ts_manager()->SetTServerState(ts_uuid, to_state,
      req->handle_missing_tserver(), server_->catalog_manager()->sys_catalog());
  if (PREDICT_FALSE(!s.ok())) {
    rpc->RespondFailure(s);
    return;
  }
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
    const auto scheme = FLAGS_raft_prepare_replacement_before_eviction
        ? ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
        : ReplicaManagementInfoPB::EVICT_FIRST;
    const auto ts_scheme = req->has_replica_management_info()
        ? req->replica_management_info().replacement_scheme()
        : ReplicaManagementInfoPB::EVICT_FIRST;
    // If the catalog manager is running with some different replica management
    // scheme, report back an error and do not register the tablet server.
    if (scheme != ts_scheme) {
      const auto& ts_uuid = req->common().ts_instance().permanent_uuid();
      const auto& ts_addr = rpc->remote_address().ToString();
      Status s = Status::ConfigurationError(
          Substitute("replica replacement scheme ($0) of the tablet server "
                     "$1 at $2 differs from the catalog manager's ($3); "
                     "they must be run with the same scheme (controlled "
                     "by the --raft_prepare_replacement_before_eviction flag)",
                     ReplicaManagementInfoPB::ReplacementScheme_Name(ts_scheme),
                     ts_uuid, ts_addr,
                     ReplicaManagementInfoPB::ReplacementScheme_Name(scheme)));
      LOG(WARNING) << s.ToString();

      auto* error = resp->mutable_error();
      StatusToPB(s, error->mutable_status());
      error->set_code(MasterErrorPB::INCOMPATIBLE_REPLICA_MANAGEMENT);

      // Yes, this is confusing: the RPC result is success, but the response
      // contains an application-level error.
      rpc->RespondSuccess();
      return;
    }
    Status s = server_->ts_manager()->RegisterTS(req->common().ts_instance(),
                                                 req->registration(),
                                                 server_->dns_resolver(),
                                                 &ts_desc);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Unable to register tserver ($0): $1",
                                 rpc->requestor_string(), s.ToString());
      // TODO(todd): add service-specific errors
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
    }
    if (!s.ok()) {
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
  ts_desc->set_num_live_replicas_by_dimension(
      TabletNumByDimensionMap(req->num_live_tablets_by_dimension().begin(),
                              req->num_live_tablets_by_dimension().end()));

  // 5. Only leaders handle tablet reports.
  if (is_leader_master && req->has_tablet_report()) {
    Status s = server_->catalog_manager()->ProcessTabletReport(
        ts_desc.get(), req->tablet_report(), resp->mutable_tablet_report(), rpc);
    if (!s.ok()) {
      rpc->RespondFailure(s.CloneAndPrepend("Failed to process tablet report"));
      return;
    }
    // If we previously needed a full tablet report for the tserver (e.g.
    // because we need to recheck replica states after exiting from maintenance
    // mode) and have just received a full report, mark that we no longer need
    // a full tablet report.
    if (!req->tablet_report().is_incremental()) {
      ts_desc->UpdateNeedsFullTabletReport(false);
    }
  }

  // 6. Only leaders sign CSR from tablet servers (if present).
  if (is_leader_master && req->has_csr_der()) {
    string cert;
    Status s = server_->cert_authority()->SignServerCSR(
        req->csr_der(), rpc->remote_user(), &cert);
    if (!s.ok()) {
      rpc->RespondFailure(s.CloneAndPrepend("invalid CSR"));
      return;
    }
    LOG(INFO) << "Signed X509 certificate for tserver " << rpc->requestor_string();
    resp->mutable_signed_cert_der()->swap(cert);
    resp->add_ca_cert_der(server_->cert_authority()->ca_cert_der());
  }

  // 7. Only leaders send public parts of non-expired TSK which the TS doesn't
  //    have, except if the '--master_non_leader_masters_propagate_tsk'
  //    test-only flag is set.
  if ((is_leader_master ||
       PREDICT_FALSE(FLAGS_master_non_leader_masters_propagate_tsk)) &&
      req->has_latest_tsk_seq_num()) {
    auto tsk_public_keys = server_->token_signer()->verifier().ExportKeys(
        req->latest_tsk_seq_num());
    for (auto& key : tsk_public_keys) {
      resp->add_tsks()->Swap(&key);
    }
  }

  // 8. Check if we need a full tablet report (e.g. the tablet server just
  //    exited maintenance mode and needs to check whether any replicas need to
  //    be moved).
  if (is_leader_master && ts_desc->needs_full_report()) {
    resp->set_needs_full_tablet_report(true);
  }

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

  CatalogManager::TSInfosDict infos_dict;
  for (const string& tablet_id : req->tablet_ids()) {
    // TODO(todd): once we have catalog data. ACL checks would also go here, probably.
    TabletLocationsPB* locs_pb = resp->add_tablet_locations();
    Status s = server_->catalog_manager()->GetTabletLocations(
        tablet_id, req->replica_type_filter(),
        locs_pb,
        req->intern_ts_infos_in_response() ? &infos_dict : nullptr,
        make_optional<const string&>(rpc->remote_user().username()));
    if (!s.ok()) {
      resp->mutable_tablet_locations()->RemoveLast();

      GetTabletLocationsResponsePB::Error* err = resp->add_errors();
      err->set_tablet_id(tablet_id);
      StatusToPB(s, err->mutable_status());
    }
  }
  for (auto& pb : infos_dict.ts_info_pbs) {
    resp->mutable_ts_infos()->AddAllocated(pb.release());
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

  Status s = server_->catalog_manager()->IsCreateTableDone(
      req, resp, make_optional<const string&>(rpc->remote_user().username()));
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

  Status s = server_->catalog_manager()->DeleteTableRpc(*req, resp, rpc);
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

  Status s = server_->catalog_manager()->AlterTableRpc(*req, resp, rpc);
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

  Status s = server_->catalog_manager()->IsAlterTableDone(
      req, resp, make_optional<const string&>(rpc->remote_user().username()));
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

  Status s = server_->catalog_manager()->ListTables(
      req, resp, make_optional<const string&>(rpc->remote_user().username()));
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTableStatistics(const GetTableStatisticsRequestPB* req,
                                           GetTableStatisticsResponsePB* resp,
                                           rpc::RpcContext* rpc) {
  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }
  Status s = server_->catalog_manager()->GetTableStatistics(
      req, resp, make_optional<const string&>(rpc->remote_user().username()));
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTableLocations(const GetTableLocationsRequestPB* req,
                                          GetTableLocationsResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  TRACE_EVENT1("master", "GetTableLocations",
               "requestor", rpc->requestor_string());

  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  if (PREDICT_FALSE(FLAGS_master_inject_latency_on_tablet_lookups_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_master_inject_latency_on_tablet_lookups_ms));
  }
  Status s = server_->catalog_manager()->GetTableLocations(
      req, resp, make_optional<const string&>(rpc->remote_user().username()));
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

  Status s = server_->catalog_manager()->GetTableSchema(
      req, resp, make_optional<const string&>(rpc->remote_user().username()),
      FLAGS_master_support_authz_tokens ? server_->token_signer() : nullptr);
  CheckRespErrorOrSetUnknown(s, resp);
  if (resp->has_error()) {
    // If there was an application error, respond to the RPC.
    rpc->RespondSuccess();
    return;
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListTabletServers(const ListTabletServersRequestPB* req,
                                          ListTabletServersResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  TSManager* ts_manager = server_->ts_manager();
  TServerStateMap states = req->include_states() ?
      ts_manager->GetTServerStates() : TServerStateMap();
  vector<std::shared_ptr<TSDescriptor>> descs;
  server_->ts_manager()->GetAllDescriptors(&descs);
  for (const std::shared_ptr<TSDescriptor>& desc : descs) {
    ListTabletServersResponsePB::Entry* entry = resp->add_servers();
    desc->GetNodeInstancePB(entry->mutable_instance_id());
    desc->GetRegistration(entry->mutable_registration());
    entry->set_millis_since_heartbeat(desc->TimeSinceHeartbeat().ToMilliseconds());
    if (desc->location()) entry->set_location(desc->location().get());

    // If we need to return states, do so.
    const auto& uuid = desc->permanent_uuid();
    entry->set_state(FindWithDefault(states, uuid, { TServerStatePB::NONE, -1 }).first);
    states.erase(uuid);
  }
  // If there are any states left (e.g. they don't correspond to a registered
  // server), report them. Set a bogus seqno, since the servers have not
  // registered.
  for (const auto& ts_and_state_timestamp : states) {
    const auto& ts = ts_and_state_timestamp.first;
    const auto& state_and_timestamp = ts_and_state_timestamp.second;
    ListTabletServersResponsePB::Entry* entry = resp->add_servers();
    NodeInstancePB* instance = entry->mutable_instance_id();
    instance->set_permanent_uuid(ts);
    instance->set_instance_seqno(-1);
    entry->set_millis_since_heartbeat(-1);
    // Note: The state map should only track non-NONE states.
    DCHECK_NE(TServerStatePB::NONE, state_and_timestamp.first);
    entry->set_state(state_and_timestamp.first);
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListMasters(const ListMastersRequestPB* req,
                                    ListMastersResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  vector<ServerEntryPB> masters;
  Status s = server_->ListMasters(&masters);
  if (!s.ok()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
    resp->mutable_error()->set_code(MasterErrorPB::UNKNOWN_ERROR);

    // Continue setting deprecated error status in order to maintain backwards compat.
    StatusToPB(s, resp->mutable_deprecated_error());
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

  // Set the info about the other masters, so that the client can verify
  // it has the full set of info.
  {
    vector<HostPortPB> hostports;
    WARN_NOT_OK(server_->GetMasterHostPorts(&hostports),
                "unable to get HostPorts for masters");
    resp->mutable_master_addrs()->Reserve(hostports.size());
    for (auto& hp : hostports) {
      *resp->add_master_addrs() = std::move(hp);
    }
  }

  const bool is_leader = l.leader_status().ok();
  if (is_leader) {
    resp->add_ca_cert_der(server_->cert_authority()->ca_cert_der());

    // Issue an authentication token for the caller, unless they are
    // already using a token to authenticate or haven't been authenticated
    // by other means. Don't issue a token if it's about to travel back to the
    // client over a non-confidential channel.
    if (rpc->is_confidential() &&
        rpc->remote_user().authenticated_by() != rpc::RemoteUser::AUTHN_TOKEN) {
      string username = rpc->remote_user().username();
      if (username.empty()) {
        static const char* const kErrMsg = "missing name of the remote user";
        StatusToPB(Status::InvalidArgument(kErrMsg),
                   resp->mutable_error()->mutable_status());
        resp->mutable_error()->set_code(MasterErrorPB::UNKNOWN_ERROR);
        rpc->RespondSuccess();
        KLOG_EVERY_N_SECS(WARNING, 60) << Substitute("invalid request from $0: $1",
                                                     rpc->requestor_string(), kErrMsg);
        return;
      }

      SignedTokenPB authn_token;
      Status s = server_->token_signer()->GenerateAuthnToken(username, &authn_token);
      if (!s.ok()) {
        LOG(FATAL) << Substitute("unable to generate signed token for $0: $1",
                                 rpc->requestor_string(), s.ToString());
      }

      // TODO(todd): this might be a good spot for some auditing code?
      resp->mutable_authn_token()->Swap(&authn_token);
    }
  }

  // Add Hive Metastore information.
  if (hms::HmsCatalog::IsEnabled()) {
    auto* metastore_config = resp->mutable_hms_config();
    metastore_config->set_hms_uris(FLAGS_hive_metastore_uris);
    metastore_config->set_hms_sasl_enabled(FLAGS_hive_metastore_sasl_enabled);
    string uuid;
    if (server_->catalog_manager()->HmsCatalog()->GetUuid(&uuid).ok()) {
      metastore_config->set_hms_uuid(std::move(uuid));
    }
  }

  // Assign a location to the client if needed.
  auto* location_cache = server_->location_cache();
  if (location_cache != nullptr &&
      FLAGS_master_client_location_assignment_enabled) {
    string location;
    const auto s = location_cache->GetLocation(
        rpc->remote_address().host(), &location);
    if (s.ok()) {
      resp->set_client_location(location);
    } else {
      LOG(WARNING) << Substitute("unable to assign location to client $0@$1: $2",
                                 rpc->remote_user().ToString(),
                                 rpc->remote_address().ToString(),
                                 s.ToString());
    }
  }

  // Rather than consulting the current consensus role, instead base it
  // on the catalog manager's view. This prevents us from advertising LEADER
  // until we have taken over all the associated responsibilities.
  resp->set_role(is_leader ? consensus::RaftPeerPB::LEADER
                           : consensus::RaftPeerPB::FOLLOWER);
  rpc->RespondSuccess();
}

void MasterServiceImpl::ReplaceTablet(const ReplaceTabletRequestPB* req,
                                      ReplaceTabletResponsePB* resp,
                                      rpc::RpcContext* rpc) {
  LOG(INFO) << "ReplaceTablet: received request to replace tablet " << req->tablet_id()
            << " from " << rpc->requestor_string();

  CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->ReplaceTablet(req->tablet_id(), resp);
  CheckRespErrorOrSetUnknown(s, resp);
  rpc->RespondSuccess();
}

void MasterServiceImpl::ResetAuthzCache(
    const ResetAuthzCacheRequestPB* /* req */,
    ResetAuthzCacheResponsePB* resp,
    rpc::RpcContext* rpc) {
  LOG(INFO) << Substitute("request to reset authz privileges cache from $0",
                          rpc->requestor_string());
  CheckRespErrorOrSetUnknown(
      server_->catalog_manager()->authz_provider()->ResetCache(), resp);
  rpc->RespondSuccess();
}

bool MasterServiceImpl::SupportsFeature(uint32_t feature) const {
  switch (feature) {
    case MasterFeatures::RANGE_PARTITION_BOUNDS:    FALLTHROUGH_INTENDED;
    case MasterFeatures::ADD_DROP_RANGE_PARTITIONS: FALLTHROUGH_INTENDED;
    case MasterFeatures::REPLICA_MANAGEMENT:
      return true;
    case MasterFeatures::GENERATE_AUTHZ_TOKEN:
      return FLAGS_master_support_authz_tokens;
    case MasterFeatures::CONNECT_TO_MASTER:
      return FLAGS_master_support_connect_to_master_rpc;
    default:
      return false;
  }
}

} // namespace master
} // namespace kudu
