// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/master_service.h"

#include <boost/foreach.hpp>
#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/webserver.h"

namespace kudu {
namespace master {

using std::string;
using std::vector;
using std::tr1::shared_ptr;

using metadata::QuorumPeerPB;

namespace {

template<class RespClass>
bool CheckCatalogManagerInitializedOrRespond(Master* master,
                                             RespClass* resp,
                                             rpc::RpcContext* rpc) {
  if (PREDICT_FALSE(!master->catalog_manager()->IsInitialized())) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::ServiceUnavailable("catalog manager has not been initialized"),
                         MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED,
                         rpc);
    return false;
  }
  return true;
}

template<class RespClass>
bool CheckIsLeaderOrRespond(Master* master,
                            RespClass* resp,
                            rpc::RpcContext* rpc) {
  if (PREDICT_FALSE(!master->catalog_manager()->IsLeaderAndReady())) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::ServiceUnavailable("operation requested can only be executed "
                                                    "on a leader master or a single node master"),
                         MasterErrorPB::NOT_THE_LEADER,
                         rpc);
    return false;
  }
  return true;
}

template<class RespClass>
bool CheckLeaderAndCatalogManagerInitializedOrRespond(Master* master,
                                                      RespClass* resp,
                                                      rpc::RpcContext* rpc) {
  return PREDICT_TRUE(CheckCatalogManagerInitializedOrRespond(master, resp, rpc) &&
                      CheckIsLeaderOrRespond(master, resp, rpc));
}

template<class RespClass>
void RespondNoLongerLeader(RespClass* resp, rpc::RpcContext* rpc) {
  SetupErrorAndRespond(
      resp->mutable_error(),
      Status::ServiceUnavailable("operation requested can only be executed on a leader"
                                 "master, but this master is no longer a leader"),
      MasterErrorPB::NOT_THE_LEADER,
      rpc);
}

} // anonymous namespace

static void SetupErrorAndRespond(MasterErrorPB* error,
                                 const Status& s,
                                 MasterErrorPB::Code code,
                                 rpc::RpcContext* rpc) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
  // TODO RespondSuccess() is better called 'Respond'.
  rpc->RespondSuccess();
}


MasterServiceImpl::MasterServiceImpl(Master* server)
  : MasterServiceIf(server->metric_context()),
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
  if (!CheckCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  resp->mutable_master_instance()->CopyFrom(server_->instance_pb());
  if (!server_->catalog_manager()->IsLeaderAndReady()) {
    // For the time being, ignore heartbeats sent to non-leader distributed
    // masters.
    //
    // TODO KUDU-493 Allow all master processes to receive heartbeat
    // information: by having the TabletServers send heartbeats to all
    // masters, or by storing heartbeat information in a replicated
    // SysTable.
    LOG(WARNING) << "Received a heartbeat, but this Master instance is not a leader or a "
                 << "single Master.";
    resp->set_leader_master(false);
    rpc->RespondSuccess();
    return;
  }
  resp->set_leader_master(true);

  shared_ptr<TSDescriptor> ts_desc;
  Status s;
  // If the TS is registering, register in the TS manager.
  if (req->has_registration()) {
    s = server_->ts_manager()->RegisterTS(req->common().ts_instance(),
                                          req->registration(),
                                          &ts_desc);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to register tablet server (" << rpc->requestor_string() << "): "
                   << s.ToString();
      // TODO: add service-specific errors
      rpc->RespondFailure(s);
      return;
    }
  }

  // TODO: KUDU-86 if something fails after this point the TS will not be able
  //       to register again.

  // Look up the TS -- if it just registered above, it will be found here.
  // This allows the TS to register and tablet-report in the same RPC.
  s = server_->ts_manager()->LookupTS(req->common().ts_instance(), &ts_desc);
  if (s.IsNotFound()) {
    LOG(INFO) << "Got heartbeat from  unknown tablet server { "
              << req->common().ts_instance().ShortDebugString()
              << " } as " << rpc->requestor_string()
              << "; Asking this server to re-register.";
    resp->set_needs_reregister(true);
    resp->set_needs_full_tablet_report(true);
    rpc->RespondSuccess();
    return;
  } else if (!s.ok()) {
    LOG(WARNING) << "Unable to look up tablet server for heartbeat request "
                 << req->DebugString() << " from " << rpc->requestor_string()
                 << "\nStatus: " << s.ToString();
    rpc->RespondFailure(s.CloneAndPrepend("Unable to lookup TS"));
    return;
  }

  ts_desc->UpdateHeartbeatTime();

  if (req->has_tablet_report()) {
    s = server_->catalog_manager()->ProcessTabletReport(
      ts_desc.get(), req->tablet_report(), resp->mutable_tablet_report(), rpc);
    if (!s.ok()) {
      rpc->RespondFailure(s.CloneAndPrepend("Failed to process tablet report"));
      return;
    }
  }

  if (!ts_desc->has_tablet_report()) {
    resp->set_needs_full_tablet_report(true);
  }

  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTabletLocations(const GetTabletLocationsRequestPB* req,
                                           GetTabletLocationsResponsePB* resp,
                                           rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  TSRegistrationPB reg;
  vector<TSDescriptor*> locs;
  BOOST_FOREACH(const string& tablet_id, req->tablet_ids()) {
    // TODO: Add some kind of verification that the tablet is actually valid
    // (i.e that it is present in the 'tablets' system table)
    // TODO: once we have catalog data. ACL checks would also go here, probably.
    TabletLocationsPB* locs_pb = resp->add_tablet_locations();
    if (!server_->catalog_manager()->GetTabletLocations(tablet_id, locs_pb)) {
      resp->mutable_tablet_locations()->RemoveLast();
    }
  }

  rpc->RespondSuccess();
}

void MasterServiceImpl::CreateTable(const CreateTableRequestPB* req,
                                    CreateTableResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->CreateTable(req, resp, rpc);
  if (!s.ok() && !resp->has_error()) {
    if (s.IsIllegalState()) {
      // TODO: This is a bit of a hack, as right now there's no way to
      // propagate why a write to a quorum has failed. However, since
      // we use Status::IllegalState() to indicate the situation where
      // a write was issued on a node that is no longer the leader,
      // this suffices until we distinguish this cause of write
      // failure more explicitly.
      RespondNoLongerLeader(resp, rpc);
      return;
    } else {
      StatusToPB(s, resp->mutable_error()->mutable_status());
    }
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                           IsCreateTableDoneResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->IsCreateTableDone(req, resp);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::DeleteTable(const DeleteTableRequestPB* req,
                                    DeleteTableResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->DeleteTable(req, resp, rpc);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::AlterTable(const AlterTableRequestPB* req,
                                   AlterTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->AlterTable(req, resp, rpc);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                                         IsAlterTableDoneResponsePB* resp,
                                         rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->IsAlterTableDone(req, resp, rpc);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListTables(const ListTablesRequestPB* req,
                                   ListTablesResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->ListTables(req, resp);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTableLocations(const GetTableLocationsRequestPB* req,
                                          GetTableLocationsResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->GetTableLocations(req, resp);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::GetTableSchema(const GetTableSchemaRequestPB* req,
                                       GetTableSchemaResponsePB* resp,
                                       rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  Status s = server_->catalog_manager()->GetTableSchema(req, resp);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListTabletServers(const ListTabletServersRequestPB* req,
                                          ListTabletServersResponsePB* resp,
                                          rpc::RpcContext* rpc) {
  if (!CheckLeaderAndCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }

  vector<std::tr1::shared_ptr<TSDescriptor> > descs;
  server_->ts_manager()->GetAllDescriptors(&descs);
  BOOST_FOREACH(const std::tr1::shared_ptr<TSDescriptor>& desc, descs) {
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
  } else {
    BOOST_FOREACH(const ServerEntryPB& master, masters) {
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
  if (!CheckCatalogManagerInitializedOrRespond(server_, resp, rpc)) {
    return;
  }
  Status s = server_->GetMasterRegistration(resp->mutable_registration());
  if (!s.ok()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  resp->set_role(server_->catalog_manager()->Role());
  rpc->RespondSuccess();
}

} // namespace master
} // namespace kudu
