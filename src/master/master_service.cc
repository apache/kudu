// Copyright (c) 2013, Cloudera, inc.

#include "master/master_service.h"

#include <boost/foreach.hpp>
#include <string>
#include <tr1/memory>
#include <vector>

#include "common/wire_protocol.h"
#include "master/catalog_manager.h"
#include "master/master.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "rpc/rpc_context.h"

using std::tr1::shared_ptr;

namespace kudu {
namespace master {

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
  resp->mutable_master_instance()->CopyFrom(server_->instance_pb());

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

  // Look up the TS -- if it just registered above, it will be found here.
  // This allows the TS to register and tablet-report in the same RPC.
  s = server_->ts_manager()->LookupTS(req->common().ts_instance(), &ts_desc);
  if (s.IsNotFound()) {
    LOG(INFO) << "Got heartbeat from " << rpc->requestor_string() << " for unknown "
              << "tablet server " << req->common().ts_instance().DebugString()
              << ": asking to re-register.";
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
      ts_desc.get(), req->tablet_report(), rpc);
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

  TSRegistrationPB reg;
  vector<TSDescriptor*> locs;
  BOOST_FOREACH(const string& tablet_id, req->tablet_ids()) {
    // TODO: Add some kind of verification that the tablet is actually valid
    // (i.e that it is present in the 'tablets' system table)
    // TODO: once we have catalog data. ACL checks would also go here, probably.
    server_->catalog_manager()->GetTabletLocations(tablet_id, &locs);

    TabletLocationsPB* locs_pb = resp->add_tablet_locations();
    locs_pb->set_tablet_id(tablet_id);

    BOOST_FOREACH(const TSDescriptor* ts_desc, locs) {
      TabletLocationsPB_ReplicaPB* replica = locs_pb->add_replicas();
      TSInfoPB* tsinfo_pb = replica->mutable_ts_info();
      tsinfo_pb->set_permanent_uuid(ts_desc->permanent_uuid());

      ts_desc->GetRegistration(&reg);
      tsinfo_pb->mutable_rpc_addresses()->Swap(reg.mutable_rpc_addresses());
    }
  }

  rpc->RespondSuccess();
}

void MasterServiceImpl::CreateTable(const CreateTableRequestPB* req,
                                    CreateTableResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  Status s = server_->catalog_manager()->CreateTable(req, resp, rpc);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::DeleteTable(const DeleteTableRequestPB* req,
                                    DeleteTableResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  Status s = server_->catalog_manager()->DeleteTable(req, resp, rpc);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

void MasterServiceImpl::ListTables(const ListTablesRequestPB* req,
                                   ListTablesResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  Status s = server_->catalog_manager()->ListTables(req, resp);
  if (!s.ok() && !resp->has_error()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
  }
  rpc->RespondSuccess();
}

} // namespace master
} // namespace kudu
