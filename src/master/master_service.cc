// Copyright (c) 2013, Cloudera, inc.

#include "master/master_service.h"

#include <string>
#include <tr1/memory>

#include "master/master.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "rpc/rpc_context.h"

using std::tr1::shared_ptr;

namespace kudu {
namespace master {

MasterServiceImpl::MasterServiceImpl(Master* server)
  : server_(server) {
}

void MasterServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* rpc) {
  rpc->RespondSuccess();
}

void MasterServiceImpl::TSHeartbeat(const TSHeartbeatRequestPB* req,
                                    TSHeartbeatResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  SetMasterInstancePB(resp->mutable_master_instance());

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
  if (!s.ok()) {
    LOG(INFO) << "Unable to find tablet server descriptor for " <<
      rpc->requestor_string() << ": " << s.ToString();

    resp->set_needs_reregister(true);
    resp->set_needs_full_tablet_report(true);
    rpc->RespondSuccess();
    return;
  }

  ts_desc->UpdateHeartbeatTime();

  if (req->has_tablet_report()) {
    s = ProcessTabletReport(ts_desc, req->tablet_report(), rpc);
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

Status MasterServiceImpl::ProcessTabletReport(const shared_ptr<TSDescriptor>& ts_desc,
                                              const TabletReportPB& report,
                                              rpc::RpcContext* rpc) {
  LOG(INFO) << "Received tablet report from " <<
    rpc->requestor_string() << ": " << report.DebugString();
  if (!ts_desc->has_tablet_report() && report.is_incremental()) {
    string msg = "Received an incremental tablet report when a full one was needed";
    LOG(WARNING) << "Invalid tablet report from " << rpc->requestor_string() << ": "
                 << msg;
    return Status::IllegalState(msg);
  }

  // TODO: actually do something with the tablets - pass them to whatever
  // component manages the tablet locations map in the master.

  ts_desc->set_has_tablet_report(true);
  return Status::OK();
}


void MasterServiceImpl::SetMasterInstancePB(NodeInstancePB* pb) const {
  // TODO: the master should have some persistent storage where, upon
  // first startup, it picks a permanent UUID, and then on other startups
  // generates an instance ID. The TS will also need this. Stubbed out
  // for now.
  pb->set_permanent_uuid("TODO permanent");
  pb->set_instance_seqno(1);
}

} // namespace master
} // namespace kudu
