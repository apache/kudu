// Copyright (c) 2013, Cloudera, inc.

#include "master/m_tablet_manager.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <string>
#include <vector>

#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "master/master.pb.h"
#include "master/ts_descriptor.h"
#include "rpc/rpc_context.h"

namespace kudu {
namespace master {

using strings::Substitute;
using rpc::RpcContext;

////////////////////////////////////////////////////////////
// MTabletManager
////////////////////////////////////////////////////////////

MTabletManager::MTabletManager() {
}

MTabletManager::~MTabletManager() {
  STLDeleteValues(&tablet_map_);
}

void MTabletManager::GetTabletLocations(const std::string& tablet_id,
                                        std::vector<TSDescriptor*>* locations) {
  boost::shared_lock<LockType> l(lock_);
  TabletInfo* info = FindPtrOrNull(tablet_map_, tablet_id);
  if (info == NULL) {
    locations->clear();
    return;
  }

  *locations = info->locations();
}

Status MTabletManager::ProcessTabletReport(TSDescriptor* ts_desc,
                                           const TabletReportPB& report,
                                           RpcContext* rpc) {
  boost::lock_guard<LockType> l(lock_);

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Received tablet report from " <<
      rpc->requestor_string() << ": " << report.DebugString();
  }
  if (!ts_desc->has_tablet_report() && report.is_incremental()) {
    string msg = "Received an incremental tablet report when a full one was needed";
    LOG(WARNING) << "Invalid tablet report from " << rpc->requestor_string() << ": "
                 << msg;
    return Status::IllegalState(msg);
  }

  // If it's non-incremental, we need to clear all tablets which we previously
  // thought were on this server.
  // TODO: should we also have a map of server->tablet, not just tablet->server,
  // so this is O(tablets on server) instead of O(total replicas)? Optimization
  // for later, unless we find some functional reason to add it, I guess.
  if (!report.is_incremental()) {
    ClearAllReplicasOnTS(ts_desc);
  }

  BOOST_FOREACH(const ReportedTabletPB& reported, report.updated_tablets()) {
    RETURN_NOT_OK_PREPEND(HandleReportedTablet(ts_desc, reported, rpc),
                          Substitute("Error handling $0", reported.ShortDebugString()));
  }

  CHECK_EQ(report.removed_tablet_ids().size(), 0) << "TODO: implement tablet removal";

  ts_desc->set_has_tablet_report(true);
  return Status::OK();
}

void MTabletManager::ClearAllReplicasOnTS(TSDescriptor* ts_desc) {
  DCHECK(lock_.is_write_locked());
  BOOST_FOREACH(TabletInfoMap::value_type& e, tablet_map_) {
    e.second->ClearReplicasOnTS(ts_desc);
  }
}

Status MTabletManager::HandleReportedTablet(TSDescriptor* ts_desc,
                                            const ReportedTabletPB& report,
                                            RpcContext* rpc) {
  DCHECK(lock_.is_write_locked());

  TabletInfo* tinfo = LookupOrInsertNew(&tablet_map_, report.tablet_id(),
                                        report.tablet_id());
  tinfo->AddReplica(ts_desc);
  return Status::OK();
}

////////////////////////////////////////////////////////////
// TabletInfo
////////////////////////////////////////////////////////////

TabletInfo::TabletInfo(const std::string& tablet_id)
  : tablet_id_(tablet_id) {
}

TabletInfo::~TabletInfo() {
}

void TabletInfo::AddReplica(TSDescriptor* ts_desc) {
  BOOST_FOREACH(const TSDescriptor* l, locations_) {
    if (l == ts_desc) return;
  }
  VLOG(2) << tablet_id_ << " reported on " << ts_desc->permanent_uuid();
  locations_.push_back(ts_desc);
}

void TabletInfo::ClearReplicasOnTS(const TSDescriptor* ts) {
  std::vector<TSDescriptor*>::iterator it = locations_.begin();
  while (it != locations_.end()) {
    if (*it == ts) {
      it = locations_.erase(it);
    } else {
      ++it;
    }
  }
}

} // namespace master
} // namespace kudu
