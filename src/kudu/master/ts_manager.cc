// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/ts_manager.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_descriptor.h"

using std::string;
using std::tr1::shared_ptr;

namespace kudu {
namespace master {

TSManager::TSManager() {
}

TSManager::~TSManager() {
}

Status TSManager::LookupTS(const NodeInstancePB& instance,
                           shared_ptr<TSDescriptor>* ts_desc) {
  boost::shared_lock<rw_spinlock> l(lock_);
  const shared_ptr<TSDescriptor>* found_ptr =
    FindOrNull(servers_by_id_, instance.permanent_uuid());
  if (!found_ptr) {
    return Status::NotFound("unknown tablet server ID", instance.ShortDebugString());
  }
  const shared_ptr<TSDescriptor>& found = *found_ptr;

  if (instance.instance_seqno() != found->latest_seqno()) {
    return Status::NotFound("mismatched instance sequence number", instance.ShortDebugString());
  }

  *ts_desc = found;
  return Status::OK();
}

bool TSManager::LookupTSByUUID(const string& uuid,
                               std::tr1::shared_ptr<TSDescriptor>* ts_desc) {
  boost::shared_lock<rw_spinlock> l(lock_);
  return FindCopy(servers_by_id_, uuid, ts_desc);
}

Status TSManager::RegisterTS(const NodeInstancePB& instance,
                             const TSRegistrationPB& registration,
                             std::tr1::shared_ptr<TSDescriptor>* desc) {
  boost::lock_guard<rw_spinlock> l(lock_);
  const string& uuid = instance.permanent_uuid();

  if (!ContainsKey(servers_by_id_, uuid)) {
    gscoped_ptr<TSDescriptor> new_desc;
    RETURN_NOT_OK(TSDescriptor::RegisterNew(instance, registration, &new_desc));
    InsertOrDie(&servers_by_id_, uuid, shared_ptr<TSDescriptor>(new_desc.release()));
    LOG(INFO) << "Registered new tablet server { " << instance.ShortDebugString()
              << " } with Master";
  } else {
    const shared_ptr<TSDescriptor>& found = FindOrDie(servers_by_id_, uuid);
    RETURN_NOT_OK(found->Register(instance, registration));
    LOG(INFO) << "Re-registered known tablet server { " << instance.ShortDebugString()
              << " } with Master";
  }

  return Status::OK();
}

void TSManager::GetAllDescriptors(std::vector<std::tr1::shared_ptr<TSDescriptor> > *descs) const {
  boost::shared_lock<rw_spinlock> l(lock_);
  descs->clear();
  AppendValuesFromMap(servers_by_id_, descs);
}

int TSManager::GetCount() const {
  boost::shared_lock<rw_spinlock> l(lock_);
  return servers_by_id_.size();
}

} // namespace master
} // namespace kudu

