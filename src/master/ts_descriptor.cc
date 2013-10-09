// Copyright (c) 2013, Cloudera, inc.

#include "gutil/strings/substitute.h"
#include "master/ts_descriptor.h"
#include "master/master.pb.h"

#include <boost/thread/mutex.hpp>

namespace kudu {
namespace master {

Status TSDescriptor::RegisterNew(const NodeInstancePB& instance,
                                 const TSRegistrationPB& registration,
                                 gscoped_ptr<TSDescriptor>* desc) {
  gscoped_ptr<TSDescriptor> ret(new TSDescriptor(instance.permanent_uuid()));
  RETURN_NOT_OK(ret->Register(instance, registration));
  desc->reset(ret.release());
  return Status::OK();
}

TSDescriptor::TSDescriptor(const std::string& perm_id)
  : permanent_uuid_(perm_id),
    latest_seqno_(-1),
    last_heartbeat_(MonoTime::Now(MonoTime::FINE)),
    has_tablet_report_(false) {
}

TSDescriptor::~TSDescriptor() {
}

Status TSDescriptor::Register(const NodeInstancePB& instance,
                              const TSRegistrationPB& registration) {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(instance.permanent_uuid(), permanent_uuid_);

  if (instance.instance_seqno() <= latest_seqno_) {
    return Status::AlreadyPresent(
      strings::Substitute("Cannot register with sequence number $0:"
                          " Already have a registration from sequence number $1",
                          instance.instance_seqno(),
                          latest_seqno_));
  }

  latest_seqno_ = instance.instance_seqno();
  // After re-registering, make the TS re-report its tablets.
  has_tablet_report_ = false;

  registration_.reset(new TSRegistrationPB(registration));

  return Status::OK();
}

void TSDescriptor::UpdateHeartbeatTime() {
  boost::lock_guard<simple_spinlock> l(lock_);
  last_heartbeat_ = MonoTime::Now(MonoTime::FINE);
}

MonoDelta TSDescriptor::TimeSinceHeartbeat() const {
  MonoTime now(MonoTime::Now(MonoTime::FINE));
  boost::lock_guard<simple_spinlock> l(lock_);
  return now.GetDeltaSince(last_heartbeat_);
}

int64_t TSDescriptor::latest_seqno() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return latest_seqno_;
}

bool TSDescriptor::has_tablet_report() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return has_tablet_report_;
}

void TSDescriptor::set_has_tablet_report(bool has_report) {
  boost::lock_guard<simple_spinlock> l(lock_);
  has_tablet_report_ = has_report;
}

void TSDescriptor::GetRegistration(TSRegistrationPB* reg) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK(registration_) << "No registration";
  CHECK_NOTNULL(reg)->CopyFrom(*registration_);
}

} // namespace master
} // namespace kudu
