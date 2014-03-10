// Copyright (c) 2013, Cloudera, inc.

#include "common/wire_protocol.h"
#include "gutil/strings/substitute.h"
#include "master/ts_descriptor.h"
#include "master/master.pb.h"
#include "tserver/tserver_service.proxy.h"
#include "util/net/net_util.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include <vector>

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
  proxy_.reset();

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

void TSDescriptor::GetNodeInstancePB(NodeInstancePB* instance_pb) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  instance_pb->set_permanent_uuid(permanent_uuid_);
  instance_pb->set_instance_seqno(latest_seqno_);
}

Status TSDescriptor::GetProxy(const std::tr1::shared_ptr<rpc::Messenger>& messenger,
                              std::tr1::shared_ptr<tserver::TabletServerServiceProxy>* proxy) {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (proxy_ == NULL) {
    HostPort hostport;
    vector<Sockaddr> addrs;
    BOOST_FOREACH(const HostPortPB& addr, registration_->rpc_addresses()) {
      hostport = HostPort(addr.host(), addr.port());
      RETURN_NOT_OK(hostport.ResolveAddresses(&addrs));
      if (addrs.size() > 0) break;
    }

    if (addrs.size() == 0) {
      return Status::NetworkError("Unable to find the TS address: ", registration_->DebugString());
    }

    if (addrs.size() > 1) {
      LOG(WARNING) << "TS address " << hostport.ToString()
                   << " resolves to " << addrs.size() << " different addresses. Using "
                   << addrs[0].ToString();
    }

    proxy_.reset(new tserver::TabletServerServiceProxy(messenger, addrs[0]));
  }
  *proxy = proxy_;
  return Status::OK();
}

} // namespace master
} // namespace kudu
