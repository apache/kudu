// Copyright (c) 2013, Cloudera, inc.

#include "tserver/heartbeater.h"

#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <vector>

#include "gutil/strings/substitute.h"
#include "master/master.h"
#include "master/master.proxy.h"
#include "server/webserver.h"
#include "tserver/tablet_server.h"
#include "tserver/tablet_server_options.h"
#include "util/countdown_latch.h"
#include "util/monotime.h"
#include "util/status.h"
#include "util/thread_util.h"

DEFINE_int32(heartbeat_rpc_timeout_ms, 15000,
             "Timeout used for the TS->Master heartbeat RPCs. "
             "(Advanced option)");

DEFINE_int32(heartbeat_interval_ms, 1000,
             "Interval at which the TS heartbeats to the master. "
             "(Advanced option)");

enum {
  kMaxConsecutiveFastHeartbeats = 3
};

using google::protobuf::RepeatedPtrField;
using kudu::master::HostPortPB;
using kudu::master::Master;
using kudu::master::MasterServiceProxy;
using kudu::rpc::RpcController;
using strings::Substitute;

namespace kudu {
namespace tserver {

// Most of the actual logic of the heartbeater is inside this inner class,
// to avoid having too many dependencies from the header itself.
//
// This is basically the "PIMPL" pattern.
class Heartbeater::Thread {
 public:
  Thread(const TabletServerOptions& opts, TabletServer* server);

  Status Start();
  Status Stop();

 private:
  void RunThread();
  Status ConnectToMaster();
  int GetMinimumHeartbeatMillis() const;
  int GetMillisUntilNextHeartbeat() const;
  Status DoHeartbeat();
  Status SetupRegistration(master::TSRegistrationPB* reg);
  void SetupCommonField(master::TSToMasterCommonPB* common);
  bool IsCurrentThread() const;

  // The host/port that we are heartbeating to.
  // We keep the HostPort around rather than a Sockaddr because
  // the master may change IP addresses, and we'd like to re-resolve
  // on every new attempt at connecting.
  HostPort master_hostport_;

  // The server for which we are heartbeating.
  TabletServer* const server_;

  // The actual running thread (NULL before it is started)
  gscoped_ptr<boost::thread> thread_;

  // RPC proxy to the master.
  gscoped_ptr<master::MasterServiceProxy> proxy_;

  // The most recent response from a heartbeat.
  master::TSHeartbeatResponsePB last_hb_response_;

  // True once at least one heartbeat has been sent.
  bool has_heartbeated_;

  // The number of heartbeats which have failed in a row.
  // This is tracked so as to back-off heartbeating.
  int consecutive_failed_heartbeats_;

  // While this hasn't counted down to 0, the thread should keep running.
  CountDownLatch run_latch_;

  DISALLOW_COPY_AND_ASSIGN(Thread);
};

////////////////////////////////////////////////////////////
// Heartbeater
////////////////////////////////////////////////////////////

Heartbeater::Heartbeater(const TabletServerOptions& opts, TabletServer* server)
  : thread_(new Thread(opts, server)) {
}
Heartbeater::~Heartbeater() {
  WARN_NOT_OK(Stop(), "Unable to stop heartbeater thread");
}

Status Heartbeater::Start() { return thread_->Start(); }
Status Heartbeater::Stop() { return thread_->Stop(); }

////////////////////////////////////////////////////////////
// Heartbeater::Thread
////////////////////////////////////////////////////////////

Heartbeater::Thread::Thread(const TabletServerOptions& opts, TabletServer* server)
  : master_hostport_(opts.master_hostport),
    server_(server),
    has_heartbeated_(false),
    consecutive_failed_heartbeats_(0),
    run_latch_(0) {
}

Status Heartbeater::Thread::ConnectToMaster() {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(master_hostport_.ResolveAddresses(&addrs));
  if (addrs.size() > 1) {
    LOG(WARNING) << "Master address '" << master_hostport_.ToString() << "' "
                 << "resolves to " << addrs.size() << " different addresses. Using "
                 << addrs[0].ToString();
  }
  gscoped_ptr<MasterServiceProxy> new_proxy(
    new MasterServiceProxy(server_->messenger(), addrs[0]));

  // Ping the master to verify that it's alive.
  master::PingRequestPB req;
  master::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_heartbeat_rpc_timeout_ms));
  RETURN_NOT_OK_PREPEND(new_proxy->Ping(req, &resp, &rpc),
                        Substitute("Failed to ping master at $0", addrs[0].ToString()));

  proxy_.reset(new_proxy.release());
  return Status::OK();
}

void Heartbeater::Thread::SetupCommonField(master::TSToMasterCommonPB* common) {
  common->mutable_ts_instance()->CopyFrom(server_->instance_pb());
}

static void AddHostPortPBs(const vector<Sockaddr>& addrs,
                           RepeatedPtrField<HostPortPB>* pbs) {
  BOOST_FOREACH(const Sockaddr& addr, addrs) {
    HostPortPB* pb = pbs->Add();
    pb->set_host(addr.host());
    pb->set_port(addr.port());
  }
}

Status Heartbeater::Thread::SetupRegistration(master::TSRegistrationPB* reg) {
  reg->Clear();

  vector<Sockaddr> addrs;
  CHECK_NOTNULL(server_->rpc_server())->GetBoundAddresses(&addrs);
  AddHostPortPBs(addrs, reg->mutable_rpc_addresses());

  addrs.clear();
  RETURN_NOT_OK_PREPEND(CHECK_NOTNULL(server_->web_server())->GetBoundAddresses(&addrs),
                        "Unable to get bound HTTP addresses");
  AddHostPortPBs(addrs, reg->mutable_http_addresses());

  return Status::OK();
}

int Heartbeater::Thread::GetMinimumHeartbeatMillis() const {
  // If we've failed a few heartbeats in a row, back off to the normal
  // interval, rather than retrying in a loop.
  if (consecutive_failed_heartbeats_ == kMaxConsecutiveFastHeartbeats) {
    LOG(WARNING) << "Failed " << consecutive_failed_heartbeats_  <<" heartbeats "
                 << "in a row: no longer allowing fast heartbeat attempts.";
  }

  return consecutive_failed_heartbeats_ > kMaxConsecutiveFastHeartbeats ?
    FLAGS_heartbeat_interval_ms : 0;
}

int Heartbeater::Thread::GetMillisUntilNextHeartbeat() const {
  // When we first start up, heartbeat immediately.
  if (!has_heartbeated_) {
    return GetMinimumHeartbeatMillis();
  }

  // If the master needs something from us, we should immediately
  // send another heartbeat with that info, rather than waiting for the interval.
  if (last_hb_response_.needs_reregister() ||
      last_hb_response_.needs_full_tablet_report()) {
    return GetMinimumHeartbeatMillis();
  }

  return FLAGS_heartbeat_interval_ms;
}

Status Heartbeater::Thread::DoHeartbeat() {
  CHECK(IsCurrentThread());

  if (!proxy_) {
    VLOG(1) << "No valid master proxy. Connecting...";
    RETURN_NOT_OK(ConnectToMaster());
    DCHECK(proxy_);
  }

  master::TSHeartbeatRequestPB req;

  SetupCommonField(req.mutable_common());
  if (last_hb_response_.needs_reregister()) {
    LOG(INFO) << "Registering TS with master...";
    RETURN_NOT_OK_PREPEND(SetupRegistration(req.mutable_registration()),
                          "Unable to set up registration");
  }

  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));

  VLOG(1) << "Sending heartbeat:\n" << req.DebugString();
  RETURN_NOT_OK_PREPEND(proxy_->TSHeartbeat(req, &last_hb_response_, &rpc),
                        "Failed to send heartbeat");
  VLOG(1) << "Received heartbeat response:\n" << last_hb_response_.DebugString();

  return Status::OK();
}

void Heartbeater::Thread::RunThread() {
  CHECK(IsCurrentThread());
  VLOG(1) << "Heartbeat thread starting";

  // Set up a fake "last heartbeat response" which indicates that we need to
  // register -- since we've never registered before, we know this to be true.
  // This avoids an extra heartbeat at startup.
  last_hb_response_.set_needs_reregister(true);
  last_hb_response_.set_needs_full_tablet_report(true);

  while (true) {
    MonoTime next_heartbeat = MonoTime::Now(MonoTime::FINE);
    next_heartbeat.AddDelta(MonoDelta::FromMilliseconds(GetMillisUntilNextHeartbeat()));
    if (run_latch_.WaitUntil(next_heartbeat)) {
      // Latch fired -- exit loop
      VLOG(1) << "Heartbeat thread finished";
      return;
    }

    Status s = DoHeartbeat();
    if (!s.ok()) {
      LOG(WARNING) << "Failed to heartbeat: " << s.ToString();
      consecutive_failed_heartbeats_++;
      continue;
    }
    consecutive_failed_heartbeats_ = 0;
    has_heartbeated_ = true;
  }
}

bool Heartbeater::Thread::IsCurrentThread() const {
  return thread_ && thread_->get_id() == boost::this_thread::get_id();
}

Status Heartbeater::Thread::Start() {
  CHECK(thread_ == NULL);

  run_latch_.Reset(1);
  return StartThread(boost::bind(&Heartbeater::Thread::RunThread, this), &thread_);
}

Status Heartbeater::Thread::Stop() {
  if (!thread_) {
    return Status::OK();
  }

  run_latch_.CountDown();
  RETURN_NOT_OK(ThreadJoiner(thread_.get(), "heartbeat thread").Join());
  thread_.reset();
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
