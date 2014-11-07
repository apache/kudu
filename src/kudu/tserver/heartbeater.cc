// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tserver/heartbeater.h"

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/server/webserver.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/thread.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

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
using kudu::HostPortPB;
using kudu::master::ListMastersResponsePB;
using kudu::master::Master;
using kudu::master::MasterServiceProxy;
using kudu::metadata::QuorumPeerPB;
using kudu::rpc::RpcController;
using std::tr1::shared_ptr;
using strings::Substitute;

namespace kudu {
namespace tserver {

namespace {

// Creates a proxy to 'hostport'.
Status MasterServiceProxyForHostPort(const HostPort& hostport,
                                     const shared_ptr<rpc::Messenger>& messenger,
                                     gscoped_ptr<MasterServiceProxy>* proxy) {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(hostport.ResolveAddresses(&addrs));
  if (addrs.size() > 1) {
    LOG(WARNING) << "Master address '" << hostport.ToString() << "' "
                 << "resolves to " << addrs.size() << " different addresses. Using "
                 << addrs[0].ToString();
  }
  proxy->reset(new MasterServiceProxy(messenger, addrs[0]));
  return Status::OK();
}

} // anonymous namespace

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
  Status FindLeaderMaster(const MonoTime& deadline,
                          HostPort* leader_hostport);
  Status ConnectToMaster();
  int GetMinimumHeartbeatMillis() const;
  int GetMillisUntilNextHeartbeat() const;
  Status DoHeartbeat();
  Status SetupRegistration(master::TSRegistrationPB* reg);
  void SetupCommonField(master::TSToMasterCommonPB* common);
  bool IsCurrentThread() const;

  // The hosts/ports of masters that we may heartbeat to.
  //
  // We keep the HostPort around rather than a Sockaddr because the
  // masters may change IP addresses, and we'd like to re-resolve on
  // every new attempt at connecting.
  vector<HostPort> master_addrs_;

  // Index of the master we last succesfully obtained the master
  // quorum information from.
  int last_locate_master_idx_;

  // The server for which we are heartbeating.
  TabletServer* const server_;

  // The actual running thread (NULL before it is started)
  scoped_refptr<kudu::Thread> thread_;

  // Host and port of the most recent leader master.
  HostPort leader_master_hostport_;

  // Current RPC proxy to the leader master.
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
  : master_addrs_(opts.master_addresses),
    last_locate_master_idx_(0),
    server_(server),
    has_heartbeated_(false),
    consecutive_failed_heartbeats_(0),
    run_latch_(0) {
  CHECK(!master_addrs_.empty());
}

Status Heartbeater::Thread::FindLeaderMaster(const MonoTime& deadline,
                                             HostPort* leader_hostport) {
  Status s = Status::OK();
  for (int num_attempts = 0; num_attempts < master_addrs_.size(); num_attempts++) {
    // If node at last_locate_master_idx_ is no longer responding, try sending the
    // request to the node at the next index, looping around to index
    // 0, until we've exhausted the list of all masters.
    if (++last_locate_master_idx_ == master_addrs_.size()) {
      last_locate_master_idx_ = 0;
    }
    gscoped_ptr<MasterServiceProxy> proxy;
    s = MasterServiceProxyForHostPort(master_addrs_[last_locate_master_idx_],
                                      server_->messenger(),
                                      &proxy);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to create a proxy for master at "
                   << master_addrs_[last_locate_master_idx_].ToString()
                   << ": " << s.ToString();
      continue;
    }
    master::ListMastersRequestPB req;
    ListMastersResponsePB resp;
    RpcController rpc;
    rpc.set_deadline(deadline);
    s = proxy->ListMasters(req, &resp, &rpc);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to get a list of masters from"
                   << master_addrs_[last_locate_master_idx_].ToString() << ": " << s.ToString();
      continue;
    }
    if (resp.has_error()) {
      s = StatusFromPB(resp.error());
    }
    s = FindLeaderHostPort(resp.masters(), leader_hostport);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to parse master from response (" << resp.ShortDebugString()
                   << "), from host " << master_addrs_[last_locate_master_idx_].ToString()
                   << ": " << s.ToString();
      continue;
    }
    return Status::OK();
  }
  return s;
}

Status Heartbeater::Thread::ConnectToMaster() {
  vector<Sockaddr> addrs;
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromMilliseconds(FLAGS_heartbeat_rpc_timeout_ms));
  // TODO send heartbeats without tablet reports to non-leader masters.
  RETURN_NOT_OK(FindLeaderMaster(deadline, &leader_master_hostport_));
  gscoped_ptr<MasterServiceProxy> new_proxy;
  MasterServiceProxyForHostPort(leader_master_hostport_,
                                server_->messenger(),
                                &new_proxy);
  RETURN_NOT_OK(leader_master_hostport_.ResolveAddresses(&addrs));

  // Ping the master to verify that it's alive.
  master::PingRequestPB req;
  master::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_heartbeat_rpc_timeout_ms));
  RETURN_NOT_OK_PREPEND(new_proxy->Ping(req, &resp, &rpc),
                        Substitute("Failed to ping master at $0", addrs[0].ToString()));
  LOG(INFO) << "Connected to a leader master server at " << leader_master_hostport_.ToString();
  proxy_.reset(new_proxy.release());
  return Status::OK();
}

void Heartbeater::Thread::SetupCommonField(master::TSToMasterCommonPB* common) {
  common->mutable_ts_instance()->CopyFrom(server_->instance_pb());
}

static Status AddHostPortPBs(const vector<Sockaddr>& addrs,
                           RepeatedPtrField<HostPortPB>* pbs) {
  BOOST_FOREACH(const Sockaddr& addr, addrs) {
    HostPortPB* pb = pbs->Add();
    if (addr.IsWildcard()) {
      RETURN_NOT_OK(GetHostname(pb->mutable_host()));
    } else {
      pb->set_host(addr.host());
    }
    pb->set_port(addr.port());
  }
  return Status::OK();
}

Status Heartbeater::Thread::SetupRegistration(master::TSRegistrationPB* reg) {
  reg->Clear();

  vector<Sockaddr> addrs;
  CHECK_NOTNULL(server_->rpc_server())->GetBoundAddresses(&addrs);
  RETURN_NOT_OK_PREPEND(AddHostPortPBs(addrs, reg->mutable_rpc_addresses()),
                        "Failed to add RPC addresses to registration");

  addrs.clear();
  RETURN_NOT_OK_PREPEND(CHECK_NOTNULL(server_->web_server())->GetBoundAddresses(&addrs),
                        "Unable to get bound HTTP addresses");
  RETURN_NOT_OK_PREPEND(AddHostPortPBs(addrs, reg->mutable_http_addresses()),
                        "Failed to add HTTP addresses to registration");
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
  if (PREDICT_FALSE(server_->fail_heartbeats_for_tests())) {
    return Status::IOError("failing all heartbeats for tests");
  }

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

  if (last_hb_response_.needs_full_tablet_report()) {
    LOG(INFO) << "Sending a full tablet report to master...";
    server_->tablet_manager()->GenerateFullTabletReport(
      req.mutable_tablet_report());
  } else {
    VLOG(2) << "Sending an incremental tablet report to master...";
    server_->tablet_manager()->GenerateIncrementalTabletReport(
      req.mutable_tablet_report());
  }

  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));

  VLOG(2) << "Sending heartbeat:\n" << req.DebugString();
  master::TSHeartbeatResponsePB resp;
  RETURN_NOT_OK_PREPEND(proxy_->TSHeartbeat(req, &resp, &rpc),
                        "Failed to send heartbeat");
  VLOG(2) << "Received heartbeat response:\n" << resp.DebugString();
  if (!resp.leader_master()) {
    // If the master is no longer a leader, reset proxy so that we can
    // determine the master and attempt to heartbeat during in the
    // next heartbeat interval.
    proxy_.reset();
    return Status::ServiceUnavailable("master is no longer the leader");
  }
  last_hb_response_.Swap(&resp);


  // TODO: Handle TSHeartbeatResponsePB (e.g. deleted tablets and schema changes)
  server_->tablet_manager()->MarkTabletReportAcknowledged(req.tablet_report());

  return Status::OK();
}

void Heartbeater::Thread::RunThread() {
  CHECK(IsCurrentThread());
  VLOG(1) << "Heartbeat thread starting";

  // Set up a fake "last heartbeat response" which indicates that we
  // need to register -- since we've never registered before, we know
  // this to be true.  This avoids an extra
  // heartbeat/response/heartbeat cycle.
  last_hb_response_.set_needs_reregister(true);
  last_hb_response_.set_needs_full_tablet_report(true);

  while (true) {
    if (run_latch_.WaitFor(MonoDelta::FromMilliseconds(GetMillisUntilNextHeartbeat()))) {
      // Latch fired -- exit loop
      VLOG(1) << "Heartbeat thread finished";
      return;
    }

    Status s = DoHeartbeat();
    if (!s.ok()) {
      if (s.IsNetworkError() && master_addrs_.size() > 1) {
        // If we encountered a network error (e.g., connection
        // refused) and there's more than one master available, try
        // determining the leader master again.
        proxy_.reset();
      }
      LOG(WARNING) << "Failed to heartbeat to " << leader_master_hostport_.ToString()
                   << ": " << s.ToString();
      consecutive_failed_heartbeats_++;
      continue;
    }
    consecutive_failed_heartbeats_ = 0;
    has_heartbeated_ = true;
  }
}

bool Heartbeater::Thread::IsCurrentThread() const {
  return thread_.get() == kudu::Thread::current_thread();
}

Status Heartbeater::Thread::Start() {
  CHECK(thread_ == NULL);

  run_latch_.Reset(1);
  return kudu::Thread::Create("heartbeater", "heartbeat",
      &Heartbeater::Thread::RunThread, this, &thread_);
}

Status Heartbeater::Thread::Stop() {
  if (!thread_) {
    return Status::OK();
  }

  run_latch_.CountDown();
  RETURN_NOT_OK(ThreadJoiner(thread_.get()).Join());
  thread_ = NULL;
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
