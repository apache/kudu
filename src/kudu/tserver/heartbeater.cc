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

#include "kudu/tserver/heartbeater.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <boost/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.proxy.h"
#include "kudu/security/cert.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/webserver.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/version_info.h"

DEFINE_int32(heartbeat_rpc_timeout_ms, 15000,
             "Timeout used for the TS->Master heartbeat RPCs.");
TAG_FLAG(heartbeat_rpc_timeout_ms, advanced);

DEFINE_int32(heartbeat_interval_ms, 1000,
             "Interval at which the TS heartbeats to the master.");
TAG_FLAG(heartbeat_interval_ms, advanced);

DEFINE_int32(heartbeat_max_failures_before_backoff, 3,
             "Maximum number of consecutive heartbeat failures until the "
             "Tablet Server backs off to the normal heartbeat interval, "
             "rather than retrying.");
TAG_FLAG(heartbeat_max_failures_before_backoff, advanced);

using kudu::master::MasterServiceProxy;
using kudu::master::TabletReportPB;
using kudu::rpc::RpcController;
using std::shared_ptr;
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
  proxy->reset(new MasterServiceProxy(messenger, addrs[0], hostport.host()));
  return Status::OK();
}

} // anonymous namespace

// Most of the actual logic of the heartbeater is inside this inner class,
// to avoid having too many dependencies from the header itself.
//
// This is basically the "PIMPL" pattern.
class Heartbeater::Thread {
 public:
  Thread(const HostPort& master_address, TabletServer* server);

  Status Start();
  Status Stop();
  void TriggerASAP();
  void MarkTabletDirty(const string& tablet_id, const string& reason);
  void GenerateIncrementalTabletReport(TabletReportPB* report);
  void GenerateFullTabletReport(TabletReportPB* report);

  // Mark that the master successfully received and processed the given
  // tablet report. This uses the report sequence number to "un-dirty" any
  // tablets which have not changed since the acknowledged report.
  void MarkTabletReportAcknowledged(const TabletReportPB& report);

 private:
  void RunThread();
  Status ConnectToMaster();
  int GetMinimumHeartbeatMillis() const;
  int GetMillisUntilNextHeartbeat() const;
  Status DoHeartbeat();
  Status SetupRegistration(ServerRegistrationPB* reg);
  void SetupCommonField(master::TSToMasterCommonPB* common);
  bool IsCurrentThread() const;

  // The host and port of the master that this thread will heartbeat to.
  //
  // We keep the HostPort around rather than a Sockaddr because the
  // master may change IP address, and we'd like to re-resolve on
  // every new attempt at connecting.
  HostPort master_address_;

  // The server for which we are heartbeating.
  TabletServer* const server_;

  // The actual running thread (NULL before it is started)
  scoped_refptr<kudu::Thread> thread_;

  // Current RPC proxy to the leader master.
  gscoped_ptr<master::MasterServiceProxy> proxy_;

  // The most recent response from a heartbeat.
  master::TSHeartbeatResponsePB last_hb_response_;

  // The number of heartbeats which have failed in a row.
  // This is tracked so as to back-off heartbeating.
  int consecutive_failed_heartbeats_;

  // Each tablet report is assigned a sequence number, so that subsequent
  // tablet reports only need to re-report those tablets which have
  // changed since the last report. Each tablet tracks the sequence
  // number at which it became dirty.
  struct TabletReportState {
    int32_t change_seq;
  };
  typedef std::unordered_map<std::string, TabletReportState> DirtyMap;

  // Tablets to include in the next incremental tablet report.
  // When a tablet is added/removed/added locally and needs to be
  // reported to the master, an entry is added to this map.
  DirtyMap dirty_tablets_;

  // Lock protecting 'dirty_tablets_'.
  //
  // Should not be held at the same time as mutex_.
  mutable simple_spinlock dirty_tablets_lock_;

  // Next tablet report seqno.
  std::atomic_int next_report_seq_;

  // Mutex/condition pair to trigger the heartbeater thread
  // to either heartbeat early or exit.
  Mutex mutex_;
  ConditionVariable cond_;

  // Protected by mutex_.
  bool should_run_;
  bool heartbeat_asap_;

  // Indicates that the thread should send a full tablet report. Set when
  // the thread detects that the master has been elected leader.
  bool send_full_tablet_report_;

  DISALLOW_COPY_AND_ASSIGN(Thread);
};

////////////////////////////////////////////////////////////
// Heartbeater
////////////////////////////////////////////////////////////

Heartbeater::Heartbeater(const TabletServerOptions& opts, TabletServer* server) {
  DCHECK_GT(opts.master_addresses.size(), 0);

  for (const auto& addr : opts.master_addresses) {
    threads_.emplace_back(new Thread(addr, server));
  }
}
Heartbeater::~Heartbeater() {
  WARN_NOT_OK(Stop(), "Unable to stop heartbeater thread");
}

Status Heartbeater::Start() {
  for (int i = 0; i < threads_.size(); i++) {
    Status first_failure = threads_[i]->Start();
    if (!first_failure.ok()) {
      // On error, stop whichever threads were started.
      for (int j = 0; j < i; j++) {
        // Ignore failures; we should try to stop every thread, and
        // 'first_failure' is the most interesting status anyway.
        threads_[j]->Stop();
      }
      return first_failure;
    }
  }

  return Status::OK();
}
Status Heartbeater::Stop() {
  // Stop all threads and return the first failure (if there was one).
  Status first_failure;
  for (const auto& thread : threads_) {
    Status s = thread->Stop();
    if (!s.ok() && first_failure.ok()) {
      first_failure = s;
    }
  }
  return first_failure;
}

void Heartbeater::TriggerASAP() {
  for (const auto& thread : threads_) {
    thread->TriggerASAP();
  }
}

void Heartbeater::MarkTabletDirty(const string& tablet_id, const string& reason) {
  for (const auto& thread : threads_) {
    thread->MarkTabletDirty(tablet_id, reason);
  }
}

vector<TabletReportPB> Heartbeater::GenerateIncrementalTabletReportsForTests() {
  vector<TabletReportPB> results;
  for (const auto& thread : threads_) {
    TabletReportPB report;
    thread->GenerateIncrementalTabletReport(&report);
    results.emplace_back(std::move(report));
  }
  return results;
}

vector<TabletReportPB> Heartbeater::GenerateFullTabletReportsForTests() {
  vector<TabletReportPB>  results;
  for (const auto& thread : threads_) {
    TabletReportPB report;
    thread->GenerateFullTabletReport(&report);
    results.emplace_back(std::move(report));
  }
  return results;
}

void Heartbeater::MarkTabletReportsAcknowledgedForTests(
    const vector<TabletReportPB>& reports) {
  CHECK_EQ(reports.size(), threads_.size());

  for (int i = 0; i < reports.size(); i++) {
    threads_[i]->MarkTabletReportAcknowledged(reports[i]);
  }
}

////////////////////////////////////////////////////////////
// Heartbeater::Thread
////////////////////////////////////////////////////////////

Heartbeater::Thread::Thread(const HostPort& master_address, TabletServer* server)
  : master_address_(master_address),
    server_(server),
    consecutive_failed_heartbeats_(0),
    next_report_seq_(0),
    cond_(&mutex_),
    should_run_(false),
    heartbeat_asap_(true),
    send_full_tablet_report_(false) {
}

Status Heartbeater::Thread::ConnectToMaster() {
  gscoped_ptr<MasterServiceProxy> new_proxy;
  RETURN_NOT_OK(MasterServiceProxyForHostPort(master_address_, server_->messenger(), &new_proxy));

  // Ping the master to verify that it's alive.
  master::PingRequestPB req;
  master::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_heartbeat_rpc_timeout_ms));
  RETURN_NOT_OK_PREPEND(new_proxy->Ping(req, &resp, &rpc),
                        Substitute("Failed to ping master at $0", master_address_.ToString()));
  LOG(INFO) << "Connected to a master server at " << master_address_.ToString();
  proxy_.reset(new_proxy.release());
  return Status::OK();
}

void Heartbeater::Thread::SetupCommonField(master::TSToMasterCommonPB* common) {
  common->mutable_ts_instance()->CopyFrom(server_->instance_pb());
}

Status Heartbeater::Thread::SetupRegistration(ServerRegistrationPB* reg) {
  reg->Clear();

  vector<Sockaddr> addrs;
  RETURN_NOT_OK(CHECK_NOTNULL(server_->rpc_server())->GetAdvertisedAddresses(&addrs));
  RETURN_NOT_OK_PREPEND(AddHostPortPBs(addrs, reg->mutable_rpc_addresses()),
                        "Failed to add RPC addresses to registration");

  addrs.clear();
  if (server_->web_server()) {
    RETURN_NOT_OK_PREPEND(server_->web_server()->GetAdvertisedAddresses(&addrs),
                          "Unable to get bound HTTP addresses");
    RETURN_NOT_OK_PREPEND(AddHostPortPBs(addrs, reg->mutable_http_addresses()),
                          "Failed to add HTTP addresses to registration");
    reg->set_https_enabled(server_->web_server()->IsSecure());
  }
  reg->set_software_version(VersionInfo::GetShortVersionString());

  return Status::OK();
}

int Heartbeater::Thread::GetMinimumHeartbeatMillis() const {
  // If we've failed a few heartbeats in a row, back off to the normal
  // interval, rather than retrying in a loop.
  if (consecutive_failed_heartbeats_ == FLAGS_heartbeat_max_failures_before_backoff) {
    LOG(WARNING) << "Failed " << consecutive_failed_heartbeats_  <<" heartbeats "
                 << "in a row: no longer allowing fast heartbeat attempts.";
  }

  return consecutive_failed_heartbeats_ >= FLAGS_heartbeat_max_failures_before_backoff ?
    FLAGS_heartbeat_interval_ms : 0;
}

int Heartbeater::Thread::GetMillisUntilNextHeartbeat() const {
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

  // Check with the TS cert manager if it has a cert that needs signing.
  // if so, send the CSR in the heartbeat for the master to sign.
  boost::optional<security::CertSignRequest> csr =
    server_->mutable_tls_context()->GetCsrIfNecessary();
  if (csr != boost::none) {
    RETURN_NOT_OK(csr->ToString(req.mutable_csr_der(), security::DataFormat::DER));
    VLOG(1) << "Sending a CSR to the master in the next heartbeat";
  }

  // Send the most recently known TSK sequence number so that the master can
  // send us knew ones if they exist.
  req.set_latest_tsk_seq_num(server_->token_verifier().GetMaxKnownKeySequenceNumber());

  if (send_full_tablet_report_) {
    LOG(INFO) << Substitute(
        "Master $0 was elected leader, sending a full tablet report...",
        master_address_.ToString());
    GenerateFullTabletReport(req.mutable_tablet_report());
    // Should the heartbeat fail, we'd want the next heartbeat to resend this
    // full tablet report. As such, send_full_tablet_report_ is only reset
    // after all error checking is complete.
  } else if (last_hb_response_.needs_full_tablet_report()) {
    LOG(INFO) << Substitute(
        "Master $0 requested a full tablet report, sending...",
        master_address_.ToString());
    GenerateFullTabletReport(req.mutable_tablet_report());
  } else {
    VLOG(2) << Substitute("Sending an incremental tablet report to master $0...",
                          master_address_.ToString());
    GenerateIncrementalTabletReport(req.mutable_tablet_report());
  }
  req.set_num_live_tablets(server_->tablet_manager()->GetNumLiveTablets());

  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_heartbeat_rpc_timeout_ms));

  VLOG(2) << "Sending heartbeat:\n" << SecureDebugString(req);
  master::TSHeartbeatResponsePB resp;
  RETURN_NOT_OK_PREPEND(proxy_->TSHeartbeat(req, &resp, &rpc),
                        "Failed to send heartbeat to master");
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  VLOG(2) << Substitute("Received heartbeat response from $0:\n$1",
                        master_address_.ToString(), SecureDebugString(resp));

  // If we've detected that our master was elected leader, send a full tablet
  // report in the next heartbeat.
  if (!last_hb_response_.leader_master() && resp.leader_master()) {
    send_full_tablet_report_ = true;
  } else {
    send_full_tablet_report_ = false;
  }

  last_hb_response_.Swap(&resp);

  for (const auto& ca_cert_der : last_hb_response_.ca_cert_der()) {
    security::Cert ca_cert;
    RETURN_NOT_OK_PREPEND(
        ca_cert.FromString(ca_cert_der, security::DataFormat::DER),
        "failed to parse CA certificate from master");
    RETURN_NOT_OK_PREPEND(
        server_->mutable_tls_context()->AddTrustedCertificate(ca_cert),
        "failed to trust master CA cert");
  }

  // If we have a new signed certificate from the master, adopt it.
  if (last_hb_response_.has_signed_cert_der()) {
    security::Cert cert;
    RETURN_NOT_OK_PREPEND(
        cert.FromString(last_hb_response_.signed_cert_der(), security::DataFormat::DER),
        "failed to parse signed certificate from master");
    RETURN_NOT_OK_PREPEND(
        server_->mutable_tls_context()->AdoptSignedCert(cert),
        "failed to adopt master-signed X509 cert");
  }

  // Import TSKs.
  if (!last_hb_response_.tsks().empty()) {
    vector<security::TokenSigningPublicKeyPB> tsks(last_hb_response_.tsks().begin(),
                                                   last_hb_response_.tsks().end());
    RETURN_NOT_OK_PREPEND(
        server_->mutable_token_verifier()->ImportKeys(tsks),
        "failed to import token signing public keys from master heartbeat");
  }

  MarkTabletReportAcknowledged(req.tablet_report());
  return Status::OK();
}

void Heartbeater::Thread::RunThread() {
  CHECK(IsCurrentThread());
  VLOG(1) << Substitute("Heartbeat thread (master $0) starting",
                        master_address_.ToString());

  // Set up a fake "last heartbeat response" which indicates that we
  // need to register -- since we've never registered before, we know
  // this to be true.  This avoids an extra
  // heartbeat/response/heartbeat cycle.
  last_hb_response_.set_needs_reregister(true);
  last_hb_response_.set_needs_full_tablet_report(true);

  while (true) {
    MonoTime next_heartbeat =
        MonoTime::Now() + MonoDelta::FromMilliseconds(GetMillisUntilNextHeartbeat());

    // Wait for either the heartbeat interval to elapse, or for an "ASAP" heartbeat,
    // or for the signal to shut down.
    {
      MutexLock l(mutex_);
      while (true) {
        MonoDelta remaining = next_heartbeat - MonoTime::Now();
        if (remaining.ToMilliseconds() <= 0 ||
            heartbeat_asap_ ||
            !should_run_) {
          break;
        }
        cond_.TimedWait(remaining);
      }

      heartbeat_asap_ = false;

      if (!should_run_) {
        VLOG(1) << Substitute("Heartbeat thread (master $0) finished",
                              master_address_.ToString());
        return;
      }
    }

    Status s = DoHeartbeat();
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Failed to heartbeat to $0: $1",
                                 master_address_.ToString(), s.ToString());
      consecutive_failed_heartbeats_++;
      // If we encountered a network error (e.g., connection
      // refused), try reconnecting.
      if (s.IsNetworkError() ||
          consecutive_failed_heartbeats_ >= FLAGS_heartbeat_max_failures_before_backoff) {
        proxy_.reset();
      }
      continue;
    }
    consecutive_failed_heartbeats_ = 0;
  }
}

bool Heartbeater::Thread::IsCurrentThread() const {
  return thread_.get() == kudu::Thread::current_thread();
}

void Heartbeater::Thread::MarkTabletReportAcknowledged(const master::TabletReportPB& report) {
  std::lock_guard<simple_spinlock> l(dirty_tablets_lock_);

  int32_t acked_seq = report.sequence_number();
  CHECK_LT(acked_seq, next_report_seq_.load());

  // Clear the "dirty" state for any tablets which have not changed since
  // this report.
  auto it = dirty_tablets_.begin();
  while (it != dirty_tablets_.end()) {
    const TabletReportState& state = it->second;
    if (state.change_seq <= acked_seq) {
      // This entry has not changed since this tablet report, we no longer need
      // to track it as dirty. If it becomes dirty again, it will be re-added
      // with a higher sequence number.
      it = dirty_tablets_.erase(it);
    } else {
      ++it;
    }
  }
}

Status Heartbeater::Thread::Start() {
  CHECK(thread_ == nullptr);

  should_run_ = true;
  return kudu::Thread::Create("heartbeater", "heartbeat",
      &Heartbeater::Thread::RunThread, this, &thread_);
}

Status Heartbeater::Thread::Stop() {
  if (!thread_) {
    return Status::OK();
  }

  {
    MutexLock l(mutex_);
    should_run_ = false;
    cond_.Signal();
  }
  RETURN_NOT_OK(ThreadJoiner(thread_.get()).Join());
  thread_ = nullptr;
  return Status::OK();
}

void Heartbeater::Thread::TriggerASAP() {
  MutexLock l(mutex_);
  heartbeat_asap_ = true;
  cond_.Signal();
}

void Heartbeater::Thread::MarkTabletDirty(const string& tablet_id, const string& reason) {
  std::lock_guard<simple_spinlock> l(dirty_tablets_lock_);

  // Even though this is an atomic load, it needs to hold the lock. To see why,
  // consider this sequence:
  // 0. Tablet t exists in dirty_tablets_.
  // 1. T1 calls MarkTabletDirty(t), loads x from next_report_seq_, and is
  //    descheduled.
  // 2. T2 generates a tablet report, incrementing next_report_seq_ to x+1.
  // 3. T3 calls MarkTabletDirty(t), loads x+1 into next_report_seq_, and
  //    writes x+1 to state->change_seq.
  // 4. T1 is scheduled. It tries to write x to state->change_seq, failing the
  //    CHECK_GE().
  int32_t seqno = next_report_seq_.load();

  TabletReportState* state = FindOrNull(dirty_tablets_, tablet_id);
  if (state != nullptr) {
    CHECK_GE(seqno, state->change_seq);
    state->change_seq = seqno;
  } else {
    TabletReportState state = { seqno };
    InsertOrDie(&dirty_tablets_, tablet_id, std::move(state));
  }
}

void Heartbeater::Thread::GenerateIncrementalTabletReport(TabletReportPB* report) {
  report->Clear();
  report->set_sequence_number(next_report_seq_.fetch_add(1));
  report->set_is_incremental(true);
  vector<string> dirty_tablet_ids;
  {
    std::lock_guard<simple_spinlock> l(dirty_tablets_lock_);
    AppendKeysFromMap(dirty_tablets_, &dirty_tablet_ids);
  }
  server_->tablet_manager()->PopulateIncrementalTabletReport(
      report, dirty_tablet_ids);
}

void Heartbeater::Thread::GenerateFullTabletReport(TabletReportPB* report) {
  report->Clear();
  report->set_sequence_number(next_report_seq_.fetch_add(1));
  report->set_is_incremental(false);
  server_->tablet_manager()->PopulateFullTabletReport(report);
}

} // namespace tserver
} // namespace kudu
