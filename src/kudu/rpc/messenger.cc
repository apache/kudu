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

#include "kudu/rpc/messenger.h"

#include <sys/socket.h>

#include <cstdlib>
#include <functional>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/outbound_call.h" // IWYU pragma: keep
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/transfer.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadpool.h"

METRIC_DEFINE_gauge_int32(server, rpc_pending_connections,
                          "Pending RPC Connections",
                          kudu::MetricUnit::kUnits,
                          "The current size of the longest backlog of pending "
                          "connections among all the listening sockets "
                          "of this RPC server",
                          kudu::MetricLevel::kInfo);

using kudu::security::RpcAuthentication;
using kudu::security::RpcEncryption;
using std::string;
using std::shared_lock;
using std::shared_ptr;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace rpc {

const int64_t MessengerBuilder::kRpcNegotiationTimeoutMs = 3000;

MessengerBuilder::MessengerBuilder(string name)
    : name_(std::move(name)),
      connection_keepalive_time_(MonoDelta::FromMilliseconds(65000)),
      acceptor_listen_backlog_(AcceptorPool::kDefaultListenBacklog),
      num_reactors_(4),
      rpc_max_message_size_(FLAGS_rpc_max_message_size),
      min_negotiation_threads_(0),
      max_negotiation_threads_(4),
      coarse_timer_granularity_(MonoDelta::FromMilliseconds(100)),
      rpc_negotiation_timeout_ms_(kRpcNegotiationTimeoutMs),
      sasl_proto_name_("kudu"),
      rpc_authentication_("optional"),
      rpc_encryption_("optional"),
      rpc_loopback_encryption_(false),
      rpc_tls_ciphers_(kudu::security::SecurityDefaults::kDefaultTlsCiphers),
      rpc_tls_ciphersuites_(kudu::security::SecurityDefaults::kDefaultTlsCipherSuites),
      rpc_tls_min_protocol_(kudu::security::SecurityDefaults::kDefaultTlsMinVersion),
      enable_inbound_tls_(false),
      reuseport_(false) {
}

Status MessengerBuilder::Build(shared_ptr<Messenger>* msgr) {
  // Initialize SASL library before we start making requests
  RETURN_NOT_OK(SaslInit(!keytab_file_.empty()));

  // See docs on Messenger::retain_self_ for info about this odd hack.
  //
  // Note: can't use make_shared() as it doesn't support custom deleters.
  shared_ptr<Messenger> new_msgr(new Messenger(*this),
                                 std::mem_fn(&Messenger::AllExternalReferencesDropped));
  if (jwt_verifier_) {
    new_msgr->jwt_verifier_ = std::move(jwt_verifier_);
  }
  RETURN_NOT_OK(ParseTriState("--rpc_authentication",
                              rpc_authentication_,
                              &new_msgr->authentication_));
  RETURN_NOT_OK(ParseTriState("--rpc_encryption",
                              rpc_encryption_,
                              &new_msgr->encryption_));
  new_msgr->loopback_encryption_ = rpc_loopback_encryption_;
  RETURN_NOT_OK(new_msgr->Init());
  if (new_msgr->encryption_ != RpcEncryption::DISABLED && enable_inbound_tls_) {
    auto* tls_context = new_msgr->mutable_tls_context();

    if (!rpc_certificate_file_.empty()) {
      CHECK(!rpc_private_key_file_.empty());
      CHECK(!rpc_ca_certificate_file_.empty());

      // TODO(KUDU-1920): should we try and enforce that the server
      // is in the subject or alt names of the cert?
      RETURN_NOT_OK(tls_context->LoadCertificateAuthority(rpc_ca_certificate_file_));
      if (rpc_private_key_password_cmd_.empty()) {
        RETURN_NOT_OK(tls_context->LoadCertificateAndKey(rpc_certificate_file_,
                                                         rpc_private_key_file_));
      } else {
        RETURN_NOT_OK(tls_context->LoadCertificateAndPasswordProtectedKey(
            rpc_certificate_file_, rpc_private_key_file_,
            [&](string* password){
              RETURN_NOT_OK_PREPEND(security::GetPasswordFromShellCommand(
                  rpc_private_key_password_cmd_, password),
                  "could not get RPC password from configured command");
              return Status::OK();
            }
        ));
      }
    } else {
      RETURN_NOT_OK(tls_context->GenerateSelfSignedCertAndKey());
    }
  }

  *msgr = std::move(new_msgr);
  return Status::OK();
}

std::atomic<uint32_t> Messenger::kInstanceCount_ = 0;

// See comment on Messenger::retain_self_ member.
void Messenger::AllExternalReferencesDropped() {
  // The last external ref may have been dropped in the context of a task
  // running on a reactor thread. If that's the case, a SYNC shutdown here
  // would deadlock.
  //
  // If a SYNC shutdown is desired, Shutdown() should be called explicitly.
  ShutdownInternal(ShutdownMode::ASYNC);

  CHECK(retain_self_.get());
  // If we have no more external references, then we no longer
  // need to retain ourself. We'll destruct as soon as all our
  // internal-facing references are dropped (ie those from reactor
  // threads).
  retain_self_.reset();
}

int32_t Messenger::GetPendingConnectionsNum() {
  // This method might be called when the messenger is shutting down;
  // making a copy of acceptor_pools_ is necessary to avoid data races.
  decltype(acceptor_pools_) acceptor_pools;
  {
    std::lock_guard guard(lock_);
    if (state_ == kClosing) {
      return -1;
    }
    acceptor_pools.reserve(acceptor_pools_.size());
    acceptor_pools = acceptor_pools_;
  }

  auto pool_reports_num = 0;
  int32_t total_count = 0;
  for (const auto& p : acceptor_pools) {
    uint32_t count;
    if (auto s = p->GetPendingConnectionsNum(&count); PREDICT_FALSE(!s.ok())) {
      KLOG_EVERY_N_SECS(WARNING, 60) << Substitute(
          "$0: no data on pending connections for acceptor pool at $1",
          s.ToString(), p->bind_address().ToString()) << THROTTLE_MSG;
      continue;
    }
    ++pool_reports_num;
    total_count += static_cast<int32_t>(count);
  }
  return pool_reports_num == 0 ? -1 : total_count;
}

void Messenger::Shutdown() {
  ShutdownInternal(ShutdownMode::SYNC);
}

void Messenger::ShutdownInternal(ShutdownMode mode) {
  if (mode == ShutdownMode::SYNC) {
    ThreadRestrictions::AssertWaitAllowed();
  }

  // Since we're shutting down, it's OK to block.
  //
  // TODO(adar): this ought to be removed (i.e. if ASYNC, waiting should be
  // forbidden, and if SYNC, we already asserted above), but that's not
  // possible while shutting down thread and acceptor pools still involves
  // joining threads.
  ThreadRestrictions::ScopedAllowWait allow_wait;

  acceptor_vec_t pools_to_shutdown;
  RpcServicesMap services_to_release;
  {
    std::lock_guard guard(lock_);
    if (state_ == kClosing) {
      return;
    }
    VLOG(1) << "shutting down messenger " << name_;
    state_ = kClosing;

    services_to_release.swap(rpc_services_);
    pools_to_shutdown.swap(acceptor_pools_);
  }

  // Destroy state outside of the lock.
  services_to_release.clear();
  for (const auto& p : pools_to_shutdown) {
    p->Shutdown();
  }

  // Need to shut down negotiation pool before the reactors, since the
  // reactors close the Connection sockets, and may race against the negotiation
  // threads' blocking reads & writes.
  client_negotiation_pool_->Shutdown();
  server_negotiation_pool_->Shutdown();

  for (Reactor* reactor : reactors_) {
    reactor->Shutdown(mode);
  }
}

Status Messenger::AddAcceptorPool(const Sockaddr& accept_addr,
                                  shared_ptr<AcceptorPool>* pool) {
  // Before listening, if we expect to require Kerberos, we want to verify
  // that everything is set up correctly. This way we'll generate errors on
  // startup rather than later on when we first receive a client connection.
  if (!keytab_file_.empty()) {
    RETURN_NOT_OK_PREPEND(ServerNegotiation::PreflightCheckGSSAPI(sasl_proto_name()),
                          "GSSAPI/Kerberos not properly configured");
  }

  Socket sock;
  RETURN_NOT_OK(sock.Init(accept_addr.family(), 0));
  RETURN_NOT_OK(sock.SetReuseAddr(true));
  if (GetIPFamily() == AF_INET6) {
    // IPV6_V6ONLY socket option is not applicable to Unix domain sockets.
    if (PREDICT_FALSE(accept_addr.is_unix())) {
      return Status::ConfigurationError(
          "IPV6_V6ONLY socket option is not applicable to Unix domain sockets.");
    }
    RETURN_NOT_OK(sock.SetIPv6Only(true));
  }
  if (reuseport_) {
    // SO_REUSEPORT socket option is not applicable to Unix domain sockets.
    if (PREDICT_FALSE(accept_addr.is_unix())) {
      return Status::ConfigurationError(
          "Port reuse is not applicable to Unix domain sockets.");
    }
    RETURN_NOT_OK(sock.SetReusePort(true));
  }
  RETURN_NOT_OK(sock.Bind(accept_addr));
  Sockaddr addr;
  RETURN_NOT_OK(sock.GetSocketAddress(&addr));

  {
    std::lock_guard guard(lock_);
    acceptor_pools_.emplace_back(std::make_shared<AcceptorPool>(
        this, &sock, addr, acceptor_listen_backlog_));
    *pool = acceptor_pools_.back();

#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
    if (acceptor_pools_.size() == 1) {
      // 'rpc_pending_connections' metric is instantiated when the messenger
      // contains exactly one acceptor pool: this metric makes sense
      // only for server-side messengers, and it's enough to instantiate the
      // metric only once.
      METRIC_rpc_pending_connections.InstantiateFunctionGauge(
          metric_entity_, [this]() { return this->GetPendingConnectionsNum(); })->
          AutoDetachToLastValue(&metric_detacher_);
    }
#endif // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ...
  }

  return Status::OK();
}

// Register a new RpcService to handle inbound requests.
Status Messenger::RegisterService(const string& service_name,
                                  const scoped_refptr<RpcService>& service) {
  DCHECK(service);
  std::lock_guard guard(lock_);
  DCHECK_NE(kServicesUnregistered, state_);
  DCHECK_NE(kClosing, state_);
  if (InsertIfNotPresent(&rpc_services_, service_name, service)) {
    return Status::OK();
  }
  return Status::AlreadyPresent("This service is already present");
}

void Messenger::UnregisterAllServices() {
  RpcServicesMap to_release;
  {
    std::lock_guard guard(lock_);
    to_release.swap(rpc_services_);
    state_ = kServicesUnregistered;
  }
  // Release the map outside of the lock.
}

Status Messenger::UnregisterService(const string& service_name) {
  scoped_refptr<RpcService> to_release;
  {
    std::lock_guard guard(lock_);
    to_release = EraseKeyReturnValuePtr(&rpc_services_, service_name);
    if (!to_release) {
      return Status::ServiceUnavailable(Substitute(
          "service $0 not registered on $1", service_name, name_));
    }
  }
  // Release the service outside of the lock.
  return Status::OK();
}

void Messenger::QueueOutboundCall(const shared_ptr<OutboundCall>& call) {
  RemoteToReactor(call->conn_id().remote())->QueueOutboundCall(call);
}

void Messenger::QueueInboundCall(unique_ptr<InboundCall> call) {
  // This lock acquisition spans the entirety of the function to avoid having to
  // take a ref on the RpcService. In doing so, we guarantee that the service
  // isn't shut down here, which would be problematic because shutdown is a
  // blocking operation and QueueInboundCall is called by the reactor thread.
  //
  // See KUDU-2946 for more details.
  shared_lock guard(lock_.get_lock());
  scoped_refptr<RpcService>* service = FindOrNull(rpc_services_,
                                                  call->remote_method().service_name());
  if (PREDICT_FALSE(!service)) {
    const auto msg = Substitute("service $0 not registered on $1",
                                call->remote_method().service_name(), name_);
    if (state_ == kServicesRegistered) {
      // NOTE: this message is only actually interesting if it's not transient.
      LOG(INFO) << msg;
      call.release()->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_SERVICE, Status::NotFound(msg));
    } else {
      call.release()->RespondFailure(
          ErrorStatusPB::ERROR_UNAVAILABLE, Status::ServiceUnavailable(msg));
    }
    return;
  }

  call->set_method_info((*service)->LookupMethod(call->remote_method()));

  // The RpcService will respond to the client on success or failure.
  WARN_NOT_OK((*service)->QueueInboundCall(std::move(call)), "Unable to handle RPC call");
}

void Messenger::QueueCancellation(const shared_ptr<OutboundCall>& call) {
  Reactor* reactor = RemoteToReactor(call->conn_id().remote());
  reactor->QueueCancellation(call);
}

void Messenger::RegisterInboundSocket(Socket* new_socket, const Sockaddr& remote) {
  RemoteToReactor(remote)->RegisterInboundSocket(new_socket, remote);
}

Messenger::Messenger(const MessengerBuilder& bld)
    : name_(bld.name_),
      state_(kStarted),
      authentication_(RpcAuthentication::REQUIRED),
      encryption_(RpcEncryption::REQUIRED),
      tls_context_(new security::TlsContext(bld.rpc_tls_ciphers_,
                                            bld.rpc_tls_ciphersuites_,
                                            bld.rpc_tls_min_protocol_,
                                            bld.rpc_tls_excluded_protocols_)),
      token_verifier_(new security::TokenVerifier),
      jwt_verifier_(nullptr),
      rpcz_store_(new RpczStore),
      metric_entity_(bld.metric_entity_),
      rpc_negotiation_timeout_ms_(bld.rpc_negotiation_timeout_ms_),
      rpc_max_message_size_(bld.rpc_max_message_size_),
      hostname_(bld.hostname_),
      sasl_proto_name_(bld.sasl_proto_name_),
      keytab_file_(bld.keytab_file_),
      reuseport_(bld.reuseport_),
      acceptor_listen_backlog_(bld.acceptor_listen_backlog_),
      retain_self_(this) {
  kInstanceCount_.fetch_add(1, std::memory_order_release);
  for (int i = 0; i < bld.num_reactors_; i++) {
    reactors_.push_back(new Reactor(retain_self_, i, bld));
  }
  CHECK_OK(ThreadPoolBuilder("client-negotiator")
      .set_min_threads(bld.min_negotiation_threads_)
      .set_max_threads(bld.max_negotiation_threads_)
      .Build(&client_negotiation_pool_));
  CHECK_OK(ThreadPoolBuilder("server-negotiator")
      .set_min_threads(bld.min_negotiation_threads_)
      .set_max_threads(bld.max_negotiation_threads_)
      .Build(&server_negotiation_pool_));
}

uint32_t Messenger::GetInstanceCount() {
  return kInstanceCount_.load(std::memory_order_acquire);
}

Messenger::~Messenger() {
  CHECK_EQ(state_, kClosing) << "Should have already shut down";
  STLDeleteElements(&reactors_);

  // KUDU(2439): kInstanceCount_'s zeroing is used as a criterion for the proper
  //             timing of OPENSSL_clean() call; so, it's crucial to make sure
  //             the sub-objects that call the OpenSSL API in their destructor
  //             are already destroyed when the instance counter reaches zero
  jwt_verifier_.reset();
  token_verifier_.reset();
  tls_context_.reset();
  kInstanceCount_.fetch_sub(1, std::memory_order_release);
}

Reactor* Messenger::RemoteToReactor(const Sockaddr& remote) const {
  // This is just a static partitioning; we could get a lot
  // fancier with assigning Sockaddrs to Reactors.
  return reactors_[remote.HashCode() % reactors_.size()];
}

Status Messenger::Init() {
  RETURN_NOT_OK(tls_context_->Init());
  for (Reactor* r : reactors_) {
    RETURN_NOT_OK(r->Init());
  }

  return Status::OK();
}

Status Messenger::DumpConnections(const DumpConnectionsRequestPB& req,
                                  DumpConnectionsResponsePB* resp) {
  for (Reactor* reactor : reactors_) {
    RETURN_NOT_OK(reactor->DumpConnections(req, resp));
  }
  return Status::OK();
}

void Messenger::ScheduleOnReactor(std::function<void(const Status&)> func,
                                  MonoDelta when) {
  DCHECK(!reactors_.empty());

  // If we're already running on a reactor thread, reuse it.
  Reactor* chosen = nullptr;
  for (Reactor* r : reactors_) {
    if (r->IsCurrentThread()) {
      chosen = r;
    }
  }
  if (chosen == nullptr) {
    // Not running on a reactor thread, pick one at random.
    chosen = reactors_[rand() % reactors_.size()];
  }

  DelayedTask* task = new DelayedTask(std::move(func), when);
  chosen->ScheduleReactorTask(task);
}

const scoped_refptr<RpcService> Messenger::rpc_service(const string& service_name) const {
  scoped_refptr<RpcService> service;
  {
    shared_lock guard(lock_.get_lock());
    if (!FindCopy(rpc_services_, service_name, &service)) {
      return scoped_refptr<RpcService>(nullptr);
    }
  }
  return service;
}

ThreadPool* Messenger::negotiation_pool(Connection::Direction dir) {
  switch (dir) {
    case Connection::CLIENT: return client_negotiation_pool_.get();
    case Connection::SERVER: return server_negotiation_pool_.get();
  }
  DCHECK(false) << "Unknown Connection::Direction value: " << dir;
  return nullptr;
}

} // namespace rpc
} // namespace kudu
