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

#include <cstdlib>
#include <functional>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/rpc/service_if.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/flags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadpool.h"

using std::string;
using std::shared_ptr;
using std::make_shared;
using strings::Substitute;

namespace boost {
template <typename Signature> class function;
}

namespace kudu {
namespace rpc {

MessengerBuilder::MessengerBuilder(std::string name)
    : name_(std::move(name)),
      connection_keepalive_time_(MonoDelta::FromMilliseconds(65000)),
      num_reactors_(4),
      min_negotiation_threads_(0),
      max_negotiation_threads_(4),
      coarse_timer_granularity_(MonoDelta::FromMilliseconds(100)),
      rpc_negotiation_timeout_ms_(3000),
      sasl_proto_name_("kudu"),
      rpc_authentication_("optional"),
      rpc_encryption_("optional"),
      rpc_tls_ciphers_(kudu::security::SecurityDefaults::kDefaultTlsCiphers),
      rpc_tls_min_protocol_(kudu::security::SecurityDefaults::kDefaultTlsMinVersion),
      enable_inbound_tls_(false),
      reuseport_(false) {
}

MessengerBuilder& MessengerBuilder::set_connection_keepalive_time(const MonoDelta &keepalive) {
  connection_keepalive_time_ = keepalive;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_num_reactors(int num_reactors) {
  num_reactors_ = num_reactors;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_min_negotiation_threads(int min_negotiation_threads) {
  min_negotiation_threads_ = min_negotiation_threads;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_max_negotiation_threads(int max_negotiation_threads) {
  max_negotiation_threads_ = max_negotiation_threads;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_coarse_timer_granularity(const MonoDelta &granularity) {
  coarse_timer_granularity_ = granularity;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_metric_entity(
    const scoped_refptr<MetricEntity>& metric_entity) {
  metric_entity_ = metric_entity;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_connection_keep_alive_time(int32_t time_in_ms) {
  connection_keepalive_time_ = MonoDelta::FromMilliseconds(time_in_ms);
  return *this;
}

MessengerBuilder &MessengerBuilder::set_rpc_negotiation_timeout_ms(int64_t time_in_ms) {
  rpc_negotiation_timeout_ms_ = time_in_ms;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_sasl_proto_name(const std::string& sasl_proto_name) {
  sasl_proto_name_ = sasl_proto_name;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_rpc_authentication(const std::string& rpc_authentication) {
  rpc_authentication_ = rpc_authentication;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_rpc_encryption(const std::string& rpc_encryption) {
  rpc_encryption_ = rpc_encryption;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_rpc_tls_ciphers(const std::string& rpc_tls_ciphers) {
  rpc_tls_ciphers_ = rpc_tls_ciphers;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_rpc_tls_min_protocol(
    const std::string& rpc_tls_min_protocol) {
  rpc_tls_min_protocol_ = rpc_tls_min_protocol;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_epki_cert_key_files(
    const std::string& cert, const std::string& private_key) {
  rpc_certificate_file_ = cert;
  rpc_private_key_file_ = private_key;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_epki_certificate_authority_file(const std::string& ca) {
  rpc_ca_certificate_file_ = ca;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_epki_private_password_key_cmd(const std::string& cmd) {
  rpc_private_key_password_cmd_ = cmd;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_keytab_file(const std::string& keytab_file) {
  keytab_file_ = keytab_file;
  return *this;
}

MessengerBuilder& MessengerBuilder::enable_inbound_tls() {
  enable_inbound_tls_ = true;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_reuseport() {
  reuseport_ = true;
  return *this;
}

Status MessengerBuilder::Build(shared_ptr<Messenger>* msgr) {
  // Initialize SASL library before we start making requests
  RETURN_NOT_OK(SaslInit(!keytab_file_.empty()));

  // See docs on Messenger::retain_self_ for info about this odd hack.
  //
  // Note: can't use make_shared() as it doesn't support custom deleters.
  shared_ptr<Messenger> new_msgr(new Messenger(*this),
                                 std::mem_fn(&Messenger::AllExternalReferencesDropped));

  RETURN_NOT_OK(ParseTriState("--rpc_authentication",
                              rpc_authentication_,
                              &new_msgr->authentication_));

  RETURN_NOT_OK(ParseTriState("--rpc_encryption",
                              rpc_encryption_,
                              &new_msgr->encryption_));

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
            [&](){
              string ret;
              WARN_NOT_OK(security::GetPasswordFromShellCommand(
                  rpc_private_key_password_cmd_, &ret),
                  "could not get RPC password from configured command");
              return ret;
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
    std::lock_guard<percpu_rwlock> guard(lock_);
    if (closing_) {
      return;
    }
    VLOG(1) << "shutting down messenger " << name_;
    closing_ = true;

    services_to_release = std::move(rpc_services_);
    pools_to_shutdown = std::move(acceptor_pools_);
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

Status Messenger::AddAcceptorPool(const Sockaddr &accept_addr,
                                  shared_ptr<AcceptorPool>* pool) {
  // Before listening, if we expect to require Kerberos, we want to verify
  // that everything is set up correctly. This way we'll generate errors on
  // startup rather than later on when we first receive a client connection.
  if (!keytab_file_.empty()) {
    RETURN_NOT_OK_PREPEND(ServerNegotiation::PreflightCheckGSSAPI(sasl_proto_name()),
                          "GSSAPI/Kerberos not properly configured");
  }

  Socket sock;
  RETURN_NOT_OK(sock.Init(0));
  RETURN_NOT_OK(sock.SetReuseAddr(true));
  if (reuseport_) {
    RETURN_NOT_OK(sock.SetReusePort(true));
  }
  RETURN_NOT_OK(sock.Bind(accept_addr));
  Sockaddr remote;
  RETURN_NOT_OK(sock.GetSocketAddress(&remote));
  auto acceptor_pool(make_shared<AcceptorPool>(this, &sock, remote));

  std::lock_guard<percpu_rwlock> guard(lock_);
  acceptor_pools_.push_back(acceptor_pool);
  pool->swap(acceptor_pool);
  return Status::OK();
}

// Register a new RpcService to handle inbound requests.
Status Messenger::RegisterService(const string& service_name,
                                  const scoped_refptr<RpcService>& service) {
  DCHECK(service);
  std::lock_guard<percpu_rwlock> guard(lock_);
  if (InsertIfNotPresent(&rpc_services_, service_name, service)) {
    return Status::OK();
  } else {
    return Status::AlreadyPresent("This service is already present");
  }
}

void Messenger::UnregisterAllServices() {
  RpcServicesMap to_release;
  {
    std::lock_guard<percpu_rwlock> guard(lock_);
    to_release = std::move(rpc_services_);
  }
  // Release the map outside of the lock.
}

Status Messenger::UnregisterService(const string& service_name) {
  scoped_refptr<RpcService> to_release;
  {
    std::lock_guard<percpu_rwlock> guard(lock_);
    to_release = EraseKeyReturnValuePtr(&rpc_services_, service_name);
    if (!to_release) {
      return Status::ServiceUnavailable(Substitute(
          "service $0 not registered on $1", service_name, name_));
    }
  }
  // Release the service outside of the lock.
  return Status::OK();
}

void Messenger::QueueOutboundCall(const shared_ptr<OutboundCall> &call) {
  Reactor *reactor = RemoteToReactor(call->conn_id().remote());
  reactor->QueueOutboundCall(call);
}

void Messenger::QueueInboundCall(gscoped_ptr<InboundCall> call) {
  // This lock acquisition spans the entirety of the function to avoid having to
  // take a ref on the RpcService. In doing so, we guarantee that the service
  // isn't shut down here, which would be problematic because shutdown is a
  // blocking operation and QueueInboundCall is called by the reactor thread.
  //
  // See KUDU-2946 for more details.
  shared_lock<rw_spinlock> guard(lock_.get_lock());
  scoped_refptr<RpcService>* service = FindOrNull(rpc_services_,
                                                  call->remote_method().service_name());
  if (PREDICT_FALSE(!service)) {
    Status s =  Status::ServiceUnavailable(Substitute("service $0 not registered on $1",
                                                      call->remote_method().service_name(), name_));
    LOG(INFO) << s.ToString();
    call.release()->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_SERVICE, s);
    return;
  }

  call->set_method_info((*service)->LookupMethod(call->remote_method()));

  // The RpcService will respond to the client on success or failure.
  WARN_NOT_OK((*service)->QueueInboundCall(std::move(call)), "Unable to handle RPC call");
}

void Messenger::QueueCancellation(const shared_ptr<OutboundCall> &call) {
  Reactor *reactor = RemoteToReactor(call->conn_id().remote());
  reactor->QueueCancellation(call);
}

void Messenger::RegisterInboundSocket(Socket *new_socket, const Sockaddr &remote) {
  Reactor *reactor = RemoteToReactor(remote);
  reactor->RegisterInboundSocket(new_socket, remote);
}

Messenger::Messenger(const MessengerBuilder &bld)
  : name_(bld.name_),
    closing_(false),
    authentication_(RpcAuthentication::REQUIRED),
    encryption_(RpcEncryption::REQUIRED),
    tls_context_(new security::TlsContext(bld.rpc_tls_ciphers_, bld.rpc_tls_min_protocol_)),
    token_verifier_(new security::TokenVerifier()),
    rpcz_store_(new RpczStore()),
    metric_entity_(bld.metric_entity_),
    rpc_negotiation_timeout_ms_(bld.rpc_negotiation_timeout_ms_),
    sasl_proto_name_(bld.sasl_proto_name_),
    keytab_file_(bld.keytab_file_),
    reuseport_(bld.reuseport_),
    retain_self_(this) {
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

Messenger::~Messenger() {
  CHECK(closing_) << "Should have already shut down";
  STLDeleteElements(&reactors_);
}

Reactor* Messenger::RemoteToReactor(const Sockaddr &remote) {
  uint32_t hashCode = remote.HashCode();
  int reactor_idx = hashCode % reactors_.size();
  // This is just a static partitioning; we could get a lot
  // fancier with assigning Sockaddrs to Reactors.
  return reactors_[reactor_idx];
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

void Messenger::ScheduleOnReactor(const boost::function<void(const Status&)>& func,
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

  DelayedTask* task = new DelayedTask(func, when);
  chosen->ScheduleReactorTask(task);
}

const scoped_refptr<RpcService> Messenger::rpc_service(const string& service_name) const {
  scoped_refptr<RpcService> service;
  {
    shared_lock<rw_spinlock> guard(lock_.get_lock());
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
