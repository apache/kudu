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

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/move.h"
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
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadpool.h"

using std::string;
using std::shared_ptr;
using std::make_shared;
using strings::Substitute;

DEFINE_string(rpc_authentication, "optional",
              "Whether to require RPC connections to authenticate. Must be one "
              "of 'disabled', 'optional', or 'required'. If 'optional', "
              "authentication will be used when the remote end supports it. If "
              "'required', connections which are not able to authenticate "
              "(because the remote end lacks support) are rejected. Secure "
              "clusters should use 'required'.");
DEFINE_string(rpc_encryption, "optional",
              "Whether to require RPC connections to be encrypted. Must be one "
              "of 'disabled', 'optional', or 'required'. If 'optional', "
              "encryption will be used when the remote end supports it. If "
              "'required', connections which are not able to use encryption "
              "(because the remote end lacks support) are rejected. If 'disabled', "
              "encryption will not be used, and RPC authentication "
              "(--rpc_authentication) must also be disabled as well. "
              "Secure clusters should use 'required'.");
TAG_FLAG(rpc_authentication, evolving);
TAG_FLAG(rpc_encryption, evolving);

DEFINE_string(rpc_certificate_file, "",
              "Path to a PEM encoded X509 certificate to use for securing RPC "
              "connections with SSL/TLS. If set, '--rpc_private_key_file' and "
              "'--rpc_ca_certificate_file' must be set as well.");
DEFINE_string(rpc_private_key_file, "",
              "Path to a PEM encoded private key paired with the certificate "
              "from '--rpc_certificate_file'");
DEFINE_string(rpc_ca_certificate_file, "",
              "Path to the PEM encoded X509 certificate of the trusted external "
              "certificate authority. The provided certificate should be the root "
              "issuer of the certificate passed in '--rpc_certificate_file'.");
DEFINE_string(rpc_private_key_password_cmd, "", "A Unix command whose output "
              "returns the password used to decrypt the RPC server's private key "
              "file specified in --rpc_private_key_file. If the .PEM key file is "
              "not password-protected, this flag does not need to be set. "
              "Trailing whitespace will be trimmed before it is used to decrypt "
              "the private key.");

// Setting TLS certs and keys via CLI flags is only necessary for external
// PKI-based security, which is not yet production ready. Instead, see
// internal PKI (ipki) and Kerberos-based authentication.
TAG_FLAG(rpc_certificate_file, experimental);
TAG_FLAG(rpc_private_key_file, experimental);
TAG_FLAG(rpc_ca_certificate_file, experimental);

DEFINE_int32(rpc_default_keepalive_time_ms, 65000,
             "If an RPC connection from a client is idle for this amount of time, the server "
             "will disconnect the client.");
TAG_FLAG(rpc_default_keepalive_time_ms, advanced);

DECLARE_string(keytab_file);
DECLARE_bool(allow_world_readable_credentials);

namespace boost {
template <typename Signature> class function;
}

namespace kudu {
namespace rpc {

template <typename T>
static Status ParseTriState(const char* flag_name, const string& flag_value, T* tri_state) {
  if (boost::iequals(flag_value, "required")) {
    *tri_state = T::REQUIRED;
  } else if (boost::iequals(flag_value, "optional")) {
    *tri_state = T::OPTIONAL;
  } else if (boost::iequals(flag_value, "disabled")) {
    *tri_state = T::DISABLED;
  } else {
    return Status::InvalidArgument(Substitute(
          "$0 flag must be one of 'required', 'optional', or 'disabled'",
          flag_name));
  }
  return Status::OK();
}

static bool ValidateRpcAuthentication(const char* flag_name, const string& flag_value) {
  RpcAuthentication result;
  Status s = ParseTriState(flag_name, flag_value, &result);
  if (!s.ok()) {
    LOG(ERROR) << s.message().ToString();
    return false;
  }
  return true;
}
DEFINE_validator(rpc_authentication, &ValidateRpcAuthentication);

static bool ValidateRpcEncryption(const char* flag_name, const string& flag_value) {
  RpcEncryption result;
  Status s = ParseTriState(flag_name, flag_value, &result);
  if (!s.ok()) {
    LOG(ERROR) << s.message().ToString();
    return false;
  }
  return true;
}
DEFINE_validator(rpc_encryption, &ValidateRpcEncryption);

static bool ValidateRpcAuthnFlags() {
  RpcAuthentication authentication;
  CHECK_OK(ParseTriState("--rpc_authentication", FLAGS_rpc_authentication, &authentication));

  RpcEncryption encryption;
  CHECK_OK(ParseTriState("--rpc_encryption", FLAGS_rpc_encryption, &encryption));

  if (encryption == RpcEncryption::DISABLED && authentication != RpcAuthentication::DISABLED) {
    LOG(ERROR) << "RPC authentication (--rpc_authentication) must be disabled "
                  "if RPC encryption (--rpc_encryption) is disabled";
    return false;
  }

  const bool has_keytab = !FLAGS_keytab_file.empty();
  const bool has_cert = !FLAGS_rpc_certificate_file.empty();
  if (authentication == RpcAuthentication::REQUIRED && !has_keytab && !has_cert) {
    LOG(ERROR) << "RPC authentication (--rpc_authentication) may not be "
                  "required unless Kerberos (--keytab_file) or external PKI "
                  "(--rpc_certificate_file et al) are configured";
    return false;
  }

  return true;
}
GROUP_FLAG_VALIDATOR(rpc_authn_flags, ValidateRpcAuthnFlags);

static bool ValidateExternalPkiFlags() {
  bool has_cert = !FLAGS_rpc_certificate_file.empty();
  bool has_key = !FLAGS_rpc_private_key_file.empty();
  bool has_ca = !FLAGS_rpc_ca_certificate_file.empty();

  if (has_cert != has_key || has_cert != has_ca) {
    LOG(ERROR) << "--rpc_certificate_file, --rpc_private_key_file, and "
                  "--rpc_ca_certificate_file flags must be set as a group; "
                  "i.e. either set all or none of them.";
    return false;
  }

  if (has_key && !FLAGS_allow_world_readable_credentials) {
    bool world_readable_private_key;
    Status s = Env::Default()->IsFileWorldReadable(FLAGS_rpc_private_key_file,
                                                   &world_readable_private_key);
    if (!s.ok()) {
      LOG(ERROR) << Substitute("$0: could not verify private key file does not have "
                               "world-readable permissions: $1",
                               FLAGS_rpc_private_key_file, s.ToString());
      return false;
    }
    if (world_readable_private_key) {
      LOG(ERROR) << "cannot use private key file with world-readable permissions: "
                 << FLAGS_rpc_private_key_file;
      return false;
    }
  }

  return true;
}
GROUP_FLAG_VALIDATOR(external_pki_flags, ValidateExternalPkiFlags);

MessengerBuilder::MessengerBuilder(std::string name)
    : name_(std::move(name)),
      connection_keepalive_time_(
          MonoDelta::FromMilliseconds(FLAGS_rpc_default_keepalive_time_ms)),
      num_reactors_(4),
      min_negotiation_threads_(0),
      max_negotiation_threads_(4),
      coarse_timer_granularity_(MonoDelta::FromMilliseconds(100)),
      enable_inbound_tls_(false) {
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

MessengerBuilder& MessengerBuilder::enable_inbound_tls() {
  enable_inbound_tls_ = true;
  return *this;
}

Status MessengerBuilder::Build(shared_ptr<Messenger> *msgr) {
  RETURN_NOT_OK(SaslInit()); // Initialize SASL library before we start making requests

  Messenger* new_msgr(new Messenger(*this));

  auto cleanup = MakeScopedCleanup([&] () {
      new_msgr->AllExternalReferencesDropped();
  });

  RETURN_NOT_OK(ParseTriState("--rpc_authentication",
                              FLAGS_rpc_authentication,
                              &new_msgr->authentication_));

  RETURN_NOT_OK(ParseTriState("--rpc_encryption",
                              FLAGS_rpc_encryption,
                              &new_msgr->encryption_));

  RETURN_NOT_OK(new_msgr->Init());
  if (new_msgr->encryption_ != RpcEncryption::DISABLED && enable_inbound_tls_) {
    auto* tls_context = new_msgr->mutable_tls_context();

    if (!FLAGS_rpc_certificate_file.empty()) {
      CHECK(!FLAGS_rpc_private_key_file.empty());
      CHECK(!FLAGS_rpc_ca_certificate_file.empty());

      // TODO(KUDU-1920): should we try and enforce that the server
      // is in the subject or alt names of the cert?
      RETURN_NOT_OK(tls_context->LoadCertificateAuthority(FLAGS_rpc_ca_certificate_file));
      if (FLAGS_rpc_private_key_password_cmd.empty()) {
        RETURN_NOT_OK(tls_context->LoadCertificateAndKey(FLAGS_rpc_certificate_file,
                                                         FLAGS_rpc_private_key_file));
      } else {
        RETURN_NOT_OK(tls_context->LoadCertificateAndPasswordProtectedKey(
            FLAGS_rpc_certificate_file, FLAGS_rpc_private_key_file,
            [&](){
              string ret;
              WARN_NOT_OK(security::GetPasswordFromShellCommand(
                  FLAGS_rpc_private_key_password_cmd, &ret),
                  "could not get RPC password from configured command");
              return ret;
            }
        ));
      }
    } else {
      RETURN_NOT_OK(tls_context->GenerateSelfSignedCertAndKey());
    }
  }

  // See docs on Messenger::retain_self_ for info about this odd hack.
  cleanup.cancel();
  *msgr = shared_ptr<Messenger>(new_msgr, std::mem_fun(&Messenger::AllExternalReferencesDropped));
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
  if (!FLAGS_keytab_file.empty()) {
    RETURN_NOT_OK_PREPEND(ServerNegotiation::PreflightCheckGSSAPI(),
                          "GSSAPI/Kerberos not properly configured");
  }

  Socket sock;
  RETURN_NOT_OK(sock.Init(0));
  RETURN_NOT_OK(sock.SetReuseAddr(true));
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
    tls_context_(new security::TlsContext()),
    token_verifier_(new security::TokenVerifier()),
    rpcz_store_(new RpczStore()),
    metric_entity_(bld.metric_entity_),
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
  std::lock_guard<percpu_rwlock> guard(lock_);
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

Status Messenger::DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                                  DumpRunningRpcsResponsePB* resp) {
  shared_lock<rw_spinlock> guard(lock_.get_lock());
  for (Reactor* reactor : reactors_) {
    RETURN_NOT_OK(reactor->DumpRunningRpcs(req, resp));
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
    std::lock_guard<percpu_rwlock> guard(lock_);
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
