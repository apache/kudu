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
#ifndef KUDU_RPC_MESSENGER_H
#define KUDU_RPC_MESSENGER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/connection.h"
#include "kudu/security/security_flags.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace boost {
template <typename Signature>
class function;
}

namespace kudu {

class Socket;
class ThreadPool;

namespace security {
class TlsContext;
class TokenVerifier;
}

namespace rpc {

using security::RpcAuthentication;
using security::RpcEncryption;

class AcceptorPool;
class DumpRunningRpcsRequestPB;
class DumpRunningRpcsResponsePB;
class InboundCall;
class Messenger;
class OutboundCall;
class Reactor;
class RpcService;
class RpczStore;

struct AcceptorPoolInfo {
 public:
  explicit AcceptorPoolInfo(Sockaddr bind_address)
      : bind_address_(bind_address) {}

  Sockaddr bind_address() const {
    return bind_address_;
  }

 private:
  Sockaddr bind_address_;
};

// Used to construct a Messenger.
class MessengerBuilder {
 public:
  friend class Messenger;
  friend class ReactorThread;

  explicit MessengerBuilder(std::string name);

  // Set the length of time we will keep a TCP connection will alive with no traffic.
  MessengerBuilder &set_connection_keepalive_time(const MonoDelta &keepalive);

  // Set the number of reactor threads that will be used for sending and
  // receiving.
  MessengerBuilder &set_num_reactors(int num_reactors);

  // Set the minimum number of connection-negotiation threads that will be used
  // to handle the blocking connection-negotiation step.
  MessengerBuilder &set_min_negotiation_threads(int min_negotiation_threads);

  // Set the maximum number of connection-negotiation threads that will be used
  // to handle the blocking connection-negotiation step.
  MessengerBuilder &set_max_negotiation_threads(int max_negotiation_threads);

  // Set the granularity with which connections are checked for keepalive.
  MessengerBuilder &set_coarse_timer_granularity(const MonoDelta &granularity);

  // Set metric entity for use by RPC systems.
  MessengerBuilder &set_metric_entity(const scoped_refptr<MetricEntity>& metric_entity);

  // Set the time in milliseconds after which an idle connection from a client will be
  // disconnected by the server.
  MessengerBuilder &set_connection_keep_alive_time(int32_t time_in_ms);

  // Set the timeout for negotiating an RPC connection.
  MessengerBuilder &set_rpc_negotiation_timeout_ms(int64_t time_in_ms);

  // Set the SASL protocol name that is used for the SASL negotiation.
  MessengerBuilder &set_sasl_proto_name(const std::string& sasl_proto_name);

  // Set the state of authentication required. If 'optional', authentication will be used when
  // the remote end supports it. If 'required', connections which are not able to authenticate
  // (because the remote end lacks support) are rejected.
  MessengerBuilder &set_rpc_authentication(const std::string& rpc_authentication);

  // Set the state of encryption required. If 'optional', encryption will be used when the
  // remote end supports it. If 'required', connections which are not able to use encryption
  // (because the remote end lacks support) are rejected. If 'disabled', encryption will not
  // be used, and RPC authentication (--rpc_authentication) must also be disabled as well.
  MessengerBuilder &set_rpc_encryption(const std::string& rpc_encryption);

  // Set the cipher suite preferences to use for TLS-secured RPC connections. Uses the OpenSSL
  // cipher preference list format. See man (1) ciphers for more information.
  MessengerBuilder &set_rpc_tls_ciphers(const std::string& rpc_tls_ciphers);

  // Set the minimum protocol version to allow when for securing RPC connections with TLS. May be
  // one of 'TLSv1', 'TLSv1.1', or 'TLSv1.2'.
  MessengerBuilder &set_rpc_tls_min_protocol(const std::string& rpc_tls_min_protocol);

  // Set the TLS server certificate and private key files paths. If this is set in conjunction
  // with enable_inbound_tls(), internal PKI will not be used for encrypted communication and
  // external PKI will be used instead.
  MessengerBuilder &set_epki_cert_key_files(
      const std::string& cert, const std::string& private_key);

  // Set the TLS Certificate Authority file path. Must always be set with set_epki_cert_key_files().
  // If this is set in conjunction with enable_inbound_tls(), internal PKI will not be used for
  // encrypted communication and external PKI will be used instead.
  MessengerBuilder &set_epki_certificate_authority_file(const std::string& ca);

  // Set a Unix command whose output returns the password used to decrypt the RPC server's private
  // key file specified via set_epki_cert_key_files(). If the .PEM key file is not
  // password-protected, this flag does not need to be set. Trailing whitespace will be trimmed
  // before it is used to decrypt the private key.
  MessengerBuilder &set_epki_private_password_key_cmd(const std::string& cmd);

  // Set the path to the Kerberos Keytab file for this server.
  MessengerBuilder &set_keytab_file(const std::string& keytab_file);

  // Configure the messenger to enable TLS encryption on inbound connections.
  MessengerBuilder& enable_inbound_tls();

  Status Build(std::shared_ptr<Messenger> *msgr);

 private:
  const std::string name_;
  MonoDelta connection_keepalive_time_;
  int num_reactors_;
  int min_negotiation_threads_;
  int max_negotiation_threads_;
  MonoDelta coarse_timer_granularity_;
  scoped_refptr<MetricEntity> metric_entity_;
  int64_t rpc_negotiation_timeout_ms_;
  std::string sasl_proto_name_;
  std::string rpc_authentication_;
  std::string rpc_encryption_;
  std::string rpc_tls_ciphers_;
  std::string rpc_tls_min_protocol_;
  std::string rpc_certificate_file_;
  std::string rpc_private_key_file_;
  std::string rpc_ca_certificate_file_;
  std::string rpc_private_key_password_cmd_;
  std::string keytab_file_;
  bool enable_inbound_tls_;
};

// A Messenger is a container for the reactor threads which run event loops
// for the RPC services. If the process is a server, a Messenger can also have
// one or more attached AcceptorPools which accept RPC connections. In this case,
// calls received over the connection are enqueued into the messenger's service_queue
// for processing by a ServicePool.
//
// Users do not typically interact with the Messenger directly except to create
// one as a singleton, and then make calls using Proxy objects.
//
// See rpc-test.cc and rpc-bench.cc for example usages.
class Messenger {
 public:
  friend class MessengerBuilder;
  friend class Proxy;
  friend class Reactor;
  friend class ReactorThread;
  typedef std::vector<std::shared_ptr<AcceptorPool> > acceptor_vec_t;
  typedef std::unordered_map<std::string, scoped_refptr<RpcService> > RpcServicesMap;

  static const uint64_t UNKNOWN_CALL_ID = 0;

  ~Messenger();

  // Stops all communication and prevents further use. If called explicitly,
  // also waits for outstanding tasks running on reactor threads to finish,
  // which means it may  not be called from a reactor task.
  //
  // It's not required to call this -- dropping the shared_ptr provided
  // from MessengerBuilder::Build will automatically call this method.
  void Shutdown();

  // Add a new acceptor pool listening to the given accept address.
  // You can create any number of acceptor pools you want, including none.
  //
  // The created pool is returned in *pool. The Messenger also retains
  // a reference to the pool, so the caller may safely drop this reference
  // and the pool will remain live.
  //
  // NOTE: the returned pool is not initially started. You must call
  // pool->Start(...) to begin accepting connections.
  //
  // If Kerberos is enabled, this also runs a pre-flight check that makes
  // sure the environment is appropriately configured to authenticate
  // clients via Kerberos. If not, this returns a RuntimeError.
  Status AddAcceptorPool(const Sockaddr &accept_addr,
                         std::shared_ptr<AcceptorPool>* pool);

  // Register a new RpcService to handle inbound requests.
  //
  // Returns an error if a service with the same name is already registered.
  Status RegisterService(const std::string& service_name,
                         const scoped_refptr<RpcService>& service);

  // Unregister an RpcService by name.
  //
  // Returns an error if no service with this name can be found.
  Status UnregisterService(const std::string& service_name);

  // Unregisters all RPC services.
  void UnregisterAllServices();

  // Queue a call for transmission. This will pick the appropriate reactor,
  // and enqueue a task on that reactor to assign and send the call.
  void QueueOutboundCall(const std::shared_ptr<OutboundCall> &call);

  // Enqueue a call for processing on the server.
  void QueueInboundCall(gscoped_ptr<InboundCall> call);

  // Queue a cancellation for the given outbound call.
  void QueueCancellation(const std::shared_ptr<OutboundCall> &call);

  // Take ownership of the socket via Socket::Release
  void RegisterInboundSocket(Socket *new_socket, const Sockaddr &remote);

  // Dump the current RPCs into the given protobuf.
  Status DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                         DumpRunningRpcsResponsePB* resp);

  // Run 'func' on a reactor thread after 'when' time elapses.
  //
  // The status argument conveys whether 'func' was run correctly (i.e.
  // after the elapsed time) or not.
  void ScheduleOnReactor(const boost::function<void(const Status&)>& func,
                         MonoDelta when);

  const security::TlsContext& tls_context() const { return *tls_context_; }
  security::TlsContext* mutable_tls_context() { return tls_context_.get(); }

  const security::TokenVerifier& token_verifier() const { return *token_verifier_; }
  security::TokenVerifier* mutable_token_verifier() { return token_verifier_.get(); }
  std::shared_ptr<security::TokenVerifier> shared_token_verifier() const {
    return token_verifier_;
  }

  boost::optional<security::SignedTokenPB> authn_token() const {
    std::lock_guard<simple_spinlock> l(authn_token_lock_);
    return authn_token_;
  }
  void set_authn_token(const security::SignedTokenPB& token) {
    std::lock_guard<simple_spinlock> l(authn_token_lock_);
    authn_token_ = token;
  }

  RpcAuthentication authentication() const { return authentication_; }
  RpcEncryption encryption() const { return encryption_; }

  ThreadPool* negotiation_pool(Connection::Direction dir);

  RpczStore* rpcz_store() { return rpcz_store_.get(); }

  int num_reactors() const { return reactors_.size(); }

  const std::string& name() const {
    return name_;
  }

  bool closing() const {
    shared_lock<rw_spinlock> l(lock_.get_lock());
    return closing_;
  }

  scoped_refptr<MetricEntity> metric_entity() const { return metric_entity_.get(); }

  const int64_t rpc_negotiation_timeout_ms() const { return rpc_negotiation_timeout_ms_; }

  const std::string& sasl_proto_name() const {
    return sasl_proto_name_;
  }

  const std::string& keytab_file() const { return keytab_file_; }

  const scoped_refptr<RpcService> rpc_service(const std::string& service_name) const;

 private:
  FRIEND_TEST(TestRpc, TestConnectionKeepalive);
  FRIEND_TEST(TestRpc, TestCredentialsPolicy);
  FRIEND_TEST(TestRpc, TestReopenOutboundConnections);

  explicit Messenger(const MessengerBuilder &bld);

  Reactor* RemoteToReactor(const Sockaddr &remote);
  Status Init();
  void RunTimeoutThread();
  void UpdateCurTime();

  // Shuts down the messenger.
  //
  // Depending on 'mode', may or may not wait on any outstanding reactor tasks.
  enum class ShutdownMode {
    SYNC,
    ASYNC,
  };
  void ShutdownInternal(ShutdownMode mode);

  // Called by external-facing shared_ptr when the user no longer holds
  // any references. See 'retain_self_' for more info.
  void AllExternalReferencesDropped();

  const std::string name_;

  // Protects closing_, acceptor_pools_, rpc_services_.
  mutable percpu_rwlock lock_;

  bool closing_;

  // Whether to require authentication and encryption on the connections managed
  // by this messenger.
  // TODO(KUDU-1928): scope these to individual proxies, so that messengers can be
  // reused by different clients.
  RpcAuthentication authentication_;
  RpcEncryption encryption_;

  // Pools which are listening on behalf of this messenger.
  // Note that the user may have called Shutdown() on one of these
  // pools, so even though we retain the reference, it may no longer
  // be listening.
  acceptor_vec_t acceptor_pools_;

  // RPC services that handle inbound requests.
  RpcServicesMap rpc_services_;

  std::vector<Reactor*> reactors_;

  // Separate client and server negotiation pools to avoid possibility of distributed
  // deadlock. See KUDU-2041.
  gscoped_ptr<ThreadPool> client_negotiation_pool_;
  gscoped_ptr<ThreadPool> server_negotiation_pool_;

  std::unique_ptr<security::TlsContext> tls_context_;

  // A TokenVerifier, which can verify client provided authentication tokens.
  std::shared_ptr<security::TokenVerifier> token_verifier_;

  // An optional token, which can be used to authenticate to a server.
  mutable simple_spinlock authn_token_lock_;
  boost::optional<security::SignedTokenPB> authn_token_;

  std::unique_ptr<RpczStore> rpcz_store_;

  scoped_refptr<MetricEntity> metric_entity_;

  // Timeout in milliseconds after which an incomplete connection negotiation will timeout.
  const int64_t rpc_negotiation_timeout_ms_;

  // The SASL protocol name that is used for the SASL negotiation.
  const std::string sasl_proto_name_;

  // Path to the Kerberos Keytab file for this server.
  const std::string keytab_file_;

  // The ownership of the Messenger object is somewhat subtle. The pointer graph
  // looks like this:
  //
  //    [User Code ]             |      [ Internal code ]
  //                             |
  //     shared_ptr[1]           |
  //         |                   |
  //         v
  //      Messenger    <------------ shared_ptr[2] --- Reactor
  //       ^    |      ------------- bare pointer  --> Reactor
  //        \__/
  //     shared_ptr[2]
  //     (retain_self_)
  //
  // shared_ptr[1] instances use Messenger::AllExternalReferencesDropped()
  //   as a deleter.
  // shared_ptr[2] are "traditional" shared_ptrs which call 'delete' on the
  //   object.
  //
  // The teardown sequence is as follows:
  // Option 1): User calls "Shutdown()" explicitly:
  //  - Messenger::Shutdown tells Reactors to shut down.
  //  - When each reactor thread finishes, it drops its shared_ptr[2].
  //  - the Messenger::retain_self instance remains, keeping the Messenger
  //    alive.
  //  - Before returning, Messenger::Shutdown waits for Reactors to shut down.
  //  - The user eventually drops its shared_ptr[1], which calls
  //    Messenger::AllExternalReferencesDropped. This drops retain_self_
  //    and results in object destruction.
  // Option 2): User drops all of its shared_ptr[1] references
  //  - Though the Reactors still reference the Messenger, AllExternalReferencesDropped
  //    will get called, which triggers Messenger::Shutdown.
  //  - AllExternalReferencesDropped drops retain_self_, so the only remaining
  //    references are from Reactor threads. But the reactor threads are shutting down.
  //  - When the last Reactor thread dies, there will be no more shared_ptr[1] references
  //    and the Messenger will be destroyed.
  //
  // The main goal of all of this confusion is that when using option 2, the
  // reactor threads need to be able to shut down asynchronously, and we need
  // to keep the Messenger alive until they do so. If normal shared_ptrs were
  // handed out to users, the Messenger destructor may be forced to Join() the
  // reactor threads, which deadlocks if the user destructs the Messenger from
  // within a Reactor thread itself.
  std::shared_ptr<Messenger> retain_self_;

  DISALLOW_COPY_AND_ASSIGN(Messenger);
};

} // namespace rpc
} // namespace kudu

#endif
