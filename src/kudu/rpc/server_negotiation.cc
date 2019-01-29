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

#include "kudu/rpc/server_negotiation.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <sasl/sasl.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/blocking_ops.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_verification_util.h"
#include "kudu/rpc/serialization.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/init.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/trace.h"

#if defined(__APPLE__)
// Almost all functions in the SASL API are marked as deprecated
// since macOS 10.11.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif // #if defined(__APPLE__)

using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

// Fault injection flags.
DEFINE_double(rpc_inject_invalid_authn_token_ratio, 0,
              "If set higher than 0, AuthenticateByToken() randomly injects "
              "errors replying with FATAL_INVALID_AUTHENTICATION_TOKEN code. "
              "The flag's value corresponds to the probability of the fault "
              "injection event. Used for only for tests.");
TAG_FLAG(rpc_inject_invalid_authn_token_ratio, runtime);
TAG_FLAG(rpc_inject_invalid_authn_token_ratio, unsafe);

DECLARE_bool(rpc_encrypt_loopback_connections);

DEFINE_string(trusted_subnets,
              "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16",
              "A trusted subnet whitelist. If set explicitly, all unauthenticated "
              "or unencrypted connections are prohibited except the ones from the "
              "specified address blocks. Otherwise, private network (127.0.0.0/8, etc.) "
              "and local subnets of all local network interfaces will be used. Set it "
              "to '0.0.0.0/0' to allow unauthenticated/unencrypted connections from all "
              "remote IP addresses. However, if network access is not otherwise restricted "
              "by a firewall, malicious users may be able to gain unauthorized access.");
TAG_FLAG(trusted_subnets, advanced);
TAG_FLAG(trusted_subnets, evolving);

static bool ValidateTrustedSubnets(const char* /*flagname*/, const string& value) {
  if (value.empty()) {
    return true;
  }

  for (const auto& t : strings::Split(value, ",", strings::SkipEmpty())) {
    kudu::Network network;
    kudu::Status s = network.ParseCIDRString(t.ToString());
    if (!s.ok()) {
      LOG(ERROR) << "Invalid subnet address: " << t
                 << ". Subnet must be specified in CIDR notation.";
      return false;
    }
  }

  return true;
}

DEFINE_validator(trusted_subnets, &ValidateTrustedSubnets);

namespace kudu {
namespace rpc {

namespace {
vector<Network>* g_trusted_subnets = nullptr;
} // anonymous namespace

static int ServerNegotiationGetoptCb(ServerNegotiation* server_negotiation,
                                     const char* plugin_name,
                                     const char* option,
                                     const char** result,
                                     unsigned* len) {
  return server_negotiation->GetOptionCb(plugin_name, option, result, len);
}

static int ServerNegotiationPlainAuthCb(sasl_conn_t* conn,
                                        ServerNegotiation* server_negotiation,
                                        const char* user,
                                        const char* pass,
                                        unsigned passlen,
                                        struct propctx* propctx) {
  return server_negotiation->PlainAuthCb(conn, user, pass, passlen, propctx);
}

ServerNegotiation::ServerNegotiation(unique_ptr<Socket> socket,
                                     const security::TlsContext* tls_context,
                                     const security::TokenVerifier* token_verifier,
                                     RpcEncryption encryption,
                                     std::string sasl_proto_name)
    : socket_(std::move(socket)),
      helper_(SaslHelper::SERVER),
      tls_context_(tls_context),
      encryption_(encryption),
      tls_negotiated_(false),
      token_verifier_(token_verifier),
      negotiated_authn_(AuthenticationType::INVALID),
      negotiated_mech_(SaslMechanism::INVALID),
      sasl_proto_name_(std::move(sasl_proto_name)),
      deadline_(MonoTime::Max()) {
  callbacks_.push_back(SaslBuildCallback(SASL_CB_GETOPT,
      reinterpret_cast<int (*)()>(&ServerNegotiationGetoptCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_SERVER_USERDB_CHECKPASS,
      reinterpret_cast<int (*)()>(&ServerNegotiationPlainAuthCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_LIST_END, nullptr, nullptr));
}

Status ServerNegotiation::EnablePlain() {
  return helper_.EnablePlain();
}

Status ServerNegotiation::EnableGSSAPI() {
  return helper_.EnableGSSAPI();
}

SaslMechanism::Type ServerNegotiation::negotiated_mechanism() const {
  return negotiated_mech_;
}

void ServerNegotiation::set_server_fqdn(const string& domain_name) {
  helper_.set_server_fqdn(domain_name);
}

void ServerNegotiation::set_deadline(const MonoTime& deadline) {
  deadline_ = deadline;
}

Status ServerNegotiation::Negotiate() {
  TRACE("Beginning negotiation");

  // Wait until starting negotiation to check that the socket, tls_context, and
  // token_verifier are not null, since they do not need to be set for
  // PreflightCheckGSSAPI.
  DCHECK(socket_);
  DCHECK(tls_context_);
  DCHECK(token_verifier_);

  // Ensure we can use blocking calls on the socket during negotiation.
  RETURN_NOT_OK(CheckInBlockingMode(socket_.get()));

  faststring recv_buf;

  // Step 1: Read the connection header.
  RETURN_NOT_OK(ValidateConnectionHeader(&recv_buf));

  { // Step 2: Receive and respond to the NEGOTIATE step message.
    NegotiatePB request;
    RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));
    RETURN_NOT_OK(HandleNegotiate(request));
    TRACE("Negotiated authn=$0", AuthenticationTypeToString(negotiated_authn_));
  }

  // Step 3: if both ends support TLS, do a TLS handshake.
  if (encryption_ != RpcEncryption::DISABLED &&
      tls_context_->has_cert() &&
      ContainsKey(client_features_, TLS)) {
    RETURN_NOT_OK(tls_context_->InitiateHandshake(security::TlsHandshakeType::SERVER,
                                                  &tls_handshake_));

    if (negotiated_authn_ != AuthenticationType::CERTIFICATE) {
      // The server does not need to verify the client's certificate unless it's
      // being used for authentication.
      tls_handshake_.set_verification_mode(security::TlsVerificationMode::VERIFY_NONE);
    }

    while (true) {
      NegotiatePB request;
      RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));
      Status s = HandleTlsHandshake(request);
      if (s.ok()) break;
      if (!s.IsIncomplete()) return s;
    }
    tls_negotiated_ = true;
  }

  // Rejects any connection from public routable IPs if encryption
  // is disabled. See KUDU-1875.
  if (!tls_negotiated_) {
    Sockaddr addr;
    RETURN_NOT_OK(socket_->GetPeerAddress(&addr));

    if (!IsTrustedConnection(addr)) {
      // Receives client response before sending error
      // message, even though the response is never used,
      // to avoid risk condition that connection gets
      // closed before client receives server's error
      // message.
      NegotiatePB request;
      RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));

      Status s = Status::NotAuthorized("unencrypted connections from publicly routable "
                                       "IPs are prohibited. See --trusted_subnets flag "
                                       "for more information.",
                                       addr.ToString());
      RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
      return s;
    }
  }

  // Step 4: Authentication
  switch (negotiated_authn_) {
    case AuthenticationType::SASL:
      RETURN_NOT_OK(AuthenticateBySasl(&recv_buf));
      break;
    case AuthenticationType::TOKEN:
      RETURN_NOT_OK(AuthenticateByToken(&recv_buf));
      break;
    case AuthenticationType::CERTIFICATE:
      RETURN_NOT_OK(AuthenticateByCertificate());
      break;
    case AuthenticationType::INVALID: LOG(FATAL) << "unreachable";
  }

  // Step 5: Receive connection context.
  RETURN_NOT_OK(RecvConnectionContext(&recv_buf));

  TRACE("Negotiation successful");
  return Status::OK();
}

Status ServerNegotiation::PreflightCheckGSSAPI(const std::string& sasl_proto_name) {
  // TODO(todd): the error messages that come from this function on el6
  // are relatively useless due to the following krb5 bug:
  // http://krbdev.mit.edu/rt/Ticket/Display.html?id=6973
  // This may not be useful anymore given the keytab login that happens
  // in security/init.cc.

  // Initialize a ServerNegotiation with a null socket, and enable
  // only GSSAPI.
  //
  // We aren't going to actually send/receive any messages, but
  // this makes it easier to reuse the initialization code.
  ServerNegotiation server(
      nullptr, nullptr, nullptr, RpcEncryption::OPTIONAL, sasl_proto_name);
  Status s = server.EnableGSSAPI();
  if (!s.ok()) {
    return Status::RuntimeError(s.message());
  }

  RETURN_NOT_OK(server.InitSaslServer());

  // Start the SASL server as if we were accepting a connection.
  const char* server_out = nullptr; // ignored
  uint32_t server_out_len = 0;
  s = WrapSaslCall(server.sasl_conn_.get(), [&]() {
      return sasl_server_start(
          server.sasl_conn_.get(),
          kSaslMechGSSAPI,
          "", 0,  // Pass a 0-length token.
          &server_out, &server_out_len);
    });

  // We expect 'Incomplete' status to indicate that the first step of negotiation
  // was correct.
  if (s.IsIncomplete()) return Status::OK();

  string err_msg = s.message().ToString();
  if (err_msg == "Permission denied") {
    // For bad keytab permissions, we get a rather vague message. So,
    // we make it more specific for better usability.
    err_msg = "error accessing keytab: " + err_msg;
  }
  return Status::RuntimeError(err_msg);
}

Status ServerNegotiation::RecvNegotiatePB(NegotiatePB* msg, faststring* recv_buf) {
  RequestHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(socket(), recv_buf, &header, &param_buf, deadline_));
  Status s = helper_.CheckNegotiateCallId(header.call_id());
  if (!s.ok()) {
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_INVALID_RPC_HEADER, s));
    return s;
  }

  s = helper_.ParseNegotiatePB(param_buf, msg);
  if (!s.ok()) {
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_DESERIALIZING_REQUEST, s));
    return s;
  }

  TRACE("Received $0 NegotiatePB request", NegotiatePB::NegotiateStep_Name(msg->step()));
  return Status::OK();
}

Status ServerNegotiation::SendNegotiatePB(const NegotiatePB& msg) {
  ResponseHeader header;
  header.set_call_id(kNegotiateCallId);

  DCHECK(socket_);
  DCHECK(msg.IsInitialized()) << "message must be initialized";
  DCHECK(msg.has_step()) << "message must have a step";

  TRACE("Sending $0 NegotiatePB response", NegotiatePB::NegotiateStep_Name(msg.step()));
  return SendFramedMessageBlocking(socket(), header, msg, deadline_);
}

Status ServerNegotiation::SendError(ErrorStatusPB::RpcErrorCodePB code, const Status& err) {
  DCHECK(!err.ok());

  // Create header with negotiation-specific callId
  ResponseHeader header;
  header.set_call_id(kNegotiateCallId);
  header.set_is_error(true);

  // Get RPC error code from Status object
  ErrorStatusPB msg;
  msg.set_code(code);
  msg.set_message(err.ToString());

  TRACE("Sending RPC error: $0: $1", ErrorStatusPB::RpcErrorCodePB_Name(code), err.ToString());
  RETURN_NOT_OK(SendFramedMessageBlocking(socket(), header, msg, deadline_));

  return Status::OK();
}

Status ServerNegotiation::ValidateConnectionHeader(faststring* recv_buf) {
  TRACE("Waiting for connection header");
  size_t num_read;
  const size_t conn_header_len = kMagicNumberLength + kHeaderFlagsLength;
  recv_buf->resize(conn_header_len);
  RETURN_NOT_OK(socket_->BlockingRecv(recv_buf->data(), conn_header_len, &num_read, deadline_));
  DCHECK_EQ(conn_header_len, num_read);

  RETURN_NOT_OK(serialization::ValidateConnHeader(*recv_buf));
  TRACE("Connection header received");
  return Status::OK();
}

// calls sasl_server_init() and sasl_server_new()
Status ServerNegotiation::InitSaslServer() {
  // TODO(unknown): Support security flags.
  unsigned secflags = 0;

  sasl_conn_t* sasl_conn = nullptr;
  RETURN_NOT_OK_PREPEND(WrapSaslCall(nullptr /* no conn */, [&]() {
      return sasl_server_new(
          // Registered name of the service using SASL. Required.
          sasl_proto_name_.c_str(),
          // The fully qualified domain name of this server.
          helper_.server_fqdn(),
          // Permits multiple user realms on server. NULL == use default.
          nullptr,
          // Local and remote IP address strings. We don't use any mechanisms
          // which need these.
          nullptr,
          nullptr,
          // Connection-specific callbacks.
          &callbacks_[0],
          // Security flags.
          secflags,
          &sasl_conn);
    }), "Unable to create new SASL server");
  sasl_conn_.reset(sasl_conn);
  return Status::OK();
}

Status ServerNegotiation::HandleNegotiate(const NegotiatePB& request) {
  if (request.step() != NegotiatePB::NEGOTIATE) {
    Status s = Status::NotAuthorized("expected NEGOTIATE step",
                                     NegotiatePB::NegotiateStep_Name(request.step()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }
  TRACE("Received NEGOTIATE request from client");

  // Fill in the set of features supported by the client.
  for (int flag : request.supported_features()) {
    // We only add the features that our local build knows about.
    RpcFeatureFlag feature_flag = RpcFeatureFlag_IsValid(flag) ?
                                  static_cast<RpcFeatureFlag>(flag) : UNKNOWN;
    if (feature_flag != UNKNOWN) {
      client_features_.insert(feature_flag);
    }
  }

  if (encryption_ == RpcEncryption::REQUIRED &&
      !ContainsKey(client_features_, RpcFeatureFlag::TLS)) {
    Status s = Status::NotAuthorized("client does not support required TLS encryption");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  // Find the set of mutually supported authentication types.
  set<AuthenticationType> authn_types;
  if (request.authn_types().empty()) {
    // If the client doesn't send any support authentication types, we assume
    // support for SASL. This preserves backwards compatibility with clients who
    // don't support security features.
    authn_types.insert(AuthenticationType::SASL);
  } else {
    for (const auto& type : request.authn_types()) {
      switch (type.type_case()) {
        case AuthenticationTypePB::kSasl:
          authn_types.insert(AuthenticationType::SASL);
          break;
        case AuthenticationTypePB::kToken:
          authn_types.insert(AuthenticationType::TOKEN);
          break;
        case AuthenticationTypePB::kCertificate:
          // We only provide authenticated TLS if the certificates are generated
          // by the internal CA.
          if (!tls_context_->is_external_cert()) {
            authn_types.insert(AuthenticationType::CERTIFICATE);
          }
          break;
        case AuthenticationTypePB::TYPE_NOT_SET: {
          Sockaddr addr;
          RETURN_NOT_OK(socket_->GetPeerAddress(&addr));
          KLOG_EVERY_N_SECS(WARNING, 60)
              << "client supports unknown authentication type, consider updating server, address: "
              << addr.ToString();
          break;
        }
      }
    }

    if (authn_types.empty()) {
      Status s = Status::NotSupported("no mutually supported authentication types");
      RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
      return s;
    }
  }

  if (encryption_ != RpcEncryption::DISABLED &&
      ContainsKey(authn_types, AuthenticationType::CERTIFICATE) &&
      tls_context_->has_signed_cert()) {
    // If the client supports it and we are locally configured with TLS and have
    // a CA-signed cert, choose cert authn.
    // TODO(KUDU-1924): consider adding the fingerprint of the CA cert which signed
    // the client's cert to the authentication message.
    negotiated_authn_ = AuthenticationType::CERTIFICATE;
  } else if (ContainsKey(authn_types, AuthenticationType::TOKEN) &&
             token_verifier_->GetMaxKnownKeySequenceNumber() >= 0 &&
             encryption_ != RpcEncryption::DISABLED &&
             tls_context_->has_signed_cert()) {
    // If the client supports it, we have a TSK to verify the client's token,
    // and we have a signed-cert so the client can verify us, choose token authn.
    // TODO(KUDU-1924): consider adding the TSK sequence number to the authentication
    // message.
    negotiated_authn_ = AuthenticationType::TOKEN;
  } else {
    // Otherwise we always can fallback to SASL.
    DCHECK(ContainsKey(authn_types, AuthenticationType::SASL));
    negotiated_authn_ = AuthenticationType::SASL;
  }

  // Fill in the NEGOTIATE step response for the client.
  NegotiatePB response;
  response.set_step(NegotiatePB::NEGOTIATE);

  // Tell the client which features we support.
  server_features_ = kSupportedServerRpcFeatureFlags;
  if (tls_context_->has_cert() && encryption_ != RpcEncryption::DISABLED) {
    server_features_.insert(TLS);
    // If the remote peer is local, then we allow using TLS for authentication
    // without encryption or integrity.
    if (socket_->IsLoopbackConnection() && !FLAGS_rpc_encrypt_loopback_connections) {
      server_features_.insert(TLS_AUTHENTICATION_ONLY);
    }
  }

  for (RpcFeatureFlag feature : server_features_) {
    response.add_supported_features(feature);
  }

  switch (negotiated_authn_) {
    case AuthenticationType::CERTIFICATE:
      response.add_authn_types()->mutable_certificate();
      break;
    case AuthenticationType::TOKEN:
      response.add_authn_types()->mutable_token();
      break;
    case AuthenticationType::SASL: {
      response.add_authn_types()->mutable_sasl();
      const set<SaslMechanism::Type>& server_mechs = helper_.EnabledMechs();
      if (PREDICT_FALSE(server_mechs.empty())) {
        // This will happen if no mechanisms are enabled before calling Init()
        Status s = Status::NotAuthorized("SASL server mechanism list is empty!");
        LOG(ERROR) << s.ToString();
        TRACE("Sending FATAL_UNAUTHORIZED response to client");
        RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
        return s;
      }

      for (auto mechanism : server_mechs) {
        response.add_sasl_mechanisms()->set_mechanism(SaslMechanism::name_of(mechanism));
      }
      break;
    }
    case AuthenticationType::INVALID: LOG(FATAL) << "unreachable";
  }

  return SendNegotiatePB(response);
}

Status ServerNegotiation::HandleTlsHandshake(const NegotiatePB& request) {
  if (PREDICT_FALSE(request.step() != NegotiatePB::TLS_HANDSHAKE)) {
    Status s =  Status::NotAuthorized("expected TLS_HANDSHAKE step",
                                      NegotiatePB::NegotiateStep_Name(request.step()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  if (PREDICT_FALSE(!request.has_tls_handshake())) {
    Status s = Status::NotAuthorized(
        "No TLS handshake token in TLS_HANDSHAKE request from client");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  string token;
  Status s = tls_handshake_.Continue(request.tls_handshake(), &token);

  if (PREDICT_FALSE(!s.IsIncomplete() && !s.ok())) {
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  // Regardless of whether this is the final handshake roundtrip (in which case
  // Continue would have returned OK), we still need to return a response.
  RETURN_NOT_OK(SendTlsHandshake(std::move(token)));
  RETURN_NOT_OK(s);

  // TLS handshake is finished.
  if (ContainsKey(server_features_, TLS_AUTHENTICATION_ONLY) &&
      ContainsKey(client_features_, TLS_AUTHENTICATION_ONLY)) {
    TRACE("Negotiated auth-only $0 with cipher $1",
          tls_handshake_.GetProtocol(), tls_handshake_.GetCipherDescription());
    return tls_handshake_.FinishNoWrap(*socket_);
  }

  TRACE("Negotiated $0 with cipher $1",
        tls_handshake_.GetProtocol(), tls_handshake_.GetCipherDescription());
  return tls_handshake_.Finish(&socket_);
}

Status ServerNegotiation::SendTlsHandshake(string tls_token) {
  NegotiatePB msg;
  msg.set_step(NegotiatePB::TLS_HANDSHAKE);
  msg.mutable_tls_handshake()->swap(tls_token);
  return SendNegotiatePB(msg);
}

Status ServerNegotiation::AuthenticateBySasl(faststring* recv_buf) {
  RETURN_NOT_OK(InitSaslServer());

  NegotiatePB request;
  RETURN_NOT_OK(RecvNegotiatePB(&request, recv_buf));
  Status s = HandleSaslInitiate(request);

  while (s.IsIncomplete()) {
    RETURN_NOT_OK(RecvNegotiatePB(&request, recv_buf));
    s = HandleSaslResponse(request);
  }
  RETURN_NOT_OK(s);

  const char* c_username = nullptr;
  int rc = sasl_getprop(sasl_conn_.get(), SASL_USERNAME,
                        reinterpret_cast<const void**>(&c_username));
  // We expect that SASL_USERNAME will always get set.
  CHECK(rc == SASL_OK && c_username != nullptr) << "No username on authenticated connection";
  if (negotiated_mech_ == SaslMechanism::GSSAPI) {
    // The SASL library doesn't include the user's realm in the username if it's the
    // same realm as the default realm of the server. So, we pass it back through the
    // Kerberos library to add back the realm if necessary.
    string principal = c_username;
    RETURN_NOT_OK_PREPEND(security::CanonicalizeKrb5Principal(&principal),
                          "could not canonicalize krb5 principal");

    // Map the principal to the corresponding local username. For example, admins
    // can set up mappings so that joe@REMOTEREALM becomes something like 'remote-joe'
    // locally for the purposes of group mapping, ACLs, etc.
    string local_name;
    RETURN_NOT_OK_PREPEND(security::MapPrincipalToLocalName(principal, &local_name),
                          strings::Substitute("could not map krb5 principal '$0' to username",
                                              principal));
    authenticated_user_.SetAuthenticatedByKerberos(std::move(local_name), std::move(principal));
  } else {
    authenticated_user_.SetUnauthenticated(c_username);
  }
  return Status::OK();
}

Status ServerNegotiation::AuthenticateByToken(faststring* recv_buf) {
  // Sanity check that TLS has been negotiated. Receiving the token on an
  // unencrypted channel is a big no-no.
  CHECK(tls_negotiated_);

  // Receive the token from the client.
  NegotiatePB pb;
  RETURN_NOT_OK(RecvNegotiatePB(&pb, recv_buf));

  if (pb.step() != NegotiatePB::TOKEN_EXCHANGE) {
    Status s =  Status::NotAuthorized("expected TOKEN_EXCHANGE step",
                                      NegotiatePB::NegotiateStep_Name(pb.step()));
  }
  if (!pb.has_authn_token()) {
    Status s = Status::NotAuthorized("TOKEN_EXCHANGE message must include an authentication token");
  }

  // TODO(KUDU-1924): propagate the specific token verification failure back to the client,
  // so it knows how to intelligently retry.
  security::TokenPB token;
  auto verification_result = token_verifier_->VerifyTokenSignature(pb.authn_token(), &token);
  ErrorStatusPB::RpcErrorCodePB error;
  Status s = ParseVerificationResult(verification_result,
      ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, &error);
  if (!s.ok()) {
    RETURN_NOT_OK(SendError(error, s));
    return s;
  }

  if (!token.has_authn()) {
    Status s = Status::NotAuthorized("non-authentication token presented for authentication");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }
  if (!token.authn().has_username()) {
    // This is a runtime error because there should be no way a client could
    // get a signed authn token without a subject.
    Status s = Status::RuntimeError("authentication token has no username");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
    return s;
  }

  if (PREDICT_FALSE(FLAGS_rpc_inject_invalid_authn_token_ratio > 0)) {
    security::VerificationResult res;
    int sel = rand() % 4;
    switch (sel) {
      case 0:
        res = security::VerificationResult::INVALID_TOKEN;
        break;
      case 1:
        res = security::VerificationResult::INVALID_SIGNATURE;
        break;
      case 2:
        res = security::VerificationResult::EXPIRED_TOKEN;
        break;
      case 3:
        res = security::VerificationResult::EXPIRED_SIGNING_KEY;
        break;
      default:
        LOG(FATAL) << "unreachable";
    }
    if (kudu::fault_injection::MaybeTrue(FLAGS_rpc_inject_invalid_authn_token_ratio)) {
      Status s = Status::NotAuthorized(VerificationResultToString(res));
      RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
      return s;
    }
  }

  authenticated_user_.SetAuthenticatedByToken(token.authn().username());

  // Respond with success message.
  pb.Clear();
  pb.set_step(NegotiatePB::TOKEN_EXCHANGE);
  return SendNegotiatePB(pb);
}

Status ServerNegotiation::AuthenticateByCertificate() {
  // Sanity check that TLS has been negotiated. Cert-based authentication is
  // only possible with TLS.
  CHECK(tls_negotiated_);

  // Grab the subject from the client's cert.
  security::Cert cert;
  RETURN_NOT_OK(tls_handshake_.GetRemoteCert(&cert));

  boost::optional<string> user_id = cert.UserId();
  boost::optional<string> principal = cert.KuduKerberosPrincipal();

  if (!user_id) {
    Status s = Status::NotAuthorized("did not find expected X509 userId extension in cert");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
    return s;
  }

  authenticated_user_.SetAuthenticatedByClientCert(*user_id, std::move(principal));

  return Status::OK();
}

Status ServerNegotiation::HandleSaslInitiate(const NegotiatePB& request) {
  if (PREDICT_FALSE(request.step() != NegotiatePB::SASL_INITIATE)) {
    Status s =  Status::NotAuthorized("expected SASL_INITIATE step",
                                      NegotiatePB::NegotiateStep_Name(request.step()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }
  TRACE("Received SASL_INITIATE request from client");

  if (request.sasl_mechanisms_size() != 1) {
    Status s = Status::NotAuthorized(
        "SASL_INITIATE request must include exactly one SASL mechanism, found",
        std::to_string(request.sasl_mechanisms_size()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  const string& mechanism = request.sasl_mechanisms(0).mechanism();
  TRACE("Client requested to use mechanism: $0", mechanism);

  negotiated_mech_ = SaslMechanism::value_of(mechanism);

  // Rejects any connection from public routable IPs if authentication mechanism
  // is plain. See KUDU-1875.
  if (negotiated_mech_ == SaslMechanism::PLAIN) {
    Sockaddr addr;
    RETURN_NOT_OK(socket_->GetPeerAddress(&addr));

    if (!IsTrustedConnection(addr)) {
      Status s = Status::NotAuthorized("unauthenticated connections from publicly "
                                       "routable IPs are prohibited. See "
                                       "--trusted_subnets flag for more information.",
                                       addr.ToString());
      RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
      return s;
    }
  }

  // If the negotiated mechanism is GSSAPI (Kerberos), configure SASL to use
  // integrity protection so that the channel bindings and nonce can be
  // verified.
  if (negotiated_mech_ == SaslMechanism::GSSAPI) {
    RETURN_NOT_OK(EnableProtection(sasl_conn_.get(), SaslProtection::kIntegrity));
  }

  const char* server_out = nullptr;
  uint32_t server_out_len = 0;
  TRACE("Calling sasl_server_start()");

  Status s = WrapSaslCall(sasl_conn_.get(), [&]() {
      return sasl_server_start(
          sasl_conn_.get(),         // The SASL connection context created by init()
          mechanism.c_str(),        // The mechanism requested by the client.
          request.token().c_str(),  // Optional string the client gave us.
          request.token().length(), // Client string len.
          &server_out,              // The output of the SASL library, might not be NULL terminated
          &server_out_len);         // Output len.
    });

  if (PREDICT_FALSE(!s.ok() && !s.IsIncomplete())) {
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  // We have a valid mechanism match
  if (s.ok()) {
    DCHECK(server_out_len == 0);
    RETURN_NOT_OK(SendSaslSuccess());
  } else { // s.IsIncomplete() (equivalent to SASL_CONTINUE)
    RETURN_NOT_OK(SendSaslChallenge(server_out, server_out_len));
  }
  return s;
}

Status ServerNegotiation::HandleSaslResponse(const NegotiatePB& request) {
  if (PREDICT_FALSE(request.step() != NegotiatePB::SASL_RESPONSE)) {
    Status s =  Status::NotAuthorized("expected SASL_RESPONSE step",
                                      NegotiatePB::NegotiateStep_Name(request.step()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }
  TRACE("Received SASL_RESPONSE request from client");

  if (!request.has_token()) {
    Status s = Status::NotAuthorized("no token in SASL_RESPONSE from client");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  const char* server_out = nullptr;
  uint32_t server_out_len = 0;
  TRACE("Calling sasl_server_step()");
  Status s = WrapSaslCall(sasl_conn_.get(), [&]() {
      return sasl_server_step(
          sasl_conn_.get(),         // The SASL connection context created by init()
          request.token().c_str(),  // Optional string the client gave us
          request.token().length(), // Client string len
          &server_out,              // The output of the SASL library, might not be NULL terminated
          &server_out_len);         // Output len
    });

  if (s.ok()) {
    DCHECK(server_out_len == 0);
    return SendSaslSuccess();
  }
  if (s.IsIncomplete()) {
    return SendSaslChallenge(server_out, server_out_len);
  }
  RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
  return s;
}

Status ServerNegotiation::SendSaslChallenge(const char* challenge, unsigned clen) {
  NegotiatePB response;
  response.set_step(NegotiatePB::SASL_CHALLENGE);
  response.mutable_token()->assign(challenge, clen);
  RETURN_NOT_OK(SendNegotiatePB(response));
  return Status::Incomplete("");
}

Status ServerNegotiation::SendSaslSuccess() {
  NegotiatePB response;
  response.set_step(NegotiatePB::SASL_SUCCESS);

  if (negotiated_mech_ == SaslMechanism::GSSAPI) {
    // Send a nonce to the client.
    nonce_ = string();
    RETURN_NOT_OK(security::GenerateNonce(nonce_.get_ptr()));
    response.set_nonce(*nonce_);

    if (tls_negotiated_) {
      // Send the channel bindings to the client.
      security::Cert cert;
      RETURN_NOT_OK(tls_handshake_.GetLocalCert(&cert));

      string plaintext_channel_bindings;
      RETURN_NOT_OK(cert.GetServerEndPointChannelBindings(&plaintext_channel_bindings));

      Slice ciphertext;
      RETURN_NOT_OK(SaslEncode(sasl_conn_.get(),
                               plaintext_channel_bindings,
                               &ciphertext));
      *response.mutable_channel_bindings() = ciphertext.ToString();
    }
  }

  RETURN_NOT_OK(SendNegotiatePB(response));
  return Status::OK();
}

Status ServerNegotiation::RecvConnectionContext(faststring* recv_buf) {
  TRACE("Waiting for connection context");
  RequestHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(socket(), recv_buf, &header, &param_buf, deadline_));
  DCHECK(header.IsInitialized());

  if (header.call_id() != kConnectionContextCallId) {
    return Status::NotAuthorized("expected ConnectionContext callid, received",
                                 std::to_string(header.call_id()));
  }

  ConnectionContextPB conn_context;
  if (!conn_context.ParseFromArray(param_buf.data(), param_buf.size())) {
    return Status::NotAuthorized("invalid ConnectionContextPB message, missing fields",
                                 conn_context.InitializationErrorString());
  }

  if (nonce_) {
    Status s;
    // Validate that the client returned the correct SASL protected nonce.
    if (!conn_context.has_encoded_nonce()) {
      return Status::NotAuthorized("ConnectionContextPB wrapped nonce missing");
    }

    Slice decoded_nonce;
    s = SaslDecode(sasl_conn_.get(), conn_context.encoded_nonce(), &decoded_nonce);
    if (!s.ok()) {
      return Status::NotAuthorized("failed to decode nonce", s.message());
    }

    if (*nonce_ != decoded_nonce) {
      Sockaddr addr;
      RETURN_NOT_OK(socket_->GetPeerAddress(&addr));
      LOG(WARNING) << "Received an invalid connection nonce from client "
                   << addr.ToString()
                   << ", this could indicate a replay attack";
      return Status::NotAuthorized("nonce mismatch");
    }
  }

  return Status::OK();
}

int ServerNegotiation::GetOptionCb(const char* plugin_name,
                                   const char* option,
                                   const char** result,
                                   unsigned* len) {
  return helper_.GetOptionCb(plugin_name, option, result, len);
}

int ServerNegotiation::PlainAuthCb(sasl_conn_t* /*conn*/,
                                   const char* user,
                                   const char* /*pass*/,
                                   unsigned /*passlen*/,
                                   struct propctx*  /*propctx*/) {
  TRACE("Received PLAIN auth, user=$0", user);
  if (PREDICT_FALSE(!helper_.IsPlainEnabled())) {
    LOG(DFATAL) << "Password authentication callback called while PLAIN auth disabled";
    return SASL_BADPARAM;
  }
  // We always allow PLAIN authentication to succeed.
  return SASL_OK;
}

bool ServerNegotiation::IsTrustedConnection(const Sockaddr& addr) {
  static std::once_flag once;
  std::call_once(once, [] {
    g_trusted_subnets = new vector<Network>();
    CHECK_OK(Network::ParseCIDRStrings(FLAGS_trusted_subnets, g_trusted_subnets));

    // If --trusted_subnets is not set explicitly, local subnets of all local network
    // interfaces as well as the default private subnets will be used.
    if (google::GetCommandLineFlagInfoOrDie("trusted_subnets").is_default) {
      std::vector<Network> local_networks;
      WARN_NOT_OK(GetLocalNetworks(&local_networks),
                  "Unable to get local networks.");

      g_trusted_subnets->insert(g_trusted_subnets->end(),
                                local_networks.begin(),
                                local_networks.end());
    }
  });

  return std::any_of(g_trusted_subnets->begin(), g_trusted_subnets->end(),
                     [&](const Network& t) { return t.WithinNetwork(addr); });
}

} // namespace rpc
} // namespace kudu

#if defined(__APPLE__)
#pragma GCC diagnostic pop
#endif // #if defined(__APPLE__)
