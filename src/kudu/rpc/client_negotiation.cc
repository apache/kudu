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

#include "kudu/rpc/client_negotiation.h"

#include <cstring>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <sasl/sasl.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/blocking_ops.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/sasl_helper.h"
#include "kudu/rpc/serialization.h"
#include "kudu/security/cert.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/trace.h"

using std::map;
using std::set;
using std::string;
using std::unique_ptr;

using strings::Substitute;

DECLARE_bool(rpc_encrypt_loopback_connections);

namespace kudu {
namespace rpc {

static int ClientNegotiationGetoptCb(ClientNegotiation* client_negotiation,
                                     const char* plugin_name,
                                     const char* option,
                                     const char** result,
                                     unsigned* len) {
  return client_negotiation->GetOptionCb(plugin_name, option, result, len);
}

static int ClientNegotiationSimpleCb(ClientNegotiation* client_negotiation,
                                     int id,
                                     const char** result,
                                     unsigned* len) {
  return client_negotiation->SimpleCb(id, result, len);
}

static int ClientNegotiationSecretCb(sasl_conn_t* conn,
                                     ClientNegotiation* client_negotiation,
                                     int id,
                                     sasl_secret_t** psecret) {
  return client_negotiation->SecretCb(conn, id, psecret);
}

// Return an appropriately-typed Status object based on an ErrorStatusPB returned
// from an Error RPC.
// In case there is no relevant Status type, return a RuntimeError.
static Status StatusFromRpcError(const ErrorStatusPB& error) {
  DCHECK(error.IsInitialized()) << "Error status PB must be initialized";
  if (PREDICT_FALSE(!error.has_code())) {
    return Status::RuntimeError(error.message());
  }
  const string code_name = ErrorStatusPB::RpcErrorCodePB_Name(error.code());
  switch (error.code()) {
    case ErrorStatusPB_RpcErrorCodePB_FATAL_UNAUTHORIZED: // fall-through
    case ErrorStatusPB_RpcErrorCodePB_FATAL_INVALID_AUTHENTICATION_TOKEN:
      return Status::NotAuthorized(code_name, error.message());
    case ErrorStatusPB_RpcErrorCodePB_ERROR_UNAVAILABLE:
      return Status::ServiceUnavailable(code_name, error.message());
    default:
      return Status::RuntimeError(code_name, error.message());
  }
}

ClientNegotiation::ClientNegotiation(unique_ptr<Socket> socket,
                                     const security::TlsContext* tls_context,
                                     boost::optional<security::SignedTokenPB> authn_token,
                                     RpcEncryption encryption)
    : socket_(std::move(socket)),
      helper_(SaslHelper::CLIENT),
      tls_context_(tls_context),
      encryption_(encryption),
      tls_negotiated_(false),
      authn_token_(std::move(authn_token)),
      psecret_(nullptr, std::free),
      negotiated_authn_(AuthenticationType::INVALID),
      negotiated_mech_(SaslMechanism::INVALID),
      deadline_(MonoTime::Max()) {
  callbacks_.push_back(SaslBuildCallback(SASL_CB_GETOPT,
      reinterpret_cast<int (*)()>(&ClientNegotiationGetoptCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_AUTHNAME,
      reinterpret_cast<int (*)()>(&ClientNegotiationSimpleCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_PASS,
      reinterpret_cast<int (*)()>(&ClientNegotiationSecretCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_LIST_END, nullptr, nullptr));
  DCHECK(socket_);
  DCHECK(tls_context_);
}

Status ClientNegotiation::EnablePlain(const string& user, const string& pass) {
  RETURN_NOT_OK(helper_.EnablePlain());
  plain_auth_user_ = user;
  plain_pass_ = pass;
  return Status::OK();
}

Status ClientNegotiation::EnableGSSAPI() {
  return helper_.EnableGSSAPI();
}

SaslMechanism::Type ClientNegotiation::negotiated_mechanism() const {
  return negotiated_mech_;
}

void ClientNegotiation::set_server_fqdn(const string& domain_name) {
  helper_.set_server_fqdn(domain_name);
}

void ClientNegotiation::set_deadline(const MonoTime& deadline) {
  deadline_ = deadline;
}

Status ClientNegotiation::Negotiate(unique_ptr<ErrorStatusPB>* rpc_error) {
  TRACE("Beginning negotiation");

  // Ensure we can use blocking calls on the socket during negotiation.
  RETURN_NOT_OK(EnsureBlockingMode(socket_.get()));

  // Step 1: send the connection header.
  RETURN_NOT_OK(SendConnectionHeader());

  faststring recv_buf;

  { // Step 2: send and receive the NEGOTIATE step messages.
    RETURN_NOT_OK(SendNegotiate());
    NegotiatePB response;
    RETURN_NOT_OK(RecvNegotiatePB(&response, &recv_buf, rpc_error));
    RETURN_NOT_OK(HandleNegotiate(response));
    TRACE("Negotiated authn=$0", AuthenticationTypeToString(negotiated_authn_));
  }

  // Step 3: if both ends support TLS, do a TLS handshake.
  // TODO(KUDU-1921): allow the client to require TLS.
  if (encryption_ != RpcEncryption::DISABLED &&
      ContainsKey(server_features_, TLS)) {
    RETURN_NOT_OK(tls_context_->InitiateHandshake(security::TlsHandshakeType::CLIENT,
                                                  &tls_handshake_));

    if (negotiated_authn_ == AuthenticationType::SASL) {
      // When using SASL authentication, verifying the server's certificate is
      // not necessary. This allows the client to still use TLS encryption for
      // connections to servers which only have a self-signed certificate.
      tls_handshake_.set_verification_mode(security::TlsVerificationMode::VERIFY_NONE);
    }

    // To initiate the TLS handshake, we pretend as if the server sent us an
    // empty TLS_HANDSHAKE token.
    NegotiatePB initial;
    initial.set_step(NegotiatePB::TLS_HANDSHAKE);
    initial.set_tls_handshake("");
    Status s = HandleTlsHandshake(initial);

    while (s.IsIncomplete()) {
      NegotiatePB response;
      RETURN_NOT_OK(RecvNegotiatePB(&response, &recv_buf, rpc_error));
      s = HandleTlsHandshake(response);
    }
    RETURN_NOT_OK(s);
    tls_negotiated_ = true;
  }

  // Step 4: Authentication
  switch (negotiated_authn_) {
    case AuthenticationType::SASL:
      RETURN_NOT_OK(AuthenticateBySasl(&recv_buf, rpc_error));
      break;
    case AuthenticationType::TOKEN:
      RETURN_NOT_OK(AuthenticateByToken(&recv_buf, rpc_error));
      break;
    case AuthenticationType::CERTIFICATE:
      // The TLS handshake has already authenticated the server.
      break;
    case AuthenticationType::INVALID: LOG(FATAL) << "unreachable";
  }

  // Step 5: Send connection context.
  RETURN_NOT_OK(SendConnectionContext());

  TRACE("Negotiation successful");
  return Status::OK();
}

Status ClientNegotiation::SendNegotiatePB(const NegotiatePB& msg) {
  RequestHeader header;
  header.set_call_id(kNegotiateCallId);

  DCHECK(socket_);
  DCHECK(msg.IsInitialized()) << "message must be initialized";
  DCHECK(msg.has_step()) << "message must have a step";

  TRACE("Sending $0 NegotiatePB request", NegotiatePB::NegotiateStep_Name(msg.step()));
  return SendFramedMessageBlocking(socket(), header, msg, deadline_);
}

Status ClientNegotiation::RecvNegotiatePB(NegotiatePB* msg,
                                          faststring* buffer,
                                          unique_ptr<ErrorStatusPB>* rpc_error) {
  ResponseHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(socket(), buffer, &header, &param_buf, deadline_));
  RETURN_NOT_OK(helper_.CheckNegotiateCallId(header.call_id()));

  if (header.is_error()) {
    return ParseError(param_buf, rpc_error);
  }

  RETURN_NOT_OK(helper_.ParseNegotiatePB(param_buf, msg));
  TRACE("Received $0 NegotiatePB response", NegotiatePB::NegotiateStep_Name(msg->step()));
  return Status::OK();
}

Status ClientNegotiation::ParseError(const Slice& err_data,
                                     unique_ptr<ErrorStatusPB>* rpc_error) {
  unique_ptr<ErrorStatusPB> error(new ErrorStatusPB);
  if (!error->ParseFromArray(err_data.data(), err_data.size())) {
    return Status::IOError("invalid error response, missing fields",
                           error->InitializationErrorString());
  }
  Status s = StatusFromRpcError(*error);
  TRACE("Received error response from server: $0", s.ToString());

  if (rpc_error) {
    rpc_error->swap(error);
  }
  return s;
}

Status ClientNegotiation::SendConnectionHeader() {
  const uint8_t buflen = kMagicNumberLength + kHeaderFlagsLength;
  uint8_t buf[buflen];
  serialization::SerializeConnHeader(buf);
  size_t nsent;
  return socket()->BlockingWrite(buf, buflen, &nsent, deadline_);
}

Status ClientNegotiation::InitSaslClient() {
  RETURN_NOT_OK(SaslInit());

  // TODO(KUDU-1922): consider setting SASL_SUCCESS_DATA
  unsigned flags = 0;

  sasl_conn_t* sasl_conn = nullptr;
  RETURN_NOT_OK_PREPEND(WrapSaslCall(nullptr /* no conn */, [&]() {
      return sasl_client_new(
          kSaslProtoName,               // Registered name of the service using SASL. Required.
          helper_.server_fqdn(),        // The fully qualified domain name of the remote server.
          nullptr,                      // Local and remote IP address strings. (we don't use
          nullptr,                      // any mechanisms which require this info.)
          &callbacks_[0],               // Connection-specific callbacks.
          flags,
          &sasl_conn);
    }), "Unable to create new SASL client");
  sasl_conn_.reset(sasl_conn);
  return Status::OK();
}

Status ClientNegotiation::SendNegotiate() {
  NegotiatePB msg;
  msg.set_step(NegotiatePB::NEGOTIATE);

  // Advertise our supported features.
  client_features_ = kSupportedClientRpcFeatureFlags;

  if (encryption_ != RpcEncryption::DISABLED) {
    client_features_.insert(TLS);
    // If the remote peer is local, then we allow using TLS for authentication
    // without encryption or integrity.
    if (socket_->IsLoopbackConnection() && !FLAGS_rpc_encrypt_loopback_connections) {
      client_features_.insert(TLS_AUTHENTICATION_ONLY);
    }
  }

  for (RpcFeatureFlag feature : client_features_) {
    msg.add_supported_features(feature);
  }

  if (!helper_.EnabledMechs().empty()) {
    msg.add_authn_types()->mutable_sasl();
  }
  if (tls_context_->has_signed_cert() && !tls_context_->is_external_cert()) {
    // We only provide authenticated TLS if the certificates are generated
    // by the internal CA.
    msg.add_authn_types()->mutable_certificate();
  }
  if (authn_token_ && tls_context_->has_trusted_cert()) {
    // TODO(KUDU-1924): check that the authn token is not expired. Can this be done
    // reliably on clients?
    msg.add_authn_types()->mutable_token();
  }

  if (PREDICT_FALSE(msg.authn_types().empty())) {
    return Status::NotAuthorized("client is not configured with an authentication type");
  }

  RETURN_NOT_OK(SendNegotiatePB(msg));
  return Status::OK();
}

Status ClientNegotiation::HandleNegotiate(const NegotiatePB& response) {
  if (PREDICT_FALSE(response.step() != NegotiatePB::NEGOTIATE)) {
    return Status::NotAuthorized("expected NEGOTIATE step",
                                 NegotiatePB::NegotiateStep_Name(response.step()));
  }
  TRACE("Received NEGOTIATE response from server");

  // Fill in the set of features supported by the server.
  for (int flag : response.supported_features()) {
    // We only add the features that our local build knows about.
    RpcFeatureFlag feature_flag = RpcFeatureFlag_IsValid(flag) ?
                                  static_cast<RpcFeatureFlag>(flag) : UNKNOWN;
    if (feature_flag != UNKNOWN) {
      server_features_.insert(feature_flag);
    }
  }

  if (encryption_ == RpcEncryption::REQUIRED &&
      !ContainsKey(server_features_, RpcFeatureFlag::TLS)) {
    return Status::NotAuthorized("server does not support required TLS encryption");
  }

  // Get the authentication type which the server would like to use.
  DCHECK_LE(response.authn_types().size(), 1);
  if (response.authn_types().empty()) {
    // If the server doesn't send back an authentication type, default to SASL
    // in order to maintain backwards compatibility.
    negotiated_authn_ = AuthenticationType::SASL;
  } else {
    const auto& authn_type = response.authn_types(0);
    switch (authn_type.type_case()) {
      case AuthenticationTypePB::kSasl:
        negotiated_authn_ = AuthenticationType::SASL;
        break;
      case AuthenticationTypePB::kToken:
        // TODO(todd): we should also be checking tls_context_->has_trusted_cert()
        // here to match the original logic we used to advertise TOKEN support,
        // or perhaps just check explicitly whether we advertised TOKEN.
        if (!authn_token_) {
          return Status::RuntimeError(
              "server chose token authentication, but client has no token");
        }
        negotiated_authn_ = AuthenticationType::TOKEN;
        return Status::OK();
      case AuthenticationTypePB::kCertificate:
        if (!tls_context_->has_signed_cert()) {
          return Status::RuntimeError(
              "server chose certificate authentication, but client has no certificate");
        }
        negotiated_authn_ = AuthenticationType::CERTIFICATE;
        return Status::OK();
      case AuthenticationTypePB::TYPE_NOT_SET:
        return Status::RuntimeError("server chose an unknown authentication type");
    }
  }

  DCHECK_EQ(negotiated_authn_, AuthenticationType::SASL);

  // Build a map of the SASL mechanisms offered by the server.
  const set<SaslMechanism::Type>& client_mechs = helper_.EnabledMechs();
  set<SaslMechanism::Type> server_mechs;
  for (const NegotiatePB::SaslMechanism& sasl_mech : response.sasl_mechanisms()) {
    auto mech = SaslMechanism::value_of(sasl_mech.mechanism());
    if (mech == SaslMechanism::INVALID) {
      continue;
    }
    server_mechs.insert(mech);
  }

  // Determine which SASL mechanism to use for authenticating the connection.
  // We pick the most preferred mechanism which is supported by both parties.
  // The preference list in order of most to least preferred:
  //  * GSSAPI
  //  * PLAIN
  set<SaslMechanism::Type> common_mechs = STLSetIntersection(client_mechs, server_mechs);

  if (common_mechs.empty()) {
    if (ContainsKey(server_mechs, SaslMechanism::GSSAPI) &&
        !ContainsKey(client_mechs, SaslMechanism::GSSAPI)) {
      return Status::NotAuthorized("server requires authentication, "
                                   "but client does not have Kerberos enabled");
    }
    if (!ContainsKey(server_mechs, SaslMechanism::GSSAPI) &&
        ContainsKey(client_mechs, SaslMechanism::GSSAPI)) {
      return Status::NotAuthorized("client requires authentication, "
                                   "but server does not have Kerberos enabled");
    }
    string msg = Substitute("client/server supported SASL mechanism mismatch; "
                            "client mechanisms: [$0], server mechanisms: [$1]",
                            JoinMapped(client_mechs, SaslMechanism::name_of, ", "),
                            JoinMapped(server_mechs, SaslMechanism::name_of, ", "));

    // For now, there should never be a SASL mechanism mismatch that isn't due
    // to one of the sides requiring Kerberos and the other not having it, so
    // lets sanity check that.
    DLOG(FATAL) << msg;
    return Status::NotAuthorized(msg);
  }

  // TODO(KUDU-1921): allow the client to require authentication.
  if (ContainsKey(common_mechs, SaslMechanism::GSSAPI)) {
    negotiated_mech_ = SaslMechanism::GSSAPI;
  } else {
    DCHECK(ContainsKey(common_mechs, SaslMechanism::PLAIN));
    negotiated_mech_ = SaslMechanism::PLAIN;
  }

  return Status::OK();
}

Status ClientNegotiation::SendTlsHandshake(string tls_token) {
  TRACE("Sending TLS_HANDSHAKE message to server");
  NegotiatePB msg;
  msg.set_step(NegotiatePB::TLS_HANDSHAKE);
  msg.mutable_tls_handshake()->swap(tls_token);
  return SendNegotiatePB(msg);
}

Status ClientNegotiation::HandleTlsHandshake(const NegotiatePB& response) {
  if (PREDICT_FALSE(response.step() != NegotiatePB::TLS_HANDSHAKE)) {
    return Status::NotAuthorized("expected TLS_HANDSHAKE step",
                                 NegotiatePB::NegotiateStep_Name(response.step()));
  }
  TRACE("Received TLS_HANDSHAKE response from server");

  if (PREDICT_FALSE(!response.has_tls_handshake())) {
    return Status::NotAuthorized("No TLS handshake token in TLS_HANDSHAKE response from server");
  }

  string token;
  Status s = tls_handshake_.Continue(response.tls_handshake(), &token);
  if (s.IsIncomplete()) {
    // Another roundtrip is required to complete the handshake.
    RETURN_NOT_OK(SendTlsHandshake(std::move(token)));
  }

  // Check that the handshake step didn't produce an error. Will also propagate
  // an Incomplete status.
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

Status ClientNegotiation::AuthenticateBySasl(faststring* recv_buf,
                                             unique_ptr<ErrorStatusPB>* rpc_error) {
  RETURN_NOT_OK(InitSaslClient());
  Status s = SendSaslInitiate();

  // HandleSasl[Initiate, Challenge] return incomplete if an additional
  // challenge step is required, or OK if a SASL_SUCCESS message is expected.
  while (s.IsIncomplete()) {
    NegotiatePB challenge;
    RETURN_NOT_OK(RecvNegotiatePB(&challenge, recv_buf, rpc_error));
    s = HandleSaslChallenge(challenge);
  }

  // Propagate failure from SendSaslInitiate or HandleSaslChallenge.
  RETURN_NOT_OK(s);

  // Server challenges are over; we now expect the success message.
  NegotiatePB success;
  RETURN_NOT_OK(RecvNegotiatePB(&success, recv_buf, rpc_error));
  return HandleSaslSuccess(success);
}

Status ClientNegotiation::AuthenticateByToken(faststring* recv_buf,
                                              unique_ptr<ErrorStatusPB>* rpc_error) {
  // Sanity check that TLS has been negotiated. Sending the token on an
  // unencrypted channel is a big no-no.
  CHECK(tls_negotiated_);

  // Send the token to the server.
  NegotiatePB pb;
  pb.set_step(NegotiatePB::TOKEN_EXCHANGE);
  *pb.mutable_authn_token() = std::move(*authn_token_);
  RETURN_NOT_OK(SendNegotiatePB(pb));
  pb.Clear();

  // Check that the server responds with a non-error TOKEN_EXCHANGE message.
  RETURN_NOT_OK(RecvNegotiatePB(&pb, recv_buf, rpc_error));
  if (pb.step() != NegotiatePB::TOKEN_EXCHANGE) {
    return Status::NotAuthorized("expected TOKEN_EXCHANGE step",
                                 NegotiatePB::NegotiateStep_Name(pb.step()));
  }

  return Status::OK();
}

Status ClientNegotiation::SendSaslInitiate() {
  TRACE("Initiating SASL $0 handshake", SaslMechanism::name_of(negotiated_mech_));

  // At this point we've already chosen the SASL mechanism to use
  // (negotiated_mech_), but we need to let the SASL library know. SASL likes to
  // choose the mechanism from among a list of possible options, so we simply
  // provide it one option, and then check that it picks that option.

  const char* init_msg = nullptr;
  unsigned init_msg_len = 0;
  const char* negotiated_mech = nullptr;

  /* select a mechanism for a connection
   *  mechlist      -- mechanisms server has available (punctuation ignored)
   * output:
   *  prompt_need   -- on SASL_INTERACT, list of prompts needed to continue
   *  clientout     -- the initial client response to send to the server
   *  mech          -- set to mechanism name
   *
   * Returns:
   *  SASL_OK       -- success
   *  SASL_CONTINUE -- negotiation required
   *  SASL_NOMEM    -- not enough memory
   *  SASL_NOMECH   -- no mechanism meets requested properties
   *  SASL_INTERACT -- user interaction needed to fill in prompt_need list
   */
  TRACE("Calling sasl_client_start()");
  const Status s = WrapSaslCall(sasl_conn_.get(), [&]() {
      return sasl_client_start(
          sasl_conn_.get(),                         // The SASL connection context created by init()
          SaslMechanism::name_of(negotiated_mech_), // The list of mechanisms to negotiate.
          nullptr,                                  // Disables INTERACT return if NULL.
          &init_msg,                                // Filled in on success.
          &init_msg_len,                            // Filled in on success.
          &negotiated_mech);                        // Filled in on success.
  });

  if (PREDICT_FALSE(!s.IsIncomplete() && !s.ok())) {
    return s;
  }

  // Check that the SASL library is using the mechanism that we picked.
  DCHECK_EQ(SaslMechanism::value_of(negotiated_mech), negotiated_mech_);

  // If the negotiated mechanism is GSSAPI (Kerberos), configure SASL to use
  // integrity protection so that the channel bindings and nonce can be
  // verified.
  if (negotiated_mech_ == SaslMechanism::GSSAPI) {
    RETURN_NOT_OK(EnableIntegrityProtection(sasl_conn_.get()));
  }

  NegotiatePB msg;
  msg.set_step(NegotiatePB::SASL_INITIATE);
  msg.mutable_token()->assign(init_msg, init_msg_len);
  msg.add_sasl_mechanisms()->set_mechanism(negotiated_mech);
  RETURN_NOT_OK(SendNegotiatePB(msg));
  return s;
}

Status ClientNegotiation::SendSaslResponse(const char* resp_msg, unsigned resp_msg_len) {
  NegotiatePB reply;
  reply.set_step(NegotiatePB::SASL_RESPONSE);
  reply.mutable_token()->assign(resp_msg, resp_msg_len);
  return SendNegotiatePB(reply);
}

Status ClientNegotiation::HandleSaslChallenge(const NegotiatePB& response) {
  if (PREDICT_FALSE(response.step() != NegotiatePB::SASL_CHALLENGE)) {
    return Status::NotAuthorized("expected SASL_CHALLENGE step",
                                 NegotiatePB::NegotiateStep_Name(response.step()));
  }
  TRACE("Received SASL_CHALLENGE response from server");
  if (PREDICT_FALSE(!response.has_token())) {
    return Status::NotAuthorized("no token in SASL_CHALLENGE response from server");
  }

  const char* out = nullptr;
  unsigned out_len = 0;
  const Status s = DoSaslStep(response.token(), &out, &out_len);
  if (PREDICT_FALSE(!s.IsIncomplete() && !s.ok())) {
    return s;
  }

  RETURN_NOT_OK(SendSaslResponse(out, out_len));
  return s;
}

Status ClientNegotiation::HandleSaslSuccess(const NegotiatePB& response) {
  if (PREDICT_FALSE(response.step() != NegotiatePB::SASL_SUCCESS)) {
    return Status::NotAuthorized("expected SASL_SUCCESS step",
                                 NegotiatePB::NegotiateStep_Name(response.step()));
  }
  TRACE("Received SASL_SUCCESS response from server");

  if (negotiated_mech_ == SaslMechanism::GSSAPI) {
    if (response.has_nonce()) {
      // Grab the nonce from the server, if it has sent one. We'll send it back
      // later with SASL integrity protection as part of the connection context.
      nonce_ = response.nonce();
    }

    if (tls_negotiated_) {
      // Check the channel bindings provided by the server against the expected channel bindings.
      if (!response.has_channel_bindings()) {
        return Status::NotAuthorized("no channel bindings provided by server");
      }

      security::Cert cert;
      RETURN_NOT_OK(tls_handshake_.GetRemoteCert(&cert));

      string expected_channel_bindings;
      RETURN_NOT_OK_PREPEND(cert.GetServerEndPointChannelBindings(&expected_channel_bindings),
                            "failed to generate channel bindings");

      string received_channel_bindings;
      RETURN_NOT_OK_PREPEND(SaslDecode(sasl_conn_.get(),
                                       response.channel_bindings(),
                                       &received_channel_bindings),
                            "failed to decode channel bindings");

      if (expected_channel_bindings != received_channel_bindings) {
        Sockaddr addr;
        ignore_result(socket_->GetPeerAddress(&addr));

        LOG(WARNING) << "Received invalid channel bindings from server "
                    << addr.ToString()
                    << ", this could indicate an active network man-in-the-middle";
        return Status::NotAuthorized("channel bindings do not match");
      }
    }
  }

  return Status::OK();
}

Status ClientNegotiation::DoSaslStep(const string& in, const char** out, unsigned* out_len) {
  TRACE("Calling sasl_client_step()");

  return WrapSaslCall(sasl_conn_.get(), [&]() {
      return sasl_client_step(sasl_conn_.get(), in.c_str(), in.length(), nullptr, out, out_len);
  });
}

Status ClientNegotiation::SendConnectionContext() {
  TRACE("Sending connection context");
  RequestHeader header;
  header.set_call_id(kConnectionContextCallId);

  ConnectionContextPB conn_context;
  // This field is deprecated but used by servers <Kudu 1.1. Newer server versions ignore
  // this and use the SASL-provided username instead.
  conn_context.mutable_deprecated_user_info()->set_real_user(
      plain_auth_user_.empty() ? "cpp-client" : plain_auth_user_);

  if (nonce_) {
    // Reply with the SASL-protected nonce. We only set the nonce when using SASL GSSAPI.
    RETURN_NOT_OK(SaslEncode(sasl_conn_.get(), *nonce_, conn_context.mutable_encoded_nonce()));
  }

  return SendFramedMessageBlocking(socket(), header, conn_context, deadline_);
}

int ClientNegotiation::GetOptionCb(const char* plugin_name, const char* option,
                            const char** result, unsigned* len) {
  return helper_.GetOptionCb(plugin_name, option, result, len);
}

// Used for PLAIN.
// SASL callback for SASL_CB_USER, SASL_CB_AUTHNAME, SASL_CB_LANGUAGE
int ClientNegotiation::SimpleCb(int id, const char** result, unsigned* len) {
  if (PREDICT_FALSE(!helper_.IsPlainEnabled())) {
    LOG(DFATAL) << "Simple callback called, but PLAIN auth is not enabled";
    return SASL_FAIL;
  }
  if (PREDICT_FALSE(result == nullptr)) {
    LOG(DFATAL) << "result outparam is NULL";
    return SASL_BADPARAM;
  }
  switch (id) {
    // TODO(unknown): Support impersonation?
    // For impersonation, USER is the impersonated user, AUTHNAME is the "sudoer".
    case SASL_CB_USER:
      TRACE("callback for SASL_CB_USER");
      *result = plain_auth_user_.c_str();
      if (len != nullptr) *len = plain_auth_user_.length();
      break;
    case SASL_CB_AUTHNAME:
      TRACE("callback for SASL_CB_AUTHNAME");
      *result = plain_auth_user_.c_str();
      if (len != nullptr) *len = plain_auth_user_.length();
      break;
    case SASL_CB_LANGUAGE:
      LOG(DFATAL) << "Unable to handle SASL callback type SASL_CB_LANGUAGE"
        << "(" << id << ")";
      return SASL_BADPARAM;
    default:
      LOG(DFATAL) << "Unexpected SASL callback type: " << id;
      return SASL_BADPARAM;
  }

  return SASL_OK;
}

// Used for PLAIN.
// SASL callback for SASL_CB_PASS: User password.
int ClientNegotiation::SecretCb(sasl_conn_t* conn, int id, sasl_secret_t** psecret) {
  if (PREDICT_FALSE(!helper_.IsPlainEnabled())) {
    LOG(DFATAL) << "Plain secret callback called, but PLAIN auth is not enabled";
    return SASL_FAIL;
  }
  switch (id) {
    case SASL_CB_PASS: {
      if (!conn || !psecret) return SASL_BADPARAM;

      size_t len = plain_pass_.length();
      *psecret = reinterpret_cast<sasl_secret_t*>(malloc(sizeof(sasl_secret_t) + len));
      if (!*psecret) {
        return SASL_NOMEM;
      }
      psecret_.reset(*psecret);  // Ensure that we free() this structure later.
      (*psecret)->len = len;
      memcpy((*psecret)->data, plain_pass_.c_str(), len + 1);
      break;
    }
    default:
      LOG(DFATAL) << "Unexpected SASL callback type: " << id;
      return SASL_BADPARAM;
  }

  return SASL_OK;
}

} // namespace rpc
} // namespace kudu
