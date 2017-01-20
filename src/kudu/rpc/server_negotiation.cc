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

#include <limits>
#include <memory>
#include <set>
#include <string>

#include <glog/logging.h>
#include <google/protobuf/message_lite.h>
#include <sasl/sasl.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/blocking_ops.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/serialization.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/trace.h"

using std::set;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace rpc {

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

ServerNegotiation::ServerNegotiation(unique_ptr<Socket> socket)
    : socket_(std::move(socket)),
      helper_(SaslHelper::SERVER),
      negotiated_mech_(SaslMechanism::INVALID),
      deadline_(MonoTime::Max()) {
  callbacks_.push_back(SaslBuildCallback(SASL_CB_GETOPT,
      reinterpret_cast<int (*)()>(&ServerNegotiationGetoptCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_SERVER_USERDB_CHECKPASS,
      reinterpret_cast<int (*)()>(&ServerNegotiationPlainAuthCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_LIST_END, nullptr, nullptr));
}

Status ServerNegotiation::EnablePlain() {
  RETURN_NOT_OK(helper_.EnablePlain());
  return Status::OK();
}

Status ServerNegotiation::EnableGSSAPI() {
  return helper_.EnableGSSAPI();
}

SaslMechanism::Type ServerNegotiation::negotiated_mechanism() const {
  return negotiated_mech_;
}

const string& ServerNegotiation::authenticated_user() const {
  return authenticated_user_;
}

void ServerNegotiation::set_local_addr(const Sockaddr& addr) {
  helper_.set_local_addr(addr);
}

void ServerNegotiation::set_remote_addr(const Sockaddr& addr) {
  helper_.set_remote_addr(addr);
}

void ServerNegotiation::set_server_fqdn(const string& domain_name) {
  helper_.set_server_fqdn(domain_name);
}

void ServerNegotiation::set_deadline(const MonoTime& deadline) {
  deadline_ = deadline;
}

Status ServerNegotiation::Negotiate() {
  TRACE("Beginning negotiation");

  // Ensure we can use blocking calls on the socket during negotiation.
  RETURN_NOT_OK(EnsureBlockingMode(socket_.get()));

  faststring recv_buf;

  // Step 1: Read the connection header.
  RETURN_NOT_OK(ValidateConnectionHeader(&recv_buf));

  { // Step 2: Receive and respond to the NEGOTIATE step message.
    NegotiatePB request;
    RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));
    RETURN_NOT_OK(HandleNegotiate(request));
  }

  // Step 3: SASL negotiation.
  RETURN_NOT_OK(InitSaslServer());
  {
    NegotiatePB request;
    RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));
    Status s = HandleSaslInitiate(request);

    while (s.IsIncomplete()) {
      RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));
      s = HandleSaslResponse(request);
    }
    RETURN_NOT_OK(s);
  }
  const char* username = nullptr;
  int rc = sasl_getprop(sasl_conn_.get(), SASL_USERNAME,
                        reinterpret_cast<const void**>(&username));
  // We expect that SASL_USERNAME will always get set.
  CHECK(rc == SASL_OK && username != nullptr) << "No username on authenticated connection";
  authenticated_user_ = username;

  // Step 4: Receive connection context.
  RETURN_NOT_OK(RecvConnectionContext(&recv_buf));

  TRACE("Negotiation successful");
  return Status::OK();
}

Status ServerNegotiation::PreflightCheckGSSAPI() {
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
  ServerNegotiation server(nullptr);
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

  TRACE("Sending RPC error: $0", ErrorStatusPB::RpcErrorCodePB_Name(code));
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
  RETURN_NOT_OK(SaslInit());

  // TODO(unknown): Support security flags.
  unsigned secflags = 0;

  sasl_conn_t* sasl_conn = nullptr;
  RETURN_NOT_OK_PREPEND(WrapSaslCall(nullptr /* no conn */, [&]() {
      return sasl_server_new(
          // Registered name of the service using SASL. Required.
          kSaslProtoName,
          // The fully qualified domain name of this server.
          helper_.server_fqdn(),
          // Permits multiple user realms on server. NULL == use default.
          nullptr,
          // Local and remote IP address strings. (NULL disables
          // mechanisms which require this info.)
          helper_.local_addr_string(),
          helper_.remote_addr_string(),
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
    return Status::NotAuthorized("expected NEGOTIATE step",
                                 NegotiatePB::NegotiateStep_Name(request.step()));
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

  set<string> server_mechs = helper_.EnabledMechs();
  if (PREDICT_FALSE(server_mechs.empty())) {
    // This will happen if no mechanisms are enabled before calling Init()
    Status s = Status::NotAuthorized("SASL server mechanism list is empty!");
    LOG(ERROR) << s.ToString();
    TRACE("Sending FATAL_UNAUTHORIZED response to client");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  RETURN_NOT_OK(SendNegotiate(server_mechs));
  return Status::OK();
}

Status ServerNegotiation::SendNegotiate(const set<string>& server_mechs) {
  NegotiatePB response;
  response.set_step(NegotiatePB::NEGOTIATE);

  for (const string& mech : server_mechs) {
    response.add_auths()->set_mechanism(mech);
  }

  // Tell the client which features we support.
  for (RpcFeatureFlag feature : kSupportedServerRpcFeatureFlags) {
    response.add_supported_features(feature);
  }

  RETURN_NOT_OK(SendNegotiatePB(response));
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

  if (request.auths_size() != 1) {
    Status s = Status::NotAuthorized(
        "SASL_INITIATE request must include exactly one SaslAuth section, found",
        std::to_string(request.auths_size()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  const NegotiatePB::SaslAuth& auth = request.auths(0);
  TRACE("Client requested to use mechanism: $0", auth.mechanism());

  const char* server_out = nullptr;
  uint32_t server_out_len = 0;
  TRACE("Calling sasl_server_start()");

  Status s = WrapSaslCall(sasl_conn_.get(), [&]() {
      return sasl_server_start(
          sasl_conn_.get(),         // The SASL connection context created by init()
          auth.mechanism().c_str(), // The mechanism requested by the client.
          request.token().c_str(),  // Optional string the client gave us.
          request.token().length(), // Client string len.
          &server_out,              // The output of the SASL library, might not be NULL terminated
          &server_out_len);         // Output len.
    });

  if (PREDICT_FALSE(!s.ok() && !s.IsIncomplete())) {
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  negotiated_mech_ = SaslMechanism::value_of(auth.mechanism());

  // We have a valid mechanism match
  if (s.ok()) {
    RETURN_NOT_OK(SendSaslSuccess(server_out, server_out_len));
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
    return SendSaslSuccess(server_out, server_out_len);
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

Status ServerNegotiation::SendSaslSuccess(const char* token, unsigned tlen) {
  NegotiatePB response;
  response.set_step(NegotiatePB::SASL_SUCCESS);
  if (PREDICT_FALSE(tlen > 0)) {
    response.mutable_token()->assign(token, tlen);
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

  // Currently none of the fields of the connection context are used.
  return Status::OK();
}

int ServerNegotiation::GetOptionCb(const char* plugin_name,
                                   const char* option,
                                   const char** result,
                                   unsigned* len) {
  return helper_.GetOptionCb(plugin_name, option, result, len);
}

int ServerNegotiation::PlainAuthCb(sasl_conn_t* /*conn*/,
                                   const char*  /*user*/,
                                   const char*  /*pass*/,
                                   unsigned /*passlen*/,
                                   struct propctx*  /*propctx*/) {
  TRACE("Received PLAIN auth.");
  if (PREDICT_FALSE(!helper_.IsPlainEnabled())) {
    LOG(DFATAL) << "Password authentication callback called while PLAIN auth disabled";
    return SASL_BADPARAM;
  }
  // We always allow PLAIN authentication to succeed.
  return SASL_OK;
}

} // namespace rpc
} // namespace kudu
