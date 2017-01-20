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

#include <string.h>

#include <map>
#include <set>
#include <string>

#include <glog/logging.h>
#include <sasl/sasl.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/rpc/blocking_ops.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/sasl_helper.h"
#include "kudu/rpc/serialization.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace rpc {

using std::map;
using std::set;
using std::string;

static int SaslClientGetoptCb(void* sasl_client, const char* plugin_name, const char* option,
                       const char** result, unsigned* len) {
  return static_cast<SaslClient*>(sasl_client)
    ->GetOptionCb(plugin_name, option, result, len);
}

static int SaslClientSimpleCb(void *sasl_client, int id,
                       const char **result, unsigned *len) {
  return static_cast<SaslClient*>(sasl_client)->SimpleCb(id, result, len);
}

static int SaslClientSecretCb(sasl_conn_t* conn, void *sasl_client, int id,
                       sasl_secret_t** psecret) {
  return static_cast<SaslClient*>(sasl_client)->SecretCb(conn, id, psecret);
}

// Return an appropriately-typed Status object based on an ErrorStatusPB returned
// from an Error RPC.
// In case there is no relevant Status type, return a RuntimeError.
static Status StatusFromRpcError(const ErrorStatusPB& error) {
  DCHECK(error.IsInitialized()) << "Error status PB must be initialized";
  if (PREDICT_FALSE(!error.has_code())) {
    return Status::RuntimeError(error.message());
  }
  string code_name = ErrorStatusPB::RpcErrorCodePB_Name(error.code());
  switch (error.code()) {
    case ErrorStatusPB_RpcErrorCodePB_FATAL_UNAUTHORIZED:
      return Status::NotAuthorized(code_name, error.message());
    default:
      return Status::RuntimeError(code_name, error.message());
  }
}

SaslClient::SaslClient(string app_name, Socket* socket)
    : app_name_(std::move(app_name)),
      sock_(socket),
      helper_(SaslHelper::CLIENT),
      client_state_(SaslNegotiationState::NEW),
      negotiated_mech_(SaslMechanism::INVALID),
      deadline_(MonoTime::Max()) {
  callbacks_.push_back(SaslBuildCallback(SASL_CB_GETOPT,
      reinterpret_cast<int (*)()>(&SaslClientGetoptCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_AUTHNAME,
      reinterpret_cast<int (*)()>(&SaslClientSimpleCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_PASS,
      reinterpret_cast<int (*)()>(&SaslClientSecretCb), this));
  callbacks_.push_back(SaslBuildCallback(SASL_CB_LIST_END, nullptr, nullptr));
}

Status SaslClient::EnablePlain(const string& user, const string& pass) {
  DCHECK_EQ(client_state_, SaslNegotiationState::NEW);
  RETURN_NOT_OK(helper_.EnablePlain());
  plain_auth_user_ = user;
  plain_pass_ = pass;
  return Status::OK();
}

Status SaslClient::EnableGSSAPI() {
  DCHECK_EQ(client_state_, SaslNegotiationState::NEW);
  return helper_.EnableGSSAPI();
}

SaslMechanism::Type SaslClient::negotiated_mechanism() const {
  DCHECK_EQ(client_state_, SaslNegotiationState::NEGOTIATED);
  return negotiated_mech_;
}

void SaslClient::set_local_addr(const Sockaddr& addr) {
  DCHECK_EQ(client_state_, SaslNegotiationState::NEW);
  helper_.set_local_addr(addr);
}

void SaslClient::set_remote_addr(const Sockaddr& addr) {
  DCHECK_EQ(client_state_, SaslNegotiationState::NEW);
  helper_.set_remote_addr(addr);
}

void SaslClient::set_server_fqdn(const string& domain_name) {
  DCHECK_EQ(client_state_, SaslNegotiationState::NEW);
  helper_.set_server_fqdn(domain_name);
}

void SaslClient::set_deadline(const MonoTime& deadline) {
  DCHECK_NE(client_state_, SaslNegotiationState::NEGOTIATED);
  deadline_ = deadline;
}

// calls sasl_client_init() and sasl_client_new()
Status SaslClient::Init(const string& service_type) {
  RETURN_NOT_OK(SaslInit(app_name_.c_str()));

  // Ensure we are not called more than once.
  if (client_state_ != SaslNegotiationState::NEW) {
    return Status::IllegalState("Init() may only be called once per SaslClient object.");
  }

  // TODO(unknown): Support security flags.
  unsigned secflags = 0;

  sasl_conn_t* sasl_conn = nullptr;
  Status s = WrapSaslCall(nullptr /* no conn */, [&]() {
      return sasl_client_new(
          service_type.c_str(),         // Registered name of the service using SASL. Required.
          helper_.server_fqdn(),        // The fully qualified domain name of the remote server.
          helper_.local_addr_string(),  // Local and remote IP address strings. (NULL disables
          helper_.remote_addr_string(), //   mechanisms which require this info.)
          &callbacks_[0],               // Connection-specific callbacks.
          secflags,                     // Security flags.
          &sasl_conn);
    });
  if (!s.ok()) {
    return Status::RuntimeError("Unable to create new SASL client",
                                s.message());
  }
  sasl_conn_.reset(sasl_conn);

  client_state_ = SaslNegotiationState::INITIALIZED;
  return Status::OK();
}

Status SaslClient::Negotiate() {
  // After negotiation, we no longer need the SASL library object, so
  // may as well free its memory since the connection may be long-lived.
  // Additionally, this works around a SEGV seen at process shutdown time:
  // if we still have SASL objects retained by Reactor when the process
  // is exiting, the SASL libraries may start destructing global state
  // and cause a crash when we sasl_dispose the connection.
  auto cleanup = MakeScopedCleanup([&]() {
      sasl_conn_.reset();
    });
  TRACE("Called SaslClient::Negotiate()");

  // Ensure we called exactly once, and in the right order.
  if (client_state_ == SaslNegotiationState::NEW) {
    return Status::IllegalState("SaslClient: Init() must be called before calling Negotiate()");
  }
  if (client_state_ == SaslNegotiationState::NEGOTIATED) {
    return Status::IllegalState("SaslClient: Negotiate() may only be called once per object.");
  }

  // Ensure we can use blocking calls on the socket during negotiation.
  RETURN_NOT_OK(EnsureBlockingMode(sock_));

  // Start by asking the server for a list of available auth mechanisms.
  RETURN_NOT_OK(SendNegotiateMessage());

  faststring recv_buf;
  nego_ok_ = false;

  // We set nego_ok_ = true when the SASL library returns SASL_OK to us.
  // We set nego_response_expected_ = true each time we send a request to the server.
  while (!nego_ok_ || nego_response_expected_) {
    ResponseHeader header;
    Slice param_buf;
    RETURN_NOT_OK(ReceiveFramedMessageBlocking(sock_, &recv_buf, &header, &param_buf, deadline_));
    nego_response_expected_ = false;

    NegotiatePB response;
    RETURN_NOT_OK(ParseNegotiatePB(header, param_buf, &response));

    switch (response.step()) {
      // NEGOTIATE: Server has sent us its list of supported SASL mechanisms.
      case NegotiatePB::NEGOTIATE:
        RETURN_NOT_OK(HandleNegotiateResponse(response));
        break;

      // SASL_CHALLENGE: Server sent us a follow-up to an SASL_INITIATE or SASL_RESPONSE request.
      case NegotiatePB::SASL_CHALLENGE:
        RETURN_NOT_OK(HandleChallengeResponse(response));
        break;

      // SASL_SUCCESS: Server has accepted our authentication request. Negotiation successful.
      case NegotiatePB::SASL_SUCCESS:
        RETURN_NOT_OK(HandleSuccessResponse(response));
        break;

      // Client sent us some unsupported SASL response.
      default:
        LOG(ERROR) << "SASL Client: Received unsupported response from server";
        return Status::InvalidArgument("RPC client doesn't support Negotiate step",
                                       NegotiatePB::NegotiateStep_Name(response.step()));
    }
  }

  TRACE("SASL Client: Successful negotiation");
  client_state_ = SaslNegotiationState::NEGOTIATED;
  return Status::OK();
}

Status SaslClient::SendNegotiatePB(const NegotiatePB& msg) {
  DCHECK_NE(client_state_, SaslNegotiationState::NEW)
      << "Must not send Negotiate messages before calling Init()";
  DCHECK_NE(client_state_, SaslNegotiationState::NEGOTIATED)
      << "Must not send Negotiate messages after negotiation succeeds";

  // Create header with SASL-specific callId
  RequestHeader header;
  header.set_call_id(kNegotiateCallId);
  return helper_.SendNegotiatePB(sock_, header, msg, deadline_);
}

Status SaslClient::ParseNegotiatePB(const ResponseHeader& header,
                                    const Slice& param_buf,
                                    NegotiatePB* response) {
  RETURN_NOT_OK(helper_.SanityCheckNegotiateCallId(header.call_id()));

  if (header.is_error()) {
    return ParseError(param_buf);
  }

  return helper_.ParseNegotiatePB(param_buf, response);
}

Status SaslClient::SendNegotiateMessage() {
  NegotiatePB msg;
  msg.set_step(NegotiatePB::NEGOTIATE);

  // Advertise our supported features.
  for (RpcFeatureFlag feature : kSupportedClientRpcFeatureFlags) {
    msg.add_supported_features(feature);
  }

  TRACE("SASL Client: Sending NEGOTIATE request to server.");
  RETURN_NOT_OK(SendNegotiatePB(msg));
  nego_response_expected_ = true;
  return Status::OK();
}

Status SaslClient::SendInitiateMessage(const NegotiatePB_SaslAuth& auth,
    const char* init_msg, unsigned init_msg_len) {
  NegotiatePB msg;
  msg.set_step(NegotiatePB::SASL_INITIATE);
  msg.mutable_token()->assign(init_msg, init_msg_len);
  msg.add_auths()->CopyFrom(auth);
  TRACE("SASL Client: Sending SASL_INITIATE request to server.");
  RETURN_NOT_OK(SendNegotiatePB(msg));
  nego_response_expected_ = true;
  return Status::OK();
}

Status SaslClient::SendResponseMessage(const char* resp_msg, unsigned resp_msg_len) {
  NegotiatePB reply;
  reply.set_step(NegotiatePB::SASL_RESPONSE);
  reply.mutable_token()->assign(resp_msg, resp_msg_len);
  TRACE("SASL Client: Sending SASL_RESPONSE request to server.");
  RETURN_NOT_OK(SendNegotiatePB(reply));
  nego_response_expected_ = true;
  return Status::OK();
}

Status SaslClient::DoSaslStep(const string& in, const char** out, unsigned* out_len) {
  TRACE("SASL Client: Calling sasl_client_step()");
  Status s = WrapSaslCall(sasl_conn_.get(), [&]() {
      return sasl_client_step(sasl_conn_.get(), in.c_str(), in.length(), nullptr, out, out_len);
    });
  if (s.ok()) {
    nego_ok_ = true;
  }
  return s;
}

Status SaslClient::HandleNegotiateResponse(const NegotiatePB& response) {
  TRACE("SASL Client: Received NEGOTIATE response from server");
  // Fill in the set of features supported by the server.
  for (int flag : response.supported_features()) {
    // We only add the features that our local build knows about.
    RpcFeatureFlag feature_flag = RpcFeatureFlag_IsValid(flag) ?
                                  static_cast<RpcFeatureFlag>(flag) : UNKNOWN;
    if (ContainsKey(kSupportedClientRpcFeatureFlags, feature_flag)) {
      server_features_.insert(feature_flag);
    }
  }

  // Build a map of the mechanisms offered by the server.
  const set<string>& local_mechs = helper_.LocalMechs();
  set<string> server_mechs;
  map<string, NegotiatePB::SaslAuth> server_mech_map;
  for (const NegotiatePB::SaslAuth& auth : response.auths()) {
    const auto& mech = auth.mechanism();
    server_mech_map[mech] = auth;
    server_mechs.insert(mech);
  }
  // Determine which server mechs are also enabled by the client.
  // Cyrus SASL 2.1.25 and later supports doing this set intersection via
  // the 'client_mech_list' option, but that version is not available on
  // RHEL 6, so we have to do it manually.
  set<string> matching_mechs = STLSetIntersection(local_mechs, server_mechs);

  if (matching_mechs.empty() &&
      ContainsKey(server_mechs, kSaslMechGSSAPI) &&
      !ContainsKey(local_mechs, kSaslMechGSSAPI)) {
    return Status::NotAuthorized("server requires GSSAPI (Kerberos) authentication and "
                                 "client was missing the required SASL module");
  }

  string matching_mechs_str = JoinElements(matching_mechs, " ");
  TRACE("SASL Client: Matching mech list: $0", matching_mechs_str);

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
  TRACE("SASL Client: Calling sasl_client_start()");
  Status s = WrapSaslCall(sasl_conn_.get(), [&]() {
      return sasl_client_start(
          sasl_conn_.get(),           // The SASL connection context created by init()
          matching_mechs_str.c_str(), // The list of mechanisms to negotiate.
          nullptr,                    // Disables INTERACT return if NULL.
          &init_msg,                  // Filled in on success.
          &init_msg_len,              // Filled in on success.
          &negotiated_mech);          // Filled in on success.
    });

  if (s.ok()) {
    nego_ok_ = true;
  } else if (!s.IsIncomplete()) {
    return s;
  }

  // The server matched one of our mechanisms.
  NegotiatePB::SaslAuth* auth = FindOrNull(server_mech_map, negotiated_mech);
  if (PREDICT_FALSE(auth == nullptr)) {
    return Status::IllegalState("Unable to find auth in map, unexpected error", negotiated_mech);
  }
  negotiated_mech_ = SaslMechanism::value_of(negotiated_mech);

  RETURN_NOT_OK(SendInitiateMessage(*auth, init_msg, init_msg_len));
  return Status::OK();
}

Status SaslClient::HandleChallengeResponse(const NegotiatePB& response) {
  TRACE("SASL Client: Received SASL_CHALLENGE response from server");
  if (PREDICT_FALSE(nego_ok_)) {
    LOG(DFATAL) << "Server sent SASL_CHALLENGE response after client library returned SASL_OK";
  }

  if (PREDICT_FALSE(!response.has_token())) {
    return Status::InvalidArgument("No token in SASL_CHALLENGE response from server");
  }

  const char* out = nullptr;
  unsigned out_len = 0;
  Status s = DoSaslStep(response.token(), &out, &out_len);
  if (!s.ok() && !s.IsIncomplete()) {
    return s;
  }
  RETURN_NOT_OK(SendResponseMessage(out, out_len));
  return Status::OK();
}

Status SaslClient::HandleSuccessResponse(const NegotiatePB& response) {
  TRACE("SASL Client: Received SASL_SUCCESS response from server");
  if (!nego_ok_) {
    const char* out = nullptr;
    unsigned out_len = 0;
    Status s = DoSaslStep(response.token(), &out, &out_len);
    if (s.IsIncomplete()) {
      return Status::IllegalState("Server indicated successful authentication, but client "
                                  "was not complete");
    }
    RETURN_NOT_OK(s);
    if (out_len > 0) {
      return Status::IllegalState("SASL client library generated spurious token after SASL_SUCCESS",
          string(out, out_len));
    }
    CHECK(nego_ok_);
  }
  return Status::OK();
}

// Parse error status message from raw bytes of an ErrorStatusPB.
Status SaslClient::ParseError(const Slice& err_data) {
  ErrorStatusPB error;
  if (!error.ParseFromArray(err_data.data(), err_data.size())) {
    return Status::IOError("Invalid error response, missing fields",
        error.InitializationErrorString());
  }
  Status s = StatusFromRpcError(error);
  TRACE("SASL Client: Received error response from server: $0", s.ToString());
  return s;
}

int SaslClient::GetOptionCb(const char* plugin_name, const char* option,
                            const char** result, unsigned* len) {
  return helper_.GetOptionCb(plugin_name, option, result, len);
}

// Used for PLAIN.
// SASL callback for SASL_CB_USER, SASL_CB_AUTHNAME, SASL_CB_LANGUAGE
int SaslClient::SimpleCb(int id, const char** result, unsigned* len) {
  if (PREDICT_FALSE(!helper_.IsPlainEnabled())) {
    LOG(DFATAL) << "SASL Client: Simple callback called, but PLAIN auth is not enabled";
    return SASL_FAIL;
  }
  if (PREDICT_FALSE(result == nullptr)) {
    LOG(DFATAL) << "SASL Client: result outparam is NULL";
    return SASL_BADPARAM;
  }
  switch (id) {
    // TODO(unknown): Support impersonation?
    // For impersonation, USER is the impersonated user, AUTHNAME is the "sudoer".
    case SASL_CB_USER:
      TRACE("SASL Client: callback for SASL_CB_USER");
      *result = plain_auth_user_.c_str();
      if (len != nullptr) *len = plain_auth_user_.length();
      break;
    case SASL_CB_AUTHNAME:
      TRACE("SASL Client: callback for SASL_CB_AUTHNAME");
      *result = plain_auth_user_.c_str();
      if (len != nullptr) *len = plain_auth_user_.length();
      break;
    case SASL_CB_LANGUAGE:
      LOG(DFATAL) << "SASL Client: Unable to handle SASL callback type SASL_CB_LANGUAGE"
        << "(" << id << ")";
      return SASL_BADPARAM;
    default:
      LOG(DFATAL) << "SASL Client: Unexpected SASL callback type: " << id;
      return SASL_BADPARAM;
  }

  return SASL_OK;
}

// Used for PLAIN.
// SASL callback for SASL_CB_PASS: User password.
int SaslClient::SecretCb(sasl_conn_t* conn, int id, sasl_secret_t** psecret) {
  if (PREDICT_FALSE(!helper_.IsPlainEnabled())) {
    LOG(DFATAL) << "SASL Client: Plain secret callback called, but PLAIN auth is not enabled";
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
      LOG(DFATAL) << "SASL Client: Unexpected SASL callback type: " << id;
      return SASL_BADPARAM;
  }

  return SASL_OK;
}

} // namespace rpc
} // namespace kudu
