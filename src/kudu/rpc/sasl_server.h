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

#ifndef KUDU_RPC_SASL_SERVER_H
#define KUDU_RPC_SASL_SERVER_H

#include <set>
#include <string>
#include <vector>

#include <sasl/sasl.h>

#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/sasl_helper.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class Slice;

namespace rpc {

using std::string;

// Class for doing SASL negotiation with a SaslClient over a bidirectional socket.
// Operations on this class are NOT thread-safe.
class SaslServer {
 public:
  // Does not take ownership of 'socket'.
  SaslServer(string app_name, Socket* socket);

  // Enable PLAIN authentication.
  // Despite PLAIN authentication taking a username and password, we disregard
  // the password and use this as a "unauthenticated" mode.
  // Must be called after Init().
  Status EnablePlain();

  // Enable GSSAPI (Kerberos) authentication.
  // Call after Init().
  Status EnableGSSAPI();

  // Returns mechanism negotiated by this connection.
  // Must be called after Negotiate().
  SaslMechanism::Type negotiated_mechanism() const;

  // Returns the set of RPC system features supported by the remote client.
  // Must be called after Negotiate().
  const std::set<RpcFeatureFlag>& client_features() const {
    return client_features_;
  }

  // Name of the user that was authenticated.
  // Must be called after a successful Negotiate().
  const std::string& authenticated_user() const;

  // Specify IP:port of local side of connection.
  // Must be called before Init(). Required for some mechanisms.
  void set_local_addr(const Sockaddr& addr);

  // Specify IP:port of remote side of connection.
  // Must be called before Init(). Required for some mechanisms.
  void set_remote_addr(const Sockaddr& addr);

  // Specify the fully-qualified domain name of the remote server.
  // Must be called before Init(). Required for some mechanisms.
  void set_server_fqdn(const string& domain_name);

  // Set deadline for connection negotiation.
  void set_deadline(const MonoTime& deadline);

  // Get deadline for connection negotiation.
  const MonoTime& deadline() const { return deadline_; }

  // Initialize a new SASL server. Must be called before Negotiate().
  // Returns OK on success, otherwise RuntimeError.
  Status Init(const string& service_type);

  // Begin negotiation with the SASL client on the other side of the fd socket
  // that this server was constructed with.
  // Returns OK on success.
  // Otherwise, it may return NotAuthorized, NotSupported, or another non-OK status.
  Status Negotiate();

  // SASL callback for plugin options, supported mechanisms, etc.
  // Returns SASL_FAIL if the option is not handled, which does not fail the handshake.
  int GetOptionCb(const char* plugin_name, const char* option,
                  const char** result, unsigned* len);

  // SASL callback for PLAIN authentication via SASL_CB_SERVER_USERDB_CHECKPASS.
  int PlainAuthCb(sasl_conn_t* conn, const char* user, const char* pass,
                  unsigned passlen, struct propctx* propctx);

  // Perform a "pre-flight check" that everything required to act as a Kerberos
  // server is properly set up.
  static Status PreflightCheckGSSAPI(const std::string& app_name);

 private:
  // Parse and validate connection header.
  Status ValidateConnectionHeader(faststring* recv_buf);

  // Parse request body. If malformed, sends an error message to the client.
  Status ParseNegotiatePB(const RequestHeader& header,
                          const Slice& param_buf,
                          NegotiatePB* request);

  // Encode and send the specified SASL message to the client.
  Status SendNegotiatePB(const NegotiatePB& msg);

  // Encode and send the specified RPC error message to the client.
  // Calls Status.ToString() for the embedded error message.
  Status SendRpcError(ErrorStatusPB::RpcErrorCodePB code, const Status& err);

  // Handle case when client sends NEGOTIATE request.
  Status HandleNegotiateRequest(const NegotiatePB& request);

  // Send a NEGOTIATE response to the client with the list of available mechanisms.
  Status SendNegotiateResponse(const std::set<string>& server_mechs);

  // Handle case when client sends INITIATE request.
  Status HandleInitiateRequest(const NegotiatePB& request);

  // Send a CHALLENGE response to the client with a challenge token.
  Status SendChallengeResponse(const char* challenge, unsigned clen);

  // Send a SUCCESS response to the client with an token (typically empty).
  Status SendSuccessResponse(const char* token, unsigned tlen);

  // Handle case when client sends RESPONSE request.
  Status HandleResponseRequest(const NegotiatePB& request);

  string app_name_;
  Socket* sock_;
  std::vector<sasl_callback_t> callbacks_;
  // The SASL connection object. This is initialized in Init() and
  // freed after Negotiate() completes (regardless whether it was successful).
  gscoped_ptr<sasl_conn_t, SaslDeleter> sasl_conn_;
  SaslHelper helper_;

  // The set of features that the client supports. Filled in
  // after we receive the NEGOTIATE request from the client.
  std::set<RpcFeatureFlag> client_features_;

  // The successfully-authenticated user, if applicable.
  string authenticated_user_;

  SaslNegotiationState::Type server_state_;

  // The mechanism we negotiated with the client.
  SaslMechanism::Type negotiated_mech_;

  // Intra-negotiation state.
  bool nego_ok_;  // During negotiation: did we get a SASL_OK response from the SASL library?

  // Negotiation timeout deadline.
  MonoTime deadline_;

  DISALLOW_COPY_AND_ASSIGN(SaslServer);
};

} // namespace rpc
} // namespace kudu

#endif  // KUDU_RPC_SASL_SERVER_H
