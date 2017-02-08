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

#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <sasl/sasl.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/sasl_helper.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

namespace kudu {

class Slice;

namespace security {
class TlsContext;
}

namespace rpc {

// Class for doing KRPC negotiation with a remote client over a bidirectional socket.
// Operations on this class are NOT thread-safe.
class ServerNegotiation {
 public:
  // Creates a new server negotiation instance, taking ownership of the
  // provided socket. After completing the negotiation process by setting the
  // desired options and calling Negotiate((), the socket can be retrieved with
  // release_socket().
  //
  // The provided TlsContext must outlive this negotiation instance.
  explicit ServerNegotiation(std::unique_ptr<Socket> socket,
                             const security::TlsContext* tls_context);

  // Enable PLAIN authentication.
  // Despite PLAIN authentication taking a username and password, we disregard
  // the password and use this as a "unauthenticated" mode.
  // Must be called before Negotiate().
  Status EnablePlain();

  // Enable GSSAPI (Kerberos) authentication.
  // Must be called before Negotiate().
  Status EnableGSSAPI();

  // Returns mechanism negotiated by this connection.
  // Must be called after Negotiate().
  SaslMechanism::Type negotiated_mechanism() const;

  // Returns the set of RPC system features supported by the remote client.
  // Must be called after Negotiate().
  std::set<RpcFeatureFlag> client_features() const {
    return client_features_;
  }

  // Returns the set of RPC system features supported by the remote client.
  // Must be called after Negotiate().
  // Subsequent calls to this method or client_features() will return an empty set.
  std::set<RpcFeatureFlag> take_client_features() {
    return std::move(client_features_);
  }

  // Name of the user that was authenticated.
  // Must be called after a successful Negotiate().
  const std::string& authenticated_user() const;

  // Specify IP:port of local side of connection.
  // Must be called before Negotiate(). Required for some mechanisms.
  void set_local_addr(const Sockaddr& addr);

  // Specify IP:port of remote side of connection.
  // Must be called before Negotiate(). Required for some mechanisms.
  void set_remote_addr(const Sockaddr& addr);

  // Specify the fully-qualified domain name of the remote server.
  // Must be called before Negotiate(). Required for some mechanisms.
  void set_server_fqdn(const std::string& domain_name);

  // Set deadline for connection negotiation.
  void set_deadline(const MonoTime& deadline);

  Socket* socket() const { return socket_.get(); }

  // Returns the socket owned by this server negotiation. The caller will own
  // the socket after this call, and the negotiation instance should no longer
  // be used. Must be called after Negotiate().
  std::unique_ptr<Socket> release_socket() { return std::move(socket_); }

  // Negotiate with the remote client. Should only be called once per
  // ServerNegotiation and socket instance, after all options have been set.
  //
  // Returns OK on success, otherwise may return NotAuthorized, NotSupported, or
  // another non-OK status.
  Status Negotiate() WARN_UNUSED_RESULT;

  // SASL callback for plugin options, supported mechanisms, etc.
  // Returns SASL_FAIL if the option is not handled, which does not fail the handshake.
  int GetOptionCb(const char* plugin_name, const char* option,
                  const char** result, unsigned* len);

  // SASL callback for PLAIN authentication via SASL_CB_SERVER_USERDB_CHECKPASS.
  int PlainAuthCb(sasl_conn_t* conn, const char* user, const char* pass,
                  unsigned passlen, struct propctx* propctx);

  // Perform a "pre-flight check" that everything required to act as a Kerberos
  // server is properly set up.
  static Status PreflightCheckGSSAPI() WARN_UNUSED_RESULT;

 private:

  // Parse a negotiate request from the client, deserializing it into 'msg'.
  // If the request is malformed, sends an error message to the client.
  Status RecvNegotiatePB(NegotiatePB* msg, faststring* recv_buf) WARN_UNUSED_RESULT;

  // Encode and send the specified negotiate response message to the server.
  Status SendNegotiatePB(const NegotiatePB& msg) WARN_UNUSED_RESULT;

  // Encode and send the specified RPC error message to the client.
  // Calls Status.ToString() for the embedded error message.
  Status SendError(ErrorStatusPB::RpcErrorCodePB code, const Status& err) WARN_UNUSED_RESULT;

  // Parse and validate connection header.
  Status ValidateConnectionHeader(faststring* recv_buf) WARN_UNUSED_RESULT;

  // Initialize the SASL server negotiation instance.
  Status InitSaslServer() WARN_UNUSED_RESULT;

  // Handle case when client sends NEGOTIATE request.
  Status HandleNegotiate(const NegotiatePB& request) WARN_UNUSED_RESULT;

  // Send a NEGOTIATE response to the client with the list of available mechanisms.
  Status SendNegotiate(const std::set<SaslMechanism::Type>& server_mechs) WARN_UNUSED_RESULT;

  // Handle a TLS_HANDSHAKE request message from the server.
  Status HandleTlsHandshake(const NegotiatePB& request) WARN_UNUSED_RESULT;

  // Send a TLS_HANDSHAKE response message to the server with the provided token.
  Status SendTlsHandshake(std::string tls_token) WARN_UNUSED_RESULT;

  // Handle case when client sends SASL_INITIATE request.
  // Returns Status::OK if the SASL negotiation is complete, or
  // Status::Incomplete if a SASL_RESPONSE step is expected.
  Status HandleSaslInitiate(const NegotiatePB& request) WARN_UNUSED_RESULT;

  // Handle case when client sends SASL_RESPONSE request.
  Status HandleSaslResponse(const NegotiatePB& request) WARN_UNUSED_RESULT;

  // Send a SASL_CHALLENGE response to the client with a challenge token.
  Status SendSaslChallenge(const char* challenge, unsigned clen) WARN_UNUSED_RESULT;

  // Send a SASL_SUCCESS response to the client.
  Status SendSaslSuccess() WARN_UNUSED_RESULT;

  // Encode the provided data and append it to 'encoded'.
  Status SaslEncode(const std::string& plaintext, std::string* encoded) WARN_UNUSED_RESULT;

  // Receive and validate the ConnectionContextPB.
  Status RecvConnectionContext(faststring* recv_buf) WARN_UNUSED_RESULT;

  // The socket to the remote client.
  std::unique_ptr<Socket> socket_;

  // SASL state.
  std::vector<sasl_callback_t> callbacks_;
  gscoped_ptr<sasl_conn_t, SaslDeleter> sasl_conn_;
  SaslHelper helper_;

  // TLS state.
  const security::TlsContext* tls_context_;
  security::TlsHandshake tls_handshake_;
  bool tls_negotiated_;

  // The set of features supported by the client. Filled in during negotiation.
  std::set<RpcFeatureFlag> client_features_;

  // The successfully-authenticated user, if applicable. Filled in during
  // negotiation.
  std::string authenticated_user_;

  // The SASL mechanism. Filled in during negotiation.
  SaslMechanism::Type negotiated_mech_;

  // Negotiation timeout deadline.
  MonoTime deadline_;
};

} // namespace rpc
} // namespace kudu
