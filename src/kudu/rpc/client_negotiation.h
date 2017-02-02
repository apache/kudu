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

namespace security {
class TlsContext;
}

namespace rpc {

class NegotiatePB;
class NegotiatePB_SaslAuth;
class ResponseHeader;

// Class for doing KRPC negotiation with a remote server over a bidirectional socket.
// Operations on this class are NOT thread-safe.
class ClientNegotiation {
 public:
  // Creates a new client negotiation instance, taking ownership of the
  // provided socket. After completing the negotiation process by setting the
  // desired options and calling Negotiate(), the socket can be retrieved with
  // 'release_socket'.
  explicit ClientNegotiation(std::unique_ptr<Socket> socket);

  // Enable PLAIN authentication.
  // Must be called before Negotiate().
  Status EnablePlain(const std::string& user,
                     const std::string& pass);

  // Enable GSSAPI authentication.
  // Must be called before Negotiate().
  Status EnableGSSAPI();

  // Returns mechanism negotiated by this connection.
  // Must be called before Negotiate().
  SaslMechanism::Type negotiated_mechanism() const;

  // Returns the set of RPC system features supported by the remote server.
  // Must be called before Negotiate().
  std::set<RpcFeatureFlag> server_features() const {
    return server_features_;
  }

  // Returns the set of RPC system features supported by the remote server.
  // Must be called after Negotiate().
  // Subsequent calls to this method or server_features() will return an empty set.
  std::set<RpcFeatureFlag> take_server_features() {
    return std::move(server_features_);
  }

  // Specify IP:port of local side of connection.
  // Must be called before Negotiate(). Required for some mechanisms.
  void set_local_addr(const Sockaddr& addr);

  // Specify IP:port of remote side of connection.
  // Must be called before Negotiate(). Required for some mechanisms.
  void set_remote_addr(const Sockaddr& addr);

  // Specify the fully-qualified domain name of the remote server.
  // Must be called before Negotiate(). Required for some mechanisms.
  void set_server_fqdn(const std::string& domain_name);

  // Allow TLS to be used on the connection. 'tls_context' must outlive this
  // ClientNegotiation.
  void EnableTls(const security::TlsContext* tls_context);

  // Set deadline for connection negotiation.
  void set_deadline(const MonoTime& deadline);

  Socket* socket() { return socket_.get(); }

  // Takes and returns the socket owned by this client negotiation. The caller
  // will own the socket after this call, and the negotiation instance should no
  // longer be used. Must be called after Negotiate(). Subsequent calls to this
  // method or socket() will return a null pointer.
  std::unique_ptr<Socket> release_socket() { return std::move(socket_); }

  // Negotiate with the remote server. Should only be called once per
  // ClientNegotiation and socket instance, after all options have been set.
  //
  // Returns OK on success, otherwise may return NotAuthorized, NotSupported, or
  // another non-OK status.
  Status Negotiate();

  // SASL callback for plugin options, supported mechanisms, etc.
  // Returns SASL_FAIL if the option is not handled, which does not fail the handshake.
  int GetOptionCb(const char* plugin_name, const char* option,
                  const char** result, unsigned* len);

  // SASL callback for SASL_CB_USER, SASL_CB_AUTHNAME, SASL_CB_LANGUAGE
  int SimpleCb(int id, const char** result, unsigned* len);

  // SASL callback for SASL_CB_PASS
  int SecretCb(sasl_conn_t* conn, int id, sasl_secret_t** psecret);

 private:

  // Encode and send the specified negotiate request message to the server.
  Status SendNegotiatePB(const NegotiatePB& msg) WARN_UNUSED_RESULT;

  // Receive a negotiate response message from the server, deserializing it into 'msg'.
  // Validates that the response is not an error.
  Status RecvNegotiatePB(NegotiatePB* msg, faststring* buffer) WARN_UNUSED_RESULT;

  // Parse error status message from raw bytes of an ErrorStatusPB.
  Status ParseError(const Slice& err_data) WARN_UNUSED_RESULT;

  Status SendConnectionHeader() WARN_UNUSED_RESULT;

  // Initialize the SASL client negotiation instance.
  Status InitSaslClient() WARN_UNUSED_RESULT;

  // Send a NEGOTIATE step message to the server.
  Status SendNegotiate() WARN_UNUSED_RESULT;

  // Handle NEGOTIATE step response from the server.
  Status HandleNegotiate(const NegotiatePB& response) WARN_UNUSED_RESULT;

  // Send a TLS_HANDSHAKE request message to the server with the provided token.
  Status SendTlsHandshake(std::string tls_token) WARN_UNUSED_RESULT;

  // Handle a TLS_HANDSHAKE response message from the server.
  Status HandleTlsHandshake(const NegotiatePB& response) WARN_UNUSED_RESULT;

  // Send an SASL_INITIATE message to the server.
  Status SendSaslInitiate() WARN_UNUSED_RESULT;

  // Send a SASL_RESPONSE message to the server.
  Status SendSaslResponse(const char* resp_msg, unsigned resp_msg_len) WARN_UNUSED_RESULT;

  // Handle case when server sends SASL_CHALLENGE response.
  Status HandleSaslChallenge(const NegotiatePB& response) WARN_UNUSED_RESULT;

  // Perform a client-side step of the SASL negotiation.
  // Input is what came from the server. Output is what we will send back to the server.
  // Returns:
  //   Status::OK if sasl_client_step returns SASL_OK.
  //   Status::Incomplete if sasl_client_step returns SASL_CONTINUE
  // otherwise returns an appropriate error status.
  Status DoSaslStep(const std::string& in, const char** out, unsigned* out_len) WARN_UNUSED_RESULT;

  Status SendConnectionContext() WARN_UNUSED_RESULT;

  // The socket to the remote server.
  std::unique_ptr<Socket> socket_;

  // SASL state.
  std::vector<sasl_callback_t> callbacks_;
  gscoped_ptr<sasl_conn_t, SaslDeleter> sasl_conn_;
  SaslHelper helper_;

  // TLS state.
  const security::TlsContext* tls_context_;
  security::TlsHandshake tls_handshake_;

  // Authentication state.
  std::string plain_auth_user_;
  std::string plain_pass_;
  gscoped_ptr<sasl_secret_t, FreeDeleter> psecret_;

  // The set of features supported by the server. Filled in during negotiation.
  std::set<RpcFeatureFlag> server_features_;

  // The SASL mechanism used by the connection. Filled in during negotiation.
  SaslMechanism::Type negotiated_mech_;

  // Negotiation timeout deadline.
  MonoTime deadline_;
};

} // namespace rpc
} // namespace kudu
