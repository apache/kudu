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
#include <string>

#include "kudu/security/openssl_util.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

namespace kudu {

class Socket;

namespace security {

enum class TlsHandshakeType {
  // The local endpoint is the TLS client (initiator).
  CLIENT,
  // The local endpoint is the TLS server (acceptor).
  SERVER,
};

// TlsHandshake manages an ongoing TLS handshake between a client and server.
//
// TlsHandshake instances are default constructed, but must be initialized
// before use using TlsContext::InitiateHandshake.
class TlsHandshake {
 public:

   TlsHandshake() = default;
   ~TlsHandshake() = default;

  // Continue or start a new handshake.
  //
  // 'recv' should contain the input buffer from the remote end, or an empty
  // string when the handshake is new.
  //
  // 'send' should contain the output buffer which must be sent to the remote
  // end.
  //
  // Returns Status::OK when the handshake is complete, however the 'send'
  // buffer may contain a message which must still be transmitted to the remote
  // end. If the send buffer is empty after this call and the return is
  // Status::OK, the socket should immediately be wrapped in the TLS channel
  // using 'Finish'. If the send buffer is not empty, the message should be sent
  // to the remote end, and then the socket should be wrapped using 'Finish'.
  //
  // Returns Status::Incomplete when the handshake must continue for another
  // round of messages.
  //
  // Returns any other status code on error.
  Status Continue(const std::string& recv, std::string* send);

  // Finishes the handshake, wrapping the provided socket in the negotiated TLS
  // channel. This 'TlsHandshake' instance should not be used again after
  // calling this.
  Status Finish(std::unique_ptr<Socket>* socket);

 private:
  friend class TlsContext;

  // Set the SSL to use during the handshake. Called once by
  // TlsContext::InitiateHandshake before starting the handshake processes.
  void adopt_ssl(c_unique_ptr<SSL> ssl) {
    CHECK(!ssl_);
    ssl_ = std::move(ssl);
  }

  SSL* ssl() {
    return ssl_.get();
  }

  // Verifies that the handshake is valid for the provided socket.
  Status Verify(const Socket& socket) const;

  // Owned SSL handle.
  c_unique_ptr<SSL> ssl_;
};

} // namespace security
} // namespace kudu
