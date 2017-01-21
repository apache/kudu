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

#include <functional>
#include <string>

#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

// TlsContext wraps data required by the OpenSSL library for creating TLS
// protected channels. A single TlsContext instance should be used per server or
// client instance.
class TlsContext {

 public:

  TlsContext();

  ~TlsContext() = default;

  Status Init();

  // Load the server certificate.
  Status LoadCertificate(const std::string& certificate_path);

  // Load the private key for the server certificate.
  Status LoadPrivateKey(const std::string& key_path);

  // Load the certificate authority.
  Status LoadCertificateAuthority(const std::string& certificate_path);

  // Initiates a new TlsHandshake instance.
  Status InitiateHandshake(TlsHandshakeType handshake_type, TlsHandshake* handshake) const;

 private:

  // Owned SSL context.
  c_unique_ptr<SSL_CTX> ctx_;
};

} // namespace security
} // namespace kudu
