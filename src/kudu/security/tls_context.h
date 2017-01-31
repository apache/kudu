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

#include "kudu/security/tls_handshake.h"
#include "kudu/util/atomic.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

class Cert;
class PrivateKey;

// TlsContext wraps data required by the OpenSSL library for creating TLS
// protected channels. A single TlsContext instance should be used per server or
// client instance.
class TlsContext {

 public:

  TlsContext();

  ~TlsContext() = default;

  Status Init();

  // Return true if this TlsContext has been configured with a cert and key to
  // accept TLS connections.
  bool has_cert() const { return has_cert_.Load(); }

  // Use 'cert' and 'key' as the cert/key for this server/client.
  //
  // If the cert is not self-signed, checks that the CA that issued
  // the signature on 'cert' is already trusted by this context
  // (e.g. by AddTrustedCertificate).
  Status UseCertificateAndKey(const Cert& cert, const PrivateKey& key);

  // Add 'cert' as a trusted certificate for this server/client.
  //
  // This determines whether other peers are trusted. It also must
  // be called for any CA certificates that are part of the certificate
  // chain for the cert passed in 'UseCertificate' above.
  Status AddTrustedCertificate(const Cert& cert);

  // Convenience functions for loading cert/CA/key from file paths.
  // -------------------------------------------------------------

  // Load the server certificate and key (PEM encoded).
  Status LoadCertificateAndKey(const std::string& certificate_path,
                               const std::string& key_path);

  // Load the certificate authority (PEM encoded).
  Status LoadCertificateAuthority(const std::string& certificate_path);

  // Initiates a new TlsHandshake instance.
  Status InitiateHandshake(TlsHandshakeType handshake_type, TlsHandshake* handshake) const;

 private:
  Status VerifyCertChain(const Cert& cert);

  AtomicBool has_cert_;

  // Owned SSL context.
  c_unique_ptr<SSL_CTX> ctx_;
};

} // namespace security
} // namespace kudu
