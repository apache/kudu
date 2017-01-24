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

#include <boost/optional/optional_fwd.hpp>

#include "kudu/gutil/macros.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

class Cert;
class CertSignRequest;
class PrivateKey;

// Manages the X509 certificate used by a server when the built-in
// PKI infrastructure is enabled.
//
// This generates an X509 certificate for the server, provides a CSR which can be
// transferred to the master, and then stores the signed certificate once provided.
//
// Note that the master, in addition to acting as a CA, also needs its own cert
// (signed by the CA cert) which it uses for its RPC server. This handles the latter.
//
// This class is thread-safe after initialization.
class ServerCertManager {
 public:
  // Construct a ServertCertManager.
  explicit ServerCertManager(std::string server_uuid);
  ~ServerCertManager();

  // Generate a private key and CSR.
  //
  // This must be called exactly once before any methods below.
  Status Init();

  // Return a new CSR in DER format, if this server's cert has not yet been
  // signed. If the cert is already signed, returns boost::none;
  boost::optional<std::string> GetCSRIfNecessary() const;

  // Adopt the given signed cert (provided in DER format) as the cert for
  // this server.
  //
  // This has no effect if the instance already has a signed cert.
  Status AdoptSignedCert(const std::string& cert_der);

  // TODO(PKI): some code to register a callback when the cert is adopted,
  // so that it can set it on the SSLFactory?

  // Return true if we currently have a signed certificate.
  // TODO(PKI): should this check validity?
  bool has_signed_cert() const;

 private:
  const std::string server_uuid_;

  // Protects the below variables.
  mutable Mutex lock_;

  // The CSR for this server. If the cert has already been
  // signed, set to empty.
  std::string csr_der_;

  // The signed cert for this server. Only set once the cert
  // has been signed.
  std::unique_ptr<Cert> signed_cert_;

  // The keypair associated with this server's cert.
  std::unique_ptr<PrivateKey> key_;

  DISALLOW_COPY_AND_ASSIGN(ServerCertManager);
};

} // namespace security
} // namespace kudu
