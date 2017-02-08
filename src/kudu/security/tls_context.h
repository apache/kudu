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

#include <boost/optional.hpp>

#include "kudu/security/cert.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/atomic.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

class Cert;
class PrivateKey;

// TlsContext wraps data required by the OpenSSL library for creating and
// accepting TLS protected channels. A single TlsContext instance should be used
// per server or client instance.
//
// Internally, a 'TlsContext' manages a single keypair which it uses for
// terminating TLS connections. It also manages a collection of trusted root CA
// certificates (a trust store), as well as a signed certificate for the
// keypair.
//
// When used on a server, the TlsContext can generate a keypair and a
// self-signed certificate, and provide a CSR for transititioning to a CA-signed
// certificate. This allows Kudu servers to start with a self-signed
// certificate, and later adopt a CA-signed certificate as it becomes available.
// See GenerateSelfSignedCertAndKey(), GetCsrIfNecessary(), and
// AdoptSignedCert() for details on how to generate the keypair and self-signed
// cert, access the CSR, and transtition to a CA-signed cert, repectively.
//
// When used in a client or a server, the TlsContext can immediately adopt a
// private key and CA-signed cert using UseCertificateAndKey(). A TlsContext
// only manages a single keypair, so if UseCertificateAndKey() is called,
// GenerateSelfSignedCertAndKey() must not be called, and vice versa.
//
// TlsContext may be used with or without a keypair and cert to initiate TLS
// connections, when mutual TLS authentication is not needed (for example, for
// token or Kerberos authenticated connections).
//
// This class is thread-safe after initialization.
class TlsContext {

 public:

  TlsContext();

  ~TlsContext() = default;

  Status Init();

  // Returns true if this TlsContext has been configured with a cert and key for
  // use with TLS-encrypted connections.
  bool has_cert() const {
    MutexLock lock(lock_);
    return has_cert_;
  }

  // Returns true if this TlsContext has been configured with a CA-signed TLS
  // cert and key for use with TLS-encrypted connections. If this method returns
  // true, then 'has_trusted_cert' will also return true.
  bool has_signed_cert() const {
    MutexLock lock(lock_);
    return has_cert_ && !csr_;
  }

  // Returns true if this TlsContext has at least one certificate in its trust store.
  bool has_trusted_cert() const {
    MutexLock lock(lock_);
    return trusted_cert_count_ > 0;
  }

  // Adds 'cert' as a trusted root CA certificate.
  //
  // This determines whether other peers are trusted. It also must be called for
  // any CA certificates that are part of the certificate chain for the cert
  // passed in to 'UseCertificateAndKey()' or 'AdoptSignedCert()'.
  //
  // Returns AlreadyPresent if the cert is already marked as trusted. Other
  // OpenSSL errors will be RuntimeError.
  Status AddTrustedCertificate(const Cert& cert);

  // Uses 'cert' and 'key' as the cert and key for use with TLS connections.
  //
  // Checks that the CA that issued the signature on 'cert' is already trusted
  // by this context (e.g. by AddTrustedCertificate()).
  Status UseCertificateAndKey(const Cert& cert, const PrivateKey& key);

  // Generates a self-signed cert and key for use with TLS connections.
  //
  // This method should only be used on the server. Once this method is called,
  // 'GetCsrIfNecessary' can be used to retrieve a CSR for generating a
  // CA-signed cert for the generated private key, and 'AdoptSignedCert' can be
  // used to transition to using the CA-signed cert with subsequent TLS
  // connections.
  Status GenerateSelfSignedCertAndKey(const std::string& server_uuid);

  // Returns a new certificate signing request (CSR) in DER format, if this
  // context's cert is self-signed. If the cert is already signed, returns
  // boost::none.
  boost::optional<CertSignRequest> GetCsrIfNecessary() const;

  // Adopts the provided CA-signed certificate for this TLS context.
  //
  // The certificate must correspond to a CSR previously returned by
  // 'GetCsrIfNecessary()'.
  //
  // Checks that the CA that issued the signature on 'cert' is already trusted
  // by this context (e.g. by AddTrustedCertificate()).
  //
  // This has no effect if the instance already has a CA-signed cert.
  Status AdoptSignedCert(const Cert& cert);

  // Convenience functions for loading cert/CA/key from file paths.
  // -------------------------------------------------------------

  // Load the server certificate and key (PEM encoded).
  Status LoadCertificateAndKey(const std::string& certificate_path,
                               const std::string& key_path);

  // Load the certificate authority (PEM encoded).
  Status LoadCertificateAuthority(const std::string& certificate_path);

  // Initiates a new TlsHandshake instance.
  Status InitiateHandshake(TlsHandshakeType handshake_type, TlsHandshake* handshake) const;

  // Return the number of certs that have been marked as trusted.
  // Used by tests.
  int trusted_cert_count_for_tests() const {
    MutexLock lock(lock_);
    return trusted_cert_count_;
  }

 private:

  Status VerifyCertChain(const Cert& cert);

  // Owned SSL context.
  c_unique_ptr<SSL_CTX> ctx_;

  // Protexts trusted_cert_count_, has_cert_ and csr_, as well as ctx_ when it
  // needs to be updated transactionally with has_cert_ and csr_.
  mutable Mutex lock_;
  int32_t trusted_cert_count_;
  bool has_cert_;
  boost::optional<CertSignRequest> csr_;
};

} // namespace security
} // namespace kudu
