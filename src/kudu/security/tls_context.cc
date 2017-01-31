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

#include "kudu/security/tls_context.h"

#include <string>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/status.h"

using strings::Substitute;
using std::string;

namespace kudu {
namespace security {

template<> struct SslTypeTraits<SSL> {
  static constexpr auto free = &SSL_free;
};
template<> struct SslTypeTraits<SSL_CTX> {
  static constexpr auto free = &SSL_CTX_free;
};
template<> struct SslTypeTraits<X509_STORE_CTX> {
  static constexpr auto free = &X509_STORE_CTX_free;
};


TlsContext::TlsContext()
    : has_cert_(false) {
  security::InitializeOpenSSL();
}

Status TlsContext::Init() {
  CHECK(!ctx_);

  // NOTE: 'SSLv23 method' sounds like it would enable only SSLv2 and SSLv3, but in fact
  // this is a sort of wildcard which enables all methods (including TLSv1 and later).
  // We explicitly disable SSLv2 and SSLv3 below so that only TLS methods remain.
  // See the discussion on https://trac.torproject.org/projects/tor/ticket/11598 for more
  // info.
  ctx_ = ssl_make_unique(SSL_CTX_new(SSLv23_method()));
  if (!ctx_) {
    return Status::RuntimeError("failed to create TLS context", GetOpenSSLErrors());
  }
  SSL_CTX_set_mode(ctx_.get(), SSL_MODE_AUTO_RETRY);

  // Disable SSLv2 and SSLv3 which are vulnerable to various issues such as POODLE.
  // We support versions back to TLSv1.0 since OpenSSL on RHEL 6.4 and earlier does not
  // not support TLSv1.1 or later.
  //
  // Disable SSL/TLS compression to free up CPU resources and be less prone
  // to attacks exploiting the compression feature:
  //   https://tools.ietf.org/html/rfc7525#section-3.3
  SSL_CTX_set_options(ctx_.get(),
                      SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 |
                      SSL_OP_NO_COMPRESSION);

  // TODO(PKI): is it possible to disable client-side renegotiation? it seems there
  // have been various CVEs related to this feature that we don't need.
  // TODO(PKI): set desired cipher suites?
  return Status::OK();
}

Status TlsContext::VerifyCertChain(const Cert& cert) {
  X509_STORE* store = SSL_CTX_get_cert_store(ctx_.get());
  auto store_ctx = ssl_make_unique<X509_STORE_CTX>(X509_STORE_CTX_new());

  OPENSSL_RET_NOT_OK(X509_STORE_CTX_init(store_ctx.get(), store, cert.GetRawData(), nullptr),
                     "could not init X509_STORE_CTX");
  int rc = X509_verify_cert(store_ctx.get());
  if (rc != 1) {
    int err = X509_STORE_CTX_get_error(store_ctx.get());
    if (err == X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT) {
      // It's OK to provide a self-signed cert.
      return Status::OK();
    }

    // Get the cert that failed to verify.
    X509* cur_cert = X509_STORE_CTX_get_current_cert(store_ctx.get());
    string cert_details;
    if (cur_cert) {
      cert_details = Substitute(" (error with cert: subject=$0, issuer=$1)",
                                X509NameToString(X509_get_subject_name(cur_cert)),
                                X509NameToString(X509_get_issuer_name(cur_cert)));
    }

    return Status::RuntimeError(
        Substitute("could not verify cert chain$0", cert_details),
        X509_verify_cert_error_string(err));
  }
  return Status::OK();
}

Status TlsContext::UseCertificateAndKey(const Cert& cert, const PrivateKey& key) {
  ERR_clear_error();

  // Verify that the cert and key match.
  OPENSSL_RET_NOT_OK(X509_check_private_key(cert.GetRawData(), key.GetRawData()),
                     "cert and private key do not match");

  // Verify that the appropriate CA certs have been loaded into the context
  // before we adopt a cert. Otherwise, client connections without the CA cert
  // available would fail.
  RETURN_NOT_OK(VerifyCertChain(cert));

  OPENSSL_RET_NOT_OK(SSL_CTX_use_PrivateKey(ctx_.get(), key.GetRawData()),
                     "failed to use private key");
  OPENSSL_RET_NOT_OK(SSL_CTX_use_certificate(ctx_.get(), cert.GetRawData()),
                     "failed to use certificate");
  has_cert_.Store(true);
  return Status::OK();
}

Status TlsContext::AddTrustedCertificate(const Cert& cert) {
  VLOG(2) << "Trusting certificate " << cert.SubjectName();

  ERR_clear_error();
  auto* cert_store = SSL_CTX_get_cert_store(ctx_.get());
  OPENSSL_RET_NOT_OK(X509_STORE_add_cert(cert_store, cert.GetRawData()),
                     "failed to add trusted certificate");
  return Status::OK();
}

Status TlsContext::LoadCertificateAndKey(const string& certificate_path,
                                         const string& key_path) {
  Cert c;
  RETURN_NOT_OK(c.FromFile(certificate_path, DataFormat::PEM));
  PrivateKey k;
  RETURN_NOT_OK(k.FromFile(key_path, DataFormat::PEM));
  return UseCertificateAndKey(c, k);
}

Status TlsContext::LoadCertificateAuthority(const string& certificate_path) {
  Cert c;
  RETURN_NOT_OK(c.FromFile(certificate_path, DataFormat::PEM));
  return AddTrustedCertificate(c);
}

Status TlsContext::InitiateHandshake(TlsHandshakeType handshake_type,
                                     TlsHandshake* handshake) const {
  CHECK(ctx_);
  CHECK(!handshake->ssl_);
  handshake->adopt_ssl(ssl_make_unique(SSL_new(ctx_.get())));
  if (!handshake->ssl_) {
    return Status::RuntimeError("failed to create SSL handle", GetOpenSSLErrors());
  }

  SSL_set_bio(handshake->ssl(),
              BIO_new(BIO_s_mem()),
              BIO_new(BIO_s_mem()));

  switch (handshake_type) {
    case TlsHandshakeType::SERVER:
      SSL_set_accept_state(handshake->ssl());
      break;
    case TlsHandshakeType::CLIENT:
      SSL_set_connect_state(handshake->ssl());
      break;
  }

  return Status::OK();
}

} // namespace security
} // namespace kudu
