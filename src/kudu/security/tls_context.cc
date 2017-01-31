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

#include "kudu/gutil/strings/substitute.h"
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

TlsContext::TlsContext() {
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

Status TlsContext::LoadCertificate(const string& certificate_path) {
  ERR_clear_error();
  errno = 0;
  if (SSL_CTX_use_certificate_file(ctx_.get(), certificate_path.c_str(), SSL_FILETYPE_PEM) != 1) {
    return Status::NotFound(Substitute("failed to load certificate file: '$0'", certificate_path),
                            GetOpenSSLErrors());
  }
  return Status::OK();
}

Status TlsContext::LoadPrivateKey(const string& key_path) {
  ERR_clear_error();
  errno = 0;
  if (SSL_CTX_use_PrivateKey_file(ctx_.get(), key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
    return Status::NotFound(Substitute("failed to load private key file: '$0'", key_path),
                            GetOpenSSLErrors());
  }
  return Status::OK();
}

Status TlsContext::LoadCertificateAuthority(const string& certificate_path) {
  ERR_clear_error();
  errno = 0;
  if (SSL_CTX_load_verify_locations(ctx_.get(), certificate_path.c_str(), nullptr) != 1) {
    return Status::NotFound(Substitute("failed to load certificate authority file: '$0'",
                                       certificate_path),
                            GetOpenSSLErrors());
  }
  return Status::OK();
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
