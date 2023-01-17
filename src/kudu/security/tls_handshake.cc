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

#include "kudu/security/tls_handshake.h"

#include <openssl/crypto.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/cert.h"
#include "kudu/security/tls_socket.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

#if OPENSSL_VERSION_NUMBER < 0x10002000L
#include "kudu/security/x509_check_host.h"
#endif // OPENSSL_VERSION_NUMBER

#ifndef TLS1_3_VERSION
#define TLS1_3_VERSION 0x0304
#endif

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace security {

void TlsHandshake::SetSSLVerify() {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(ssl_);
  CHECK(!has_started_);
  int ssl_mode = 0;
  switch (verification_mode_) {
    case TlsVerificationMode::VERIFY_NONE:
      ssl_mode = SSL_VERIFY_NONE;
      break;
    case TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST:
      // Server mode: the server sends a client certificate request to the client. The
      // certificate returned (if any) is checked. If the verification process fails, the TLS/SSL
      // handshake is immediately terminated with an alert message containing the reason for the
      // verification failure. The behaviour can be controlled by the additional
      // SSL_VERIFY_FAIL_IF_NO_PEER_CERT and SSL_VERIFY_CLIENT_ONCE flags.

      // Client mode: the server certificate is verified. If the verification process fails, the
      // TLS/SSL handshake is immediately terminated with an alert message containing the reason
      // for the verification failure. If no server certificate is sent, because an anonymous
      // cipher is used, SSL_VERIFY_PEER is ignored.
      ssl_mode |= SSL_VERIFY_PEER;

      // Server mode: if the client did not return a certificate, the TLS/SSL handshake is
      // immediately terminated with a "handshake failure" alert. This flag must be used
      // together with SSL_VERIFY_PEER.
      ssl_mode |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
      // Server mode: only request a client certificate on the initial TLS/SSL handshake. Do
      // not ask for a client certificate again in case of a renegotiation. This flag must be
      // used together with SSL_VERIFY_PEER.
      ssl_mode |= SSL_VERIFY_CLIENT_ONCE;
      break;
  }

  SSL_set_verify(ssl_.get(), ssl_mode, /* callback = */nullptr);
}

TlsHandshake::TlsHandshake(TlsHandshakeType type)
    : type_(type) {
}

Status TlsHandshake::Init(c_unique_ptr<SSL> s) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  DCHECK(s);

  if (ssl_) {
    return Status::IllegalState("TlsHandshake is already initialized");
  }

  auto rbio = ssl_make_unique(BIO_new(BIO_s_mem()));
  if (!rbio) {
    return Status::RuntimeError(
        "failed to create memory-based read BIO", GetOpenSSLErrors());
  }
  auto wbio = ssl_make_unique(BIO_new(BIO_s_mem()));
  if (!wbio) {
    return Status::RuntimeError(
        "failed to create memory-based write BIO", GetOpenSSLErrors());
  }
  ssl_ = std::move(s);
  auto* ssl = ssl_.get();
  SSL_set_bio(ssl, rbio.release(), wbio.release());

  switch (type_) {
    case TlsHandshakeType::SERVER:
      SSL_set_accept_state(ssl);
      break;
    case TlsHandshakeType::CLIENT:
      SSL_set_connect_state(ssl);
      break;
  }
  return Status::OK();
}

Status TlsHandshake::Continue(const string& recv, string* send) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  if (!has_started_) {
    SetSSLVerify();
    has_started_ = true;
  }
  DCHECK(ssl_);
  auto* ssl = ssl_.get();

  // +------+                       +-----+
  // |      |--> BIO_write(rbio) -->|     |--> SSL_read(ssl)  --> IN
  // | SASL |                       | SSL |
  // |      |<--  BIO_read(wbio) <--|     |<-- SSL_write(ssl) <-- OUT
  // +------+                       +-----+
  BIO* rbio = SSL_get_rbio(ssl);
  int n = BIO_write(rbio, recv.data(), recv.size());
  DCHECK(n == recv.size() || (n == -1 && recv.empty()));
  DCHECK_EQ(BIO_ctrl_pending(rbio), recv.size());

  int rc = SSL_do_handshake(ssl);
  if (rc != 1) {
    int ssl_err = SSL_get_error(ssl, rc);
    // WANT_READ and WANT_WRITE indicate that the handshake is not yet complete.
    if (ssl_err != SSL_ERROR_WANT_READ && ssl_err != SSL_ERROR_WANT_WRITE) {
      return Status::RuntimeError("TLS Handshake error", GetSSLErrorDescription(ssl_err));
    }
    // In the case that we got SSL_ERROR_WANT_READ or SSL_ERROR_WANT_WRITE,
    // the OpenSSL implementation guarantees that there is no error entered into
    // the ERR error queue, so no need to ERR_clear_error() here.
  }

  BIO* wbio = SSL_get_wbio(ssl);
  int pending = BIO_ctrl_pending(wbio);
  DCHECK_GE(pending, 0);

  send->resize(pending);
  if (pending > 0) {
    int bytes_read = BIO_read(wbio, &(*send)[0], send->size());
    DCHECK_EQ(bytes_read, send->size());
    DCHECK_EQ(BIO_ctrl_pending(wbio), 0);
  }

  if (rc == 1) {
    // SSL_do_handshake() must have read all the pending data.
    DCHECK_EQ(0, BIO_ctrl_pending(rbio));
    VLOG(2) << Substitute("TSL Handshake complete");
    return Status::OK();
  }
  return Status::Incomplete("TLS Handshake incomplete");
}

bool TlsHandshake::NeedsExtraStep(const Status& continue_status,
                                  const string& token) const {
  DCHECK(has_started_);
  DCHECK(ssl_);
  DCHECK(continue_status.ok() || continue_status.IsIncomplete()) << continue_status.ToString();

  if (continue_status.IsIncomplete()) {
    return true;
  }
  if (continue_status.ok()) {
    switch (type_) {
      case TlsHandshakeType::CLIENT:
        return !token.empty();
      case TlsHandshakeType::SERVER:
        if (SSL_version(ssl_.get()) == TLS1_3_VERSION) {
          return false;
        }
        return !token.empty();
    }
  }
  return false;
}

void TlsHandshake::StorePendingData(string data) {
  DCHECK(!data.empty());
  // This is used only for the TLSv1.3 protocol.
  DCHECK_EQ(TLS1_3_VERSION, SSL_version(ssl_.get()));
  rbio_pending_data_ = std::move(data);
}

Status TlsHandshake::Verify(const Socket& /*socket*/) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  DCHECK(SSL_is_init_finished(ssl_.get()));
  CHECK(ssl_);

  if (verification_mode_ == TlsVerificationMode::VERIFY_NONE) {
    return Status::OK();
  }
  DCHECK(verification_mode_ == TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST);

  int rc = SSL_get_verify_result(ssl_.get());
  if (rc != X509_V_OK) {
    return Status::NotAuthorized(Substitute("SSL cert verification failed: $0",
                                            X509_verify_cert_error_string(rc)),
                                 GetOpenSSLErrors());
  }

  // Get the peer certificate.
  X509* cert = remote_cert_.GetTopOfChainX509();
  if (!cert) {
    if (SSL_get_verify_mode(ssl_.get()) & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) {
      return Status::NotAuthorized("Handshake failed: unable to retreive peer certificate");
    }
    // No cert, but we weren't requiring one.
    TRACE("Got no cert from peer, but not required");
    return Status::OK();
  }

  // TODO(KUDU-1886): Do hostname verification.
  /*
  TRACE("Verifying peer cert");

  // Get the peer's hostname
  Sockaddr peer_addr;
  if (!socket.GetPeerAddress(&peer_addr).ok()) {
    return Status::NotAuthorized(
        "TLS certificate hostname verification failed: unable to get peer address");
  }
  string peer_hostname;
  RETURN_NOT_OK_PREPEND(peer_addr.LookupHostname(&peer_hostname),
      "TLS certificate hostname verification failed: unable to lookup peer hostname");

  // Check if the hostname matches with either the Common Name or any of the Subject Alternative
  // Names of the certificate.
  int match = X509_check_host(cert,
                              peer_hostname.c_str(),
                              peer_hostname.length(),
                              0,
                              nullptr);
  if (match == 0) {
    return Status::NotAuthorized("TLS certificate hostname verification failed");
  }
  if (match < 0) {
    return Status::RuntimeError("TLS certificate hostname verification error", GetOpenSSLErrors());
  }
  DCHECK_EQ(match, 1);
  */
  return Status::OK();
}

Status TlsHandshake::GetCerts() {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  X509* cert = SSL_get_certificate(ssl_.get());
  if (cert) {
    // For whatever reason, SSL_get_certificate (unlike SSL_get_peer_certificate)
    // does not increment the X509's reference count.
    local_cert_.AdoptAndAddRefX509(cert);
  }

  cert = SSL_get_peer_certificate(ssl_.get());
  if (cert) {
    remote_cert_.AdoptX509(cert);
  }
  return Status::OK();
}

Status TlsHandshake::Finish(unique_ptr<Socket>* socket) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  RETURN_NOT_OK(GetCerts());
  RETURN_NOT_OK(Verify(**socket));

  int fd = (*socket)->Release();
  auto* ssl = ssl_.get();

  // Nothing should left in the memory-based BIOs upon Finish() is called.
  // Otherwise, the buffered data would be lost because those BIOs are destroyed
  // when SSL_set_fd() is called below.
  DCHECK_EQ(0, SSL_pending(ssl));

  BIO* wbio = SSL_get_wbio(ssl);
  DCHECK_EQ(0, BIO_ctrl_pending(wbio));
  DCHECK_EQ(0, BIO_ctrl_wpending(wbio));

  BIO* rbio = SSL_get_rbio(ssl);
  DCHECK_EQ(0, BIO_ctrl_pending(rbio));
  DCHECK_EQ(0, BIO_ctrl_wpending(rbio));

  // Give the socket to the SSL instance. This will automatically free the
  // read and write memory BIO instances.
  int ret = SSL_set_fd(ssl, fd);
  if (ret != 1) {
    return Status::RuntimeError("TLS handshake error", GetOpenSSLErrors());
  }

  const auto data_size = rbio_pending_data_.size();
  if (data_size != 0) {
    int fd = SSL_get_fd(ssl);
    Socket sock(fd);
    const uint8_t* data = reinterpret_cast<const uint8_t*>(rbio_pending_data_.data());
    int32_t written = 0;
    RETURN_NOT_OK(sock.Write(data, data_size, &written));
    if (written != data_size) {
      // The socket should be in blocking mode, so Write() should return with
      // success only if all the data is written.
      return Status::IllegalState(
          Substitute("wrote only $0 out of $1 bytes", written, data_size));
    }
    sock.Release(); // do not close the descriptor when Socket goes out of scope
    rbio_pending_data_.clear();
  }

  // Transfer the SSL instance to the socket.
  socket->reset(new TlsSocket(fd, std::move(ssl_)));

  return Status::OK();
}

Status TlsHandshake::FinishNoWrap(const Socket& socket) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  RETURN_NOT_OK(GetCerts());
  return Verify(socket);
}

Status TlsHandshake::GetLocalCert(Cert* cert) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  if (!local_cert_.GetRawData()) {
    return Status::RuntimeError("no local certificate");
  }
  cert->AdoptAndAddRefRawData(local_cert_.GetRawData());
  return Status::OK();
}

Status TlsHandshake::GetRemoteCert(Cert* cert) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  if (!remote_cert_.GetRawData()) {
    return Status::RuntimeError("no remote certificate");
  }
  cert->AdoptAndAddRefRawData(remote_cert_.GetRawData());
  return Status::OK();
}

string TlsHandshake::GetCipherSuite() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(has_started_);
  return SSL_get_cipher_name(ssl_.get());
}

string TlsHandshake::GetProtocol() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(has_started_);
  return SSL_get_version(ssl_.get());
}

string TlsHandshake::GetCipherDescription() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(has_started_);
  const SSL_CIPHER* cipher = SSL_get_current_cipher(ssl_.get());
  if (!cipher) {
    return "NONE";
  }
  char buf[512];
  const char* description = SSL_CIPHER_description(cipher, buf, sizeof(buf));
  if (!description) {
    return "NONE";
  }
  string ret(description);
  StripTrailingNewline(&ret);
  StripDupCharacters(&ret, ' ', 0);
  return ret;
}

} // namespace security
} // namespace kudu
