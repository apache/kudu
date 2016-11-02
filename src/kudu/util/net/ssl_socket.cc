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

#include <errno.h>

#include <vector>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/errno.h"
#include "kudu/util/net/ssl_factory.h"
#include "kudu/util/net/ssl_socket.h"
#include "kudu/util/net/sockaddr.h"

#if OPENSSL_VERSION_NUMBER < 0x10002000L
#include "kudu/util/x509_check_host.h"
#endif // OPENSSL_VERSION_NUMBER

namespace kudu {

SSLSocket::SSLSocket(int fd, SSL* ssl, bool is_server) :
    Socket(fd), ssl_(ssl), is_server_(is_server) {
  SSL_set_fd(ssl_, fd);
}

SSLSocket::~SSLSocket() {
}

Status SSLSocket::DoHandshake() {
  CHECK(ssl_);
  ERR_clear_error();
  errno = 0;
  int ret;
  if (is_server_) {
    ret = SSL_accept(ssl_);
  } else {
    ret = SSL_connect(ssl_);
  }
  if (ret <= 0) return Status::NetworkError(SSLFactory::GetLastError(errno));

  // Verify if the handshake was successful.
  int rc = SSL_get_verify_result(ssl_);
  if (rc != X509_V_OK) {
    return Status::NetworkError("SSL_get_verify_result()", X509_verify_cert_error_string(rc));
  }

  // Get the peer certificate.
  std::unique_ptr<X509, void(*)(X509*)> cert(
      SSL_get_peer_certificate(ssl_), [](X509* x) { X509_free(x); });
  cert.reset(SSL_get_peer_certificate(ssl_));
  if (cert == nullptr) {
    if (SSL_get_verify_mode(ssl_) & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) {
      return Status::NetworkError("Handshake failed: Could not retreive peer certificate");
    }
  }

  // Get the peer's hostname
  Sockaddr peer_addr;
  if (!GetPeerAddress(&peer_addr).ok()) {
    return Status::NetworkError("Handshake failed: Could not retrieve peer address");
  }
  std::string peer_hostname;
  RETURN_NOT_OK(peer_addr.LookupHostname(&peer_hostname));

  // Check if the hostname matches with either the Common Name or any of the Subject Alternative
  // Names of the certificate.
  int match;
  if ((match = X509_check_host(
      cert.get(), peer_hostname.c_str(), peer_hostname.length(), 0, nullptr)) == 0) {
    return Status::NetworkError("Handshake failed: Could not verify host with certificate");
  }
  if (match < 0) {
    return Status::NetworkError("Handshake failed:", SSLFactory::GetLastError(errno));
  }
  CHECK(match == 1);
  return Status::OK();
}

Status SSLSocket::Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) {
  CHECK(ssl_);
  ERR_clear_error();
  errno = 0;
  int32_t bytes_written = SSL_write(ssl_, buf, amt);
  if (bytes_written <= 0) {
    if (SSL_get_error(ssl_, bytes_written) == SSL_ERROR_WANT_WRITE) {
      // Socket not ready to write yet.
      *nwritten = 0;
      return Status::OK();
    }
    return Status::NetworkError("SSL_write", SSLFactory::GetLastError(errno));
  }
  *nwritten = bytes_written;
  return Status::OK();
}

Status SSLSocket::Writev(const struct ::iovec *iov, int iov_len,
                      int32_t *nwritten) {
  CHECK(ssl_);
  ERR_clear_error();
  int32_t total_written = 0;
  // Allows packets to be aggresively be accumulated before sending.
  RETURN_NOT_OK(SetTcpCork(1));
  Status write_status = Status::OK();
  for (int i = 0; i < iov_len; ++i) {
    int32_t frame_size = iov[i].iov_len;
    // Don't return before unsetting TCP_CORK.
    write_status = Write(static_cast<uint8_t*>(iov[i].iov_base), frame_size, nwritten);
    total_written += *nwritten;
    if (*nwritten < frame_size) break;
  }
  RETURN_NOT_OK(SetTcpCork(0));
  *nwritten = total_written;
  return write_status;
}

Status SSLSocket::Recv(uint8_t *buf, int32_t amt, int32_t *nread) {
  CHECK(ssl_);
  ERR_clear_error();
  errno = 0;
  int32_t bytes_read = SSL_read(ssl_, buf, amt);
  if (bytes_read <= 0) {
    if (bytes_read == 0 && SSL_get_shutdown(ssl_) == SSL_RECEIVED_SHUTDOWN) {
      return Status::NetworkError("SSLSocket::Recv() for EOF from remote", Slice(), ESHUTDOWN);
    }
    if (SSL_get_error(ssl_, bytes_read) == SSL_ERROR_WANT_READ) {
      // Nothing available to read yet.
      *nread = 0;
      return Status::OK();
    }
    return Status::NetworkError("SSL_read", SSLFactory::GetLastError(errno));
  }
  *nread = bytes_read;
  return Status::OK();
}

Status SSLSocket::Close() {
  CHECK(ssl_);
  ERR_clear_error();
  errno = 0;
  int32_t ret = SSL_shutdown(ssl_);
  Status shutdown_status;
  if (ret < 0 && errno != EAGAIN) {
    // We still need to close the underlying socket, so don't return just yet.
    shutdown_status = Status::NetworkError("SSL_Shutdown", SSLFactory::GetLastError(errno));
  }
  SSL_free(ssl_);
  ssl_ = nullptr;
  ERR_remove_state(0);

  Status close_status = Socket::Close();
  if (!close_status.ok()) return close_status.CloneAndPrepend(shutdown_status.message());
  return shutdown_status;
}

} // namespace kudu
