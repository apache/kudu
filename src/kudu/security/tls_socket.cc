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

#include "kudu/security/tls_socket.h"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include "kudu/security/openssl_util.h"

namespace kudu {
namespace security {

TlsSocket::TlsSocket(int fd, c_unique_ptr<SSL> ssl)
    : Socket(fd),
      ssl_(std::move(ssl)) {
}

TlsSocket::~TlsSocket() {
  ignore_result(Close());
}

Status TlsSocket::Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) {
  CHECK(ssl_);
  ERR_clear_error();
  errno = 0;
  int32_t bytes_written = SSL_write(ssl_.get(), buf, amt);
  if (bytes_written <= 0) {
    auto error_code = SSL_get_error(ssl_.get(), bytes_written);
    if (error_code == SSL_ERROR_WANT_WRITE) {
      // Socket not ready to write yet.
      *nwritten = 0;
      return Status::OK();
    }
    return Status::NetworkError("TlsSocket::Write", GetSSLErrorDescription(error_code));
  }
  *nwritten = bytes_written;
  return Status::OK();
}

Status TlsSocket::Writev(const struct ::iovec *iov, int iov_len, int32_t *nwritten) {
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

Status TlsSocket::Recv(uint8_t *buf, int32_t amt, int32_t *nread) {
  CHECK(ssl_);
  ERR_clear_error();
  int32_t bytes_read = SSL_read(ssl_.get(), buf, amt);
  if (bytes_read <= 0) {
    if (bytes_read == 0 && SSL_get_shutdown(ssl_.get()) == SSL_RECEIVED_SHUTDOWN) {
      return Status::NetworkError("TlsSocket::Recv", "received remote shutdown", ESHUTDOWN);
    }
    auto error_code = SSL_get_error(ssl_.get(), bytes_read);
    if (error_code == SSL_ERROR_WANT_READ) {
      // Nothing available to read yet.
      *nread = 0;
      return Status::OK();
    }
    return Status::NetworkError("TlsSocket::Recv", GetSSLErrorDescription(error_code));
  }
  *nread = bytes_read;
  return Status::OK();
}

Status TlsSocket::Close() {
  ERR_clear_error();
  errno = 0;

  if (!ssl_) {
    // Socket is already closed.
    return Status::OK();
  }

  // Start the TLS shutdown processes. We don't care about waiting for the
  // response, since the underlying socket will not be reused.
  int32_t ret = SSL_shutdown(ssl_.get());
  Status ssl_shutdown;
  if (ret >= 0) {
    ssl_shutdown = Status::OK();
  } else {
    auto error_code = SSL_get_error(ssl_.get(), ret);
    ssl_shutdown = Status::NetworkError("TlsSocket::Close", GetSSLErrorDescription(error_code));
  }

  ssl_.reset();
  ERR_remove_state(0);

  // Close the underlying socket.
  RETURN_NOT_OK(Socket::Close());
  return ssl_shutdown;
}

} // namespace security
} // namespace kudu
