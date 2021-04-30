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
#include <sys/socket.h>

#include <cerrno>
#include <cstddef>
#include <functional>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/errno.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"

using std::string;
using strings::Substitute;

namespace kudu {
namespace security {

TlsSocket::TlsSocket(int fd, c_unique_ptr<SSL> ssl)
    : Socket(fd),
      ssl_(std::move(ssl)) {
  use_cork_ = true;

#ifndef __APPLE__
// `SO_DOMAIN` is not available on macOS. This code can be safely
// skipped because SetTcpCork() is a no-op on macOS.
  if (fd >= 0) {
    int dom;
    socklen_t len = sizeof(dom);
    if (getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &dom, &len) == 0 &&
        dom == AF_UNIX) {
      use_cork_ = false;
    }
  }
#endif
}

TlsSocket::~TlsSocket() {
  ignore_result(Close());
}

Status TlsSocket::Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) {
  CHECK(ssl_);
  SCOPED_OPENSSL_NO_PENDING_ERRORS;

  *nwritten = 0;
  if (PREDICT_FALSE(amt == 0)) {
    // Writing an empty buffer is a no-op. This happens occasionally, eg in the
    // case where the response has an empty sidecar. We have to special case
    // it, because SSL_write can return '0' to indicate certain types of errors.
    return Status::OK();
  }

  errno = 0;
  int32_t bytes_written = SSL_write(ssl_.get(), buf, amt);
  int save_errno = errno;
  if (bytes_written <= 0) {
    auto error_code = SSL_get_error(ssl_.get(), bytes_written);
    if (error_code == SSL_ERROR_WANT_WRITE) {
      if (save_errno != 0) {
        return Status::NetworkError("SSL_write error",
                                    ErrnoToString(save_errno), save_errno);
      }
      // Socket not ready to write yet.
      return Status::OK();
    }
    return Status::NetworkError("failed to write to TLS socket",
                                GetSSLErrorDescription(error_code));
  }
  *nwritten = bytes_written;
  return Status::OK();
}

Status TlsSocket::Writev(const struct ::iovec *iov, int iov_len, int64_t *nwritten) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(ssl_);

  // Since OpenSSL doesn't support any kind of writev() call itself, this function
  // sets TCP_CORK and then calls Write() for each of the buffers in the iovec,
  // then unsets TCP_CORK. This causes the Linux kernel to buffer up the packets
  // while corked and then send a minimal number of packets upon uncorking, whereas
  // otherwise it would have sent at least packet per Write call. This is beneficial
  // since it avoids generating lots of small packets, each of which has overhead in
  // the network stack, etc.
  //
  // The downside, though, is that we need to make (iov_len + 2) system calls, each
  // of which has some significant overhead (even moreso after spectre/meltdown
  // mitigations were enabled). This can take significant CPU in the reactor thread,
  // especially when the underlying buffers are small.
  //
  // To mitigate this, we handle a common case where the iovec has a few buffers,
  // but the total iovec length is actually short. This is the case in many types
  // of RPC requests/responses. In this case, it's cheaper to copy all of the buffers
  // into a socket-local buffer 'buf_' and do a single Write call, vs doing the
  // emulated Writev approach described above.
  if (iov_len > 1) {
    size_t total_size = 0;
    for (int i = 0; i < iov_len; i++) {
      total_size += iov[i].iov_len;
    }
    // Assume we can copy about 8 bytes per cycle, and a syscall takes about 1300 cycles,
    // based on some quick benchmarking of 'setsockopt' on a GCP Sky Lake VM.
    //
    // cycles for memcpy and one write = total_size / 8 + syscall
    // cycles for cork, N writes, uncork = syscall * (2 + iov_len)
    //
    // Solve the inequality to find where memcpy is faster:
    // total_size / 8 + syscall < syscall * (2 + iov_len)
    // total_size / 8 < syscall * (1 + iov_len)
    // total_size < 8 * syscall * (1 + iov_len)
    size_t max_copy_size = 8 * 1300 * (1 + iov_len);
    if (total_size <= max_copy_size) {
      buf_.clear();
      buf_.reserve(total_size);
      for (int i = 0; i < iov_len; i++) {
        buf_.append(iov[i].iov_base, iov[i].iov_len);
      }
      // TODO(todd) Write()'s 'nwritten' parameter is int32_t* instead of int64_t*
      // so we need this temporary. We should change Write() to use size_t as well.
      int32_t n = 0;
      Status s = Write(buf_.data(), buf_.size(), &n);
      *nwritten = n;
      return s;
    }
  }

  *nwritten = 0;
  // Allows packets to be aggresively be accumulated before sending.
  bool do_cork = use_cork_ && iov_len > 1;
  if (do_cork) {
    RETURN_NOT_OK(SetTcpCork(1));
  }
  Status write_status = Status::OK();
  for (int i = 0; i < iov_len; ++i) {
    int32_t frame_size = iov[i].iov_len;
    int32_t bytes_written;
    // Don't return before unsetting TCP_CORK.
    write_status = Write(static_cast<uint8_t*>(iov[i].iov_base), frame_size, &bytes_written);
    if (!write_status.ok()) break;

    // nwritten should have the correct amount written.
    *nwritten += bytes_written;
    if (bytes_written < frame_size) break;
  }
  if (do_cork) {
    RETURN_NOT_OK(SetTcpCork(0));
  }
  // If we did manage to write something, but not everything, due to a temporary socket
  // error, then we should still return an OK status indicating a successful _partial_
  // write.
  if (*nwritten > 0 && Socket::IsTemporarySocketError(write_status.posix_code())) {
    return Status::OK();
  }
  return write_status;
}

Status TlsSocket::Recv(uint8_t *buf, int32_t amt, int32_t *nread) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;

  CHECK(ssl_);
  errno = 0;
  int32_t bytes_read = SSL_read(ssl_.get(), buf, amt);
  int save_errno = errno;
  if (bytes_read <= 0) {
    Sockaddr remote;
    Status s = GetPeerAddress(&remote);
    const string remote_str = s.ok() ? remote.ToString() : "unknown";
    string kErrString = Substitute("failed to read from TLS socket (remote: $0)",
                                   remote_str);
    if (bytes_read == 0 && SSL_get_shutdown(ssl_.get()) == SSL_RECEIVED_SHUTDOWN) {
      kErrString += GetOpenSSLErrors();
      return Status::NetworkError(kErrString, ErrnoToString(ESHUTDOWN), ESHUTDOWN);
    }
    auto error_code = SSL_get_error(ssl_.get(), bytes_read);
    if (error_code == SSL_ERROR_WANT_READ) {
      if (save_errno != 0) {
        return Status::NetworkError("SSL_read error from " + remote_str,
                                    ErrnoToString(save_errno), save_errno);
      }
      // Nothing available to read yet.
      *nread = 0;
      return Status::OK();
    }
    if (error_code == SSL_ERROR_SYSCALL && ERR_peek_error() == 0) {
      // From the OpenSSL docs:
      //   Some I/O error occurred.  The OpenSSL error queue may contain more
      //   information on the error.  If the error queue is empty (i.e.
      //   ERR_get_error() returns 0), ret can be used to find out more about
      //   the error: If ret == 0, an EOF was observed that violates the pro-
      //   tocol.  If ret == -1, the underlying BIO reported an I/O error (for
      //   socket I/O on Unix systems, consult errno for details).
      if (bytes_read == 0) {
        // "EOF was observed that violates the protocol" (eg the other end disconnected)
        return Status::NetworkError(kErrString, ErrnoToString(ECONNRESET), ECONNRESET);
      }
      if (bytes_read == -1 && save_errno != 0) {
        return Status::NetworkError(kErrString, ErrnoToString(save_errno), save_errno);
      }
      return Status::NetworkError(kErrString, "unknown ERROR_SYSCALL");
    }
    return Status::NetworkError(kErrString, GetSSLErrorDescription(error_code));
  }
  *nread = bytes_read;
  return Status::OK();
}

Status TlsSocket::Close() {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
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

  // Close the underlying socket.
  RETURN_NOT_OK(Socket::Close());
  return ssl_shutdown;
}

string TlsSocket::GetProtocolName() const {
  return ::kudu::security::GetProtocolName(ssl_.get());
}

string TlsSocket::GetCipherDescription() const {
  return ::kudu::security::GetCipherDescription(ssl_.get());
}


} // namespace security
} // namespace kudu
