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

#include "kudu/security/openssl_util.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

struct ssl_st;
typedef ssl_st SSL;

namespace kudu {
namespace security {

class Cert;

class TlsSocket : public Socket {
 public:

  ~TlsSocket() override;

  // Retrieve the local certificate. This will return an error status if there
  // is no local certificate.
  Status GetLocalCert(Cert* cert) const WARN_UNUSED_RESULT;

  // Retrieve the remote peer's certificate. This will return an error status if
  // there is no remote certificate.
  Status GetRemoteCert(Cert* cert) const WARN_UNUSED_RESULT;

  Status Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) override WARN_UNUSED_RESULT;

  Status Writev(const struct ::iovec *iov,
                int iov_len,
                int32_t *nwritten) override WARN_UNUSED_RESULT;

  Status Recv(uint8_t *buf, int32_t amt, int32_t *nread) override WARN_UNUSED_RESULT;

  Status Close() override WARN_UNUSED_RESULT;

 private:

  friend class TlsHandshake;

  TlsSocket(int fd, c_unique_ptr<SSL> ssl);

  // Owned SSL handle.
  c_unique_ptr<SSL> ssl_;
};

} // namespace security
} // namespace kudu
