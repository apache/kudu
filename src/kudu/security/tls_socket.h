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

class TlsSocket : public Socket {
 public:

  ~TlsSocket() override;

  Status Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) override;

  Status Writev(const struct ::iovec *iov, int iov_len, int32_t *nwritten) override;

  Status Recv(uint8_t *buf, int32_t amt, int32_t *nread) override;

  Status Close() override;

 private:

  friend class TlsHandshake;

  TlsSocket(int fd, c_unique_ptr<SSL> ssl);

  // Owned SSL handle.
  c_unique_ptr<SSL> ssl_;
};

} // namespace security
} // namespace kudu
