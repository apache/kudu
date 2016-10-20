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

#include <string>
#include <memory>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

struct ssl_ctx_st;
typedef ssl_ctx_st SSL_CTX;

namespace kudu {

class Sockaddr;
class SSLSocket;

class SSLFactory {
 public:
  SSLFactory();

  ~SSLFactory();

  // Set up the SSL_CTX and choose encryption preferences.
  Status Init();

  // Load the server certificate.
  Status LoadCertificate(const std::string& certificate_path);

  // Load the private key for the server certificate.
  Status LoadPrivateKey(const std::string& key_path);

  // Load the certificate authority.
  Status LoadCertificateAuthority(const std::string& certificate_path);

  // Create an SSLSocket wrapped around the file descriptor 'socket_fd'. 'is_server' denotes if it's
  // a server socket. The 'socket_fd' is closed when this object is destroyed.
  std::unique_ptr<SSLSocket> CreateSocket(int socket_fd, bool is_server);

 private:
  friend class SSLSocket;
  std::unique_ptr<SSL_CTX, std::function<void(SSL_CTX*)>> ctx_;

  // Gets the last error from the thread local SSL error queue. If no error exists, it returns
  // the error corresponding to 'errno_copy'.
  static std::string GetLastError(int errno_copy);
};

} // namespace kudu
