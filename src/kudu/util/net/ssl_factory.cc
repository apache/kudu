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

#include <mutex>
#include <vector>

#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/mutex.h"
#include "kudu/util/thread.h"
#include "kudu/util/net/ssl_factory.h"
#include "kudu/util/net/ssl_socket.h"

namespace kudu {

// These non-POD elements will be alive for the lifetime of the process, so don't allocate in
// static storage.
static std::vector<Mutex*> ssl_mutexes;

// Lock/Unlock the nth lock. Only to be used by OpenSSL.
static void CryptoLockingCallback(int mode, int n, const char* /*unused*/, int /*unused*/) {
  if (mode & CRYPTO_LOCK) {
    ssl_mutexes[n]->Acquire();
  } else {
    ssl_mutexes[n]->Release();
  }
}

// Return the current pthread's tid. Only to be used by OpenSSL.
static void CryptoThreadIDCallback(CRYPTO_THREADID* id) {
  return CRYPTO_THREADID_set_numeric(id, Thread::UniqueThreadId());
}

void DoSSLInit() {
  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();
  RAND_poll();

  for (int i = 0; i < CRYPTO_num_locks(); ++i) {
    debug::ScopedLeakCheckDisabler d;
    ssl_mutexes.push_back(new Mutex());
  }

  // Callbacks used by OpenSSL required in a multi-threaded setting.
  CRYPTO_set_locking_callback(CryptoLockingCallback);
  CRYPTO_THREADID_set_callback(CryptoThreadIDCallback);
}

SSLFactory::SSLFactory() : ctx_(nullptr, SSL_CTX_free) {
  static std::once_flag ssl_once;
  std::call_once(ssl_once, DoSSLInit);
}

SSLFactory::~SSLFactory() {
}

Status SSLFactory::Init() {
  CHECK(!ctx_.get());
  // NOTE: 'SSLv23 method' sounds like it would enable only SSLv2 and SSLv3, but in fact
  // this is a sort of wildcard which enables all methods (including TLSv1 and later).
  // We explicitly disable SSLv2 and SSLv3 below so that only TLS methods remain.
  // See the discussion on https://trac.torproject.org/projects/tor/ticket/11598 for more
  // info.
  ctx_.reset(SSL_CTX_new(SSLv23_method()));
  if (!ctx_) {
    return Status::RuntimeError("Could not create SSL context");
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
  SSL_CTX_set_verify(ctx_.get(),
      SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT | SSL_VERIFY_CLIENT_ONCE, nullptr);
  return Status::OK();
}

std::string SSLFactory::GetLastError(int errno_copy) {
  int error_code = ERR_get_error();
  if (error_code == 0) return kudu::ErrnoToString(errno_copy);
  const char* error_reason = ERR_reason_error_string(error_code);
  if (error_reason != NULL) return error_reason;
  return strings::Substitute("SSL error $0", error_code);
}

Status SSLFactory::LoadCertificate(const std::string& certificate_path) {
  ERR_clear_error();
  errno = 0;
  if (SSL_CTX_use_certificate_file(ctx_.get(), certificate_path.c_str(), SSL_FILETYPE_PEM) != 1) {
    return Status::NotFound(
        "Failed to load certificate file '" + certificate_path + "': " + GetLastError(errno));
  }
  return Status::OK();
}

Status SSLFactory::LoadPrivateKey(const std::string& key_path) {
  ERR_clear_error();
  errno = 0;
  if (SSL_CTX_use_PrivateKey_file(ctx_.get(), key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
    return Status::NotFound(
        "Failed to load private key file '" + key_path + "': " + GetLastError(errno));
  }
  return Status::OK();
}

Status SSLFactory::LoadCertificateAuthority(const std::string& certificate_path) {
  ERR_clear_error();
  errno = 0;
  if (SSL_CTX_load_verify_locations(ctx_.get(), certificate_path.c_str(), nullptr) != 1) {
    return Status::NotFound(
        "Failed to load certificate authority file '" + certificate_path + "': " +
            GetLastError(errno));
  }
  return Status::OK();
}

std::unique_ptr<SSLSocket> SSLFactory::CreateSocket(int socket_fd, bool is_server) {
  CHECK(ctx_);
  // Create SSL object and transfer ownership to the SSLSocket object created.
  SSL* ssl = SSL_new(ctx_.get());
  if (ssl == nullptr) {
    return nullptr;
  }
  std::unique_ptr<SSLSocket> socket(new SSLSocket(socket_fd, ssl, is_server));
  return socket;
  //return new SSLSocket(socket_fd, ssl, is_server);
}

} // namespace kudu
