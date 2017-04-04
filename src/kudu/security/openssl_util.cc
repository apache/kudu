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

#include "kudu/security/openssl_util.h"

#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>

#include <glog/logging.h>
#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/errno.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using std::ostringstream;
using std::string;

namespace kudu {
namespace security {

namespace {

// Determine whether initialization was ever called.
//
// Thread safety:
// - written by DoInitializeOpenSSL (single-threaded, due to std::call_once)
// - read by DisableOpenSSLInitialization (must not be concurrent with above)
bool g_ssl_is_initialized = false;

// If true, then we expect someone else has initialized SSL.
//
// Thread safety:
// - read by DoInitializeOpenSSL (single-threaded, due to std::call_once)
// - written by DisableOpenSSLInitialization (must not be concurrent with above)
bool g_disable_ssl_init = false;

// Array of locks used by OpenSSL.
// We use an intentionally-leaked C-style array here to avoid non-POD static data.
Mutex* kCryptoLocks = nullptr;

// Lock/Unlock the nth lock. Only to be used by OpenSSL.
void LockingCB(int mode, int type, const char* /*file*/, int /*line*/) {
  DCHECK(kCryptoLocks);
  Mutex* m = &kCryptoLocks[type];
  if (mode & CRYPTO_LOCK) {
    m->lock();
  } else {
    m->unlock();
  }
}

Status CheckOpenSSLInitialized() {
  if (!CRYPTO_get_locking_callback()) {
    return Status::RuntimeError("Locking callback not initialized");
  }
  auto ctx = ssl_make_unique(SSL_CTX_new(SSLv23_method()));
  if (!ctx) {
    ERR_clear_error();
    return Status::RuntimeError("SSL library appears uninitialized (cannot create SSL_CTX)");
  }
  return Status::OK();
}

void DoInitializeOpenSSL() {
  // In case the user's thread has left some error around, clear it.
  ERR_clear_error();
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  if (g_disable_ssl_init) {
    VLOG(2) << "Not initializing OpenSSL (disabled by application)";
    return;
  }

  // Check that OpenSSL isn't already initialized. If it is, it's likely
  // we are embedded in (or embedding) another application/library which
  // initializes OpenSSL, and we risk installing conflicting callbacks
  // or crashing due to concurrent initialization attempts. In that case,
  // log a warning.
  auto ctx = ssl_make_unique(SSL_CTX_new(SSLv23_method()));
  if (ctx) {
    LOG(WARNING) << "It appears that OpenSSL has been previously initialized by "
                 << "code outside of Kudu. Please use kudu::client::DisableOpenSSLInitialization() "
                 << "to avoid potential crashes due to conflicting initialization.";
    // Continue anyway; all of the below is idempotent, except for the locking callback,
    // which we check before overriding. They aren't thread-safe, however -- that's why
    // we try to get embedding applications to do the right thing here rather than risk a
    // potential initialization race.
  } else {
    // As expected, SSL is not initialized, so SSL_CTX_new() failed. Make sure
    // it didn't leave anything in our error queue.
    ERR_clear_error();
  }

  SSL_load_error_strings();
  SSL_library_init();
  OpenSSL_add_all_algorithms();
  RAND_poll();

  if (!CRYPTO_get_locking_callback()) {
    // Initialize the OpenSSL mutexes. We intentionally leak these, so ignore
    // LSAN warnings.
    debug::ScopedLeakCheckDisabler d;
    int num_locks = CRYPTO_num_locks();
    CHECK(!kCryptoLocks);
    kCryptoLocks = new Mutex[num_locks];

    // Callbacks used by OpenSSL required in a multi-threaded setting.
    CRYPTO_set_locking_callback(LockingCB);
  }

  g_ssl_is_initialized = true;
}

} // anonymous namespace

Status DisableOpenSSLInitialization() {
  if (g_disable_ssl_init) return Status::OK();
  if (g_ssl_is_initialized) {
    return Status::IllegalState("SSL already initialized. Initialization can only be disabled "
                                "before first usage.");
  }
  RETURN_NOT_OK(CheckOpenSSLInitialized());
  g_disable_ssl_init = true;
  return Status::OK();
}

void InitializeOpenSSL() {
  static std::once_flag ssl_once;
  std::call_once(ssl_once, DoInitializeOpenSSL);
}

string GetOpenSSLErrors() {
  ostringstream serr;
  uint32_t l;
  int line, flags;
  const char *file, *data;
  bool is_first = true;
  while ((l = ERR_get_error_line_data(&file, &line, &data, &flags)) != 0) {
    if (is_first) {
      is_first = false;
    } else {
      serr << " ";
    }

    char buf[256];
    ERR_error_string_n(l, buf, sizeof(buf));
    serr << buf << ":" << file << ":" << line;
    if (flags & ERR_TXT_STRING) {
      serr << ":" << data;
    }
  }
  return serr.str();
}

string GetSSLErrorDescription(int error_code) {
  switch (error_code) {
    case SSL_ERROR_NONE: return "";
    case SSL_ERROR_ZERO_RETURN: return "SSL_ERROR_ZERO_RETURN";
    case SSL_ERROR_WANT_READ: return "SSL_ERROR_WANT_READ";
    case SSL_ERROR_WANT_WRITE: return "SSL_ERROR_WANT_WRITE";
    case SSL_ERROR_WANT_CONNECT: return "SSL_ERROR_WANT_CONNECT";
    case SSL_ERROR_WANT_ACCEPT: return "SSL_ERROR_WANT_ACCEPT";
    case SSL_ERROR_WANT_X509_LOOKUP: return "SSL_ERROR_WANT_X509_LOOKUP";
    case SSL_ERROR_SYSCALL: {
      string queued_error = GetOpenSSLErrors();
      if (!queued_error.empty()) {
        return queued_error;
      }
      return kudu::ErrnoToString(errno);
    };
    default: return GetOpenSSLErrors();
  }
}

const string& DataFormatToString(DataFormat fmt) {
  static const string kStrFormatUnknown = "UNKNOWN";
  static const string kStrFormatDer = "DER";
  static const string kStrFormatPem = "PEM";
  switch (fmt) {
    case DataFormat::DER:
      return kStrFormatDer;
    case DataFormat::PEM:
      return kStrFormatPem;
    default:
      return kStrFormatUnknown;
  }
}

} // namespace security
} // namespace kudu
