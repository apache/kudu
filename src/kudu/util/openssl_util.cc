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

#include "kudu/util/openssl_util.h"

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/rand.h> // IWYU pragma: keep

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <glog/raw_logging.h>

#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#include "kudu/util/debug/leakcheck_disabler.h"
#endif
#include "kudu/util/errno.h"
#include "kudu/util/flags.h"
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#include "kudu/util/mutex.h"
#endif
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#include "kudu/util/thread.h"
#endif

// Some ancient Linux distros have a strange arrangement with the actual version
// of the OpenSSL library reported both by the library runtime and the actual
// contents of the header files under /usr/include/openssl. In particular,
// Ubuntu 18.04.1 LTS has OpenSSL of version 1.1.1-1ubuntu2.1~18.04.13 which
// openssl's binary outputs 'OpenSSL 1.1.1  11 Sep 2018' when running
// 'openssl version', but the header files do not have OPENSSL_INIT_NO_ATEXIT
// macro defined.
#if OPENSSL_VERSION_NUMBER >= 0x10101000L && !defined(OPENSSL_INIT_NO_ATEXIT)
#define OPENSSL_INIT_NO_ATEXIT  0x00080000L
#endif

using std::ostringstream;
using std::string;
using std::vector;

namespace kudu {
namespace security {

namespace {

// Determine whether initialization was ever called.
//
// Thread safety:
// - written by DoInitializeOpenSSL (single-threaded, due to std::call_once)
// - read by IsOpenSSLInitialized (must not be concurrent with above)
bool g_ssl_is_initialized = false;

// If true, then we expect someone else has initialized SSL.
//
// Thread safety:
// - read by DoInitializeOpenSSL (single-threaded, due to std::call_once)
// - written by DisableOpenSSLInitialization (must not be concurrent with above)
bool g_disable_ssl_init = false;

// Whether the OpenSSL library is used in the context of a standalone
// application that controls what happens before and after its main() function.
// Kudu servers and the 'kudu' CLI utility are examples of such. This affects
// how the initialization and shutdown of the OpenSSL library are performed.
// Essentially, this is to decide whether to add OPENSSL_INIT_NO_ATEXIT option
// for OPENSSL_init_ssl(), and call OPENSSL_cleanup() in DoFinalizeOpenSSL().
bool g_is_standalone_init = false;

// Array of locks used by OpenSSL.
// We use an intentionally-leaked C-style array here to avoid non-POD static data.
//
// As of OpenSSL 1.1, locking callbacks are no longer used.
#if OPENSSL_VERSION_NUMBER < 0x10100000L
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

void ThreadIdCB(CRYPTO_THREADID* tid) {
  CRYPTO_THREADID_set_numeric(tid, Thread::UniqueThreadId());
}
#endif

void CheckFIPSMode() {
#if OPENSSL_VERSION_NUMBER < 0x30000000L
  int fips_mode = FIPS_mode();
#else
  int fips_mode = EVP_default_properties_is_fips_enabled(NULL);
#endif
  // If the environment variable KUDU_REQUIRE_FIPS_MODE is set to "1", we
  // check if FIPS approved mode is enabled. If not, we crash the process.
  // As this is used in clients as well, we can't use gflags to set this.
  if (GetBooleanEnvironmentVariable("KUDU_REQUIRE_FIPS_MODE")) {
    // NOTE: using RAW_CHECK() because this might be called before main()
    RAW_CHECK(fips_mode, "FIPS mode required by environment variable "
                         "KUDU_REQUIRE_FIPS_MODE, but it is not enabled");
  }
}

Status CheckOpenSSLInitialized() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  // Starting with OpenSSL 1.1.0, the old thread API became obsolete
  // (see changelist 2e52e7df5 in the OpenSSL upstream repo), and
  // CRYPTO_get_locking_callback() always returns nullptr. Also, the library
  // always initializes its internals for multi-threaded usage.
  // Another point is that starting with version 1.1.0, SSL_CTX_new()
  // initializes the OpenSSL library under the hood, so SSL_CTX_new() would
  // not return nullptr unless there was an error during the initialization
  // of the library. That makes this code in CheckOpenSSLInitialized() obsolete
  // starting with OpenSSL version 1.1.0.
  //
  // Starting with OpenSSL 1.1.0, there isn't a straightforward way to
  // determine whether the library has already been initialized if using just
  // the API (well, there is CRYPTO_secure_malloc_initialized() but that's just
  // for the crypto library and it's implementation-dependent). But from the
  // other side, the whole idea that this method should check whether the
  // library has already been initialized is not relevant anymore: even if it's
  // not yet initialized, the first call to SSL_CTX_new() (from, say,
  // TlsContext::Init()) will initialize the library under the hood, so the
  // library will be ready for multi-thread usage by Kudu.
  if (!CRYPTO_get_locking_callback()) {
    return Status::RuntimeError("Locking callback not initialized");
  }
  auto ctx = ssl_make_unique(SSL_CTX_new(SSLv23_method()));
  if (!ctx) {
    ERR_clear_error();
    return Status::RuntimeError(
        "SSL library appears uninitialized (cannot create SSL_CTX)");
  }
#endif
  CheckFIPSMode();
  return Status::OK();
}

void DoInitializeOpenSSL() {
  // If running in standalone mode, do not call _anything_ from the OpenSSL
  // library prior to initializing it with the necessary custom options below.
  // Otherwise, that might lead to OPENSSL_init_crypto()/OPENSSL_init_ssl()
  // being called from within the library itself, initializing it with
  // the default or other undesirable set of options. Doing so might result
  // in registering unnecessary atexit() handlers and other unexpected behavior.
  // See [1] and [2] for more details:
  //
  //   Numerous internal OpenSSL functions call OPENSSL_init_crypto().
  //   Therefore, in order to perform nondefault initialisation,
  //   OPENSSL_init_crypto() MUST be called by application code
  //   prior to any other OpenSSL function calls.
  //
  //   Numerous internal OpenSSL functions call OPENSSL_init_ssl().
  //   Therefore, in order to perform nondefault initialisation,
  //   OPENSSL_init_ssl() MUST be called by application code
  //   prior to any other OpenSSL function calls.
  //
  // [1] https://docs.openssl.org/1.1.1/man3/OPENSSL_init_crypto/
  // [2] https://docs.openssl.org/1.1.1/man3/OPENSSL_init_ssl/
  if (!g_is_standalone_init) {
    // In non-standalone mode (e.g., running as the Kudu C++ client library
    // in a user application), if the user's thread has left some error
    // around, clear it to avoid reporting unrelated 'ghost' errors later on.
    ERR_clear_error();
    SCOPED_OPENSSL_NO_PENDING_ERRORS;
  }
  if (g_disable_ssl_init) {
    return;
  }

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  // Set custom options for the OpenSSL library:
  // automatic loading of the libcrypto and the libssl error strings. That are
  // the default options, but their presence guarantees that any (unexpected)
  // follow-up requests to change the behavior by specifying
  // OPENSSL_INIT_NO_LOAD_CRYPTO_STRINGS and/or OPENSSL_INIT_NO_LOAD_SSL_STRINGS
  // will be ignored.
  int init_opt =
      OPENSSL_INIT_LOAD_CRYPTO_STRINGS |
      OPENSSL_INIT_LOAD_SSL_STRINGS;

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
  // KUDU-3635: add OPENSSL_INIT_NO_ATEXIT option for fine-grained control
  // over the OpenSSL library's lifecycle to prevent races with finalization
  // of the tcmalloc library's runtime. The OPENSSL_INIT_NO_ATEXIT option was
  // introduced in OpenSSL 1.1.1.
  if (g_is_standalone_init) {
    init_opt |= OPENSSL_INIT_NO_ATEXIT;
  }
#endif  // #if OPENSSL_VERSION_NUMBER >= 0x10101000L ...

  // The OPENSSL_init_ssl manpage [1] says "As of version 1.1.0 OpenSSL will
  // automatically allocate all resources it needs so no explicit initialisation
  // is required." However, eliding library initialization leads to a memory
  // leak in some versions of OpenSSL 1.1 when the first OpenSSL call is
  // ERR_peek_error (see [2] for details; the issue was addressed in OpenSSL
  // 1.1.0i (OPENSSL_VERSION_NUMBER 0x1010009f)). In Kudu this is often the
  // case due to prolific application of the SCOPED_OPENSSL_NO_PENDING_ERRORS
  // macro.
  //
  // Rather than determine whether this particular OpenSSL instance is
  // leak-free, we'll initialize the library explicitly.
  //
  // 1. https://docs.openssl.org/1.1.1/man3/OPENSSL_init_ssl/
  // 2. https://github.com/openssl/openssl/issues/5899
  //
  // NOTE: using RAW_CHECK() because this might be called before main().
  RAW_CHECK(1 == OPENSSL_init_ssl(init_opt, nullptr),
            "failed to initialize OpenSSL");
#else   // #if OPENSSL_VERSION_NUMBER >= 0x10100000L ...
  // Check that OpenSSL isn't already initialized. If it is, it's likely
  // we are embedded in (or embedding) another application/library which
  // initializes OpenSSL, and we risk installing conflicting callbacks
  // or crashing due to concurrent initialization attempts. In that case,
  // log a warning.
  auto ctx = ssl_make_unique(SSL_CTX_new(SSLv23_method()));
  if (ctx) {
    RAW_LOG(WARNING, "It appears that OpenSSL has been previously initialized by "
                     "code outside of Kudu. Please first properly initialize "
                     "OpenSSL for multi-threaded usage (setting thread callback "
                     "functions for OpenSSL of versions earlier than 1.1.0) and "
                     "then call kudu::client::DisableOpenSSLInitialization() "
                     "to avoid potential crashes due to conflicting initialization");
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
    RAW_CHECK(!kCryptoLocks, "crypto locks are already initialized");
    kCryptoLocks = new Mutex[num_locks];

    // Callbacks used by OpenSSL required in a multi-threaded setting.
    CRYPTO_set_locking_callback(LockingCB);

    CRYPTO_THREADID_set_callback(ThreadIdCB);
  }
#endif // #if OPENSSL_VERSION_NUMBER >= 0x10100000L ... #else ...
  CheckFIPSMode();
  g_ssl_is_initialized = true;
}

void DoFinalizeOpenSSL() {
  if (!g_ssl_is_initialized) {
    // If we haven't yet initialized the library, don't try to finalize it.
    return;
  }

  // In case the user's thread has left some error around, clear it.
  // Do so only in release builds, but catch corresponding programming errors
  // if anything is left on the error stack otherwise.
  {
#ifdef NDEBUG
    ERR_clear_error();
#endif
    // NOTE: it's important to call ScopedCheckNoPendingSSLErrors' destructor
    // before OPENSSL_cleanup() is invoked because the latter cleans up the
    // global state of the OpenSSL library, along with error stacks.
    SCOPED_OPENSSL_NO_PENDING_ERRORS;
  }
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
  if (g_is_standalone_init) {
    // If OPENSSL_INIT_NO_ATEXIT option was used for OPENSSL_init_ssl() or
    // OPENSSL_init_crypto(), it's time to perform the necessary cleanup.
    OPENSSL_cleanup();
  }
#endif
}

} // anonymous namespace

// Reads a STACK_OF(X509) from the BIO and returns it.
STACK_OF(X509)* PEM_read_STACK_OF_X509(BIO* bio, void* /* unused */, pem_password_cb* /* unused */,
    void* /* unused */) {
  // Extract information from the chain certificate.
  STACK_OF(X509_INFO)* info = PEM_X509_INFO_read_bio(bio, nullptr, nullptr, nullptr);
  if (!info) return nullptr;
  SCOPED_CLEANUP({
    sk_X509_INFO_pop_free(info, X509_INFO_free);
  });

  // Initialize the Stack.
  STACK_OF(X509)* sk = sk_X509_new_null();

  // Iterate through the chain certificate and add each one to the stack.
  for (int i = 0; i < sk_X509_INFO_num(info); ++i) {
    X509_INFO *stack_item = sk_X509_INFO_value(info, i);
    sk_X509_push(sk, stack_item->x509);
    // We don't want the ScopedCleanup to free the x509 certificates as well since we will
    // use it as a part of the STACK_OF(X509) object to be returned, so we set it to nullptr.
    // We will take the responsibility of freeing it when we are done with the STACK_OF(X509).
    stack_item->x509 = nullptr;
  }
  return sk;
}

// Writes a STACK_OF(X509) to the BIO.
int PEM_write_STACK_OF_X509(BIO* bio, STACK_OF(X509)* obj) {
  int chain_len = sk_X509_num(obj);
  // Iterate through the stack and add each one to the BIO.
  for (int i = 0; i < chain_len; ++i) {
    X509* cert_item = sk_X509_value(obj, i);
    int ret = PEM_write_bio_X509(bio, cert_item);
    if (ret <= 0) return ret;
  }
  return 1;
}

// Reads a single X509 certificate and returns a STACK_OF(X509) with the single certificate.
STACK_OF(X509)* DER_read_STACK_OF_X509(BIO* bio, void* /* unused */) {
  // We don't support chain certificates written in DER format.
  auto x = ssl_make_unique(d2i_X509_bio(bio, nullptr));
  if (!x) return nullptr;
  STACK_OF(X509)* sk = sk_X509_new_null();
  if (sk_X509_push(sk, x.get()) == 0) {
    return nullptr;
  }
  x.release();
  return sk;
}

// Writes a single X509 certificate that it gets from the STACK_OF(X509) 'obj'.
int DER_write_STACK_OF_X509(BIO* bio, STACK_OF(X509)* obj) {
  int chain_len = sk_X509_num(obj);
  // We don't support chain certificates written in DER format.
  DCHECK_EQ(chain_len, 1);
  X509* cert_item = sk_X509_value(obj, 0);
  if (cert_item == nullptr) return 0;
  return i2d_X509_bio(bio, cert_item);
}

void free_STACK_OF_X509(STACK_OF(X509)* sk) {
  sk_X509_pop_free(sk, X509_free);
}

Status DisableOpenSSLInitialization() {
  if (g_disable_ssl_init) {
    return Status::OK();
  }
  if (IsOpenSSLInitialized()) {
    return Status::IllegalState(
        "OpenSSL is already initialized. Initialization can only be disabled "
        "before first usage.");
  }
  RETURN_NOT_OK(CheckOpenSSLInitialized());
  g_disable_ssl_init = true;
  return Status::OK();
}

void SetStandaloneInit(bool is_standalone) {
  // No synchronization is necessary since this function is should be called
  // only during static initialization phase.
  g_is_standalone_init = is_standalone;
}

void InitializeOpenSSL() {
  static std::once_flag ssl_once;
  std::call_once(ssl_once, DoInitializeOpenSSL);
}

void FinalizeOpenSSL() {
  static std::once_flag flag;
  std::call_once(flag, DoFinalizeOpenSSL);
}

bool IsOpenSSLInitialized() {
  return g_ssl_is_initialized;
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

Status GetPasswordFromShellCommand(const string& cmd, string* password) {
  vector<string> argv = strings::Split(cmd, " ", strings::SkipEmpty());
  if (argv.empty()) {
    return Status::RuntimeError("invalid empty private key password command");
  }
  string stderr, stdout;
  Status s = Subprocess::Call(argv, "" /* stdin */, &stdout, &stderr);
  if (!s.ok()) {
    return Status::RuntimeError(strings::Substitute(
        "failed to run private key password command: $0", s.ToString()), stderr);
  }
  StripTrailingWhitespace(&stdout);
  *password = stdout;
  return Status::OK();
}

string GetProtocolName(const SSL* ssl) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  return SSL_get_version(ssl);
}

string GetCipherDescription(const SSL* ssl) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  const SSL_CIPHER* cipher = SSL_get_current_cipher(ssl);
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
