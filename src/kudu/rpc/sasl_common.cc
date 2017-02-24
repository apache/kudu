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

#include "kudu/rpc/sasl_common.h"

#include <string.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <string>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <regex.h>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/once.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/rpc/constants.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mutex.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/security/init.h"

using std::set;

DECLARE_string(keytab_file);

namespace kudu {
namespace rpc {

const char* const kSaslMechPlain = "PLAIN";
const char* const kSaslMechGSSAPI = "GSSAPI";
extern const size_t kSaslMaxOutBufLen = 1024;

// See WrapSaslCall().
static __thread string* g_auth_failure_capture = nullptr;

// Determine whether initialization was ever called
static Status sasl_init_status = Status::OK();
static bool sasl_is_initialized = false;

// If true, then we expect someone else has initialized SASL.
static bool g_disable_sasl_init = false;

// Output Sasl messages.
// context: not used.
// level: logging level.
// message: message to output;
static int SaslLogCallback(void* context, int level, const char* message) {

  if (message == nullptr) return SASL_BADPARAM;

  switch (level) {
    case SASL_LOG_NONE:
      break;

    case SASL_LOG_ERR:
      LOG(ERROR) << "SASL: " << message;
      break;

    case SASL_LOG_WARN:
      LOG(WARNING) << "SASL: " << message;
      break;

    case SASL_LOG_NOTE:
      LOG(INFO) << "SASL: " << message;
      break;

    case SASL_LOG_FAIL:
      // Capture authentication failures in a thread-local to be picked up
      // by WrapSaslCall() below.
      VLOG(1) << "SASL: " << message;
      if (g_auth_failure_capture) {
        *g_auth_failure_capture = message;
      }
      break;

    case SASL_LOG_DEBUG:
      VLOG(1) << "SASL: " << message;
      break;

    case SASL_LOG_TRACE:
    case SASL_LOG_PASS:
      VLOG(3) << "SASL: " << message;
      break;
  }

  return SASL_OK;
}

// Get Sasl option.
// context: not used
// plugin_name: name of plugin for which an option is being requested.
// option: option requested
// result: set to result which persists until next getopt in same thread,
//         unchanged if option not found
// len: length of the result
// Return SASL_FAIL if the option is not handled, this does not fail the handshake.
static int SaslGetOption(void* context, const char* plugin_name, const char* option,
                         const char** result, unsigned* len) {
  // Handle Sasl Library options
  if (plugin_name == nullptr) {
    // Return the logging level that we want the sasl library to use.
    if (strcmp("log_level", option) == 0) {
      int level = SASL_LOG_NOTE;
      if (VLOG_IS_ON(1)) {
        level = SASL_LOG_DEBUG;
      } else if (VLOG_IS_ON(3)) {
        level = SASL_LOG_TRACE;
      }
      // The library's contract for this method is that the caller gets to keep
      // the returned buffer until the next call by the same thread, so we use a
      // threadlocal for the buffer.
      static __thread char buf[4];
      snprintf(buf, arraysize(buf), "%d", level);
      *result = buf;
      if (len != nullptr) *len = strlen(buf);
      return SASL_OK;
    }
    // Options can default so don't complain.
    VLOG(4) << "SaslGetOption: Unknown library option: " << option;
    return SASL_FAIL;
  }
  VLOG(4) << "SaslGetOption: Unknown plugin: " << plugin_name;
  return SASL_FAIL;
}

// Array of callbacks for the sasl library.
static sasl_callback_t callbacks[] = {
  { SASL_CB_LOG, reinterpret_cast<int (*)()>(&SaslLogCallback), nullptr },
  { SASL_CB_GETOPT, reinterpret_cast<int (*)()>(&SaslGetOption), nullptr },
  { SASL_CB_LIST_END, nullptr, nullptr }
  // TODO(todd): provide a CANON_USER callback? This is necessary if we want
  // to support some kind of auth-to-local mapping of Kerberos principals
  // to local usernames. See Impala's implementation for inspiration.
};


// SASL requires mutexes for thread safety, but doesn't implement
// them itself. So, we have to hook them up to our mutex implementation.
static void* SaslMutexAlloc() {
  return static_cast<void*>(new Mutex());
}
static void SaslMutexFree(void* m) {
  delete static_cast<Mutex*>(m);
}
static int SaslMutexLock(void* m) {
  static_cast<Mutex*>(m)->lock();
  return 0; // indicates success.
}
static int SaslMutexUnlock(void* m) {
  static_cast<Mutex*>(m)->unlock();
  return 0; // indicates success.
}

namespace internal {
void SaslSetMutex() {
  sasl_set_mutex(&SaslMutexAlloc, &SaslMutexLock, &SaslMutexUnlock, &SaslMutexFree);
}
} // namespace internal

// Sasl initialization detection methods. The OS X SASL library doesn't define
// the sasl_global_utils symbol, so we have to use less robust methods of
// detection.
#if defined(__APPLE__)
static bool SaslIsInitialized() {
  return sasl_global_listmech() != nullptr;
}
static bool SaslMutexImplementationProvided() {
  return SaslIsInitialized();
}
#else

// This symbol is exported by the SASL library but not defined
// in the headers. It's marked as an API in the library source,
// so seems safe to rely on.
extern "C" sasl_utils_t* sasl_global_utils;
static bool SaslIsInitialized() {
  return sasl_global_utils != nullptr;
}
static bool SaslMutexImplementationProvided() {
  if (!SaslIsInitialized()) return false;
  void* m = sasl_global_utils->mutex_alloc();
  sasl_global_utils->mutex_free(m);
  // The default implementation of mutex_alloc just returns the constant pointer 0x1.
  // This is a bit of an ugly heuristic, but seems unlikely that anyone would ever
  // provide a valid implementation that returns an invalid pointer value.
  return m != reinterpret_cast<void*>(1);
}
#endif

// Actually perform the initialization for the SASL subsystem.
// Meant to be called via GoogleOnceInit().
static void DoSaslInit() {
  VLOG(3) << "Initializing SASL library";

  bool sasl_initialized = SaslIsInitialized();
  if (sasl_initialized && !g_disable_sasl_init) {
    LOG(WARNING) << "SASL was initialized prior to Kudu's initialization. Skipping "
                 << "initialization. Call kudu::client::DisableSaslInitialization() "
                 << "to suppress this message.";
    g_disable_sasl_init = true;
  }

  if (g_disable_sasl_init) {
    if (!sasl_initialized) {
      sasl_init_status = Status::RuntimeError(
          "SASL initialization was disabled, but SASL was not externally initialized.");
      return;
    }
    if (!SaslMutexImplementationProvided()) {
      LOG(WARNING)
          << "SASL appears to be initialized by code outside of Kudu "
          << "but was not provided with a mutex implementation! If "
          << "manually initializing SASL, use sasl_set_mutex(3).";
    }
    return;
  }
  internal::SaslSetMutex();
  int result = sasl_client_init(&callbacks[0]);
  if (result != SASL_OK) {
    sasl_init_status = Status::RuntimeError("Could not initialize SASL client",
                                            sasl_errstring(result, nullptr, nullptr));
    return;
  }

  result = sasl_server_init(&callbacks[0], kSaslAppName);
  if (result != SASL_OK) {
    sasl_init_status = Status::RuntimeError("Could not initialize SASL server",
                                            sasl_errstring(result, nullptr, nullptr));
    return;
  }

  sasl_is_initialized = true;
}

Status DisableSaslInitialization() {
  if (g_disable_sasl_init) return Status::OK();
  if (sasl_is_initialized) {
    return Status::IllegalState("SASL already initialized. Initialization can only be disabled "
                                "before first usage.");
  }
  g_disable_sasl_init = true;
  return Status::OK();
}

Status SaslInit() {
  // Only execute SASL initialization once
  static GoogleOnceType once = GOOGLE_ONCE_INIT;
  GoogleOnceInit(&once, &DoSaslInit);
  return sasl_init_status;
}

static string CleanSaslError(const string& err) {
  // In the case of GSS failures, we often get the actual error message
  // buried inside a bunch of generic cruft. Use a regexp to extract it
  // out. Note that we avoid std::regex because it appears to be broken
  // with older libstdcxx.
  static regex_t re;
  static std::once_flag once;

#if defined(__APPLE__)
  static const char* kGssapiPattern = "GSSAPI Error:  Miscellaneous failure \\(see text \\((.+)\\)";
#else
  static const char* kGssapiPattern = "Unspecified GSS failure. +"
                                      "Minor code may provide more information +"
                                      "\\((.+)\\)";
#endif

  std::call_once(once, []{ CHECK_EQ(0, regcomp(&re, kGssapiPattern, REG_EXTENDED)); });
  regmatch_t matches[2];
  if (regexec(&re, err.c_str(), arraysize(matches), matches, 0) == 0) {
    return err.substr(matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so);
  }
  return err;
}

static string SaslErrDesc(int status, sasl_conn_t* conn) {
  string err;
  if (conn != nullptr) {
    err = sasl_errdetail(conn);
  } else {
    err = sasl_errstring(status, nullptr, nullptr);
  }

  return CleanSaslError(err);
}

Status WrapSaslCall(sasl_conn_t* conn, const std::function<int()>& call) {
  // In many cases, the GSSAPI SASL plugin will generate a nice error
  // message as a message logged at SASL_LOG_FAIL logging level, but then
  // return a useless one in sasl_errstring(). So, we set a global thread-local
  // variable to capture any auth failure log message while we make the
  // call into the library.
  //
  // The thread-local thing is a bit of a hack, but the logging callback
  // is set globally rather than on a per-connection basis.
  string err;
  g_auth_failure_capture = &err;

  // Take the 'kerberos_reinit_lock' here to avoid a possible race with ticket renewal.
  bool kerberos_supported = !FLAGS_keytab_file.empty();
  if (kerberos_supported) kudu::security::KerberosReinitLock()->ReadLock();
  int rc = call();
  if (kerberos_supported) kudu::security::KerberosReinitLock()->ReadUnlock();
  g_auth_failure_capture = nullptr;

  switch (rc) {
    case SASL_OK:
      return Status::OK();
    case SASL_CONTINUE:
      return Status::Incomplete("");
    case SASL_FAIL:      // Generic failure (encompasses missing krb5 credentials).
    case SASL_BADAUTH:   // Authentication failure.
    case SASL_BADMAC:    // Decode failure.
    case SASL_NOAUTHZ:   // Authorization failure.
    case SASL_NOUSER:    // User not found.
    case SASL_WRONGMECH: // Server doesn't support requested mechanism.
    case SASL_BADSERV: { // Server failed mutual authentication.
      if (err.empty()) {
        err = SaslErrDesc(rc, conn);
      } else {
        err = CleanSaslError(err);
      }
      return Status::NotAuthorized(err);
    }
    default:
      return Status::RuntimeError(SaslErrDesc(rc, conn));
  }
}

Status SaslEncode(sasl_conn_t* conn, const std::string& plaintext, std::string* encoded) {
  size_t offset = 0;

  // The SASL library can only encode up to a maximum amount at a time, so we
  // have to call encode multiple times if our input is larger than this max.
  while (offset < plaintext.size()) {
    const char* out;
    unsigned out_len;
    size_t len = std::min(kSaslMaxOutBufLen, plaintext.size() - offset);

    RETURN_NOT_OK(WrapSaslCall(conn, [&]() {
        return sasl_encode(conn, plaintext.data() + offset, len, &out, &out_len);
    }));

    encoded->append(out, out_len);
    offset += len;
  }

  return Status::OK();
}

Status SaslDecode(sasl_conn_t* conn, const string& encoded, string* plaintext) {
  size_t offset = 0;

  // The SASL library can only decode up to a maximum amount at a time, so we
  // have to call decode multiple times if our input is larger than this max.
  while (offset < encoded.size()) {
    const char* out;
    unsigned out_len;
    size_t len = std::min(kSaslMaxOutBufLen, encoded.size() - offset);

    RETURN_NOT_OK(WrapSaslCall(conn, [&]() {
        return sasl_decode(conn, encoded.data() + offset, len, &out, &out_len);
    }));

    plaintext->append(out, out_len);
    offset += len;
  }

  return Status::OK();
}

string SaslIpPortString(const Sockaddr& addr) {
  string addr_str = addr.ToString();
  size_t colon_pos = addr_str.find(':');
  if (colon_pos != string::npos) {
    addr_str[colon_pos] = ';';
  }
  return addr_str;
}

set<SaslMechanism::Type> SaslListAvailableMechs() {
  set<SaslMechanism::Type> mechs;

  // Array of NULL-terminated strings. Array terminated with NULL.
  for (const char** mech_strings = sasl_global_listmech();
       mech_strings != nullptr && *mech_strings != nullptr;
       mech_strings++) {
    auto mech = SaslMechanism::value_of(*mech_strings);
    if (mech != SaslMechanism::INVALID) {
      mechs.insert(mech);
    }
  }
  return mechs;
}

sasl_callback_t SaslBuildCallback(int id, int (*proc)(void), void* context) {
  sasl_callback_t callback;
  callback.id = id;
  callback.proc = proc;
  callback.context = context;
  return callback;
}

Status EnableIntegrityProtection(sasl_conn_t* sasl_conn) {
  sasl_security_properties_t sec_props;
  memset(&sec_props, 0, sizeof(sec_props));
  sec_props.min_ssf = 1;
  sec_props.max_ssf = std::numeric_limits<sasl_ssf_t>::max();
  sec_props.maxbufsize = kSaslMaxOutBufLen;

  RETURN_NOT_OK_PREPEND(WrapSaslCall(sasl_conn, [&] () {
    return sasl_setprop(sasl_conn, SASL_SEC_PROPS, &sec_props);
  }), "failed to set SASL security properties");
  return Status::OK();
}

SaslMechanism::Type SaslMechanism::value_of(const string& mech) {
  if (boost::iequals(mech, "PLAIN")) {
    return PLAIN;
  }
  if (boost::iequals(mech, "GSSAPI")) {
    return GSSAPI;
  }
  return INVALID;
}

const char* SaslMechanism::name_of(SaslMechanism::Type val) {
  switch (val) {
    case PLAIN: return "PLAIN";
    case GSSAPI: return "GSSAPI";
    default:
      return "INVALID";
  }
}

} // namespace rpc
} // namespace kudu
