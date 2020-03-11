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

#include "kudu/security/init.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <string>
#include <type_traits>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <krb5/krb5.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/security/kinit_context.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

#if defined(__APPLE__)
// Almost all functions in the krb5 API are marked as deprecated in favor
// of GSS.framework in macOS.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif // #if defined(__APPLE__)

#ifndef __APPLE__
static constexpr bool kDefaultSystemAuthToLocal = true;
#else
// macOS's Heimdal library has a no-op implementation of
// krb5_aname_to_localname, so instead we just use the simple
// implementation.
static constexpr bool kDefaultSystemAuthToLocal = false;
#endif

DEFINE_bool(use_system_auth_to_local, kDefaultSystemAuthToLocal,
            "When enabled, use the system krb5 library to map Kerberos principal "
            "names to local (short) usernames. If not enabled, the first component "
            "of the principal will be used as the short name. For example, "
            "'kudu/foo.example.com@EXAMPLE' will map to 'kudu'.");
TAG_FLAG(use_system_auth_to_local, advanced);

DEFINE_string(principal, "kudu/_HOST",
              "Kerberos principal that this daemon will log in as. The special token "
              "_HOST will be replaced with the FQDN of the local host.");
TAG_FLAG(principal, experimental);
// This is currently tagged as unsafe because there is no way for users to configure
// clients to expect a non-default principal. As such, configuring a server to login
// as a different one would end up with a cluster that can't be connected to.
// See KUDU-1884.
TAG_FLAG(principal, unsafe);

DEFINE_string(keytab_file, "",
              "Path to the Kerberos Keytab file for this server. Specifying a "
              "keytab file will cause the server to kinit, and enable Kerberos "
              "to be used to authenticate RPC connections.");
TAG_FLAG(keytab_file, stable);

using std::mt19937;
using std::random_device;
using std::string;
using std::uniform_int_distribution;
using std::uniform_real_distribution;
using strings::Substitute;

namespace kudu {
namespace security {

// Global instance of the context used by the kinit/reacquire thread.
KinitContext* g_kinit_ctx;

namespace {

// Global context for usage of the Krb5 library.
krb5_context g_krb5_ctx;

// This lock is used to avoid a race while reacquiring the kerberos ticket.
// The race can occur between the time we reinitialize the cache and the
// time when we actually store the new credentials back in the cache.
RWMutex* g_kerberos_reinit_lock;

Status Krb5CallToStatus(krb5_context ctx, krb5_error_code code) {
  if (code == 0) return Status::OK();

  std::unique_ptr<const char, std::function<void(const char*)>> err_msg(
      krb5_get_error_message(ctx, code),
      std::bind(krb5_free_error_message, ctx, std::placeholders::_1));
  return Status::RuntimeError(err_msg.get());
}
#define KRB5_RETURN_NOT_OK_PREPEND(call, prepend) \
  RETURN_NOT_OK_PREPEND(Krb5CallToStatus(g_krb5_ctx, (call)), (prepend))


void InitKrb5Ctx() {
  static std::once_flag once;
  std::call_once(once, [&]() {
      CHECK_EQ(krb5_init_context(&g_krb5_ctx), 0);
      // Work around the lack of thread safety in krb5_parse_name() by implicitly
      // initializing g_krb5_ctx->default_realm once. The assumption is that this
      // function is called once in a single thread environment during initialization.
      //
      // TODO(KUDU-2706): Fix unsafe sharing of 'g_krb5_ctx'.
      // According to Kerberos documentation
      // (https://github.com/krb5/krb5/blob/master/doc/threads.txt), any use of
      // krb5_context must be confined to one thread at a time by the application code.
      // The current way of sharing of 'g_krb5_ctx' between threads is actually unsafe.
      char* unused_realm;
      CHECK_EQ(krb5_get_default_realm(g_krb5_ctx, &unused_realm), 0);
      krb5_free_default_realm(g_krb5_ctx, unused_realm);

      g_kerberos_reinit_lock = new RWMutex(RWMutex::Priority::PREFER_WRITING);
    });
}

// Port of the data_eq() implementation from krb5/k5-int.h
inline int data_eq(krb5_data d1, krb5_data d2) {
    return (d1.length == d2.length && !memcmp(d1.data, d2.data, d1.length));
}

// Port of the data_eq_string() implementation from krb5/k5-int.h
inline int data_eq_string(krb5_data d, const char *s) {
    return (d.length == strlen(s) && !memcmp(d.data, s, d.length));
}

Status Krb5UnparseName(krb5_principal princ, string* name) {
  char* c_name;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_unparse_name(g_krb5_ctx, princ, &c_name),
                             "krb5_unparse_name");
  SCOPED_CLEANUP({
      krb5_free_unparsed_name(g_krb5_ctx, c_name);
    });
  *name = c_name;
  return Status::OK();
}

// Periodically calls DoRenewal().
void RenewThread() {
  uint32_t failure_retries = 0;
  while (true) {
    // This thread is run immediately after the first Kinit, so sleep first.
    int64_t renew_interval_s = g_kinit_ctx->GetNextRenewInterval(failure_retries);
    if (failure_retries > 0) {
      // Log in the abnormal case where something failed.
      LOG(INFO) << Substitute("Renew thread sleeping after $0 failures for $1s",
          failure_retries, renew_interval_s);
    }
    SleepFor(MonoDelta::FromSeconds(renew_interval_s));

    Status s = g_kinit_ctx->DoRenewal();
    WARN_NOT_OK(s, "Kerberos reacquire error: ");
    if (!s.ok()) {
      ++failure_retries;
    } else {
      failure_retries = 0;
    }
  }
}
} // anonymous namespace

KinitContext::KinitContext() {}

KinitContext::~KinitContext() {
  // Free memory associated with these objects.
  if (principal_ != nullptr) krb5_free_principal(g_krb5_ctx, principal_);
  if (keytab_ != nullptr) krb5_kt_close(g_krb5_ctx, keytab_);
  if (ccache_ != nullptr) krb5_cc_close(g_krb5_ctx, ccache_);
  if (opts_ != nullptr) krb5_get_init_creds_opt_free(g_krb5_ctx, opts_);
}

int32_t KinitContext::GetNextRenewInterval(uint32_t num_retries) {
  int32_t time_remaining = ticket_end_timestamp_ - time(nullptr);

  // If the last ticket reacqusition was a failure, we back off our retry attempts exponentially.
  if (num_retries > 0) return GetBackedOffRenewInterval(time_remaining, num_retries);

  // If the time remaining between now and ticket expiry is:
  // * > 10 minutes:   We attempt to reacquire the ticket between 5 seconds and 5 minutes before the
  //                   ticket expires.
  // * 5 - 10 minutes: We attempt to reacquire the ticket betwen 5 seconds and 1 minute before the
  //                   ticket expires.
  // * < 5 minutes:    Attempt to reacquire the ticket every 'time_remaining'.
  // The jitter is added to make sure that every server doesn't flood the KDC at the same time.
  random_device rd;
  mt19937 generator(rd());
  if (time_remaining > 600) {
    uniform_int_distribution<> dist(5, 300);
    return time_remaining - dist(generator);
  } else if (time_remaining > 300) {
    uniform_int_distribution<> dist(5, 60);
    return time_remaining - dist(generator);
  }
  return time_remaining;
}

int32_t KinitContext::GetBackedOffRenewInterval(int32_t time_remaining, uint32_t num_retries) {
  // The minimum sleep interval after a failure will be 60 seconds.
  int32_t next_interval = std::max(time_remaining, 60);
  // Don't back off more than 30 minutes.
  int32_t base_time = std::min(next_interval * (1 << num_retries), 1800);
  random_device rd;
  mt19937 generator(rd());
  uniform_real_distribution<> dist(0.5, 1.5);
  return static_cast<int32_t>(base_time * dist(generator));
}

Status KinitContext::DoRenewal() {
  bool found_in_cache;
  RETURN_NOT_OK(DoRenewalInternal(&found_in_cache));
  if (!found_in_cache) {
    // Cache appears to be corrupted somehow - redo the kinit and retry.
    RETURN_NOT_OK_PREPEND(KinitInternal(), "redoing kinit after error");
    RETURN_NOT_OK(DoRenewalInternal(&found_in_cache));
    if (!found_in_cache) {
      return Status::RuntimeError("Could not find TGT in cache after kinit");
    }
  }
  return Status::OK();
}

Status KinitContext::DoRenewalInternal(bool* found_in_cache) {
  *found_in_cache = false;
  krb5_cc_cursor cursor;
  // Setup a cursor to iterate through the credential cache.
  Status status =
      Krb5CallToStatus(g_krb5_ctx, krb5_cc_start_seq_get(g_krb5_ctx, ccache_, &cursor));
  if (!status.ok()) {
    LOG(WARNING) << Substitute("Error while opening credential cache '$0' of type $1: $2",
        krb5_cc_get_name(g_krb5_ctx, ccache_), krb5_cc_get_type(g_krb5_ctx, ccache_),
        status.ToString());
    return Status::OK();
  }
  SCOPED_CLEANUP({
      krb5_cc_end_seq_get(g_krb5_ctx, ccache_, &cursor); });

  krb5_creds creds;
  memset(&creds, 0, sizeof(krb5_creds));

  krb5_error_code rc;
  // Iterate through the credential cache.
  while (!(rc = krb5_cc_next_cred(g_krb5_ctx, ccache_, &cursor, &creds))) {
    SCOPED_CLEANUP({
        krb5_free_cred_contents(g_krb5_ctx, &creds); });
    if (krb5_is_config_principal(g_krb5_ctx, creds.server)) continue;

    // We only want to reacquire the TGT (Ticket Granting Ticket). Ignore all other tickets.
    // This follows the same format as is_local_tgt() from krb5:src/clients/klist/klist.c
    if (creds.server->length != 2 ||
        data_eq(creds.server->data[1], principal_->realm) == 0 ||
        data_eq_string(creds.server->data[0], KRB5_TGS_NAME) == 0 ||
        data_eq(creds.server->realm, principal_->realm) == 0) {
      continue;
    }
    *found_in_cache = true;

    krb5_creds new_creds;
    memset(&new_creds, 0, sizeof(krb5_creds));
    SCOPED_CLEANUP({
        krb5_free_cred_contents(g_krb5_ctx, &new_creds); });
    // Acquire a new ticket using the keytab. This ticket will automatically be put into the
    // credential cache.
    {
      std::lock_guard<RWMutex> l(*g_kerberos_reinit_lock);
      KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_keytab(g_krb5_ctx, &new_creds, principal_,
                                                            keytab_, 0 /* valid from now */,
                                                            nullptr /* TKT service name */,
                                                            opts_),
                                 "Reacquire error: unable to login from keytab");
#if !defined(HAVE_KRB5_GET_INIT_CREDS_OPT_SET_OUT_CCACHE)
      // Heimdal krb5 doesn't have the 'krb5_get_init_creds_opt_set_out_ccache' option,
      // so use this alternate route.
      KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_initialize(g_krb5_ctx, ccache_, principal_),
                                 "Reacquire error: could not init ccache");

      KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_store_cred(g_krb5_ctx, ccache_, &new_creds),
                                 "Reacquire error: could not store creds in cache");
#endif
    }
    LOG(INFO) << "Successfully reacquired a new kerberos TGT";
    ticket_end_timestamp_ = new_creds.times.endtime;
    break;
  }
  if (!*found_in_cache) {
    LOG(WARNING) << Substitute(
        "Could not find kerberos principal in credential cache '$0' of type $1",
        krb5_cc_get_name(g_krb5_ctx, ccache_), krb5_cc_get_type(g_krb5_ctx, ccache_));
  }
  return Status::OK();
}

Status KinitContext::Kinit(const string& keytab_path, const string& principal) {
  InitKrb5Ctx();

  // Parse the principal
  KRB5_RETURN_NOT_OK_PREPEND(krb5_parse_name(g_krb5_ctx, principal.c_str(), &principal_),
                             "could not parse principal");

  KRB5_RETURN_NOT_OK_PREPEND(krb5_kt_resolve(g_krb5_ctx, keytab_path.c_str(), &keytab_),
                             "unable to resolve keytab");

  KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_default(g_krb5_ctx, &ccache_),
                             "unable to get default credentials cache");

  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_opt_alloc(g_krb5_ctx, &opts_),
                             "unable to allocate get_init_creds_opt struct");
  return KinitInternal();
}

Status KinitContext::KinitInternal() {
#if defined(HAVE_KRB5_GET_INIT_CREDS_OPT_SET_OUT_CCACHE)
  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_opt_set_out_ccache(g_krb5_ctx, opts_, ccache_),
                             "unable to set init_creds options");
#endif

  krb5_creds creds;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_keytab(g_krb5_ctx, &creds, principal_, keytab_,
                                                        0 /* valid from now */,
                                                        nullptr /* TKT service name */, opts_),
                             "unable to login from keytab");
  SCOPED_CLEANUP({
      krb5_free_cred_contents(g_krb5_ctx, &creds); });

  ticket_end_timestamp_ = creds.times.endtime;

#if !defined(HAVE_KRB5_GET_INIT_CREDS_OPT_SET_OUT_CCACHE)
  // Heimdal krb5 doesn't have the 'krb5_get_init_creds_opt_set_out_ccache' option,
  // so use this alternate route.
  KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_initialize(g_krb5_ctx, ccache_, principal_),
                             "could not init ccache");

  KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_store_cred(g_krb5_ctx, ccache_, &creds),
                             "could not store creds in cache");
#endif

  // Convert the logged-in principal back to a string. This may be different than
  // 'principal', since the default realm will be filled in based on the Kerberos
  // configuration if not originally specified.
  RETURN_NOT_OK_PREPEND(Krb5UnparseName(principal_, &principal_str_),
                        "could not stringify the logged-in principal");
  RETURN_NOT_OK_PREPEND(MapPrincipalToLocalName(principal_str_, &username_str_),
                        "could not map own logged-in principal to a short username");

  LOG(INFO) << "Logged in from keytab as " << principal_str_
            << " (short username " << username_str_ << ")";

  return Status::OK();
}

RWMutex* KerberosReinitLock() {
  return g_kerberos_reinit_lock;
}

Status CanonicalizeKrb5Principal(std::string* principal) {
  InitKrb5Ctx();
  krb5_principal princ;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_parse_name(g_krb5_ctx, principal->c_str(), &princ),
                             "could not parse principal");
  SCOPED_CLEANUP({
      krb5_free_principal(g_krb5_ctx, princ);
    });
  RETURN_NOT_OK_PREPEND(Krb5UnparseName(princ, principal),
                        "failed to convert principal back to string");
  return Status::OK();
}

Status MapPrincipalToLocalName(const std::string& principal, std::string* local_name) {
  InitKrb5Ctx();
  krb5_principal princ;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_parse_name(g_krb5_ctx, principal.c_str(), &princ),
                             "could not parse principal");
  SCOPED_CLEANUP({
      krb5_free_principal(g_krb5_ctx, princ);
    });
  char buf[1024];
  krb5_error_code rc = KRB5_LNAME_NOTRANS;
  if (FLAGS_use_system_auth_to_local) {
    rc = krb5_aname_to_localname(g_krb5_ctx, princ, arraysize(buf), buf);
  }
  if (rc == KRB5_LNAME_NOTRANS || rc == KRB5_PLUGIN_NO_HANDLE) {
    // No name mapping specified, or krb5-based name mapping is disabled.
    //
    // We fall back to simply taking the first component of the principal, for
    // compatibility with the default behavior of Hadoop.
    //
    // NOTE: KRB5_PLUGIN_NO_HANDLE isn't typically expected here, but works around
    // a bug in SSSD's auth_to_local implementation: https://pagure.io/SSSD/sssd/issue/3459
    //
    // TODO(todd): we should support custom configured auth-to-local mapping, since
    // most Hadoop ecosystem components do not load them from krb5.conf.
    if (princ->length > 0) {
      local_name->assign(princ->data[0].data, princ->data[0].length);
      return Status::OK();
    }
    return Status::NotFound("unable to find first component of principal");
  }
  if (rc == KRB5_CONFIG_NOTENUFSPACE) {
    return Status::InvalidArgument("mapped username too large");
  }
  KRB5_RETURN_NOT_OK_PREPEND(rc, "krb5_aname_to_localname");
  if (strlen(buf) == 0) {
    return Status::InvalidArgument("principal mapped to empty username");
  }
  local_name->assign(buf);
  return Status::OK();
}

Status GetConfiguredPrincipal(const string& in_principal, string* out_principal) {
  *out_principal = in_principal;
  static const auto& kHostToken = "_HOST";
  if (in_principal.find(kHostToken) != string::npos) {
    string hostname;
    // Try to fill in either the FQDN or hostname.
    if (!GetFQDN(&hostname).ok()) {
      RETURN_NOT_OK(GetHostname(&hostname));
    }
    // Hosts in principal names are canonicalized to lower-case.
    std::transform(hostname.begin(), hostname.end(), hostname.begin(), tolower);
    GlobalReplaceSubstring(kHostToken, hostname, out_principal);
  }
  return Status::OK();
}

boost::optional<string> GetLoggedInPrincipalFromKeytab() {
  if (!g_kinit_ctx) return boost::none;
  return g_kinit_ctx->principal_str();
}

boost::optional<string> GetLoggedInUsernameFromKeytab() {
  if (!g_kinit_ctx) return boost::none;
  return g_kinit_ctx->username_str();
}

Status Krb5ParseName(const string& principal, string* service_name,
                     string* hostname, string* realm) {
  krb5_principal princ;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_parse_name(g_krb5_ctx, principal.c_str(), &princ),
      "could not parse principal");
  SCOPED_CLEANUP({
      krb5_free_principal(g_krb5_ctx, princ);
    });
  if (princ->length != 2) {
    return Status::InvalidArgument(Substitute("$0: principal should include "
                                              "service name, hostname and realm", principal));
  }
  realm->assign(princ->realm.data, princ->realm.length);
  service_name->assign(princ->data[0].data, princ->data[0].length);
  hostname->assign(princ->data[1].data, princ->data[1].length);
  return Status::OK();
}

Status InitKerberosForServer(const std::string& raw_principal, const std::string& keytab_file,
    const std::string& krb5ccname, bool disable_krb5_replay_cache) {
  if (keytab_file.empty()) return Status::OK();

  setenv("KRB5CCNAME", krb5ccname.c_str(), 1);
  setenv("KRB5_KTNAME", keytab_file.c_str(), 1);

  if (disable_krb5_replay_cache) {
    // KUDU-1897: disable the Kerberos replay cache. The KRPC protocol includes a
    // per-connection server-generated nonce to protect against replay attacks
    // when authenticating via Kerberos. The replay cache has many performance and
    // implementation issues.
    setenv("KRB5RCACHETYPE", "none", 1);
  }

  g_kinit_ctx = new KinitContext();
  string configured_principal;
  RETURN_NOT_OK(GetConfiguredPrincipal(raw_principal, &configured_principal));
  RETURN_NOT_OK_PREPEND(g_kinit_ctx->Kinit(
      keytab_file, configured_principal), "unable to kinit");

  scoped_refptr<Thread> reacquire_thread;
  // Start the reacquire thread.
  RETURN_NOT_OK(Thread::Create("kerberos", "reacquire thread", &RenewThread, &reacquire_thread));

  return Status::OK();
}

} // namespace security
} // namespace kudu

#if defined(__APPLE__)
#pragma GCC diagnostic pop
#endif // #if defined(__APPLE__)
