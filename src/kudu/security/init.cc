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

#include <krb5/krb5.h>

#include <algorithm>
#include <mutex>
#include <random>
#include <string>
#include <vector>

#include <boost/optional.hpp>

#include "kudu/gutil/strings/util.h"
#include "kudu/util/flags.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/thread.h"

DEFINE_string(keytab_file, "",
              "Path to the Kerberos Keytab file for this server. Specifying a "
              "keytab file will cause the server to kinit, and enable Kerberos "
              "to be used to authenticate RPC connections.");
TAG_FLAG(keytab_file, stable);

DEFINE_string(principal, "kudu/_HOST",
              "Kerberos principal that this daemon will log in as. The special token "
              "_HOST will be replaced with the FQDN of the local host.");
TAG_FLAG(principal, experimental);
// This is currently tagged as unsafe because there is no way for users to configure
// clients to expect a non-default principal. As such, configuring a server to login
// as a different one would end up with a cluster that can't be connected to.
// See KUDU-1884.
TAG_FLAG(principal, unsafe);

using std::mt19937;
using std::random_device;
using std::string;
using std::uniform_int_distribution;
using std::uniform_real_distribution;
using std::vector;

namespace kudu {
namespace security {

namespace {

class KinitContext;

// Global context for usage of the Krb5 library.
krb5_context g_krb5_ctx;

// Global instance of the context used by the kinit/renewal thread.
KinitContext* g_kinit_ctx;

// This lock is used to avoid a race while renewing the kerberos ticket.
// The race can occur between the time we reinitialize the cache and the
// time when we actually store the renewed credential back in the cache.
RWMutex* g_kerberos_reinit_lock;

class KinitContext {
 public:
  KinitContext();

  // Equivalent implementation of 'kinit -kt <keytab path> <principal>'.
  //
  // This logs in from the given keytab as the given principal, returning
  // RuntimeError if any part of this process fails.
  //
  // If the log-in is successful, then the default ticket cache is overwritten
  // with the credentials of the newly logged-in principal.
  Status Kinit(const string& keytab_path, const string& principal);

  // Acquires a new Ticket Granting Ticket (TGT).
  //
  // Renews the existing ticket if possible, or acquires a new Ticket Granting
  // Ticket (TGT).
  Status DoRenewal();

  // Calculates the next sleep interval based on the 'ticket_end_timestamp_' and
  // adds some jitter so that all the nodes do not hit the KDC at the same time.
  //
  // If 'num_retries' > 0, it calls GetBackedOffRenewInterval() to return a backed
  // off interval.
  int32_t GetNextRenewInterval(uint32_t num_retries);

  // Returns a value based on 'time_remaining' that increases exponentially with
  // 'num_retries', with a random jitter of +/- 0%-50% of that value.
  int32_t GetBackedOffRenewInterval(int32_t time_remaining, uint32_t num_retries);

  const string& principal_str() const { return principal_str_; }
  const string& username_str() const { return username_str_; }

 private:
  krb5_principal principal_;
  krb5_keytab keytab_;
  krb5_ccache ccache_;
  krb5_get_init_creds_opt* opts_;

  // The stringified principal and username that we are logged in as.
  string principal_str_, username_str_;

  // This is the time that the current TGT in use expires.
  int32_t ticket_end_timestamp_;
};

Status Krb5CallToStatus(krb5_context ctx, krb5_error_code code) {
  if (code == 0) return Status::OK();
  return Status::RuntimeError(krb5_get_error_message(ctx, code));
}
#define KRB5_RETURN_NOT_OK_PREPEND(call, prepend) \
  RETURN_NOT_OK_PREPEND(Krb5CallToStatus(g_krb5_ctx, (call)), (prepend))


void InitKrb5Ctx() {
  static std::once_flag once;
  std::call_once(once, [&]() {
      CHECK_EQ(krb5_init_context(&g_krb5_ctx), 0);
    });
}

KinitContext::KinitContext() {}

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
  auto cleanup_name = MakeScopedCleanup([&]() {
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
    SleepFor(MonoDelta::FromSeconds(g_kinit_ctx->GetNextRenewInterval(failure_retries)));

    Status s = g_kinit_ctx->DoRenewal();
    WARN_NOT_OK(s, "Kerberos reacquire error: ");
    if (!s.ok()) {
      ++failure_retries;
    } else {
      failure_retries = 0;
    }
  }
}

int32_t KinitContext::GetNextRenewInterval(uint32_t num_retries) {
  int32_t time_remaining = ticket_end_timestamp_ - time(nullptr);

  // If the last ticket renewal was a failure, we back off our retry attempts exponentially.
  if (num_retries > 0) return GetBackedOffRenewInterval(time_remaining, num_retries);

  // If the time remaining between now and ticket expiry is:
  // * > 10 minutes:   We attempt to renew the ticket between 5 seconds and 5 minutes before the
  //                   ticket expires.
  // * 5 - 10 minutes: We attempt to renew the ticket betwen 5 seconds and 1 minute before the
  //                   ticket expires.
  // * < 5 minutes:    Attempt to renew the ticket every 'time_remaining'.
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
  int32_t base_time = std::min(next_interval * (1 << num_retries), INT32_MAX);
  random_device rd;
  mt19937 generator(rd());
  uniform_real_distribution<> dist(0.5, 1.5);
  return static_cast<int32_t>(base_time * dist(generator));
}

Status KinitContext::DoRenewal() {

  krb5_cc_cursor cursor;
  // Setup a cursor to iterate through the credential cache.
  KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_start_seq_get(g_krb5_ctx, ccache_, &cursor),
                             "Failed to peek into ccache");
  auto cleanup_cursor = MakeScopedCleanup([&]() {
      krb5_cc_end_seq_get(g_krb5_ctx, ccache_, &cursor); });

  krb5_creds creds;
  memset(&creds, 0, sizeof(krb5_creds));

  krb5_error_code rc;
  // Iterate through the credential cache.
  while (!(rc = krb5_cc_next_cred(g_krb5_ctx, ccache_, &cursor, &creds))) {
    auto cleanup_creds = MakeScopedCleanup([&]() {
        krb5_free_cred_contents(g_krb5_ctx, &creds); });
    if (krb5_is_config_principal(g_krb5_ctx, creds.server)) continue;

    // We only want to renew the TGT (Ticket Granting Ticket). Ignore all other tickets.
    // This follows the same format as is_local_tgt() from krb5:src/clients/klist/klist.c
    if (creds.server->length != 2 ||
        data_eq(creds.server->data[1], principal_->realm) == 0 ||
        data_eq_string(creds.server->data[0], KRB5_TGS_NAME) == 0 ||
        data_eq(creds.server->realm, principal_->realm) == 0) {
      continue;
    }

    time_t now = time(nullptr);
    time_t ticket_expiry = creds.times.endtime;
    time_t renew_till = creds.times.renew_till;
    time_t renew_deadline = renew_till - 30;

    krb5_creds new_creds;
    memset(&new_creds, 0, sizeof(krb5_creds));
    auto cleanup_new_creds = MakeScopedCleanup([&]() {
        krb5_free_cred_contents(g_krb5_ctx, &new_creds); });
    // If the ticket has already expired or if there's only a short period before which the
    // renew window closes, we acquire a new ticket.
    if (ticket_expiry < now || renew_deadline < now) {
      // Acquire a new ticket using the keytab. This ticket will automatically be put into the
      // credential cache.
      {
        std::lock_guard<RWMutex> l(*g_kerberos_reinit_lock);
        KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_keytab(g_krb5_ctx, &new_creds, principal_,
                                                              keytab_, 0 /* valid from now */,
                                                              nullptr /* TKT service name */,
                                                              opts_),
                                   "Reacquire error: unable to login from keytab");
#ifdef __APPLE__
        // Heimdal krb5 doesn't have the 'krb5_get_init_creds_opt_set_out_ccache' option,
        // so use this alternate route.
        KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_initialize(g_krb5_ctx, ccache_, principal_),
                                   "Reacquire error: could not init ccache");

        KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_store_cred(g_krb5_ctx, ccache_, &creds),
                                   "Reacquire error: could not store creds in cache");
#endif
      }
      LOG(INFO) << "Successfully reacquired a new kerberos TGT";
    } else {
      // Renew existing ticket.
      KRB5_RETURN_NOT_OK_PREPEND(krb5_get_renewed_creds(g_krb5_ctx, &new_creds, principal_,
                                                        ccache_, nullptr),
                                 "Failed to renew ticket");

      {
        // Take the write lock here so that any connections undergoing negotiation have to wait
        // until the new credentials are placed in the cache.
        std::lock_guard<RWMutex> l(*g_kerberos_reinit_lock);
        // Clear existing credentials in cache.
        KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_initialize(g_krb5_ctx, ccache_, principal_),
                                   "Failed to re-initialize ccache");

        // Store the new credentials in the cache.
        KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_store_cred(g_krb5_ctx, ccache_, &new_creds),
                                   "Failed to store credentials in ccache");
      }
      LOG(INFO) << "Successfully renewed kerberos TGT";
    }
    ticket_end_timestamp_ = new_creds.times.endtime;
    break;
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

#ifndef __APPLE__
  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_opt_set_out_ccache(g_krb5_ctx, opts_, ccache_),
                             "unable to set init_creds options");
#endif

  krb5_creds creds;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_keytab(g_krb5_ctx, &creds, principal_, keytab_,
                                                        0 /* valid from now */,
                                                        nullptr /* TKT service name */, opts_),
                             "unable to login from keytab");
  auto cleanup_creds = MakeScopedCleanup([&]() {
      krb5_free_cred_contents(g_krb5_ctx, &creds); });

  ticket_end_timestamp_ = creds.times.endtime;

#ifdef __APPLE__
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

Status GetConfiguredPrincipal(string* principal) {
  string p = FLAGS_principal;
  string hostname;
  // Try to fill in either the FQDN or hostname.
  if (!GetFQDN(&hostname).ok()) {
    RETURN_NOT_OK(GetHostname(&hostname));
  }
  GlobalReplaceSubstring("_HOST", hostname, &p);
  *principal = p;
  return Status::OK();
}
} // anonymous namespace


RWMutex* KerberosReinitLock() {
  return g_kerberos_reinit_lock;
}

Status CanonicalizeKrb5Principal(std::string* principal) {
  InitKrb5Ctx();
  krb5_principal princ;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_parse_name(g_krb5_ctx, principal->c_str(), &princ),
                             "could not parse principal");
  auto cleanup = MakeScopedCleanup([&]() {
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
  auto cleanup = MakeScopedCleanup([&]() {
      krb5_free_principal(g_krb5_ctx, princ);
    });
  char buf[1024];
  krb5_error_code rc;
#ifndef __APPLE__
  rc = krb5_aname_to_localname(g_krb5_ctx, princ, arraysize(buf), buf);
#else
  // macOS's Heimdal library has a no-op implementation of
  // krb5_aname_to_localname, so instead we fall down to below and grab the
  // first component of the principal.
  rc = KRB5_LNAME_NOTRANS;
#endif
  if (rc == KRB5_LNAME_NOTRANS || rc == KRB5_PLUGIN_NO_HANDLE) {
    // No name mapping specified. We fall back to simply taking the first component
    // of the principal, for compatibility with the default behavior of Hadoop.
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

boost::optional<string> GetLoggedInPrincipalFromKeytab() {
  if (!g_kinit_ctx) return boost::none;
  return g_kinit_ctx->principal_str();
}

boost::optional<string> GetLoggedInUsernameFromKeytab() {
  if (!g_kinit_ctx) return boost::none;
  return g_kinit_ctx->username_str();
}

Status InitKerberosForServer() {
  if (FLAGS_keytab_file.empty()) return Status::OK();

  // Have the daemons use an in-memory ticket cache, so they don't accidentally
  // pick up credentials from test cases or any other daemon.
  // TODO(todd): extract these krb5 env vars into some constants since they're
  // typo-prone.
  setenv("KRB5CCNAME", "MEMORY:kudu", 1);
  setenv("KRB5_KTNAME", FLAGS_keytab_file.c_str(), 1);

  // KUDU-1897: disable the Kerberos replay cache. The KRPC protocol includes a
  // per-connection server-generated nonce to protect against replay attacks
  // when authenticating via Kerberos. The replay cache has many performance and
  // implementation issues.
  setenv("KRB5RCACHETYPE", "none", 1);

  g_kinit_ctx = new KinitContext();
  string principal;
  RETURN_NOT_OK(GetConfiguredPrincipal(&principal));
  RETURN_NOT_OK_PREPEND(g_kinit_ctx->Kinit(FLAGS_keytab_file, principal), "unable to kinit");

  g_kerberos_reinit_lock = new RWMutex(RWMutex::Priority::PREFER_WRITING);
  scoped_refptr<Thread> renew_thread;
  // Start the renewal thread.
  RETURN_NOT_OK(Thread::Create("kerberos", "renewal thread", &RenewThread, &renew_thread));

  return Status::OK();
}

} // namespace security
} // namespace kudu
