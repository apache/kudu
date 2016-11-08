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
#include <string>

#include "kudu/gutil/strings/util.h"
#include "kudu/util/flags.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"

DEFINE_string(keytab, "", "Path to the Kerberos Keytab for this server");
TAG_FLAG(keytab, experimental);

DEFINE_string(kerberos_principal, "kudu/_HOST",
              "Kerberos principal that this daemon will log in as. The special token "
              "_HOST will be replaced with the FQDN of the local host.");
TAG_FLAG(kerberos_principal, experimental);
// TODO(todd): this currently only affects the keytab login which is used
// for client credentials, but doesn't affect the SASL server code path.
// We probably need to plumb the same configuration into the RPC code.

using std::string;

namespace kudu {
namespace security {

namespace {

Status Krb5CallToStatus(krb5_context ctx, krb5_error_code code) {
  if (code == 0) return Status::OK();
  return Status::RuntimeError(krb5_get_error_message(ctx, code));
}
#define KRB5_RETURN_NOT_OK_PREPEND(call, prepend) \
  RETURN_NOT_OK_PREPEND(Krb5CallToStatus(ctx, (call)), (prepend))

// Equivalent implementation of 'kinit -kt <keytab path> <principal>'.
//
// This logs in from the given keytab as the given principal, returning
// RuntimeError if any part of this process fails.
//
// If the log-in is successful, then the default ticket cache is overwritten
// with the credentials of the newly logged-in principal.
Status Kinit(const string& keytab_path, const string& principal) {
  krb5_context ctx;
  if (krb5_init_context(&ctx) != 0) {
    return Status::RuntimeError("could not initialize krb5 library");
  }
  auto cleanup_ctx = MakeScopedCleanup([&]() { krb5_free_context(ctx); });

  // Parse the principal
  krb5_principal client_principal;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_parse_name(ctx, principal.c_str(), &client_principal),
                             "could not parse principal");
  auto cleanup_client_principal = MakeScopedCleanup([&]() {
      krb5_free_principal(ctx, client_principal); });

  krb5_keytab keytab;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_kt_resolve(ctx, keytab_path.c_str(), &keytab),
                             "unable to resolve keytab");
  auto cleanup_keytab = MakeScopedCleanup([&]() { krb5_kt_close(ctx, keytab); });

  krb5_ccache ccache;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_default(ctx, &ccache),
                             "unable to get default credentials cache");
  auto cleanup_ccache = MakeScopedCleanup([&]() { krb5_cc_close(ctx, ccache); });


  krb5_get_init_creds_opt* opt;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_opt_alloc(ctx, &opt),
                             "unable to allocate get_init_creds_opt struct");
  auto cleanup_opt = MakeScopedCleanup([&]() { krb5_get_init_creds_opt_free(ctx, opt); });

#ifndef __APPLE__
  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_opt_set_out_ccache(ctx, opt, ccache),
                             "unable to set init_creds options");
#endif

  krb5_creds creds;
  KRB5_RETURN_NOT_OK_PREPEND(krb5_get_init_creds_keytab(ctx, &creds, client_principal,
                                                        keytab, 0 /* valid from now */,
                                                        nullptr /* TKT service name */,
                                                        opt),
                             "unable to login from keytab");
  auto cleanup_creds = MakeScopedCleanup([&]() { krb5_free_cred_contents(ctx, &creds); });

#ifdef __APPLE__
  // Heimdal krb5 doesn't have the 'krb5_get_init_creds_opt_set_out_ccache' option,
  // so use this alternate route.
  KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_initialize(ctx, ccache, client_principal),
                             "could not init ccache");

  KRB5_RETURN_NOT_OK_PREPEND(krb5_cc_store_cred(ctx, ccache, &creds),
                             "could not store creds in cache");
#endif
  return Status::OK();
}

Status GetLoginPrincipal(string* principal) {
  string p = FLAGS_kerberos_principal;
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

Status InitKerberosForServer() {
  if (FLAGS_keytab.empty()) return Status::OK();

  // Have the daemons use an in-memory ticket cache, so they don't accidentally
  // pick up credentials from test cases or any other daemon.
  // TODO(todd): extract these krb5 env vars into some constants since they're
  // typo-prone.
  setenv("KRB5CCNAME", "MEMORY:kudu", 1);
  setenv("KRB5_KTNAME", FLAGS_keytab.c_str(), 1);

  string principal;
  RETURN_NOT_OK(GetLoginPrincipal(&principal));
  RETURN_NOT_OK_PREPEND(Kinit(FLAGS_keytab, principal), "unable to kinit");

  // TODO(todd) we likely need to start a "renewal thread" here, since the credentials
  // can expire.

  return Status::OK();
}

} // namespace security
} // namespace kudu
