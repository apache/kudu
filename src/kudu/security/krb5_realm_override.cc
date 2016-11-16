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

// This file provides a workaround for tests running with Kerberos 1.11 or earlier.
// These versions of Kerberos are missing a fix which allows service principals
// to use IP addresses in their host component:
//
//   http://krbdev.mit.edu/rt/Ticket/Display.html?id=7603
//
// We use such principals in external minicluster tests, where servers have IP addresses
// like 127.x.y.z that have no corresponding reverse DNS.
//
// The file contains an implementation of krb5_get_host_realm which wraps the one
// in the Kerberos library. It detects the return code that indicates the
// above problem and falls back to the default realm/
//
// The wrapper is injected via linking it into tests as well as the
// "security" library. The linkage invocation uses the '-Wl,--undefined'
// linker flag to force linking even though no symbol here is explicitly
// referenced.

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <krb5/krb5.h>
#include <glog/logging.h>

extern "C" {

// This symbol is exported from the static library so that other static-linked binaries
// can reference it and force this compilation unit to be linked. Otherwise the linker
// thinks it's unused and doesn't link it.
int krb5_realm_override_loaded = 1;

// Save the original function from the Kerberos library itself.
// We use dlsym() to load all of them, since this file gets linked into
// some test binaries that themselves may not link against libkrb5.so at all.
static void* g_orig_krb5_get_host_realm;
static void* g_orig_krb5_get_default_realm;
static void* g_orig_krb5_free_default_realm;

// We only enable our workaround if this environment variable is set.
constexpr static const char* kEnvVar = "KUDU_ENABLE_KRB5_REALM_FIX";

#define CALL_ORIG(func_name, ...) \
  ((decltype(&func_name))g_orig_ ## func_name)(__VA_ARGS__)

__attribute__((constructor))
static void init_orig_func() {
  g_orig_krb5_get_host_realm = dlsym(RTLD_NEXT, "krb5_get_host_realm");
  g_orig_krb5_get_default_realm = dlsym(RTLD_NEXT, "krb5_get_default_realm");
  g_orig_krb5_free_default_realm = dlsym(RTLD_NEXT, "krb5_free_default_realm");
}

krb5_error_code krb5_get_host_realm(krb5_context context, const char* host, char*** realmsp) {
  CHECK(g_orig_krb5_get_host_realm);
  CHECK(g_orig_krb5_get_default_realm);
  CHECK(g_orig_krb5_free_default_realm);

  krb5_error_code rc = CALL_ORIG(krb5_get_host_realm, context, host, realmsp);
  if (rc != KRB5_ERR_NUMERIC_REALM || getenv(kEnvVar) == nullptr) {
    return rc;
  }
  // If we get KRB5_ERR_NUMERIC_REALM, this is indicative of a Kerberos version
  // which has not provided support for numeric addresses as service host names
  // So, we fill in the default realm instead.
  char* default_realm;
  rc = CALL_ORIG(krb5_get_default_realm, context, &default_realm);
  if (rc != 0) {
    return rc;
  }

  char** ret_realms;
  ret_realms = static_cast<char**>(malloc(2 * sizeof(*ret_realms)));
  if (ret_realms == nullptr) return ENOMEM;
  ret_realms[0] = strdup(default_realm);
  if (ret_realms[0] == nullptr) {
    free(ret_realms);
    return ENOMEM;
  }
  ret_realms[1] = 0;
  *realmsp = ret_realms;

  CALL_ORIG(krb5_free_default_realm, context, default_realm);
  return 0;
}

} // extern "C"
