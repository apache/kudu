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

namespace boost {
template <class T>
class optional;
}

namespace kudu {

class RWMutex;
class Status;

namespace security {

// The default kerberos credential cache name.
// Have the daemons use an in-memory ticket cache, so they don't accidentally
// pick up credentials from test cases or any other daemon.
static const std::string kKrb5CCName = "MEMORY:kudu";

// Parses the given Kerberos principal into service name, hostname, and realm.
Status Krb5ParseName(const std::string& principal,
                     std::string* service_name,
                     std::string* hostname,
                     std::string* realm);

// Initializes Kerberos for a server. In particular, this processes
// the '--keytab_file' command line flag.
// 'raw_principal' is the principal to Kinit with after calling GetConfiguredPrincipal()
// on it.
// 'keytab_file' is the path to the kerberos keytab file. If it's an empty string, kerberos
// will not be initialized.
// 'krb5ccname' is passed into the KRB5CCNAME env var.
// 'disable_krb5_replay_cache' if set to true, disables the kerberos replay cache by setting
// the KRB5RCACHETYPE env var to "none".
Status InitKerberosForServer(const std::string& raw_principal,
                             const std::string& keytab_file,
                             const std::string& krb5ccname = kKrb5CCName,
                             bool disable_krb5_replay_cache = true);

// Returns the process lock 'kerberos_reinit_lock'
// This lock is taken in write mode while the ticket is being reacquired, and
// taken in read mode before using the SASL library which might require a ticket.
RWMutex* KerberosReinitLock();

// Return the full principal (user/host@REALM) that the server has used to
// log in from the keytab.
//
// If the server has not logged in from a keytab, returns boost::none.
boost::optional<std::string> GetLoggedInPrincipalFromKeytab();

// Same, but returns the mapped short username.
boost::optional<std::string> GetLoggedInUsernameFromKeytab();

// Canonicalize the given principal name by adding '@DEFAULT_REALM' in the case that
// the principal has no realm.
//
// TODO(todd): move to kerberos_util.h in the later patch in this series (the file doesn't
// exist yet, and trying to avoid rebase pain).
Status CanonicalizeKrb5Principal(std::string* principal);

// Map the given Kerberos principal 'principal' to a short username (i.e. with no realm or
// host component).
//
// This respects the "auth-to-local" mappings from the system krb5.conf. However, if no such
// mapping can be found, we fall back to simply taking the first component of the principal.
//
// TODO(todd): move to kerberos_util.h in the later patch in this series (the file doesn't
// exist yet, and trying to avoid rebase pain).
Status MapPrincipalToLocalName(const std::string& principal, std::string* local_name);

// Get the configured principal. 'in_principal' is the user specified principal to use with
// Kerberos. It may have a token in the string of the form '_HOST', which if present, needs
// to be replaced with the FQDN of the current host. 'out_principal' has the final principal
// with which one may Kinit.
Status GetConfiguredPrincipal(const std::string& in_principal, std::string* out_principal);

// Get the Kerberos config file location. It defaults to /etc/krb5.conf and it
// can be overridden by the KRB5_CONFIG environment variable. As the Kerberos
// libraries use the environment variable directly, this is not required
// normally, but it can be useful if the file needs to be accessed directly
// (e.g. when starting a Java subprocess, as Java doesn't respect the
// environment variable).
std::string GetKrb5ConfigFile();

} // namespace security
} // namespace kudu
