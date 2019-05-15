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

#include "kudu/server/webserver_options.h"

#include <cstdlib>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/security_flags.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"

using std::string;

namespace kudu {

static std::string GetDefaultDocumentRoot();

} // namespace kudu

// Flags defining web server behavior. The class implementation should
// not use these directly, but rather access them via WebserverOptions.
// This makes it easier to instantiate web servers with different options
// within a single unit test.
DEFINE_string(webserver_interface, "",
    "Interface to start debug webserver on. If blank, webserver binds to 0.0.0.0");
TAG_FLAG(webserver_interface, advanced);

DEFINE_string(webserver_advertised_addresses, "",
              "Comma-separated list of addresses to advertise externally for "
              "HTTP(S) connections. Ephemeral ports (i.e. port 0) are not "
              "allowed. This should be configured when the locally bound "
              "webserver address specified in --webserver_interface and "
              "--webserver_port are not externally resolvable, for example, if "
              "Kudu is deployed in a container.");
TAG_FLAG(webserver_advertised_addresses, advanced);

DEFINE_string(webserver_doc_root, kudu::GetDefaultDocumentRoot(),
    "Files under <webserver_doc_root> are accessible via the debug webserver. "
    "Defaults to $KUDU_HOME/www, or if $KUDU_HOME is not set, disables the document "
    "root");
TAG_FLAG(webserver_doc_root, advanced);

DEFINE_bool(webserver_enable_doc_root, true,
    "If true, webserver may serve static files from the webserver_doc_root");
TAG_FLAG(webserver_enable_doc_root, advanced);

// SSL configuration.
DEFINE_string(webserver_certificate_file, "",
    "The location of the debug webserver's SSL certificate file, in PEM format. If "
    "empty, webserver SSL support is not enabled. If --webserver_private_key_file "
    "is set, this option must be set as well.");
DEFINE_string(webserver_private_key_file, "", "The full path to the private key used as a"
    " counterpart to the public key contained in --webserver_certificate_file. If "
    "--webserver_certificate_file is set, this option must be set as well.");
DEFINE_string(webserver_private_key_password_cmd, "", "A Unix command whose output "
    "returns the password used to decrypt the Webserver's certificate private key file "
    "specified in --webserver_private_key_file. If the PEM key file is not "
    "password-protected, this flag does not need to be set. Trailing whitespace will be trimmed "
    "before it is used to decrypt the private key");
TAG_FLAG(webserver_certificate_file, stable);
TAG_FLAG(webserver_private_key_file, stable);
TAG_FLAG(webserver_private_key_password_cmd, stable);

DEFINE_string(webserver_authentication_domain, "",
    "Domain used for debug webserver authentication");
DEFINE_string(webserver_password_file, "",
    "(Optional) Location of .htpasswd file containing user names and hashed passwords for"
    " debug webserver authentication");

DEFINE_int32(webserver_num_worker_threads, 50,
             "Maximum number of threads to start for handling web server requests");
TAG_FLAG(webserver_num_worker_threads, advanced);

DEFINE_int32(webserver_port, 0,
             "Port to bind to for the web server");
TAG_FLAG(webserver_port, stable);

DEFINE_string(webserver_tls_ciphers,
              // See security/tls_context.cc for origin of this list.
              kudu::security::SecurityDefaults::kDefaultTlsCiphers,
              "The cipher suite preferences to use for webserver HTTPS connections. "
              "Uses the OpenSSL cipher preference list format. See man (1) ciphers "
              "for more information.");
TAG_FLAG(webserver_tls_ciphers, advanced);

DEFINE_string(webserver_tls_min_protocol, kudu::security::SecurityDefaults::kDefaultTlsMinVersion,
              "The minimum protocol version to allow when for webserver HTTPS "
              "connections. May be one of 'TLSv1', 'TLSv1.1', or 'TLSv1.2'.");
TAG_FLAG(webserver_tls_min_protocol, advanced);

DEFINE_bool(webserver_require_spnego, false,
            "Require connections to the web server to authenticate via Kerberos "
            "using SPNEGO.");
TAG_FLAG(webserver_require_spnego, evolving);

namespace kudu {

static bool ValidateTlsFlags() {
  bool has_cert = !FLAGS_webserver_certificate_file.empty();
  bool has_key = !FLAGS_webserver_private_key_file.empty();
  bool has_passwd = !FLAGS_webserver_private_key_password_cmd.empty();

  if (has_key != has_cert) {
    LOG(ERROR) << "--webserver_certificate_file and --webserver_private_key_file "
                  "must be set as a group; i.e. either set all or none of them.";
    return false;
  }
  if (has_passwd && !has_key) {
    LOG(ERROR) << "--webserver_private_key_password_cmd may not be set without "
                  "--webserver_private_key_file";
    return false;
  }

  return true;
}
GROUP_FLAG_VALIDATOR(webserver_tls_options, ValidateTlsFlags);

// Returns KUDU_HOME if set, otherwise we won't serve any static files.
static string GetDefaultDocumentRoot() {
  char* kudu_home = getenv("KUDU_HOME");
  // Empty document root means don't serve static files
  return kudu_home ? strings::Substitute("$0/www", kudu_home) : "";
}

WebserverOptions::WebserverOptions()
  : bind_interface(FLAGS_webserver_interface),
    webserver_advertised_addresses(FLAGS_webserver_advertised_addresses),
    port(FLAGS_webserver_port),
    doc_root(FLAGS_webserver_doc_root),
    enable_doc_root(FLAGS_webserver_enable_doc_root),
    certificate_file(FLAGS_webserver_certificate_file),
    private_key_file(FLAGS_webserver_private_key_file),
    private_key_password_cmd(FLAGS_webserver_private_key_password_cmd),
    authentication_domain(FLAGS_webserver_authentication_domain),
    password_file(FLAGS_webserver_password_file),
    tls_ciphers(FLAGS_webserver_tls_ciphers),
    tls_min_protocol(FLAGS_webserver_tls_min_protocol),
    num_worker_threads(FLAGS_webserver_num_worker_threads),
    require_spnego(FLAGS_webserver_require_spnego) {
}

} // namespace kudu
