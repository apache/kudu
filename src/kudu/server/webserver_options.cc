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

#include <string>
#include <gflags/gflags.h>
#include <string.h>
#include <stdlib.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"

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
    "empty, webserver SSL support is not enabled");
DEFINE_string(webserver_private_key_file, "", "The full path to the private key used as a"
    " counterpart to the public key contained in --ssl_server_certificate. If "
    "--ssl_server_certificate is set, this option must be set as well.");
DEFINE_string(webserver_private_key_password_cmd, "", "A Unix command whose output "
    "returns the password used to decrypt the Webserver's certificate private key file "
    "specified in --webserver_private_key_file. If the PEM key file is not "
    "password-protected, this command will not be invoked. The output of the command "
    "will be truncated to 1024 bytes, and then all trailing whitespace will be trimmed "
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

namespace kudu {

// Returns KUDU_HOME if set, otherwise we won't serve any static files.
static string GetDefaultDocumentRoot() {
  char* kudu_home = getenv("KUDU_HOME");
  // Empty document root means don't serve static files
  return kudu_home ? strings::Substitute("$0/www", kudu_home) : "";
}

WebserverOptions::WebserverOptions()
  : bind_interface(FLAGS_webserver_interface),
    port(FLAGS_webserver_port),
    doc_root(FLAGS_webserver_doc_root),
    enable_doc_root(FLAGS_webserver_enable_doc_root),
    certificate_file(FLAGS_webserver_certificate_file),
    private_key_file(FLAGS_webserver_private_key_file),
    private_key_password_cmd(FLAGS_webserver_private_key_password_cmd),
    authentication_domain(FLAGS_webserver_authentication_domain),
    password_file(FLAGS_webserver_password_file),
    num_worker_threads(FLAGS_webserver_num_worker_threads) {
}

} // namespace kudu
