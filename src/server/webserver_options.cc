// Copyright (c) 2013, Cloudera, inc.

#include "server/webserver_options.h"

#include <gflags/gflags.h>
#include <string.h>
#include <stdlib.h>

using std::string;

namespace kudu {

static const char* GetDefaultDocumentRoot();

} // namespace kudu

// Flags defining web server behavior. The class implementation should
// not use these directly, but rather access them via WebserverOptions.
// This makes it easier to instantiate web servers with different options
// within a single unit test.
DEFINE_int32(webserver_port, 25000, "Port to start debug webserver on");
DEFINE_string(webserver_interface, "",
    "Interface to start debug webserver on. If blank, webserver binds to 0.0.0.0");
DEFINE_string(webserver_doc_root, kudu::GetDefaultDocumentRoot(),
    "Files under <webserver_doc_root>/www are accessible via the debug webserver. "
    "Defaults to KUDU_HOME, or if KUDU_HOME is not set, disables the document "
    "root");
DEFINE_bool(enable_webserver_doc_root, true,
    "If true, webserver may serve static files from the webserver_doc_root");

DEFINE_string(webserver_certificate_file, "",
    "The location of the debug webserver's SSL certificate file, in .pem format. If "
    "empty, webserver SSL support is not enabled");
DEFINE_string(webserver_authentication_domain, "",
    "Domain used for debug webserver authentication");
DEFINE_string(webserver_password_file, "",
    "(Optional) Location of .htpasswd file containing user names and hashed passwords for"
    " debug webserver authentication");
DEFINE_int32(webserver_num_worker_threads, 50,
             "Number of threads to start for handling web server requests");

namespace kudu {

// Returns KUDU_HOME if set, otherwise we won't serve any static files.
static const char* GetDefaultDocumentRoot() {
  char* kudu_home = getenv("KUDU_HOME");
  // Empty document root means don't serve static files
  return kudu_home ? kudu_home : "";
}

WebserverOptions::WebserverOptions()
  : bind_interface(FLAGS_webserver_interface),
    port(FLAGS_webserver_port),
    doc_root(FLAGS_webserver_doc_root),
    enable_doc_root(FLAGS_enable_webserver_doc_root),
    certificate_file(FLAGS_webserver_certificate_file),
    authentication_domain(FLAGS_webserver_authentication_domain),
    password_file(FLAGS_webserver_password_file),
    num_worker_threads(FLAGS_webserver_num_worker_threads) {
}

} // namespace kudu
