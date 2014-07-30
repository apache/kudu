// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "kudu/server/webserver.h"

#include <stdio.h>
#include <signal.h>
#include <string>
#include <map>
#include <vector>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <squeasel.h>

#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/env.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/url-coding.h"

using std::string;
using std::stringstream;
using std::vector;
using std::make_pair;

namespace kudu {

static const char* DOC_FOLDER = "/www/";
static const int DOC_FOLDER_LEN = strlen(DOC_FOLDER);

Webserver::Webserver(const WebserverOptions& opts)
  : opts_(opts),
    context_(NULL) {
  string host = opts.bind_interface.empty() ? "0.0.0.0" : opts.bind_interface;
  http_address_ = host + ":" + boost::lexical_cast<string>(opts.port);
}

Webserver::~Webserver() {
  Stop();
}

void Webserver::RootHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  (*output) << "<h2>Status Pages</h2>";
  BOOST_FOREACH(const PathHandlerMap::value_type& handler, path_handlers_) {
    if (handler.second.is_on_nav_bar()) {
      (*output) << "<a href=\"" << handler.first << "\">" << handler.first << "</a><br/>";
    }
  }
}

void Webserver::BuildArgumentMap(const string& args, ArgumentMap* output) {
  vector<StringPiece> arg_pairs = strings::Split(args, "&");

  BOOST_FOREACH(const StringPiece& arg_pair, arg_pairs) {
    vector<StringPiece> key_value = strings::Split(arg_pair, "=");
    if (key_value.empty()) continue;

    string key;
    if (!UrlDecode(key_value[0].ToString(), &key)) continue;
    string value;
    if (!UrlDecode((key_value.size() >= 2 ? key_value[1].ToString() : ""), &value)) continue;
    boost::to_lower(key);
    (*output)[key] = value;
  }
}

bool Webserver::IsSecure() const {
  return !opts_.certificate_file.empty();
}

Status Webserver::BuildListenSpec(string* spec) const {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(ParseAddressList(http_address_, 80, &addrs));

  vector<string> parts;
  BOOST_FOREACH(const Sockaddr& addr, addrs) {
    // Mongoose makes sockets with 's' suffixes accept SSL traffic only
    parts.push_back(addr.ToString() + (IsSecure() ? "s" : ""));
  }

  JoinStrings(parts, ",", spec);
  return Status::OK();
}

Status Webserver::Start() {
  LOG(INFO) << "Starting webserver on " << http_address_;

  vector<const char*> options;

  if (static_pages_available()) {
    LOG(INFO) << "Document root: " << opts_.doc_root;
    options.push_back("document_root");
    options.push_back(opts_.doc_root.c_str());
  } else {
    LOG(INFO)<< "Document root disabled";
  }

  if (IsSecure()) {
    LOG(INFO) << "Webserver: Enabling HTTPS support";
    options.push_back("ssl_certificate");
    options.push_back(opts_.certificate_file.c_str());
  }

  if (!opts_.authentication_domain.empty()) {
    options.push_back("authentication_domain");
    options.push_back(opts_.authentication_domain.c_str());
  }

  if (!opts_.password_file.empty()) {
    // Mongoose doesn't log anything if it can't stat the password file (but will if it
    // can't open it, which it tries to do during a request)
    if (!Env::Default()->FileExists(opts_.password_file)) {
      stringstream ss;
      ss << "Webserver: Password file does not exist: " << opts_.password_file;
      return Status::InvalidArgument(ss.str());
    }
    LOG(INFO) << "Webserver: Password file is " << opts_.password_file;
    options.push_back("global_passwords_file");
    options.push_back(opts_.password_file.c_str());
  }

  options.push_back("listening_ports");
  string listening_str;
  RETURN_NOT_OK(BuildListenSpec(&listening_str));
  options.push_back(listening_str.c_str());

  // Num threads
  options.push_back("num_threads");
  string num_threads_str = SimpleItoa(opts_.num_worker_threads);
  options.push_back(num_threads_str.c_str());

  // Options must be a NULL-terminated list
  options.push_back(NULL);

  // mongoose ignores SIGCHLD and we need it to run kinit. This means that since
  // mongoose does not reap its own children CGI programs must be avoided.
  // Save the signal handler so we can restore it after mongoose sets it to be ignored.
  sighandler_t sig_chld = signal(SIGCHLD, SIG_DFL);

  sq_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.begin_request = &Webserver::BeginRequestCallbackStatic;
  callbacks.log_message = &Webserver::LogMessageCallbackStatic;

  // To work around not being able to pass member functions as C callbacks, we store a
  // pointer to this server in the per-server state, and register a static method as the
  // default callback. That method unpacks the pointer to this and calls the real
  // callback.
  context_ = sq_start(&callbacks, reinterpret_cast<void*>(this), &options[0]);

  // Restore the child signal handler so wait() works properly.
  signal(SIGCHLD, sig_chld);

  if (context_ == NULL) {
    stringstream error_msg;
    error_msg << "Webserver: Could not start on address " << http_address_;
    return Status::NetworkError(error_msg.str());
  }

  PathHandlerCallback default_callback =
    boost::bind<void>(boost::mem_fn(&Webserver::RootHandler), this, _1, _2);

  RegisterPathHandler("/", default_callback);

  vector<Sockaddr> addrs;
  RETURN_NOT_OK(GetBoundAddresses(&addrs));
  string bound_addresses_str;
  BOOST_FOREACH(const Sockaddr& addr, addrs) {
    if (!bound_addresses_str.empty()) {
      bound_addresses_str += ", ";
    }
    StrAppend(&bound_addresses_str, addr.ToString());
  }

  LOG(INFO) << "Webserver started. Bound to: " << bound_addresses_str;
  return Status::OK();
}

void Webserver::Stop() {
  if (context_ != NULL) {
    sq_stop(context_);
    context_ = NULL;
  }
}

Status Webserver::GetBoundAddresses(std::vector<Sockaddr>* addrs) const {
  if (!context_) {
    return Status::IllegalState("Not started");
  }

  struct sockaddr_in** sockaddrs;
  int num_addrs;

  if (sq_get_bound_addresses(context_, &sockaddrs, &num_addrs)) {
    return Status::NetworkError("Unable to get bound addresses from Mongoose");
  }

  addrs->reserve(num_addrs);

  for (int i = 0; i < num_addrs; i++) {
    addrs->push_back(Sockaddr(*sockaddrs[i]));
    free(sockaddrs[i]);
  }
  free(sockaddrs);

  return Status::OK();
}

int Webserver::LogMessageCallbackStatic(const struct sq_connection* connection,
                                        const char* message) {
  if (message != NULL) {
    LOG(INFO) << "Webserver: " << message;
    return 1;
  }
  return 0;
}

int Webserver::BeginRequestCallbackStatic(struct sq_connection* connection) {
  struct sq_request_info* request_info = sq_get_request_info(connection);
  Webserver* instance = reinterpret_cast<Webserver*>(request_info->user_data);
  return instance->BeginRequestCallback(connection, request_info);
}

int Webserver::BeginRequestCallback(struct sq_connection* connection,
                                    struct sq_request_info* request_info) {
  if (!opts_.doc_root.empty() && opts_.enable_doc_root) {
    if (strncmp(DOC_FOLDER, request_info->uri, DOC_FOLDER_LEN) == 0) {
      VLOG(2) << "HTTP File access: " << request_info->uri;
      // Let Mongoose deal with this request; returning NULL will fall through
      // to the default handler which will serve files.
      return 0;
    }
  }
  boost::shared_lock<boost::shared_mutex> lock(path_handlers_lock_);
  PathHandlerMap::const_iterator it = path_handlers_.find(request_info->uri);
  if (it == path_handlers_.end()) {
    sq_printf(connection, "HTTP/1.1 404 Not Found\r\n"
              "Content-Type: text/plain\r\n\r\n");
    sq_printf(connection, "No handler for URI %s\r\n\r\n", request_info->uri);
    return 1;
  }

  // Should we render with css styles?
  bool use_style = true;

  map<string, string> arguments;
  if (request_info->query_string != NULL) {
    BuildArgumentMap(request_info->query_string, &arguments);
  }
  if (!it->second.is_styled() || arguments.find("raw") != arguments.end()) {
    use_style = false;
  }

  stringstream output;
  if (use_style) BootstrapPageHeader(&output);
  BOOST_FOREACH(const PathHandlerCallback& callback_, it->second.callbacks()) {
    callback_(arguments, &output);
  }
  if (use_style) BootstrapPageFooter(&output);

  string str = output.str();
  // Without styling, render the page as plain text
  if (arguments.find("raw") != arguments.end()) {
    sq_printf(connection, "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/plain\r\n"
              "Content-Length: %zd\r\n"
              "\r\n", str.length());
  } else {
    sq_printf(connection, "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/html\r\n"
              "Content-Length: %zd\r\n"
              "\r\n", str.length());
  }

  // Make sure to use sq_write for printing the body; sq_printf truncates at 8kb
  sq_write(connection, str.c_str(), str.length());
  return 1;
}

void Webserver::RegisterPathHandler(const string& path,
    const PathHandlerCallback& callback, bool is_styled, bool is_on_nav_bar) {
  boost::lock_guard<boost::shared_mutex> lock(path_handlers_lock_);
  PathHandlerMap::iterator it = path_handlers_.find(path);
  if (it == path_handlers_.end()) {
    it = path_handlers_.insert(
        make_pair(path, PathHandler(is_styled, is_on_nav_bar))).first;
  }
  it->second.AddCallback(callback);
}

const char* const PAGE_HEADER = "<!DOCTYPE html>"
" <html>"
"   <head><title>Cloudera Kudu</title>"
" <link href='www/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />"
"  <style>"
"  body {"
"    padding-top: 60px; "
"  }"
"  </style>"
" </head>"
" <body>";

static const char* const PAGE_FOOTER = "</div></body></html>";

static const char* const NAVIGATION_BAR_PREFIX =
"<div class='navbar navbar-inverse navbar-fixed-top'>"
"      <div class='navbar-inner'>"
"        <div class='container'>"
"          <a href='/'>"
"            <img src=\"/www/logo.png\" width='55' height='52' alt=\"Kudu\" style=\"float:left\"/>"
"          </a>"
"          <div class='nav-collapse collapse'>"
"            <ul class='nav'>";

static const char* const NAVIGATION_BAR_SUFFIX =
"            </ul>"
"          </div>"
"        </div>"
"      </div>"
"    </div>"
"    <div class='container'>";

void Webserver::BootstrapPageHeader(stringstream* output) {
  (*output) << PAGE_HEADER;
  (*output) << NAVIGATION_BAR_PREFIX;
  BOOST_FOREACH(const PathHandlerMap::value_type& handler, path_handlers_) {
    if (handler.second.is_on_nav_bar()) {
      (*output) << "<li><a href=\"" << handler.first << "\">" << handler.first
                << "</a></li>";
    }
  }
  (*output) << NAVIGATION_BAR_SUFFIX;

  if (!static_pages_available()) {
    (*output) << "<div style=\"color: red\"><strong>"
              << "Static pages not available. Configure KUDU_HOME or use the --webserver_doc_root "
              << "flag to fix page styling.</strong></div>\n";
  }
}

bool Webserver::static_pages_available() const {
  return !opts_.doc_root.empty() && opts_.enable_doc_root;
}

void Webserver::BootstrapPageFooter(stringstream* output) {
  (*output) << PAGE_FOOTER;
}

} // namespace kudu
