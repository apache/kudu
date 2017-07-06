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
#include "kudu/server/webserver.h"

#include <cstdio>
#include <signal.h>

#include <algorithm>
#include <functional>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <mustache.h>
#include <squeasel.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/url-coding.h"
#include "kudu/util/version_info.h"

#if defined(__APPLE__)
typedef sig_t sighandler_t;
#endif

using mustache::RenderTemplate;
using std::ostringstream;
using std::stringstream;
using std::string;
using std::vector;
using strings::Substitute;

DEFINE_int32(webserver_max_post_length_bytes, 1024 * 1024,
             "The maximum length of a POST request that will be accepted by "
             "the embedded web server.");
TAG_FLAG(webserver_max_post_length_bytes, advanced);
TAG_FLAG(webserver_max_post_length_bytes, runtime);

DEFINE_string(webserver_x_frame_options, "DENY",
              "The webserver will add an 'X-Frame-Options' HTTP header with this value "
              "to all responses. This can help prevent clickjacking attacks.");
TAG_FLAG(webserver_x_frame_options, advanced);


namespace {
  // Last error message from the webserver.
  string kWebserverLastErrMsg;
}  // anonymous namespace

namespace kudu {

Webserver::Webserver(const WebserverOptions& opts)
  : opts_(opts),
    context_(nullptr) {
  string host = opts.bind_interface.empty() ? "0.0.0.0" : opts.bind_interface;
  http_address_ = host + ":" + std::to_string(opts.port);
}

Webserver::~Webserver() {
  Stop();
  STLDeleteValues(&path_handlers_);
}

void Webserver::RootHandler(const Webserver::WebRequest& /* args */, EasyJson* output) {
  EasyJson path_handlers = output->Set("path_handlers", EasyJson::kArray);
  for (const PathHandlerMap::value_type& handler : path_handlers_) {
    if (handler.second->is_on_nav_bar()) {
      EasyJson path_handler = path_handlers.PushBack(EasyJson::kObject);
      path_handler["path"] = handler.first;
      path_handler["alias"] = handler.second->alias();
    }
  }
  (*output)["version_info"] = EscapeForHtmlToString(VersionInfo::GetAllVersionInfo());
}

void Webserver::BuildArgumentMap(const string& args, ArgumentMap* output) {
  vector<StringPiece> arg_pairs = strings::Split(args, "&");

  for (const StringPiece& arg_pair : arg_pairs) {
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
  for (const Sockaddr& addr : addrs) {
    // Mongoose makes sockets with 's' suffixes accept SSL traffic only
    parts.push_back(addr.ToString() + (IsSecure() ? "s" : ""));
  }

  JoinStrings(parts, ",", spec);
  return Status::OK();
}

Status Webserver::Start() {
  LOG(INFO) << "Starting webserver on " << http_address_;

  vector<string> options;

  if (static_pages_available()) {
    LOG(INFO) << "Document root: " << opts_.doc_root;
    options.emplace_back("document_root");
    options.push_back(opts_.doc_root);
    options.emplace_back("enable_directory_listing");
    options.emplace_back("no");
  } else {
    LOG(INFO)<< "Document root disabled";
  }

  if (IsSecure()) {
    LOG(INFO) << "Webserver: Enabling HTTPS support";

    // Initialize OpenSSL, and prevent Squeasel from also performing global OpenSSL
    // initialization.
    security::InitializeOpenSSL();
    options.emplace_back("ssl_global_init");
    options.emplace_back("false");

    options.emplace_back("ssl_certificate");
    options.push_back(opts_.certificate_file);

    if (!opts_.private_key_file.empty()) {
      options.emplace_back("ssl_private_key");
      options.push_back(opts_.private_key_file);

      string key_password;
      if (!opts_.private_key_password_cmd.empty()) {
        RETURN_NOT_OK(security::GetPasswordFromShellCommand(opts_.private_key_password_cmd,
                                                            &key_password));
      }
      options.emplace_back("ssl_private_key_password");
      options.push_back(key_password); // maybe empty if not configured.
    }
  }

  if (!opts_.authentication_domain.empty()) {
    options.emplace_back("authentication_domain");
    options.push_back(opts_.authentication_domain);
  }

  if (!opts_.password_file.empty()) {
    // Mongoose doesn't log anything if it can't stat the password file (but will if it
    // can't open it, which it tries to do during a request)
    if (!Env::Default()->FileExists(opts_.password_file)) {
      ostringstream ss;
      ss << "Webserver: Password file does not exist: " << opts_.password_file;
      return Status::InvalidArgument(ss.str());
    }
    LOG(INFO) << "Webserver: Password file is " << opts_.password_file;
    options.emplace_back("global_auth_file");
    options.push_back(opts_.password_file);
  }

  options.emplace_back("listening_ports");
  string listening_str;
  RETURN_NOT_OK(BuildListenSpec(&listening_str));
  options.push_back(listening_str);

  // initialize the advertised addresses
  if (!opts_.webserver_advertised_addresses.empty()) {
    RETURN_NOT_OK(ParseAddressList(opts_.webserver_advertised_addresses,
                                   opts_.port,
                                   &webserver_advertised_addresses_));
    for (const Sockaddr& addr : webserver_advertised_addresses_) {
      if (addr.port() == 0) {
        return Status::InvalidArgument("advertising an ephemeral webserver port is not supported",
                                       addr.ToString());
      }
    }
  }

  // Num threads
  options.emplace_back("num_threads");
  options.push_back(std::to_string(opts_.num_worker_threads));

  // mongoose ignores SIGCHLD and we need it to run kinit. This means that since
  // mongoose does not reap its own children CGI programs must be avoided.
  // Save the signal handler so we can restore it after mongoose sets it to be ignored.
  sighandler_t sig_chld = signal(SIGCHLD, SIG_DFL);

  sq_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.begin_request = &Webserver::BeginRequestCallbackStatic;
  callbacks.log_message = &Webserver::LogMessageCallbackStatic;

  // Options must be a NULL-terminated list of C strings.
  vector<const char*> c_options;
  for (const auto& opt : options) {
    c_options.push_back(opt.c_str());
  }
  c_options.push_back(nullptr);

  // To work around not being able to pass member functions as C callbacks, we store a
  // pointer to this server in the per-server state, and register a static method as the
  // default callback. That method unpacks the pointer to this and calls the real
  // callback.
  context_ = sq_start(&callbacks, reinterpret_cast<void*>(this), &c_options[0]);

  // Restore the child signal handler so wait() works properly.
  signal(SIGCHLD, sig_chld);

  if (context_ == nullptr) {
    Sockaddr addr;
    addr.set_port(opts_.port);
    TryRunLsof(addr);
    string err_msg = Substitute("Webserver: could not start on address $0", http_address_);
    if (!kWebserverLastErrMsg.empty()) {
      err_msg = Substitute("$0: $1", err_msg, kWebserverLastErrMsg);
    }
    return Status::RuntimeError(err_msg);
  }

  PathHandlerCallback default_callback =
      std::bind<void>(std::mem_fn(&Webserver::RootHandler),
                      this, std::placeholders::_1, std::placeholders::_2);

  RegisterPathHandler("/", "Home", default_callback, true /* styled */, true /* on_nav_bar */);

  vector<Sockaddr> addrs;
  RETURN_NOT_OK(GetBoundAddresses(&addrs));
  string bound_addresses_str;
  for (const Sockaddr& addr : addrs) {
    if (!bound_addresses_str.empty()) {
      bound_addresses_str += ", ";
    }
    bound_addresses_str += "http://" + addr.ToString() + "/";
  }

  LOG(INFO) << "Webserver started. Bound to: " << bound_addresses_str;
  return Status::OK();
}

void Webserver::Stop() {
  if (context_ != nullptr) {
    sq_stop(context_);
    context_ = nullptr;
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

Status Webserver::GetAdvertisedAddresses(vector<Sockaddr>* addresses) const {
  if (!context_) {
    return Status::IllegalState("Not started");
  }
  if (webserver_advertised_addresses_.empty()) {
    return GetBoundAddresses(addresses);
  }
  *addresses = webserver_advertised_addresses_;
  return Status::OK();
}

int Webserver::LogMessageCallbackStatic(const struct sq_connection* /*connection*/,
                                        const char* message) {
  if (message != nullptr) {
    // Using the ERROR severity for squeasel messages: as per source code at
    // https://github.com/cloudera/squeasel/blob/\
    //     c304d3f3481b07bf153979155f02e0aab24d01de/squeasel.c#L392
    // the squeasel server uses the log callback to report on errors.
    {
      static simple_spinlock kErrMsgLock_;
      std::unique_lock<simple_spinlock> l(kErrMsgLock_);
      kWebserverLastErrMsg = message;
    }
    LOG(ERROR) << "Webserver: " << message;
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
  PathHandler* handler;
  {
    shared_lock<RWMutex> l(lock_);
    PathHandlerMap::const_iterator it = path_handlers_.find(request_info->uri);
    if (it == path_handlers_.end()) {
      // Let Mongoose deal with this request; returning NULL will fall through
      // to the default handler which will serve files.
      if (!opts_.doc_root.empty() && opts_.enable_doc_root) {
        VLOG(2) << "HTTP File access: " << request_info->uri;
        return 0;
      } else {
        sq_printf(connection, "HTTP/1.1 404 Not Found\r\n"
                  "Content-Type: text/plain\r\n\r\n");
        sq_printf(connection, "No handler for URI %s\r\n\r\n", request_info->uri);
        return 1;
      }
    }
    handler = it->second;
  }

  return RunPathHandler(*handler, connection, request_info);
}

int Webserver::RunPathHandler(const PathHandler& handler,
                              struct sq_connection* connection,
                              struct sq_request_info* request_info) {
  // Should we render with css styles?
  bool use_style = true;

  WebRequest req;
  if (request_info->query_string != nullptr) {
    req.query_string = request_info->query_string;
    BuildArgumentMap(request_info->query_string, &req.parsed_args);
  }
  req.request_method = request_info->request_method;
  if (req.request_method == "POST") {
    const char* content_len_str = sq_get_header(connection, "Content-Length");
    int32_t content_len = 0;
    if (content_len_str == nullptr ||
        !safe_strto32(content_len_str, &content_len)) {
      sq_printf(connection, "HTTP/1.1 411 Length Required\r\n");
      return 1;
    }
    if (content_len > FLAGS_webserver_max_post_length_bytes) {
      // TODO: for this and other HTTP requests, we should log the
      // remote IP, etc.
      LOG(WARNING) << "Rejected POST with content length " << content_len;
      sq_printf(connection, "HTTP/1.1 413 Request Entity Too Large\r\n");
      return 1;
    }

    char buf[8192];
    int rem = content_len;
    while (rem > 0) {
      int n = sq_read(connection, buf, std::min<int>(sizeof(buf), rem));
      if (n <= 0) {
        LOG(WARNING) << "error reading POST data: expected "
                     << content_len << " bytes but only read "
                     << req.post_data.size();
        sq_printf(connection, "HTTP/1.1 500 Internal Server Error\r\n");
        return 1;
      }

      req.post_data.append(buf, n);
      rem -= n;
    }
  }

  if (!handler.is_styled() || ContainsKey(req.parsed_args, "raw")) {
    use_style = false;
  }

  ostringstream content;
  handler.callback()(req, &content);

  string full_content;
  if (use_style) {
    stringstream output;
    RenderMainTemplate(content.str(), &output);
    full_content = output.str();
  } else {
    full_content = content.str();
  }

  // Without styling, render the page as plain text
  string headers = strings::Substitute(
      "HTTP/1.1 200 OK\r\n"
      "Content-Type: $0\r\n"
      "Content-Length: $1\r\n"
      "X-Frame-Options: $2\r\n"
      "\r\n",
      use_style ? "text/html" : "text/plain",
      full_content.length(),
      FLAGS_webserver_x_frame_options);
  // Make sure to use sq_write for printing the body; sq_printf truncates at 8kb
  sq_write(connection, headers.c_str(), headers.length());
  sq_write(connection, full_content.c_str(), full_content.length());
  return 1;
}

void Webserver::RegisterPathHandler(const string& path, const string& alias,
    const PathHandlerCallback& callback, bool is_styled, bool is_on_nav_bar) {
  string render_path = (path == "/") ? "/home" : path;
  auto wrapped_cb = [=](const WebRequest& args, ostringstream* output) {
    EasyJson ej;
    callback(args, &ej);
    stringstream out;
    Render(render_path, ej, is_styled, &out);
    (*output) << out.rdbuf();
  };
  RegisterPrerenderedPathHandler(path, alias, wrapped_cb, is_styled, is_on_nav_bar);
}

void Webserver::RegisterPrerenderedPathHandler(const string& path, const string& alias,
    const PrerenderedPathHandlerCallback& callback, bool is_styled, bool is_on_nav_bar) {
  std::lock_guard<RWMutex> l(lock_);
  InsertOrDie(&path_handlers_, path, new PathHandler(is_styled, is_on_nav_bar, alias, callback));
}

string Webserver::MustachePartialTag(const string& path) const {
  return Substitute("{{> $0.mustache}}", path);
}

bool Webserver::MustacheTemplateAvailable(const string& path) const {
  if (!static_pages_available()) {
    return false;
  }
  return Env::Default()->FileExists(Substitute("$0$1.mustache", opts_.doc_root, path));
}

static const char* const kMainTemplate = R"(
<!DOCTYPE html>
<html>
  <head>
    <title>Kudu</title>
    <meta charset='utf-8'/>
    <link href='/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />
    <script src='/jquery-1.11.1.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap.min.js' defer></script>
    <link href='/kudu.css' rel='stylesheet' />
  </head>
  <body>
    <div class='navbar navbar-inverse navbar-fixed-top'>
      <div class='navbar-inner'>
        <div class='container-fluid'>
          <a href='/'>
            <img src="/logo.png" width='61' height='45' alt="Kudu" style="float:left"/>
          </a>
          <div class='nav-collapse collapse'>
            <ul class='nav'>
              {{#path_handlers}}
              <li><a href="{{path}}">{{alias}}</a></li>
              {{/path_handlers}}
            </ul>
          </div>
        </div>
      </div>
    </div>
    <div class='container-fluid'>
      {{^static_pages_available}}
      <div style="color: red">
        <strong>Static pages not available. Configure KUDU_HOME or use the --webserver_doc_root
        flag to fix page styling.</strong>
      </div>
      {{/static_pages_available}}
      {{{content}}}
    </div>
    {{#footer_html}}
    <footer class="footer"><div class="container text-muted">
      {{{.}}}
    </div></footer>
    {{/footer_html}}
  </body>
</html>
)";

void Webserver::RenderMainTemplate(const string& content, stringstream* output) {
  EasyJson ej;
  ej["static_pages_available"] = static_pages_available();
  ej["content"] = content;
  {
    shared_lock<RWMutex> l(lock_);
    ej["footer_html"] = footer_html_;
  }
  EasyJson path_handlers = ej.Set("path_handlers", EasyJson::kArray);
  for (const PathHandlerMap::value_type& handler : path_handlers_) {
    if (handler.second->is_on_nav_bar()) {
      EasyJson path_handler = path_handlers.PushBack(EasyJson::kObject);
      path_handler["path"] = handler.first;
      path_handler["alias"] = handler.second->alias();
    }
  }
  RenderTemplate(kMainTemplate, opts_.doc_root, ej.value(), output);
}

void Webserver::Render(const string& path, const EasyJson& ej, bool use_style,
                       stringstream* output) {
  if (MustacheTemplateAvailable(path)) {
    RenderTemplate(MustachePartialTag(path), opts_.doc_root, ej.value(), output);
  } else if (use_style) {
    (*output) << "<pre>" << ej.ToString() << "</pre>";
  } else {
    (*output) << ej.ToString();
  }
}

bool Webserver::static_pages_available() const {
  return !opts_.doc_root.empty() && opts_.enable_doc_root;
}

void Webserver::set_footer_html(const std::string& html) {
  std::lock_guard<RWMutex> l(lock_);
  footer_html_ = html;
}

} // namespace kudu
