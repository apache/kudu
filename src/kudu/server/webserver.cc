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

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp> // IWYU pragma: keep
#include <boost/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <mustache.h>
#include <squeasel.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/gssapi.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/url-coding.h"
#include "kudu/util/version_info.h"
#include "kudu/util/zlib.h"

struct sockaddr_in;

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


namespace kudu {

namespace {

// Last error message from the webserver.
// TODO(todd) global strings are somewhat messy and lint is complaining
// about this. Clean this up.
string kWebserverLastErrMsg; // NOLINT(runtime/string)

string HttpStatusCodeToString(kudu::HttpStatusCode code) {
  switch (code) {
    case kudu::HttpStatusCode::Ok:
      return "200 OK";
    case kudu::HttpStatusCode::BadRequest:
      return "400 Bad Request";
    case kudu::HttpStatusCode::NotFound:
      return "404 Not Found";
    case kudu::HttpStatusCode::LengthRequired:
      return "411 Length Required";
    case kudu::HttpStatusCode::RequestEntityTooLarge:
      return "413 Request Entity Too Large";
    case kudu::HttpStatusCode::InternalServerError:
      return "500 Internal Server Error";
    case kudu::HttpStatusCode::ServiceUnavailable:
      return "503 Service Unavailable";
  }
  LOG(FATAL) << "Unexpected HTTP response code";
}

void SendPlainResponse(struct sq_connection* connection,
                       const string& response_code_line,
                       const string& content,
                       const vector<string>& header_lines) {
  sq_printf(connection, "HTTP/1.1 %s\r\n", response_code_line.c_str());
  for (const auto& h : header_lines) {
    sq_printf(connection, "%s\r\n", h.c_str());
  }
  sq_printf(connection, "Content-Type: text/plain\r\n");
  sq_printf(connection, "Content-Length: %zd\r\n\r\n", content.size());
  sq_printf(connection, "%s", content.c_str());
}

// Return the address of the remote user from the squeasel request info.
Sockaddr GetRemoteAddress(const struct sq_request_info* req) {
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = NetworkByteOrder::FromHost16(req->remote_port);
  addr.sin_addr.s_addr = NetworkByteOrder::FromHost32(req->remote_ip);
  return Sockaddr(addr);
}


// Performs a step of SPNEGO authorization by parsing the HTTP Authorization
// header 'authz_header' and running it through GSSAPI.
//
// If authentication fails or the header is invalid, a bad Status will be
// returned (and the other out-parameters left untouched). Otherwise, the
// out-parameters will be written to, and the function will return either OK or
// Incomplete depending on whether additional SPNEGO steps are required.
Status RunSpnegoStep(const char* authz_header, string* resp_header,
                     string* authn_user) {
  static const char* const kNegotiateStr = "WWW-Authenticate: Negotiate";
  static const Status kIncomplete = Status::Incomplete("authn incomplete");

  VLOG(2) << "Handling Authorization header "
          << (authz_header ? KUDU_REDACT(authz_header) : "<null>");

  if (!authz_header) {
    *resp_header = kNegotiateStr;
    return kIncomplete;
  }

  string neg_token;
  if (!TryStripPrefixString(authz_header, "Negotiate ", &neg_token)) {
    return Status::InvalidArgument("bad Negotiate header");
  }

  string resp_token_b64;
  bool is_complete;
  RETURN_NOT_OK(gssapi::SpnegoStep(neg_token, &resp_token_b64, &is_complete, authn_user));

  VLOG(2) << "SPNEGO step complete, response token: " << KUDU_REDACT(resp_token_b64);

  if (!resp_token_b64.empty()) {
    *resp_header = Substitute("$0 $1", kNegotiateStr, resp_token_b64);
  }
  return is_complete ? Status::OK() : kIncomplete;
}

}  // anonymous namespace

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

void Webserver::RootHandler(const Webserver::WebRequest& /* args */,
                            Webserver::WebResponse* resp) {
  EasyJson path_handlers = resp->output->Set("path_handlers", EasyJson::kArray);
  for (const PathHandlerMap::value_type& handler : path_handlers_) {
    if (handler.second->is_on_nav_bar()) {
      EasyJson path_handler = path_handlers.PushBack(EasyJson::kObject);
      path_handler["path"] = handler.first;
      path_handler["alias"] = handler.second->alias();
    }
  }
  (*resp->output)["version_info"] = EscapeForHtmlToString(VersionInfo::GetAllVersionInfo());
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
  parts.reserve(addrs.size());
  for (const Sockaddr& addr : addrs) {
    // Mongoose makes sockets with 's' suffixes accept SSL traffic only.
    parts.emplace_back(addr.ToString() + (IsSecure() ? "s" : ""));
  }

  JoinStrings(parts, ",", spec);
  return Status::OK();
}

Status Webserver::Start() {
  vector<string> options;
  if (static_pages_available()) {
    options.emplace_back("document_root");
    options.push_back(opts_.doc_root);
    options.emplace_back("enable_directory_listing");
    options.emplace_back("no");
  }

  if (IsSecure()) {
    // Initialize OpenSSL, and prevent Squeasel from also performing global
    // OpenSSL initialization.
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
      options.push_back(key_password); // May be empty if not configured.
    }

    options.emplace_back("ssl_ciphers");
    options.emplace_back(opts_.tls_ciphers);
    options.emplace_back("ssl_min_version");
    options.emplace_back(opts_.tls_min_protocol);
  }

  if (!opts_.authentication_domain.empty()) {
    options.emplace_back("authentication_domain");
    options.push_back(opts_.authentication_domain);
  }

  if (!opts_.password_file.empty()) {
    // Mongoose doesn't log anything if it can't stat the password file (but
    // will if it can't open it, which it tries to do during a request).
    if (!Env::Default()->FileExists(opts_.password_file)) {
      ostringstream ss;
      ss << "Webserver: Password file does not exist: " << opts_.password_file;
      return Status::InvalidArgument(ss.str());
    }
    options.emplace_back("global_auth_file");
    options.push_back(opts_.password_file);
  }

  if (opts_.require_spnego) {
    // We assume that security::InitKerberosForServer() has already been called, which
    // ensures that the keytab path has been propagated into this environment variable
    // where the GSSAPI calls will pick it up.
    const char* kt_file = getenv("KRB5_KTNAME");
    if (!kt_file || !Env::Default()->FileExists(kt_file)) {
      return Status::InvalidArgument("Unable to configure web server for SPNEGO authentication: "
                                     "must configure a keytab file for the server");
    }
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

  RegisterPathHandler("/", "Home", default_callback,
                      /*is_styled=*/true, /*is_on_nav_bar=*/true);

  vector<Sockaddr> addrs;
  RETURN_NOT_OK(GetBoundAddresses(&addrs));
  string bound_addresses_str;
  for (const Sockaddr& addr : addrs) {
    if (!bound_addresses_str.empty()) {
      bound_addresses_str += ", ";
    }
    bound_addresses_str += Substitute("$0$1/",
                                      IsSecure() ? "https://" : "http://",
                                      addr.ToString());
  }

  LOG(INFO) << Substitute(
      "Webserver started at $0 using document root $1 and password file $2",
      bound_addresses_str,
      static_pages_available() ? opts_.doc_root : "<none>",
      opts_.password_file.empty() ? "<none>" : opts_.password_file);
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
    return Status::ServiceUnavailable("Not started");
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
    return Status::ServiceUnavailable("Not started");
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

sq_callback_result_t Webserver::BeginRequestCallbackStatic(
    struct sq_connection* connection) {
  struct sq_request_info* request_info = sq_get_request_info(connection);
  Webserver* instance = reinterpret_cast<Webserver*>(request_info->user_data);
  return instance->BeginRequestCallback(connection, request_info);
}

sq_callback_result_t Webserver::BeginRequestCallback(
    struct sq_connection* connection,
    struct sq_request_info* request_info) {
  if (opts_.require_spnego) {
    const char* authz_header = sq_get_header(connection, "Authorization");
    string resp_header, authn_princ;
    Status s = RunSpnegoStep(authz_header, &resp_header, &authn_princ);
    if (s.IsIncomplete()) {
      SendPlainResponse(connection, "401 Authentication Required",
                         "Must authenticate with SPNEGO.",
                         { resp_header });
      return SQ_HANDLED_OK;
    }
    if (s.ok() && authn_princ.empty()) {
      s = Status::RuntimeError("SPNEGO indicated complete, but got empty principal");
      // Crash in debug builds, but fall through to treating as an error 500 in
      // release.
      LOG(DFATAL) << "Got no authenticated principal for SPNEGO-authenticated "
                  << " connection from "
                  << GetRemoteAddress(request_info).ToString()
                  << ": " << s.ToString();
    }
    if (!s.ok()) {
      LOG(WARNING) << "Failed to authenticate request from "
                   << GetRemoteAddress(request_info).ToString()
                   << " via SPNEGO: " << s.ToString();
      const char* http_status = s.IsNotAuthorized() ? "401 Authentication Required" :
          "500 Internal Server Error";

      SendPlainResponse(connection, http_status, s.ToString(), {});
      return SQ_HANDLED_OK;
    }

    if (opts_.spnego_post_authn_callback) {
      opts_.spnego_post_authn_callback(authn_princ);
    }

    request_info->remote_user = strdup(authn_princ.c_str());

    // NOTE: According to the SPNEGO RFC (https://tools.ietf.org/html/rfc4559) it
    // is possible that a non-empty token will be returned along with the HTTP 200
    // response:
    //
    //     A status code 200 status response can also carry a "WWW-Authenticate"
    //     response header containing the final leg of an authentication.  In
    //     this case, the gssapi-data will be present.  Before using the
    //     contents of the response, the gssapi-data should be processed by
    //     gss_init_security_context to determine the state of the security
    //     context.  If this function indicates success, the response can be
    //     used by the application.  Otherwise, an appropriate action, based on
    //     the authentication status, should be taken.
    //
    //     For example, the authentication could have failed on the final leg if
    //     mutual authentication was requested and the server was not able to
    //     prove its identity.  In this case, the returned results are suspect.
    //     It is not always possible to mutually authenticate the server before
    //     the HTTP operation.  POST methods are in this category.
    //
    // In fact, from inspecting the MIT krb5 source code, it appears that this
    // only happens when the client requests mutual authentication by passing
    // 'GSS_C_MUTUAL_FLAG' when establishing its side of the protocol. In practice,
    // this seems to be widely unimplemented:
    //
    // - curl has some source code to support GSS_C_MUTUAL_FLAG, but in order to
    //   enable it, you have to modify a FALSE constant to TRUE and recompile curl.
    //   In fact, it was broken for all of 2015 without anyone noticing (see curl
    //   commit 73f1096335d468b5be7c3cc99045479c3314f433)
    //
    // - Chrome doesn't support mutual auth at all -- see DelegationTypeToFlag(...)
    //   in src/net/http/http_auth_gssapi_posix.cc.
    //
    // In practice, users depend on TLS to authenticate the server, and SPNEGO
    // is used to authenticate the client.
    //
    // Given this, and because actually sending back the token on an OK response
    // would require significant code restructuring (eg buffering the header until
    // after the response handler has run) we just ignore any response token, but
    // log a periodic warning just in case it turns out we're wrong about the above.
    if (!resp_header.empty()) {
      KLOG_EVERY_N_SECS(WARNING, 5) << "ignoring SPNEGO token on HTTP 200 response "
                                    << "for user " << authn_princ << " at host "
                                    << GetRemoteAddress(request_info).ToString();
    }
  }

  PathHandler* handler;
  {
    shared_lock<RWMutex> l(lock_);
    PathHandlerMap::const_iterator it = path_handlers_.find(request_info->uri);
    if (it == path_handlers_.end()) {
      // Let Mongoose deal with this request; returning NULL will fall through
      // to the default handler which will serve files.
      if (!opts_.doc_root.empty() && opts_.enable_doc_root) {
        VLOG(2) << "HTTP File access: " << request_info->uri;
        return SQ_CONTINUE_HANDLING;
      }
      sq_printf(connection,
                "HTTP/1.1 %s\r\nContent-Type: text/plain\r\n\r\n",
                HttpStatusCodeToString(HttpStatusCode::NotFound).c_str());
      sq_printf(connection, "No handler for URI %s\r\n\r\n", request_info->uri);
      return SQ_HANDLED_OK;
    }
    handler = it->second;
  }

  return RunPathHandler(*handler, connection, request_info);
}

sq_callback_result_t Webserver::RunPathHandler(
    const PathHandler& handler,
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
      sq_printf(connection,
                "HTTP/1.1 %s\r\n",
                HttpStatusCodeToString(HttpStatusCode::LengthRequired).c_str());
      return SQ_HANDLED_CLOSE_CONNECTION;
    }
    if (content_len > FLAGS_webserver_max_post_length_bytes) {
      // TODO(wdb): for this and other HTTP requests, we should log the
      // remote IP, etc.
      LOG(WARNING) << "Rejected POST with content length " << content_len;
      sq_printf(connection,
                "HTTP/1.1 %s\r\n",
                HttpStatusCodeToString(HttpStatusCode::RequestEntityTooLarge).c_str());
      return SQ_HANDLED_CLOSE_CONNECTION;
    }

    char buf[8192];
    int rem = content_len;
    while (rem > 0) {
      int n = sq_read(connection, buf, std::min<int>(sizeof(buf), rem));
      if (n <= 0) {
        LOG(WARNING) << "error reading POST data: expected "
                     << content_len << " bytes but only read "
                     << req.post_data.size();
        sq_printf(connection,
                  "HTTP/1.1 %s\r\n",
                  HttpStatusCodeToString(HttpStatusCode::InternalServerError).c_str());
        return SQ_HANDLED_CLOSE_CONNECTION;
      }

      req.post_data.append(buf, n);
      rem -= n;
    }
  }

  if (!handler.is_styled() || ContainsKey(req.parsed_args, "raw")) {
    use_style = false;
  }

  ostringstream content;
  PrerenderedWebResponse resp { HttpStatusCode::Ok, HttpResponseHeaders{}, &content };
  // Enable or disable redaction from the web UI based on the setting of --redact.
  // This affects operations like default value and scan predicate pretty printing.
  if (kudu::g_should_redact == kudu::RedactContext::ALL) {
    handler.callback()(req, &resp);
  } else {
    ScopedDisableRedaction s;
    handler.callback()(req, &resp);
  }

  string full_content;
  if (use_style) {
    stringstream output;
    RenderMainTemplate(content.str(), &output);
    full_content = output.str();
  } else {
    full_content = content.str();
  }

  // Check if the gzip compression is accepted by the caller. If so, compress the content.
  const char* accept_encoding_str = sq_get_header(connection, "Accept-Encoding");
  bool is_compressed = false;
  vector<string> encodings = strings::Split(accept_encoding_str, ",");
  for (string& encoding : encodings) {
    StripWhiteSpace(&encoding);
    if (encoding == "gzip") {
      ostringstream oss;
      Status s = zlib::Compress(Slice(full_content), &oss);
      if (s.ok()) {
        full_content = oss.str();
        is_compressed = true;
      } else {
        LOG(WARNING) << "Could not compress output: " << s.ToString();
      }
      break;
    }
  }

  ostringstream headers_stream;
  headers_stream << Substitute("HTTP/1.1 $0\r\n", HttpStatusCodeToString(resp.status_code));
  headers_stream << Substitute("Content-Type: $0\r\n", use_style ? "text/html" : "text/plain");
  headers_stream << Substitute("Content-Length: $0\r\n", full_content.length());
  if (is_compressed) headers_stream << "Content-Encoding: gzip\r\n";
  headers_stream << Substitute("X-Frame-Options: $0\r\n", FLAGS_webserver_x_frame_options);
  std::unordered_set<string> invalid_headers{"Content-Type", "Content-Length", "X-Frame-Options"};
  for (const auto& entry : resp.response_headers) {
    // It's forbidden to override the above headers.
    if (ContainsKey(invalid_headers, entry.first)) {
      LOG(FATAL) << "Reserved header " << entry.first << " was overridden "
          "by handler for " << handler.alias();
    }
    headers_stream << Substitute("$0: $1\r\n", entry.first, entry.second);
  }
  headers_stream << "\r\n";
  string headers = headers_stream.str();

  // Make sure to use sq_write for printing the body; sq_printf truncates at 8KB.
  sq_write(connection, headers.c_str(), headers.length());
  sq_write(connection, full_content.c_str(), full_content.length());
  return SQ_HANDLED_OK;
}

void Webserver::RegisterPathHandler(const string& path, const string& alias,
    const PathHandlerCallback& callback, bool is_styled, bool is_on_nav_bar) {
  string render_path = (path == "/") ? "/home" : path;
  auto wrapped_cb = [=](const WebRequest& args, PrerenderedWebResponse* rendered_resp) {
    EasyJson ej;
    WebResponse resp { HttpStatusCode::Ok, HttpResponseHeaders{}, &ej };
    callback(args, &resp);
    stringstream out;
    Render(render_path, ej, is_styled, &out);
    rendered_resp->status_code = resp.status_code;
    rendered_resp->response_headers = std::move(resp.response_headers);
    *rendered_resp->output << out.rdbuf();
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
    <link href='/bootstrap/css/bootstrap-table.min.css' rel='stylesheet' media='screen' />
    <script src='/jquery-3.2.1.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap-table.min.js' defer></script>
    <script src='/kudu.js' defer></script>
    <link href='/kudu.css' rel='stylesheet' />
  </head>
  <body>

    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" style="padding-top: 5px;" href="/">
            <img src="/logo.png" width='61' height='45' alt="Kudu" />
          </a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
           {{#path_handlers}}
            <li><a class="nav-link"href="{{path}}">{{alias}}</a></li>
           {{/path_handlers}}
          </ul>
        </div><!--/.nav-collapse -->
      </div><!--/.container-fluid -->
    </nav>
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
