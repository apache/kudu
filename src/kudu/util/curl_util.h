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

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

typedef void CURL;

namespace kudu {

class faststring;

enum class CurlAuthType {
  NONE,
  BASIC,
  DIGEST,
  SPNEGO,
};

// Simple wrapper around curl's "easy" interface, allowing the user to
// fetch web pages into memory using a blocking API.
//
// This is not thread-safe.
class EasyCurl {
 public:
  EasyCurl();
  ~EasyCurl();

  // Fetch the given URL into the provided buffer.
  // Any existing data in the buffer is replaced.
  // The optional param 'headers' holds additional headers.
  // e.g. {"Accept-Encoding: gzip"}
  Status FetchURL(const std::string& url,
                  faststring* dst,
                  const std::vector<std::string>& headers = {});

  // Issue an HTTP POST to the given URL with the given data.
  // Returns results in 'dst' as above.
  // The optional param 'headers' holds additional headers.
  // e.g. {"Accept-Encoding: gzip"}
  Status PostToURL(const std::string& url,
                   const std::string& post_data,
                   faststring* dst,
                   const std::vector<std::string>& headers = {});

  // Set whether to verify the server's SSL certificate in the case of an HTTPS
  // connection.
  void set_verify_peer(bool verify) {
    verify_peer_ = verify;
  }

  void set_return_headers(bool v) {
    return_headers_ = v;
  }

  void set_timeout(MonoDelta t) {
    timeout_ = t;
  }

  Status set_auth(CurlAuthType auth_type, std::string username = "", std::string password = "") {
    auth_type_ = std::move(auth_type);
    username_ = std::move(username);
    password_ = std::move(password);

    return Status::OK();
  }

  // Enable verbose mode for curl. This dumps debugging output to stderr, so
  // is only really useful in the context of tests.
  void set_verbose(bool v) {
    verbose_ = v;
  }

  // Overrides curl's HTTP method handling with a custom method string.
  void set_custom_method(std::string m) {
    custom_method_ = std::move(m);
  }

  // A comma-separated list of host names to avoid requests being proxied to,
  // or "*" glob to disable proxying of any requests even if proxying is
  // configured via CURLOPT_PROXY or '{http,https}_proxy' environment variables.
  // An empty string "" clears the setting. By default, it's set to "*" since
  // EasyCurl is primarily used in scenarios fetching data from embedded
  // webservers of kudu-master/kudu-tserver running at the same host from where
  // a request is issued, while 'http_proxy' and 'https_proxy' environment
  // variables might be disruptive in that regard.
  // See 'man CURLOPT_NOPROXY' for details.
  void set_noproxy(std::string noproxy) {
    noproxy_ = std::move(noproxy);
  }

  // Whether to return an error if server responds with HTTP code >= 400.
  // By default, curl returns the returned content and the response code
  // since it's handy in case of auth-related HTTP response codes such as
  // 401 and 407. See 'man CURLOPT_FAILONERROR' for details.
  void set_fail_on_http_error(bool fail_on_http_error) {
    fail_on_http_error_ = fail_on_http_error;
  }

  // Returns the number of new connections created to achieve the previous transfer.
  int num_connects() const {
    return num_connects_;
  }

 private:
  static const constexpr size_t kErrBufSize = 256;

  // Do a request. If 'post_data' is non-NULL, does a POST.
  // Otherwise, does a GET.
  Status DoRequest(const std::string& url,
                   const std::string* post_data,
                   faststring* dst,
                   const std::vector<std::string>& headers = {});

  CURL* curl_;

  std::string custom_method_;

  std::string noproxy_;

  // Whether to verify the server certificate.
  bool verify_peer_ = true;

  // Whether to return the HTTP headers with the response.
  bool return_headers_ = false;

  bool verbose_ = false;

  // The default setting for CURLOPT_FAILONERROR in libcurl is 0 (false).
  bool fail_on_http_error_ = false;

  MonoDelta timeout_;

  int num_connects_ = 0;

  char errbuf_[kErrBufSize];

  std::string username_;

  std::string password_;

  CurlAuthType auth_type_ = CurlAuthType::NONE;

  DISALLOW_COPY_AND_ASSIGN(EasyCurl);
};

} // namespace kudu
