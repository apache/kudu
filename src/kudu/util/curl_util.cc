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

#include "kudu/util/curl_util.h"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <curl/curl.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/scoped_cleanup.h"

using std::string;
using std::vector;

namespace kudu {

namespace {

inline Status TranslateError(CURLcode code) {
  if (code == CURLE_OK) {
    return Status::OK();
  }
  if (code == CURLE_OPERATION_TIMEDOUT) {
    return Status::TimedOut("curl timeout", curl_easy_strerror(code));
  }
  return Status::NetworkError("curl error", curl_easy_strerror(code));
}

extern "C" {
size_t WriteCallback(void* buffer, size_t size, size_t nmemb, void* user_ptr) {
  size_t real_size = size * nmemb;
  faststring* buf = reinterpret_cast<faststring*>(user_ptr);
  CHECK_NOTNULL(buf)->append(reinterpret_cast<const uint8_t*>(buffer), real_size);
  return real_size;
}
} // extern "C"

} // anonymous namespace

EasyCurl::EasyCurl() {
  // Use our own SSL initialization, and disable curl's.
  // Both of these calls are idempotent.
  security::InitializeOpenSSL();
  // curl_global_init() is not thread safe and multiple calls have the
  // same effect as one call.
  // See more details: https://curl.haxx.se/libcurl/c/curl_global_init.html
  static std::once_flag once;
  std::call_once(once, []() {
    CHECK_EQ(0, curl_global_init(CURL_GLOBAL_DEFAULT & ~CURL_GLOBAL_SSL));
  });
  curl_ = curl_easy_init();
  CHECK(curl_) << "Could not init curl";
}

EasyCurl::~EasyCurl() {
  curl_easy_cleanup(curl_);
}

Status EasyCurl::FetchURL(const string& url, faststring* dst,
                          const vector<string>& headers) {
  return DoRequest(url, nullptr, dst, headers);
}

Status EasyCurl::PostToURL(const string& url,
                           const string& post_data,
                           faststring* dst) {
  return DoRequest(url, &post_data, dst);
}

Status EasyCurl::DoRequest(const string& url,
                           const string* post_data,
                           faststring* dst,
                           const vector<string>& headers) {
  CHECK_NOTNULL(dst)->clear();

  if (!verify_peer_) {
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(
        curl_, CURLOPT_SSL_VERIFYHOST, 0)));
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(
        curl_, CURLOPT_SSL_VERIFYPEER, 0)));
  }

  if (use_spnego_) {
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(
        curl_, CURLOPT_HTTPAUTH, CURLAUTH_NEGOTIATE)));
    // It's necessary to pass an empty user/password to trigger the authentication
    // code paths in curl, even though SPNEGO doesn't use them.
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(
        curl_, CURLOPT_USERPWD, ":")));
  }

  if (verbose_) {
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(
        curl_, CURLOPT_VERBOSE, 1)));
  }

  // Add headers if specified.
  struct curl_slist* curl_headers = nullptr;
  auto clean_up_curl_slist = MakeScopedCleanup([&]() {
    curl_slist_free_all(curl_headers);
  });

  for (const auto& header : headers) {
    curl_headers = CHECK_NOTNULL(curl_slist_append(curl_headers, header.c_str()));
  }
  RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, curl_headers)));

  RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_URL, url.c_str())));
  if (return_headers_) {
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_HEADER, 1)));
  }
  RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback)));
  RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_WRITEDATA,
                                                static_cast<void *>(dst))));
  if (post_data) {
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_POSTFIELDS,
                                                  post_data->c_str())));
  }

  // Done after CURLOPT_POSTFIELDS in case that resets the method (the docs[1]
  // are unclear on whether that happens).
  //
  // 1. https://curl.haxx.se/libcurl/c/CURLOPT_POSTFIELDS.html
  if (!custom_method_.empty()) {
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST,
                                                  custom_method_.c_str())));
  }

  RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_HTTPAUTH, CURLAUTH_ANY)));
  if (timeout_.Initialized()) {
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1)));
    RETURN_NOT_OK(TranslateError(curl_easy_setopt(curl_, CURLOPT_TIMEOUT_MS,
        timeout_.ToMilliseconds())));
  }
  RETURN_NOT_OK(TranslateError(curl_easy_perform(curl_)));
  long val; // NOLINT(*) curl wants a long
  RETURN_NOT_OK(TranslateError(curl_easy_getinfo(curl_, CURLINFO_NUM_CONNECTS, &val)));
  num_connects_ = val;

  RETURN_NOT_OK(TranslateError(curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &val)));
  if (val != 200) {
    return Status::RemoteError(strings::Substitute("HTTP $0", val));
  }
  return Status::OK();
}

} // namespace kudu
