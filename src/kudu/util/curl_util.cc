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

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <curl/curl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"

using std::string;
using std::vector;
using strings::Substitute;

DEFINE_string(trusted_certificate_file, "",
              "Path to a file that contains certificate(s) (in PEM format) "
              "to trust when Kudu establishes a TLS-protected connection "
              "to HTTP/HTTPS server (e.g., KMS service, JWKS server, etc.)");

namespace kudu {

namespace {

inline Status TranslateError(CURLcode code, const char* errbuf) {
  if (code == CURLE_OK) {
    return Status::OK();
  }

  string err_msg = curl_easy_strerror(code);
  if (strlen(errbuf) != 0) {
    err_msg += Substitute(": $0", errbuf);
  }

  if (code == CURLE_OPERATION_TIMEDOUT) {
    return Status::TimedOut("curl timeout", err_msg);
  }
  return Status::NetworkError("curl error", err_msg);
}

bool ValidateTrustedCertFile() {
  const auto& fpath = FLAGS_trusted_certificate_file;
  if (fpath.empty()) {
    // No validation is needed.
    return true;
  }

  // Make sure the file in question does exist, is readable, and non-empty.
  // There might be extra verification to load the certificate(s) from the file,
  // but since cURL could have some particular requirements on the contents
  // of the file, let's skip that extra validation step and defer to the time
  // when cURL loads the file on its own. Also, the validators might run
  // at the time when the OpenSSL-based crypto runtime context isn't yet
  // initialized, so it's safer to defer to TlsContext::Init() where the
  // initialization is done in a proper way.
  std::unique_ptr<RandomAccessFile> raf;
  if (auto s = Env::Default()->NewRandomAccessFile(fpath, &raf); !s.ok()) {
    LOG(ERROR) << Substitute("could not open file for reading: $0", s.ToString());
    return false;
  }
  // Read just a single byte to make sure that the file is readable.
  uint8_t scratch[1];
  Slice data(scratch, sizeof(scratch));
  if (auto s = raf->Read(0, data); !s.ok()) {
    LOG(ERROR) << Substitute("could not read from file '$0': $1",
                             fpath, s.ToString());
    return false;
  }
  return true;
}

// The validator uses Env API, so it's necessary to use GROUP_FLAG_VALIDATOR()
// instead of regular gflag's DEFINE_validator() macro to allow for custom
// behavior of PosixEnv per settings of various flags defined in env_posix.cc.
GROUP_FLAG_VALIDATOR(validate_trusted_certificate_file,
                     ValidateTrustedCertFile);

extern "C" {
size_t WriteCallback(void* buffer, size_t size, size_t nmemb, void* user_ptr) {
  size_t real_size = size * nmemb;
  faststring* buf = reinterpret_cast<faststring*>(user_ptr);
  CHECK_NOTNULL(buf)->append(reinterpret_cast<const uint8_t*>(buffer), real_size);
  return real_size;
}
} // extern "C"

} // anonymous namespace

// This is an internal EasyCurl's utility macro.
#define CURL_RETURN_NOT_OK(expr) \
  RETURN_NOT_OK(TranslateError((expr), errbuf_))

EasyCurl::EasyCurl()
    : noproxy_("*") {
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

  // Set the error buffer to enhance error messages with more details, when
  // available.
  static_assert(kErrBufSize >= CURL_ERROR_SIZE, "kErrBufSize is too small");
  const auto code = curl_easy_setopt(curl_, CURLOPT_ERRORBUFFER, errbuf_);
  CHECK_EQ(CURLE_OK, code);
}

EasyCurl::~EasyCurl() {
  curl_easy_cleanup(curl_);
}

Status EasyCurl::FetchURL(const string& url, faststring* dst,
                          const vector<string>& headers) {
  return DoRequest(url, nullptr, dst, headers);
}

Status EasyCurl::FetchURL(const vector<string>& urls, faststring* dst,
                          const vector<string>& headers) {
  return DoRequest(urls, nullptr, dst, headers);
}

Status EasyCurl::PostToURL(const string& url,
                           const string& post_data,
                           faststring* dst,
                           const vector<string>& headers) {
  return DoRequest(url, &post_data, dst, headers);
}

Status EasyCurl::PostToURL(const vector<string>& urls,
                           const string& post_data,
                           faststring* dst,
                           const vector<string>& headers) {
  return DoRequest(urls, &post_data, dst, headers);
}

Status EasyCurl::DoRequest(const vector<string>& urls,
                           const string* post_data,
                           faststring* dst,
                           const vector<string>& headers) {
  DCHECK(!urls.empty());
  Status s;
  for (const auto& url : urls) {
    s = DoRequest(url, post_data, dst, headers);
    if (s.IsNetworkError() || s.IsTimedOut()) {
      continue;
    }
    break;
  }
  return s;
}

Status EasyCurl::DoRequest(const string& url,
                           const string* post_data,
                           faststring* dst,
                           const vector<string>& headers) {
  CHECK_NOTNULL(dst)->clear();

  // Mark the error buffer as cleared.
  errbuf_[0] = 0;

  if (!verify_peer_) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 0));
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 0));
  } else if (!FLAGS_trusted_certificate_file.empty()) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_CAINFO,
                                        FLAGS_trusted_certificate_file.c_str()));
  }

  switch (auth_type_) {
    case CurlAuthType::SPNEGO:
      CURL_RETURN_NOT_OK(curl_easy_setopt(
            curl_, CURLOPT_HTTPAUTH, CURLAUTH_NEGOTIATE));
      break;
    case CurlAuthType::DIGEST:
      CURL_RETURN_NOT_OK(curl_easy_setopt(
            curl_, CURLOPT_HTTPAUTH, CURLAUTH_DIGEST));
      break;
    case CurlAuthType::BASIC:
      CURL_RETURN_NOT_OK(curl_easy_setopt(
            curl_, CURLOPT_HTTPAUTH, CURLAUTH_BASIC));
      break;
    case CurlAuthType::NONE:
      break;
    default:
      CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_HTTPAUTH, CURLAUTH_ANY));
      break;
  }

  if (auth_type_ != CurlAuthType::NONE) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_USERNAME, username_.c_str()));
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_PASSWORD, password_.c_str()));
  }

  if (verbose_) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_VERBOSE, 1));
  }
  if (fail_on_http_error_) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_FAILONERROR, 1));
  }

  uint16_t tls_min_version;
  switch (tls_min_version_) {
    case TlsVersion::TLSv1:
      tls_min_version = CURL_SSLVERSION_TLSv1_0;
      break;
    case TlsVersion::TLSv1_1:
      tls_min_version = CURL_SSLVERSION_TLSv1_1;
      break;
    case TlsVersion::TLSv1_2:
      tls_min_version = CURL_SSLVERSION_TLSv1_2;
      break;
    case TlsVersion::TLSv1_3:
      tls_min_version = CURL_SSLVERSION_TLSv1_3;
      break;
    case TlsVersion::ANY:
      tls_min_version = CURL_SSLVERSION_DEFAULT;
      break;
  }
  CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_SSLVERSION, tls_min_version));

  uint64_t tls_max_version;
  switch (tls_max_version_) {
    case TlsVersion::TLSv1:
      tls_max_version = CURL_SSLVERSION_MAX_TLSv1_0;
      break;
    case TlsVersion::TLSv1_1:
      tls_max_version = CURL_SSLVERSION_MAX_TLSv1_1;
      break;
    case TlsVersion::TLSv1_2:
      tls_max_version = CURL_SSLVERSION_MAX_TLSv1_2;
      break;
    case TlsVersion::TLSv1_3:
      tls_max_version = CURL_SSLVERSION_MAX_TLSv1_3;
      break;
    case TlsVersion::ANY:
      tls_max_version = CURL_SSLVERSION_MAX_DEFAULT;
      break;
  }
  CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_SSLVERSION, tls_max_version));


  // Add headers if specified.
  struct curl_slist* curl_headers = nullptr;
  auto clean_up_curl_slist = MakeScopedCleanup([&]() {
    curl_slist_free_all(curl_headers);
  });

  for (const auto& header : headers) {
    curl_headers = CHECK_NOTNULL(curl_slist_append(curl_headers, header.c_str()));
  }
  CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, curl_headers));

  CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_URL, url.c_str()));
  if (return_headers_) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_HEADER, 1));
  }
  CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback));
  CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_WRITEDATA, static_cast<void *>(dst)));
  if (post_data) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(
        curl_, CURLOPT_POSTFIELDS, post_data->c_str()));
  }

  // Done after CURLOPT_POSTFIELDS in case that resets the method (the docs[1]
  // are unclear on whether that happens).
  //
  // 1. https://curl.haxx.se/libcurl/c/CURLOPT_POSTFIELDS.html
  if (!custom_method_.empty()) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(
        curl_, CURLOPT_CUSTOMREQUEST, custom_method_.c_str()));
  }

  if (!noproxy_.empty()) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_NOPROXY, noproxy_.c_str()));
  }

  if (timeout_.Initialized()) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1));
    CURL_RETURN_NOT_OK(curl_easy_setopt(
        curl_, CURLOPT_TIMEOUT_MS, timeout_.ToMilliseconds()));
  }

  if (!dns_servers_.empty()) {
    CURL_RETURN_NOT_OK(curl_easy_setopt(curl_, CURLOPT_DNS_SERVERS, dns_servers_.c_str()));
  }

  CURL_RETURN_NOT_OK(curl_easy_perform(curl_));
  long val; // NOLINT(*) curl wants a long
  CURL_RETURN_NOT_OK(curl_easy_getinfo(curl_, CURLINFO_NUM_CONNECTS, &val));
  num_connects_ = static_cast<int>(val);

  CURL_RETURN_NOT_OK(curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &val));
  if (val < 200 || val >= 300) {
    return Status::RemoteError(Substitute("HTTP $0", val));
  }
  return Status::OK();
}

} // namespace kudu
