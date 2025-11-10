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

#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/security/test/test_pass.h"
#include "kudu/server/default_path_handlers.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/web_callback_registry.h"
#include "kudu/util/zlib.h"

using std::string;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

DECLARE_bool(webserver_enable_csp);
DECLARE_bool(webserver_hsts_include_sub_domains);
DECLARE_int32(webserver_hsts_max_age_seconds);
DECLARE_int32(webserver_max_post_length_bytes);
DECLARE_string(ip_config_mode);
DECLARE_string(spnego_keytab_file);
DECLARE_string(trusted_certificate_file);
DECLARE_string(webserver_cache_control_options);
DECLARE_string(webserver_interface);
DECLARE_string(webserver_x_content_type_options);
DECLARE_string(webserver_x_frame_options);

DEFINE_bool(test_sensitive_flag, false, "a sensitive flag");
TAG_FLAG(test_sensitive_flag, sensitive);

namespace kudu {

constexpr char kIpv6Localhost[] = "[::1]";
constexpr char kIpDual[] = "[::]";

const bool kIsFips = security::IsFIPSEnabled();

namespace {
void SetSslOptions(WebserverOptions* opts) {
  string password;
  CHECK_OK(security::CreateTestSSLCertWithEncryptedKey(GetTestDataDirectory(),
                                                       &opts->certificate_file,
                                                       &opts->private_key_file,
                                                       &password));
  opts->private_key_password_cmd = Substitute("echo $0", password);
}

void SetHTPasswdOptions(WebserverOptions* opts) {
  CHECK_OK(security::CreateTestHTPasswd(GetTestDataDirectory(),
                                        &opts->password_file));
}

string GetWebserverInterface(IPMode mode) {
  switch (mode) {
    case IPMode::IPV6:
      return kIpv6Localhost;
    case IPMode::DUAL:
      return kIpDual;
    case IPMode::IPV4:
    default:
      // Return default webserver interface.
      return FLAGS_webserver_interface;
  }
}

} // anonymous namespace

class WebserverTest : public KuduTest,
                      public ::testing::WithParamInterface<IPMode> {
 public:
  WebserverTest()
    : mode_(GetParam()) {
    static_dir_ = GetTestPath("webserver-docroot");
    CHECK_OK(env_->CreateDir(static_dir_));
    FLAGS_ip_config_mode = IPModeToString(mode_);
    if (mode_ == IPMode::DUAL || mode_ == IPMode::IPV6) {
      // The wildcard address is applicable to both 'IPV6' as well as 'DUAL' modes.
      // For 'IPV6' mode, IPV6_V6ONLY is enabled on server socket option that ensures
      // IPv4 connections are rejected by the server.
      FLAGS_webserver_interface = "[::]";
    }
}

  void SetUp() override {
    KuduTest::SetUp();

    WebserverOptions opts;
    switch (mode_) {
      case IPMode::IPV6:
      case IPMode::DUAL:
        opts.bind_interface = "[::]";
        break;
      default:
        // Default value taken from FLAGS_webserver_interface.
        break;
    }
    opts.port = 0;
    opts.doc_root = static_dir_;
    opts.enable_doc_root = enable_doc_root();
    if (use_ssl()) {
      SetSslOptions(&opts);
      cert_path_ = opts.certificate_file;
      if (use_tls1_3()) {
        opts.tls_min_protocol = "TLSv1.3";
      }
    }
    if (use_htpasswd()) SetHTPasswdOptions(&opts);
    MaybeSetupSpnego(&opts);
    server_.reset(new Webserver(opts));

    AddPreInitializedDefaultPathHandlers(server_.get());
    AddPostInitializedDefaultPathHandlers(server_.get());
    if (!use_htpasswd() || !kIsFips) {
      ASSERT_OK(server_->Start());

      vector<Sockaddr> addrs;
      ASSERT_OK(server_->GetBoundAddresses(&addrs));
      ASSERT_EQ(addrs.size(), 1);
      ASSERT_TRUE(addrs[0].IsWildcard());
      if (mode_ == IPMode::IPV4) {
        // For 'IPV4' mode, set IPv4 loopback address as URL host.
        ASSERT_OK(addr_.ParseString("127.0.0.1", addrs[0].port()));
      } else {
        // For both IPV6 and DUAL mode, choose IPv6 loopback address as URL host.
        // Both are expected to work with IPv6 loopback address.
        ASSERT_OK(addr_.ParseString("[::1]", addrs[0].port()));
      }
      url_ = Substitute(use_ssl() ? "https://$0/" : "http://$0", addr_.ToString());
      // For testing purposes, we assume the server has been initialized. Typically this
      // is set to true after the rpc server is started in the server startup process.
      server_->SetStartupComplete(true);
    }
  }

  void RunTestOptions() {
    curl_.set_custom_method("OPTIONS");
    curl_.set_return_headers(true);
    ASSERT_OK(curl_.FetchURL(url_, &buf_));
    // The root "/" is registered as STYLED, so OPTIONS should return
    // only the allowed methods for STYLED pages.
    ASSERT_STR_CONTAINS(buf_.ToString(), "Allow: GET, HEAD, OPTIONS");
  }

  string ServicePrincipalName() const {
    if (mode_ == IPMode::IPV4) {
      return "HTTP/127.0.0.1";
    }
    return "HTTP/::1";
  }

 protected:
  virtual void MaybeSetupSpnego(WebserverOptions* /*opts*/) {}

  // Overridden by subclasses.
  virtual bool enable_doc_root() const { return true; }
  virtual bool use_ssl() const { return false; }
  virtual bool use_htpasswd() const { return false; }
  virtual bool use_tls1_3() const { return false; }

  EasyCurl curl_;
  faststring buf_;
  unique_ptr<Webserver> server_;
  Sockaddr addr_;
  string url_;
  string static_dir_;
  string cert_path_;
  const IPMode mode_;
};

class SslWebserverTest : public WebserverTest {
 protected:
  bool use_ssl() const override { return true; }
};

class Tls13WebserverTest : public SslWebserverTest {
 protected:
  bool use_tls1_3() const override { return true; }
};

class PasswdWebserverTest : public WebserverTest {
 protected:
  bool use_htpasswd() const override { return true; }
};

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, PasswdWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

// Send a HTTP request with no username and password. It should reject
// the request as the .htpasswd is presented to webserver.
TEST_P(PasswdWebserverTest, TestPasswdMissing) {
  if (kIsFips) {
    GTEST_SKIP();
  }
  Status status = curl_.FetchURL(url_, &buf_);
  ASSERT_EQ("Remote error: HTTP 401", status.ToString());
}

TEST_P(PasswdWebserverTest, TestPasswdPresent) {
  if (kIsFips) {
    GTEST_SKIP();
  }
  curl_.set_auth(CurlAuthType::DIGEST,
                 security::kTestAuthUsername,
                 security::kTestAuthPassword);
  ASSERT_OK(curl_.FetchURL(addr_.ToString(), &buf_));
}

TEST_P(PasswdWebserverTest, TestCrashInFIPSMode) {
  if (!kIsFips) {
    GTEST_SKIP();
  }

  Status s = server_->Start();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Digest authentication in FIPS approved mode");
}

class SpnegoWebserverTest : public WebserverTest {
 protected:
  void MaybeSetupSpnego(WebserverOptions* opts) override {
    kdc_.reset(new MiniKdc(MiniKdcOptions{}));
    ASSERT_OK(kdc_->Start());
    ASSERT_OK(kdc_->SetKrb5Environment());
    string kt_path;
    ASSERT_OK(kdc_->CreateServiceKeytab(ServicePrincipalName(), &kt_path));
    PCHECK(setenv("KRB5_KTNAME", kt_path.c_str(), 1) == 0);
    ASSERT_OK(kdc_->CreateUserPrincipal("alice"));

    opts->require_spnego = true;
    opts->spnego_post_authn_callback = [&](const string& spn) {
      last_authenticated_spn_ = spn;
    };
  }

  Status DoSpnegoCurl() {
    curl_.set_auth(CurlAuthType::SPNEGO);
    if (VLOG_IS_ON(1)) {
      curl_.set_verbose(true);
    }
    return curl_.FetchURL(url_, &buf_);
  }

  unique_ptr<MiniKdc> kdc_;
  const char* const kNotAuthn = "<none>";
  string last_authenticated_spn_ = kNotAuthn;

  // A SPNEGO token manually captured from a client exchange during some prior test run.
  // This is used as a basis for some fuzz tests.
  const char* kWellFormedTokenBase64 =
      "YIICVwYGKwYBBQUCoIICSzCCAkegDTALBgkqhkiG9xIBAgKiggI0BIICMGCCAiwGCSqGSIb3EgECAgEA"
      "boICGzCCAhegAwIBBaEDAgEOogcDBQAgAAAAo4IBP2GCATswggE3oAMCAQWhDRsLS1JCVEVTVC5DT02i"
      "HDAaoAMCAQOhEzARGwRIVFRQGwkxMjcuMC4wLjGjggEBMIH+oAMCARGhAwIBA6KB8QSB7pQrIA2cH2l4"
      "yfHpwhKz2HKYNxoxOw1j++ODByOfN3O/j9/Pp9PwJzQ7hjo5p5nK2OD+2S5YVuS92Ax/LiX8WaYxt9LC"
      "Hew8TkssFOiDffhag1taEcMG5KksPVxZejs+4NYiLj8dCwow3kShl/fpaLYXFFUgChaM7mVEDfMEIdos"
      "WB56k/KMJas7kuAkqDy8sEdPpgzbV7tPmkIFecXPKugZFTttkMREe19LcGO2KnOFflLj7s5F4euWzhrG"
      "v3oZXxDh0G6iyTouSEH+oh/LG97I0umcHrcEit6CjcjVewNhIUaP/Vn2Cu6X0FsF45qkgb4wgbugAwIB"
      "EaKBswSBsLrv38pBLMZo74lMEWHyOrbwrBG0kHfLHVSnxJJYikOwjAoNUm0/NUJc801TtbQZX/e6nRjS"
      "4spS2eU1xnPLcVBbtnonkG7xWSDv/Sl/k73oy7rObVWGQAtYkCJdcfWj1mxeojtrOPcKa9ivBiAuKcKl"
      "EdT2XD6lk161ygu306e7eH8pcuHv+bl9zP42rj85S0c3q0KXRXvsegAFUFk34+AC3fbmKLddEBUoYxms"
      "f+uj";

};

class SpnegoDedicatedKeytabWebserverTest : public SpnegoWebserverTest {
 protected:
  void MaybeSetupSpnego(WebserverOptions* opts) override {
    kdc_.reset(new MiniKdc(MiniKdcOptions{}));
    ASSERT_OK(kdc_->Start());
    ASSERT_OK(kdc_->SetKrb5Environment());
    string kt_path;
    ASSERT_OK(kdc_->CreateServiceKeytabWithName(ServicePrincipalName(),
                                                "spnego.dedicated",
                                                &kt_path));
    ASSERT_OK(kdc_->CreateUserPrincipal("alice"));
    FLAGS_spnego_keytab_file = kt_path;
    opts->require_spnego = true;
    opts->spnego_post_authn_callback = [&](const string& spn) {
      last_authenticated_spn_ = spn;
    };
  }

};

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, SpnegoDedicatedKeytabWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(SpnegoDedicatedKeytabWebserverTest, TestAuthenticated) {
  ASSERT_OK(kdc_->Kinit("alice"));
  ASSERT_OK(DoSpnegoCurl());
  EXPECT_EQ("alice@KRBTEST.COM", last_authenticated_spn_);
  EXPECT_STR_CONTAINS(buf_.ToString(), "Kudu");
}

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, SpnegoWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

// Tests that execute DoSpnegoCurl() are ignored in MacOS (except the first test case)
// MacOS heimdal kerberos caches kdc port number somewhere so that all the test cases
// executing DoSpnegoCurl() use same kdc port number and it is test defect.
// Please refer to KUDU-3533(https://issues.apache.org/jira/browse/KUDU-3533)
#ifndef __APPLE__

TEST_P(SpnegoWebserverTest, TestAuthenticated) {
  ASSERT_OK(kdc_->Kinit("alice"));
  ASSERT_OK(DoSpnegoCurl());
  EXPECT_EQ("alice@KRBTEST.COM", last_authenticated_spn_);
  EXPECT_STR_CONTAINS(buf_.ToString(), "Kudu");
}

TEST_P(SpnegoWebserverTest, TestUnauthenticatedBadKeytab) {
  ASSERT_OK(kdc_->Kinit("alice"));
  // Randomize the server's key in the KDC so that the key in the keytab doesn't match the
  // one for which the client will get a ticket. This is just an easy way to provoke an
  // error and make sure that our error handling works.
  ASSERT_OK(kdc_->RandomizePrincipalKey(ServicePrincipalName()));

  Status s = DoSpnegoCurl();
  EXPECT_EQ(s.ToString(), "Remote error: HTTP 401");
  EXPECT_EQ(kNotAuthn, last_authenticated_spn_);
  // The essence here is to get HTTP 401 error status in the server's response.
  // There might be different messages returned from webserver because of
  //   * different messages from GSSAPI on Linux and macOS (actually depends
  //     on the version of SASL library, see kGssapiPattern in CleanSaslError())
  //   * different GSSAPI failure paths depending on libkrb5 and libcurl
  //     libraries: with the randomized keytab, the server fails with SASL step
  //     with Status::Incomplete() on macOS. On Linux, the server fails the
  //     SASL step with Status::NotAuthorized(). Instead of finding some
  //     universal way of screwing up the keytab to get the same behavior on
  //     both macOS and Linux, it's easier to rely on the fact that the required
  //     HTTP error code is received and acknowledge for various error messages.
  ASSERT_STR_MATCHES(buf_.ToString(),
                     "(Unspecified GSS failure|"
                     "GSSAPI Error: Miscellaneous failure|"
                     "Must authenticate with SPNEGO)");
}

TEST_P(SpnegoWebserverTest, TestUnauthenticatedNoClientAuth) {
  Status curl_status = DoSpnegoCurl();
  EXPECT_EQ("Remote error: HTTP 401", curl_status.ToString());
  EXPECT_EQ("Must authenticate with SPNEGO.", buf_.ToString());
  EXPECT_EQ(kNotAuthn, last_authenticated_spn_);
}

#endif

// Test some malformed authorization headers.
TEST_P(SpnegoWebserverTest, TestInvalidHeaders) {
  EXPECT_EQ(curl_.FetchURL(url_, &buf_, { "Authorization: blahblah" }).ToString(),
            "Remote error: HTTP 500");
  EXPECT_STR_CONTAINS(buf_.ToString(), "bad Negotiate header");
  EXPECT_EQ(curl_.FetchURL(url_, &buf_, { "Authorization: Negotiate aaa" }).ToString(),
            "Remote error: HTTP 401");
  EXPECT_STR_CONTAINS(buf_.ToString(), "Not authorized");
  // Error messages about an invalid token come from the Kerberos library, and
  // different versions of the library have different messages.
  ASSERT_STR_MATCHES(buf_.ToString(),
                     "(Invalid token was supplied|A token was invalid)");
}

// Test that if no authorization header at all is provided, the response
// contains an empty "WWW-Authenticate: Negotiate" header.
TEST_P(SpnegoWebserverTest, TestNoAuthHeader) {
  curl_.set_return_headers(true);
  ASSERT_EQ(curl_.FetchURL(url_, &buf_).ToString(), "Remote error: HTTP 401");
  ASSERT_STR_CONTAINS(buf_.ToString(), "WWW-Authenticate: Negotiate\r\n");
}

// Test all single-bit-flips of a well-formed token, to make sure we don't
// crash.
//
// NOTE: the original token is *well-formed* but not *valid* -- i.e. even if unmodified,
// it would not produce a successful authentication result, since it is a saved constant
// from some previous run of SPNEGO on a different KDC. This test is primarily concerned
// with defending against remote buffer overflows during token parsing, etc.
TEST_P(SpnegoWebserverTest, TestBitFlippedTokens) {
  string token;
  CHECK(strings::Base64Unescape(kWellFormedTokenBase64, &token));

  for (int i = 0; i < token.size(); i++) {
    SCOPED_TRACE(i);
    for (int bit = 0; bit < 8; bit++) {
      SCOPED_TRACE(bit);
      token[i] ^= 1 << bit;
      string b64_token;
      strings::Base64Escape(token, &b64_token);
      string header = Substitute("Authorization: Negotiate $0", b64_token);
      Status s = curl_.FetchURL(url_, &buf_, { header });
      EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
      token[i] ^= 1 << bit;
    }
  }
}

// Test all truncations of a well-formed token, to make sure we don't
// crash.
//
// NOTE: see above regarding "well-formed" vs "valid".
TEST_P(SpnegoWebserverTest, TestTruncatedTokens) {
  string token;
  CHECK(strings::Base64Unescape(kWellFormedTokenBase64, &token));

  do {
    token.resize(token.size() - 1);
    SCOPED_TRACE(token.size());
    string b64_token;
    strings::Base64Escape(token, &b64_token);
    string header = Substitute("Authorization: Negotiate $0", b64_token);
    Status s = curl_.FetchURL(url_, &buf_, { header });
    EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
  } while (!token.empty());
}

// Tests that even if we don't provide adequate authentication information in
// an OPTIONS request, the server still honors it.
TEST_P(SpnegoWebserverTest, TestAuthNotRequiredForOptions) {
  NO_FATALS(RunTestOptions());
}

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, WebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(WebserverTest, TestIndexPage) {
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_));

  // Check for the expected headers.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Cache-Control: no-store");
  ASSERT_STR_CONTAINS(buf_.ToString(), "X-Frame-Options: DENY");
  ASSERT_STR_CONTAINS(buf_.ToString(), "X-Content-Type-Options: nosniff");

  FLAGS_webserver_hsts_max_age_seconds = 1000;
  // The HTTP strict transport security policy (HSTS) header should be absent
  // in the response sent from the plain HTTP (i.e. non-TLS) endpoint.
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "Strict-Transport-Security");

  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(buf_.ToString(), "memz");

  // Check that particular headers are generated as expected when customizing
  // the Cache-Control and X-Content-Type-Options headers.
  FLAGS_webserver_cache_control_options = "no-cache";
  FLAGS_webserver_x_frame_options = "SAMEORIGIN";
  FLAGS_webserver_x_content_type_options = "nosnuff";
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Cache-Control: no-cache");
  ASSERT_STR_CONTAINS(buf_.ToString(), "X-Frame-Options: SAMEORIGIN");
  ASSERT_STR_CONTAINS(buf_.ToString(), "X-Content-Type-Options: nosnuff");

  // Check that particular headers aren't added when corresponding flags
  // have empty settings.
  FLAGS_webserver_cache_control_options = "";
  FLAGS_webserver_x_frame_options = "";
  FLAGS_webserver_x_content_type_options = "";
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "Cache-Control");
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "X-Frame-Options");
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "X-Content-Type-Options");
}

TEST_P(WebserverTest, TestHttpCompression) {
  std::ostringstream oss;
  string decoded_str;

  // Curl with gzip compression enabled.
  ASSERT_OK(curl_.FetchURL(url_, &buf_, {"Accept-Encoding: deflate, br, gzip"}));

  // If compressed successfully, we should be able to uncompress.
  ASSERT_OK(zlib::Uncompress(Slice(buf_.ToString()), &oss));
  decoded_str = oss.str();

  // Should have expected title.
  ASSERT_STR_CONTAINS(decoded_str, "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(decoded_str, "memz");

  // Should have expected header when compressed with headers returned.
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_,
                          {"Accept-Encoding: deflate, megaturbogzip,  gzip , br"}));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Content-Encoding: gzip");


  // Curl with compression disabled.
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  // Check expected header.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Content-Type:");

  // Check unexpected header.
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "Content-Encoding: gzip");

  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(buf_.ToString(), "memz");


  // Curl with compression enabled but not accepted by Kudu.
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_, {"Accept-Encoding: megaturbogzip, deflate, xz"}));
  // Check expected header.
  ASSERT_STR_CONTAINS(buf_.ToString(), "HTTP/1.1 200 OK");

  // Check unexpected header.
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "Content-Encoding: gzip");

  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(buf_.ToString(), "memz");
}

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, SslWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(SslWebserverTest, TestSSL) {
  // We use a self-signed cert, so we have to trust it manually.
  FLAGS_trusted_certificate_file = cert_path_;

  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");
}

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, Tls13WebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(Tls13WebserverTest, TestTlsMinVersion) {
  FLAGS_trusted_certificate_file = cert_path_;
  curl_.set_tls_max_version(TlsVersion::TLSv1_2);

  Status s = curl_.FetchURL(url_, &buf_);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "TLS connect error");

  curl_.set_tls_min_version(TlsVersion::TLSv1_3);
  curl_.set_tls_max_version(TlsVersion::ANY);

  ASSERT_OK(curl_.FetchURL(url_, &buf_));
}

TEST_P(SslWebserverTest, StrictTransportSecurtyPolicyHeaders) {
  constexpr const char* const kHstsHeader = "Strict-Transport-Security";
  // Since the server uses a self-signed TLS certificate, disable the cert
  // validation in curl.
  curl_.set_verify_peer(false);
  curl_.set_return_headers(true);

  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  // Basic sanity check: the page should have the expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // The HSTS policy is disabled by default for the embedded Kudu webserver.
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), kHstsHeader);

  // There should be the HTTP strict transport security policy (HSTS) header
  // in the response sent from the HTTPS (i.e. TLS-protected) endpoint.
  for (auto max_age : {0, 1, 1000, 31536000}) {
    FLAGS_webserver_hsts_max_age_seconds = max_age;
    ASSERT_OK(curl_.FetchURL(url_, &buf_));
    ASSERT_STR_CONTAINS(buf_.ToString(), Substitute(
        "$0: max-age=$1", kHstsHeader, max_age));
    ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "includeSubDomains");
  }

  // Setting the flag to a negative value should result in no HSTS headers.
  for (auto max_age : {-31536000, -100, -1 }) {
    FLAGS_webserver_hsts_max_age_seconds = max_age;
    ASSERT_OK(curl_.FetchURL(url_, &buf_));
    ASSERT_STR_NOT_CONTAINS(buf_.ToString(), kHstsHeader);
  }

  constexpr auto kMaxAge = 888;
  FLAGS_webserver_hsts_max_age_seconds = kMaxAge;
  FLAGS_webserver_hsts_include_sub_domains = true;
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), Substitute(
      "$0: max-age=$1; includeSubDomains", kHstsHeader, kMaxAge));
}

TEST_P(WebserverTest, TestDefaultPaths) {
  // Test memz
  ASSERT_OK(curl_.FetchURL(Substitute("$0/memz?raw=1", url_), &buf_));
#ifdef TCMALLOC_ENABLED
  ASSERT_STR_CONTAINS(buf_.ToString(), "Bytes in use by application");
#else
  ASSERT_STR_CONTAINS(buf_.ToString(), "not available unless tcmalloc is enabled");
#endif

  // Test varz -- check for one of the built-in gflags flags.
  ASSERT_OK(curl_.FetchURL(Substitute("$0/varz?raw=1", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "--v=");

  // Test version -- check for version information
  ASSERT_OK(curl_.FetchURL(Substitute("$0/version", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "version_info");

  // Test healthz -- check for OK
  ASSERT_OK(curl_.FetchURL(Substitute("$0/healthz", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "OK");
}

TEST_P(WebserverTest, TestRedactFlagsDump) {
  kudu::g_should_redact = kudu::RedactContext::ALL;
  // Test varz -- check for the sensitive flag is redacted and HTML-escaped.
  ASSERT_OK(curl_.FetchURL(Substitute("$0/varz", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "--test_sensitive_flag=&lt;redacted&gt;");

  // Test varz?raw -- check for the sensitive flag is redacted and not HTML-escaped.
  ASSERT_OK(curl_.FetchURL(Substitute("$0/varz?raw=1", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), Substitute("--test_sensitive_flag=$0",
                                                  kRedactionMessage));
}

TEST_P(WebserverTest, TestCSPHeader) {
  constexpr const char* kCspHeader = "Content-Security-Policy";

  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  // Basic sanity check: the page should have the expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // The CSP policy is enabled by default for the embedded Kudu webserver.
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), kCspHeader);

  // Check if response doesn't contain CSP header when disabled.
  FLAGS_webserver_enable_csp = false;
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), kCspHeader);
}

// Used in symbolization test below.
void SomeMethodForSymbolTest1() {}
// Used in symbolization test below.
void SomeMethodForSymbolTest2() {}

TEST_P(WebserverTest, TestPprofPaths) {
  // Test /pprof/cmdline GET
  ASSERT_OK(curl_.FetchURL(Substitute("$0/pprof/cmdline", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "webserver-test");
  ASSERT_TRUE(!HasSuffixString(buf_.ToString(), string("\x00", 1)))
    << "should not have trailing NULL: " << Slice(buf_).ToDebugString();

  // Test /pprof/symbol GET
  ASSERT_OK(curl_.FetchURL(Substitute("$0/pprof/symbol", url_), &buf_));
  ASSERT_EQ(buf_.ToString(), "num_symbols: 1");

  // Test /pprof/symbol POST
  {
    // Formulate a request with some valid symbol addresses.
    string req = StringPrintf("%p+%p",
                              &SomeMethodForSymbolTest1,
                              &SomeMethodForSymbolTest2);
    SCOPED_TRACE(req);
    ASSERT_OK(curl_.PostToURL(Substitute("$0/pprof/symbol", url_), req, &buf_));
    ASSERT_EQ(buf_.ToString(),
              StringPrintf("%p\tkudu::SomeMethodForSymbolTest1()\n"
                           "%p\tkudu::SomeMethodForSymbolTest2()\n",
                           &SomeMethodForSymbolTest1,
                           &SomeMethodForSymbolTest2));
  }
}

// Send a POST request with too much data. It should reject
// the request with the correct HTTP error code.
TEST_P(WebserverTest, TestPostTooBig) {
  FLAGS_webserver_max_post_length_bytes = 10;
  string req(10000, 'c');
  Status s = curl_.PostToURL(Substitute("$0/pprof/symbol", url_), req, &buf_);
  ASSERT_EQ("Remote error: HTTP 413", s.ToString());
}

// Test that static files are served and that directory listings are
// disabled.
TEST_P(WebserverTest, TestStaticFiles) {
  // Fetch a non-existent static file.
  Status s = curl_.FetchURL(Substitute("$0/foo.txt", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 404", s.ToString());

  // Create the file and fetch again. This time it should succeed.
  ASSERT_OK(WriteStringToFile(env_, "hello world",
                              Substitute("$0/foo.txt", static_dir_)));
  ASSERT_OK(curl_.FetchURL(Substitute("$0/foo.txt", url_), &buf_));
  ASSERT_EQ("hello world", buf_.ToString());

  // Create a directory and ensure that subdirectory listing is disabled.
  ASSERT_OK(env_->CreateDir(Substitute("$0/dir", static_dir_)));
  s = curl_.FetchURL(Substitute("$0/dir/", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 403", s.ToString());
}

TEST_P(WebserverTest, TestDeleteMethodNotAllowed) {
  curl_.set_custom_method("DELETE");
  Status s = curl_.FetchURL(Substitute("$0/index.html", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 401", s.ToString());
}

TEST_P(WebserverTest, TestPutMethodNotAllowed) {
  curl_.set_custom_method("PUT");
  Status s = curl_.FetchURL(Substitute("$0/index.html", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 401", s.ToString());
}

// Handlers for testing HTTP method restrictions
static void StyledPageHandler(const Webserver::WebRequest& /*req*/,
                              Webserver::WebResponse* resp) {
  resp->output["message"] = "Styled page content";
}

static void FunctionalEndpointHandler(const Webserver::WebRequest& req,
                                     Webserver::PrerenderedWebResponse* resp) {
  resp->output << "Endpoint method: " << req.request_method;
}

class HttpMethodRestrictionTest : public WebserverTest {
 protected:
  void SetUp() override {
    WebserverTest::SetUp();
    // Register a styled display page (should be restricted to GET/HEAD)
    server_->RegisterPathHandler("/styled-page", "Styled", StyledPageHandler,
                                StyleMode::STYLED, true);
    // Register functional endpoints (should accept all methods)
    server_->RegisterPrerenderedPathHandler("/functional", "Functional",
                                           FunctionalEndpointHandler,
                                           StyleMode::UNSTYLED, true);
    server_->RegisterJsonPathHandler("/json", "JSON", FunctionalEndpointHandler, false);
  }
};

INSTANTIATE_TEST_SUITE_P(Parameters,
                         HttpMethodRestrictionTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(HttpMethodRestrictionTest, TestStyledPageAcceptsGet) {
  ASSERT_OK(curl_.FetchURL(Substitute("$0/styled-page", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Styled page content");
}

TEST_P(HttpMethodRestrictionTest, TestStyledPageAcceptsHead) {
  curl_.set_custom_method("HEAD");
  ASSERT_OK(curl_.FetchURL(Substitute("$0/styled-page", url_), &buf_));
}

TEST_P(HttpMethodRestrictionTest, TestStyledPageRejectsPost) {
  curl_.set_custom_method("POST");
  Status s = curl_.FetchURL(Substitute("$0/styled-page", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 405", s.ToString());
  ASSERT_STR_CONTAINS(buf_.ToString(), "Method Not Allowed");
}

TEST_P(HttpMethodRestrictionTest, TestStyledPageRejectsPut) {
  curl_.set_custom_method("PUT");
  Status s = curl_.FetchURL(Substitute("$0/styled-page", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 405", s.ToString());
  ASSERT_STR_CONTAINS(buf_.ToString(), "Method Not Allowed");
}

TEST_P(HttpMethodRestrictionTest, TestStyledPageRejectsDelete) {
  curl_.set_custom_method("DELETE");
  Status s = curl_.FetchURL(Substitute("$0/styled-page", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 405", s.ToString());
  ASSERT_STR_CONTAINS(buf_.ToString(), "Method Not Allowed");
}

TEST_P(HttpMethodRestrictionTest, TestStyledPageOptionsReturnsCorrectAllowHeader) {
  // OPTIONS should return only the methods that STYLED pages actually accept
  curl_.set_custom_method("OPTIONS");
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(Substitute("$0/styled-page", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Allow: GET, HEAD, OPTIONS");
}

TEST_P(HttpMethodRestrictionTest, TestFunctionalEndpointAcceptsPost) {
  // Verify functional endpoints (UNSTYLED) accept POST even if on nav bar
  ASSERT_OK(curl_.PostToURL(Substitute("$0/functional", url_), "test", &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Endpoint method: POST");
}

TEST_P(HttpMethodRestrictionTest, TestJSONEndpointAcceptsPost) {
  ASSERT_OK(curl_.PostToURL(Substitute("$0/json", url_), "test", &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Endpoint method: POST");
}

TEST_P(HttpMethodRestrictionTest, TestFunctionalEndpointOptionsReturnsAllMethods) {
  // Verify OPTIONS on UNSTYLED endpoints returns full set of allowed methods
  curl_.set_custom_method("OPTIONS");
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(Substitute("$0/functional", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Allow: GET, POST, HEAD, PUT, DELETE, OPTIONS");
}

TEST_P(HttpMethodRestrictionTest, TestJSONEndpointOptionsReturnsAllMethods) {
  // Verify OPTIONS on JSON endpoints returns full set of allowed methods
  curl_.set_custom_method("OPTIONS");
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(Substitute("$0/json", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Allow: GET, POST, HEAD, PUT, DELETE, OPTIONS");
}

// Test that authenticated principal is correctly passed to the handler.
static void Handler(const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
  resp->output << req.username;
}

class AuthnWebserverTest : public SpnegoWebserverTest {
 protected:
  void SetUp() override {
    WebserverTest::SetUp();
    server_->RegisterPrerenderedPathHandler(
        "/authn", "AuthnWebserverTest", Handler, StyleMode::UNSTYLED, false);
  }
};

class NoAuthnWebserverTest : public WebserverTest {
 protected:
  void SetUp() override {
    WebserverTest::SetUp();
    server_->RegisterPrerenderedPathHandler(
        "/authn", "NoAuthnWebserverTest", Handler, StyleMode::UNSTYLED, false);
  }
};

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, NoAuthnWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(NoAuthnWebserverTest, TestUnauthenticatedUser) {
  ASSERT_OK(curl_.FetchURL(Substitute("$0/authn", url_), &buf_));
  ASSERT_EQ(buf_.ToString(), "");
}

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, AuthnWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

// The following tests are skipped on macOS due to inconsistent behavior of SPNEGO.
// macOS heimdal kerberos caches the KDC port number, which can cause subsequent tests to fail.
// For more details, refer to KUDU-3533 (https://issues.apache.org/jira/browse/KUDU-3533)
#ifndef __APPLE__

TEST_P(AuthnWebserverTest, TestAuthenticatedUserPassedToHandler) {
  ASSERT_OK(kdc_->Kinit("alice"));
  curl_.set_auth(CurlAuthType::SPNEGO);
  ASSERT_OK(curl_.FetchURL(Substitute("$0/authn", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "alice");
}

TEST_P(AuthnWebserverTest, TestUnauthenticatedBadKeytab) {
  // Test based on the SpnegoWebserverTest::TestUnauthenticatedBadKeytab test.
  ASSERT_OK(kdc_->Kinit("alice"));
  ASSERT_OK(kdc_->RandomizePrincipalKey(ServicePrincipalName()));
  curl_.set_auth(CurlAuthType::SPNEGO);
  Status s = curl_.FetchURL(Substitute("$0/authn", url_), &buf_);
  EXPECT_EQ(s.ToString(), "Remote error: HTTP 401");
  ASSERT_STR_MATCHES(buf_.ToString(),
                     "(Unspecified GSS failure|"
                     "GSSAPI Error: Miscellaneous failure|"
                     "Must authenticate with SPNEGO)");
}

TEST_P(AuthnWebserverTest, TestUnauthenticatedRequestWithoutClientAuth) {
  // Test based on the SpnegoWebserverTest::TestUnauthenticatedNoClientAuth test.
  curl_.set_auth(CurlAuthType::SPNEGO);
  Status curl_status = curl_.FetchURL(Substitute("$0/authn", url_), &buf_);
  EXPECT_EQ("Remote error: HTTP 401", curl_status.ToString());
  EXPECT_EQ("Must authenticate with SPNEGO.", buf_.ToString());
}

#endif

namespace {

// Handler that echoes back the path parameters and query parameters in key-value pairs.
void PathParamHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  EasyJson* output = &resp->output;

  for (const auto& param : req.path_params) {
    (*output)["path_params"][param.first] = param.second;
  }

  for (const auto& param : req.parsed_args) {
    (*output)["query_params"][param.first] = param.second;
  }
}

class PathParamWebserverTest : public WebserverTest {
 protected:
  void SetUp() override {
    WebserverTest::SetUp();
    server_->RegisterPathHandler("/api/tables/<table_id>/tablets/<tablet_id>",
                                 "PathParamWebserverTest",
                                 PathParamHandler,
                                 StyleMode::UNSTYLED,
                                 false);
    server_->RegisterPathHandler("/api/tables/<table_id>",
                                 "PathParamWebserverTest",
                                 PathParamHandler,
                                 StyleMode::UNSTYLED,
                                 false);
    server_->RegisterPathHandler("/columns/ <column_id>",
                                 "PathParamWebserverTest",
                                 PathParamHandler,
                                 StyleMode::UNSTYLED,
                                 false);
  }
};
}  // anonymous namespace

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, PathParamWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(PathParamWebserverTest, TestPathParameterAtEnd) {
  ASSERT_OK(
      curl_.FetchURL(Substitute("$0/api/tables/45dc8d192549427b8dca871dbbb20bb3", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "\"table_id\":\"45dc8d192549427b8dca871dbbb20bb3\"");
}

TEST_P(PathParamWebserverTest, TestPathParameterInMiddleAndEnd) {
  ASSERT_OK(curl_.FetchURL(
      Substitute("$0/api/tables/45dc8d192549427b8dca871dbbb20bb3/tablets/1", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "\"table_id\":\"45dc8d192549427b8dca871dbbb20bb3\"");
  ASSERT_STR_CONTAINS(buf_.ToString(), "\"tablet_id\":\"1\"");
}

TEST_P(PathParamWebserverTest, TestInvalidPathParameter) {
  Status s = curl_.FetchURL(Substitute("$0/api/tables//tablets/1", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 404", s.ToString());
}

// Test that the query string is correctly parsed and returned in the response,
// even when the path contains a path parameter.
TEST_P(PathParamWebserverTest, TestPathWithQueryString) {
  ASSERT_OK(curl_.FetchURL(
      Substitute("$0/api/tables/45dc8d192549427b8dca871dbbb20bb3?foo=bar", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(),
                      "\"path_params\":{\"table_id\":\"45dc8d192549427b8dca871dbbb20bb3\"}");
  ASSERT_STR_CONTAINS(buf_.ToString(), "\"query_params\":{\"foo\":\"bar\"}");
}

TEST_P(PathParamWebserverTest, TestInvalidPathWithSpace) {
  Status s = curl_.FetchURL(
      Substitute("$0/api/tables/45dc8d192549427b8dca871dbbb20bb3 /tablets/1", url_), &buf_);
  ASSERT_EQ(
      "Network error: curl error: URL using bad/illegal format or missing URL: URL rejected: "
      "Malformed input to a URL function",
      s.ToString());
}

TEST_P(PathParamWebserverTest, TestRegisteredPathWithSpace) {
  Status s = curl_.FetchURL(Substitute("$0/columns/45dc8d192549427b8dca871dbbb20bb3", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 404", s.ToString());
  Status s2 =
      curl_.FetchURL(Substitute("$0/columns/ 45dc8d192549427b8dca871dbbb20bb3", url_), &buf_);
  ASSERT_EQ(
      "Network error: curl error: URL using bad/illegal format or missing URL: URL rejected: "
      "Malformed input to a URL function",
      s2.ToString());
}

TEST_P(PathParamWebserverTest, TestPathParamWithNonAsciiCharacter) {
  Status s = curl_.FetchURL(
      Substitute("$0/api/tables/45dc8d192549427b8dca871dbbb20bb3ü20/tablets/1", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 400", s.ToString());
}

class DisabledDocRootWebserverTest : public WebserverTest {
 protected:
  bool enable_doc_root() const override { return false; }
};

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, DisabledDocRootWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(DisabledDocRootWebserverTest, TestHandlerNotFound) {
  Status s = curl_.FetchURL(Substitute("$0/foo", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 404", s.ToString());
  ASSERT_STR_CONTAINS(buf_.ToString(), "No handler for URI /foo");
}

// Test that HTTP OPTIONS requests are permitted.
TEST_P(WebserverTest, TestHttpOptions) {
  NO_FATALS(RunTestOptions());
}

// Test that we're able to reuse connections for subsequent fetches.
TEST_P(WebserverTest, TestConnectionReuse) {
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_EQ(1, curl_.num_connects());
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_EQ(0, curl_.num_connects());
}

class WebserverAdvertisedAddressesTest : public KuduTest,
                                         public ::testing::WithParamInterface<IPMode> {
 public:
  WebserverAdvertisedAddressesTest()
    : mode_(GetParam()) {
      FLAGS_ip_config_mode = IPModeToString(mode_);
  }

  void SetUp() override {
    KuduTest::SetUp();

    WebserverOptions opts;
    opts.port = 0;
    string iface = use_webserver_interface();
    int32 port = use_webserver_port();
    string advertised = use_advertised_addresses();
    if (!iface.empty()) {
      opts.bind_interface = iface;
    }
    if (port != 0) {
      opts.port = port;
    }
    if (!advertised.empty()) {
      opts.webserver_advertised_addresses = advertised;
    }
    server_.reset(new Webserver(opts));

    ASSERT_OK(server_->Start());
  }

 protected:
  // Overridden by subclasses.
  virtual string use_webserver_interface() {
    return GetWebserverInterface(mode_);
  }
  virtual int32 use_webserver_port() const { return 0; }
  virtual string use_advertised_addresses() { return ""; }

  void GetAddresses(vector<Sockaddr>* bound_addrs,
                    vector<Sockaddr>* advertised_addrs) {
    ASSERT_OK(server_->GetBoundAddresses(bound_addrs));
    ASSERT_OK(server_->GetAdvertisedAddresses(advertised_addrs));
  }

  unique_ptr<Webserver> server_;
  std::string expected_webserver_host_;
  std::string expected_advertised_host_;
  const IPMode mode_;
};

class AdvertisedOnlyWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() override {
    switch (mode_) {
      case IPMode::IPV4:
        expected_advertised_host_ = "1.2.3.4";
        return "1.2.3.4:1234";
      case IPMode::IPV6:
        expected_advertised_host_ = "1234:4567::1";
        return "[1234:4567::1]:1234";
      default:
        CHECK(false) << "Unexpected IP configuration: " << FLAGS_ip_config_mode;
    }
  }
};

class BoundOnlyWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_webserver_interface() override {
    switch (mode_) {
      case IPMode::IPV4:
        expected_webserver_host_ = expected_advertised_host_ = "127.0.0.1";
        return expected_webserver_host_;
      case IPMode::IPV6:
        expected_webserver_host_ = expected_advertised_host_ = "::1";
        return "[::1]";
      default:
        CHECK(false) << "Unexpected IP configuration: " << FLAGS_ip_config_mode;
    }
  }
  int32 use_webserver_port() const override { return 9999; }
};

class BothBoundAndAdvertisedWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() override {
    switch (mode_) {
      case IPMode::IPV4:
        expected_advertised_host_ = "1.2.3.4";
        return "1.2.3.4:1234";
      case IPMode::IPV6:
        expected_advertised_host_ = "1234:4567::1";
        return "[1234:4567::1]:1234";
      default:
        CHECK(false) << "Unexpected IP configuration: " << FLAGS_ip_config_mode;
    }
  }
  string use_webserver_interface() override {
    switch (mode_) {
      case IPMode::IPV4:
        expected_webserver_host_ = "127.0.0.1";
        return expected_webserver_host_;
      case IPMode::IPV6:
        expected_webserver_host_ = "::1";
        return "[::1]";
      default:
        CHECK(false) << "Unexpected IP configuration: " << FLAGS_ip_config_mode;
    }
  }

  int32 use_webserver_port() const override { return 9999; }
};

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, AdvertisedOnlyWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6));

TEST_P(AdvertisedOnlyWebserverTest, OnlyAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ(expected_advertised_host_, advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
  ASSERT_NE(9999, bound_addrs[0].port());
}

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, BoundOnlyWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6));

TEST_P(BoundOnlyWebserverTest, OnlyBoundAddresses) {
  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ(expected_advertised_host_, advertised_addrs[0].host());
  ASSERT_EQ(9999, advertised_addrs[0].port());
  ASSERT_EQ(expected_webserver_host_, bound_addrs[0].host());
  ASSERT_EQ(9999, bound_addrs[0].port());
}

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, BothBoundAndAdvertisedWebserverTest,
                         testing::Values(IPMode::IPV4, IPMode::IPV6));

TEST_P(BothBoundAndAdvertisedWebserverTest, BothBoundAndAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());

  ASSERT_EQ(expected_advertised_host_, advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());

  ASSERT_EQ(expected_webserver_host_, bound_addrs[0].host());
  ASSERT_EQ(9999, bound_addrs[0].port());
}

// Various tests for failed webserver startup cases.
class WebserverNegativeTests : public KuduTest,
                               public ::testing::WithParamInterface<IPMode> {
 protected:

  // Tries to start the webserver, expecting it to fail.
  // 'func' is used to set webserver options before starting it.
  template<class OptsFunc>
  void ExpectFailedStartup(const OptsFunc& func) {
    FLAGS_ip_config_mode = IPModeToString(GetParam());
    WebserverOptions opts;
    opts.port = 0;
    func(&opts);
    opts.bind_interface = GetWebserverInterface(GetParam());
    Webserver server(opts);
    Status s = server.Start();
    ASSERT_FALSE(s.ok()) << s.ToString();
  }
};

// This is used to run all parameterized tests with different IP modes.
INSTANTIATE_TEST_SUITE_P(Parameters, WebserverNegativeTests,
                         testing::Values(IPMode::IPV4, IPMode::IPV6, IPMode::DUAL));

TEST_P(WebserverNegativeTests, BadCertFile) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->certificate_file = "/dev/null";
    });
}

TEST_P(WebserverNegativeTests, BadKeyFile) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_file = "/dev/null";
    });
}

TEST_P(WebserverNegativeTests, WrongPassword) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "echo wrong_pass";
    });
}

TEST_P(WebserverNegativeTests, BadPasswordCommand) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "/bin/false";
    });
}

TEST_P(WebserverNegativeTests, BadAdvertisedAddresses) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->webserver_advertised_addresses = ";;;;;";
    });
}

TEST_P(WebserverNegativeTests, BadAdvertisedAddressesZeroPort) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->webserver_advertised_addresses = "localhost:0";
    });
}

TEST_P(WebserverNegativeTests, SpnegoWithoutKeytab) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->require_spnego = true;
    });
}

} // namespace kudu
