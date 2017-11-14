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

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/security/test/test_pass.h"
#include "kudu/server/default_path_handlers.h"
#include "kudu/server/webserver.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;
using std::unique_ptr;

DECLARE_int32(webserver_max_post_length_bytes);

DEFINE_bool(test_sensitive_flag, false, "a sensitive flag");
TAG_FLAG(test_sensitive_flag, sensitive);

namespace kudu {

namespace {
void SetSslOptions(WebserverOptions* opts) {
  string password;
  CHECK_OK(security::CreateTestSSLCertWithEncryptedKey(GetTestDataDirectory(),
                                                       &opts->certificate_file,
                                                       &opts->private_key_file,
                                                       &password));
  opts->private_key_password_cmd = strings::Substitute("echo $0", password);
}

void SetHTPasswdOptions(WebserverOptions* opts) {
  CHECK_OK(security::CreateTestHTPasswd(GetTestDataDirectory(),
                                        &opts->password_file));
}
} // anonymous namespace

class WebserverTest : public KuduTest {
 public:
  WebserverTest() {
    static_dir_ = GetTestPath("webserver-docroot");
    CHECK_OK(env_->CreateDir(static_dir_));
  }

  void SetUp() override {
    KuduTest::SetUp();

    WebserverOptions opts;
    opts.port = 0;
    opts.doc_root = static_dir_;
    if (use_ssl()) SetSslOptions(&opts);
    if (use_htpasswd()) SetHTPasswdOptions(&opts);
    server_.reset(new Webserver(opts));

    AddDefaultPathHandlers(server_.get());
    ASSERT_OK(server_->Start());

    vector<Sockaddr> addrs;
    ASSERT_OK(server_->GetBoundAddresses(&addrs));
    ASSERT_EQ(addrs.size(), 1);
    addr_ = addrs[0];
  }

 protected:
  // Overridden by subclasses.
  virtual bool use_ssl() const { return false; }
  virtual bool use_htpasswd() const { return false; }

  EasyCurl curl_;
  faststring buf_;
  unique_ptr<Webserver> server_;
  Sockaddr addr_;

  string static_dir_;
};

class SslWebserverTest : public WebserverTest {
 protected:
  bool use_ssl() const override { return true; }
};

class PasswdWebserverTest : public WebserverTest {
 protected:
  bool use_htpasswd() const override { return true; }
};

// Send a HTTP request with no username and password. It should reject
// the request as the .htpasswd is presented to webserver.
TEST_F(PasswdWebserverTest, TestPasswdMissing) {
  Status status = curl_.FetchURL(strings::Substitute("http://$0/", addr_.ToString()),
                                 &buf_);
  ASSERT_EQ("Remote error: HTTP 401", status.ToString());
}

TEST_F(PasswdWebserverTest, TestPasswdPresent) {
  string auth_url = strings::Substitute("http://$0@$1/", security::kTestAuthString,
                                        addr_.ToString());
  ASSERT_OK(curl_.FetchURL(auth_url, &buf_));
}

TEST_F(WebserverTest, TestIndexPage) {
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/", addr_.ToString()),
                           &buf_));
  // Check expected header.
  ASSERT_STR_CONTAINS(buf_.ToString(), "X-Frame-Options: DENY");

  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(buf_.ToString(), "memz");
}

TEST_F(SslWebserverTest, TestSSL) {
  // We use a self-signed cert, so we need to disable cert verification in curl.
  curl_.set_verify_peer(false);

  ASSERT_OK(curl_.FetchURL(strings::Substitute("https://$0/", addr_.ToString()),
                           &buf_));
  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");
}

TEST_F(WebserverTest, TestDefaultPaths) {
  // Test memz
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/memz?raw=1", addr_.ToString()),
                           &buf_));
#ifdef TCMALLOC_ENABLED
  ASSERT_STR_CONTAINS(buf_.ToString(), "Bytes in use by application");
#else
  ASSERT_STR_CONTAINS(buf_.ToString(), "not available unless tcmalloc is enabled");
#endif

  // Test varz -- check for one of the built-in gflags flags.
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/varz?raw=1", addr_.ToString()),
                           &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "--v=");
}

TEST_F(WebserverTest, TestRedactFlagsDump) {
  kudu::g_should_redact = kudu::RedactContext::ALL;
  // Test varz -- check for the sensitive flag is redacted and HTML-escaped.
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/varz", addr_.ToString()),
                           &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "--test_sensitive_flag=&lt;redacted&gt;");

  // Test varz?raw -- check for the sensitive flag is redacted and not HTML-escaped.
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/varz?raw=1", addr_.ToString()),
                           &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), strings::Substitute("--test_sensitive_flag=$0",
                                                           kRedactionMessage));
}

// Used in symbolization test below.
void SomeMethodForSymbolTest1() {}
// Used in symbolization test below.
void SomeMethodForSymbolTest2() {}

TEST_F(WebserverTest, TestPprofPaths) {
  // Test /pprof/cmdline GET
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/pprof/cmdline", addr_.ToString()),
                           &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "webserver-test");
  ASSERT_TRUE(!HasSuffixString(buf_.ToString(), string("\x00", 1)))
    << "should not have trailing NULL: " << Slice(buf_).ToDebugString();

  // Test /pprof/symbol GET
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/pprof/symbol", addr_.ToString()),
                           &buf_));
  ASSERT_EQ(buf_.ToString(), "num_symbols: 1");

  // Test /pprof/symbol POST
  {
    // Formulate a request with some valid symbol addresses.
    string req = StringPrintf("%p+%p",
                              &SomeMethodForSymbolTest1,
                              &SomeMethodForSymbolTest2);
    SCOPED_TRACE(req);
    ASSERT_OK(curl_.PostToURL(strings::Substitute("http://$0/pprof/symbol", addr_.ToString()),
                              req, &buf_));
    ASSERT_EQ(buf_.ToString(),
              StringPrintf("%p\tkudu::SomeMethodForSymbolTest1()\n"
                           "%p\tkudu::SomeMethodForSymbolTest2()\n",
                           &SomeMethodForSymbolTest1,
                           &SomeMethodForSymbolTest2));
  }
}

// Send a POST request with too much data. It should reject
// the request with the correct HTTP error code.
TEST_F(WebserverTest, TestPostTooBig) {
  FLAGS_webserver_max_post_length_bytes = 10;
  string req(10000, 'c');
  Status s = curl_.PostToURL(strings::Substitute("http://$0/pprof/symbol", addr_.ToString()),
                             req, &buf_);
  ASSERT_EQ("Remote error: HTTP 413", s.ToString());
}

// Test that static files are served and that directory listings are
// disabled.
TEST_F(WebserverTest, TestStaticFiles) {
  // Fetch a non-existent static file.
  Status s = curl_.FetchURL(strings::Substitute("http://$0/foo.txt", addr_.ToString()),
                            &buf_);
  ASSERT_EQ("Remote error: HTTP 404", s.ToString());

  // Create the file and fetch again. This time it should succeed.
  ASSERT_OK(WriteStringToFile(env_, "hello world",
                              strings::Substitute("$0/foo.txt", static_dir_)));
  ASSERT_OK(curl_.FetchURL(strings::Substitute("http://$0/foo.txt", addr_.ToString()),
                           &buf_));
  ASSERT_EQ("hello world", buf_.ToString());

  // Create a directory and ensure that subdirectory listing is disabled.
  ASSERT_OK(env_->CreateDir(strings::Substitute("$0/dir", static_dir_)));
  s = curl_.FetchURL(strings::Substitute("http://$0/dir/", addr_.ToString()),
                     &buf_);
  ASSERT_EQ("Remote error: HTTP 403", s.ToString());
}

class WebserverAdvertisedAddressesTest : public KuduTest {
 public:
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
  virtual string use_webserver_interface() const { return ""; }
  virtual int32 use_webserver_port() const { return 0; }
  virtual string use_advertised_addresses() const { return ""; }

  void GetAddresses(vector<Sockaddr>* bound_addrs,
                    vector<Sockaddr>* advertised_addrs) {
    ASSERT_OK(server_->GetBoundAddresses(bound_addrs));
    ASSERT_OK(server_->GetAdvertisedAddresses(advertised_addrs));
  }

  unique_ptr<Webserver> server_;
};

class AdvertisedOnlyWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
};

class BoundOnlyWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_webserver_interface() const override { return "127.0.0.1"; }
  int32 use_webserver_port() const override { return 9999; }
};

class BothBoundAndAdvertisedWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
  string use_webserver_interface() const override { return "127.0.0.1"; }
  int32 use_webserver_port() const override { return 9999; }
};

TEST_F(AdvertisedOnlyWebserverTest, OnlyAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
  ASSERT_NE(9999, bound_addrs[0].port());
}

TEST_F(BoundOnlyWebserverTest, OnlyBoundAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("127.0.0.1", advertised_addrs[0].host());
  ASSERT_EQ(9999, advertised_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(9999, bound_addrs[0].port());
}

TEST_F(BothBoundAndAdvertisedWebserverTest, BothBoundAndAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(9999, bound_addrs[0].port());
}

// Various tests for failed webserver startup cases.
class WebserverNegativeTests : public KuduTest {
 protected:

  // Tries to start the webserver, expecting it to fail.
  // 'func' is used to set webserver options before starting it.
  template<class OptsFunc>
  void ExpectFailedStartup(const OptsFunc& func) {
    WebserverOptions opts;
    opts.port = 0;
    func(&opts);
    Webserver server(opts);
    Status s = server.Start();
    ASSERT_FALSE(s.ok()) << s.ToString();
  }
};

TEST_F(WebserverNegativeTests, BadCertFile) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->certificate_file = "/dev/null";
    });
}

TEST_F(WebserverNegativeTests, BadKeyFile) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_file = "/dev/null";
    });
}

TEST_F(WebserverNegativeTests, WrongPassword) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "echo wrong_pass";
    });
}

TEST_F(WebserverNegativeTests, BadPasswordCommand) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "/bin/false";
    });
}

TEST_F(WebserverNegativeTests, BadAdvertisedAddresses) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->webserver_advertised_addresses = ";;;;;";
    });
}

TEST_F(WebserverNegativeTests, BadAdvertisedAddressesZeroPort) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->webserver_advertised_addresses = "localhost:0";
    });
}

} // namespace kudu
