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

#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/server/default-path-handlers.h"
#include "kudu/server/webserver.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/test_util.h"

using std::string;

DECLARE_int32(webserver_max_post_length_bytes);

DEFINE_bool(test_sensitive_flag, false, "a sensitive flag");
TAG_FLAG(test_sensitive_flag, sensitive);

namespace kudu {

namespace {
void SetSslOptions(WebserverOptions* opts) {
  string password;
  CHECK_OK(security::CreateTestSSLCerts(GetTestDataDirectory(),
                                        &opts->certificate_file,
                                        &opts->private_key_file,
                                        &password));
  opts->private_key_password_cmd = strings::Substitute("echo $0", password);
}
} // anonymous namespace

class WebserverTest : public KuduTest {
 public:
  WebserverTest() {
    static_dir_ = GetTestPath("webserver-docroot");
    CHECK_OK(env_->CreateDir(static_dir_));
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    WebserverOptions opts;
    opts.port = 0;
    opts.doc_root = static_dir_;
    if (use_ssl()) SetSslOptions(&opts);
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

  EasyCurl curl_;
  faststring buf_;
  gscoped_ptr<Webserver> server_;
  Sockaddr addr_;

  string static_dir_;
};

class SslWebserverTest : public WebserverTest {
 protected:
  bool use_ssl() const override { return true; }
};

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
    ASSERT_FALSE(s.ok());
  }
};

TEST_F(WebserverNegativeTests, BadCertFile) {
  ExpectFailedStartup([this](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->certificate_file = "/dev/null";
    });
}

TEST_F(WebserverNegativeTests, BadKeyFile) {
  ExpectFailedStartup([this](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_file = "/dev/null";
    });
}

TEST_F(WebserverNegativeTests, WrongPassword) {
  ExpectFailedStartup([this](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "echo wrong_pass";
    });
}

TEST_F(WebserverNegativeTests, BadPasswordCommand) {
  ExpectFailedStartup([this](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "/bin/false";
    });
}

} // namespace kudu
