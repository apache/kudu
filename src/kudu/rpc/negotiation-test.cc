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

#include "kudu/rpc/rpc-test-base.h"

#include <stdlib.h>
#include <sys/stat.h>

#include <functional>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include <sasl/sasl.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/subprocess.h"

using std::string;
using std::thread;

// HACK: MIT Kerberos doesn't have any way of determining its version number,
// but the error messages in krb5-1.10 and earlier are broken due to
// a bug: http://krbdev.mit.edu/rt/Ticket/Display.html?id=6973
//
// Since we don't have any way to explicitly figure out the version, we just
// look for this random macro which was added in 1.11 (the same version in which
// the above bug was fixed).
#ifndef KRB5_RESPONDER_QUESTION_PASSWORD
#define KRB5_VERSION_LE_1_10
#endif

DEFINE_bool(is_test_child, false,
            "Used by tests which require clean processes. "
            "See TestDisableInit.");

namespace kudu {
namespace rpc {

class TestSaslRpc : public RpcTestBase {
 public:
  virtual void SetUp() OVERRIDE {
    RpcTestBase::SetUp();
    ASSERT_OK(SaslInit(kSaslAppName));
  }
};

// Test basic initialization of the objects.
TEST_F(TestSaslRpc, TestBasicInit) {
  SaslServer server(kSaslAppName, nullptr);
  server.EnablePlain();
  ASSERT_OK(server.Init(kSaslAppName));
  SaslClient client(kSaslAppName, nullptr);
  client.EnablePlain("test", "test");
  ASSERT_OK(client.Init(kSaslAppName));
}

// A "Callable" that takes a Socket* param, for use with starting a thread.
// Can be used for SaslServer or SaslClient threads.
typedef std::function<void(Socket*)> SocketCallable;

// Call Accept() on the socket, then pass the connection to the server runner
static void RunAcceptingDelegator(Socket* acceptor,
                                  const SocketCallable& server_runner) {
  Socket conn;
  Sockaddr remote;
  CHECK_OK(acceptor->Accept(&conn, &remote, 0));
  server_runner(&conn);
}

// Set up a socket and run a SASL negotiation.
static void RunNegotiationTest(const SocketCallable& server_runner,
                               const SocketCallable& client_runner) {
  Socket server_sock;
  CHECK_OK(server_sock.Init(0));
  ASSERT_OK(server_sock.BindAndListen(Sockaddr(), 1));
  Sockaddr server_bind_addr;
  ASSERT_OK(server_sock.GetSocketAddress(&server_bind_addr));
  thread server(RunAcceptingDelegator, &server_sock, server_runner);

  Socket client_sock;
  CHECK_OK(client_sock.Init(0));
  ASSERT_OK(client_sock.Connect(server_bind_addr));
  thread client(client_runner, &client_sock);

  LOG(INFO) << "Waiting for test threads to terminate...";
  client.join();
  LOG(INFO) << "Client thread terminated.";

  // TODO(todd): if the client fails to negotiate, it doesn't
  // always result in sending a nice error message to the
  // other side.
  client_sock.Close();

  server.join();
  LOG(INFO) << "Server thread terminated.";
}

////////////////////////////////////////////////////////////////////////////////

static void RunPlainNegotiationServer(Socket* conn) {
  SaslServer sasl_server(kSaslAppName, conn);
  CHECK_OK(sasl_server.EnablePlain());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  CHECK_OK(sasl_server.Negotiate());
  CHECK(ContainsKey(sasl_server.client_features(), APPLICATION_FEATURE_FLAGS));
  CHECK_EQ("my-username", sasl_server.authenticated_user());
}

static void RunPlainNegotiationClient(Socket* conn) {
  SaslClient sasl_client(kSaslAppName, conn);
  CHECK_OK(sasl_client.EnablePlain("my-username", "ignored password"));
  CHECK_OK(sasl_client.Init(kSaslAppName));
  CHECK_OK(sasl_client.Negotiate());
  CHECK(ContainsKey(sasl_client.server_features(), APPLICATION_FEATURE_FLAGS));
}

// Test SASL negotiation using the PLAIN mechanism over a socket.
TEST_F(TestSaslRpc, TestPlainNegotiation) {
  RunNegotiationTest(RunPlainNegotiationServer, RunPlainNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////


template<class T>
using CheckerFunction = std::function<void(const Status&, T&)>;

// Run GSSAPI negotiation from the server side. Runs
// 'post_check' after negotiation to verify the result.
static void RunGSSAPINegotiationServer(
    Socket* conn,
    const CheckerFunction<SaslServer>& post_check) {
  SaslServer sasl_server(kSaslAppName, conn);
  sasl_server.set_server_fqdn("127.0.0.1");
  CHECK_OK(sasl_server.EnableGSSAPI());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  post_check(sasl_server.Negotiate(), sasl_server);
}

// Run GSSAPI negotiation from the client side. Runs
// 'post_check' after negotiation to verify the result.
static void RunGSSAPINegotiationClient(
    Socket* conn,
    const CheckerFunction<SaslClient>& post_check) {
  SaslClient sasl_client(kSaslAppName, conn);
  sasl_client.set_server_fqdn("127.0.0.1");
  CHECK_OK(sasl_client.EnableGSSAPI());
  CHECK_OK(sasl_client.Init(kSaslAppName));
  post_check(sasl_client.Negotiate(), sasl_client);
}

// Test configuring a client to allow but not require Kerberos/GSSAPI,
// and connect to a server which requires Kerberos/GSSAPI.
//
// They should negotiate to use Kerberos/GSSAPI.
TEST_F(TestSaslRpc, TestRestrictiveServer_NonRestrictiveClient) {
  MiniKdc kdc;
  ASSERT_OK(kdc.Start());

  // Create the server principal and keytab.
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));

  // Create and kinit as a client user.
  ASSERT_OK(kdc.CreateUserPrincipal("testuser"));
  ASSERT_OK(kdc.Kinit("testuser"));
  ASSERT_OK(kdc.SetKrb5Environment());

  // Authentication should now succeed on both sides.
  RunNegotiationTest(
      std::bind(RunGSSAPINegotiationServer, std::placeholders::_1,
                [](const Status& s, SaslServer& server) {
                  CHECK_OK(s);
                  CHECK_EQ(SaslMechanism::GSSAPI, server.negotiated_mechanism());
                  CHECK_EQ("testuser", server.authenticated_user());
                }),
      [](Socket* conn) {
        SaslClient sasl_client(kSaslAppName, conn);
        sasl_client.set_server_fqdn("127.0.0.1");
        // The client enables both PLAIN and GSSAPI.
        CHECK_OK(sasl_client.EnablePlain("foo", "bar"));
        CHECK_OK(sasl_client.EnableGSSAPI());
        CHECK_OK(sasl_client.Init(kSaslAppName));
        CHECK_OK(sasl_client.Negotiate());
        CHECK_EQ(SaslMechanism::GSSAPI, sasl_client.negotiated_mechanism());
      });
}

// Test configuring a client to only support PLAIN, and a server which
// only supports GSSAPI. This would happen, for example, if an old Kudu
// client tries to talk to a secure-only cluster.
TEST_F(TestSaslRpc, TestNoMatchingMechanisms) {
  MiniKdc kdc;
  ASSERT_OK(kdc.Start());

  // Create the server principal and keytab.
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/localhost", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));

  RunNegotiationTest(
      std::bind(RunGSSAPINegotiationServer, std::placeholders::_1,
                [](const Status& s, SaslServer& server) {
                  // The client fails to find a matching mechanism and
                  // doesn't send any failure message to the server.
                  // Instead, it just disconnects.
                  //
                  // TODO(todd): this could produce a better message!
                  ASSERT_STR_CONTAINS(s.ToString(), "got EOF from remote");
                }),
      [](Socket* conn) {
        SaslClient sasl_client(kSaslAppName, conn);
        sasl_client.set_server_fqdn("127.0.0.1");
        // The client enables both PLAIN and GSSAPI.
        CHECK_OK(sasl_client.EnablePlain("foo", "bar"));
        CHECK_OK(sasl_client.Init(kSaslAppName));
        Status s = sasl_client.Negotiate();
        ASSERT_STR_CONTAINS(s.ToString(), "client was missing the required SASL module");
      });
}

// Test SASL negotiation using the GSSAPI (kerberos) mechanism over a socket.
TEST_F(TestSaslRpc, TestGSSAPINegotiation) {
  MiniKdc kdc;
  ASSERT_OK(kdc.Start());

  // Create the server principal and keytab.
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));

  // Create and kinit as a client user.
  ASSERT_OK(kdc.CreateUserPrincipal("testuser"));
  ASSERT_OK(kdc.Kinit("testuser"));
  ASSERT_OK(kdc.SetKrb5Environment());

  // Authentication should succeed on both sides.
  RunNegotiationTest(
      std::bind(RunGSSAPINegotiationServer, std::placeholders::_1,
                [](const Status& s, SaslServer& server) {
                  CHECK_OK(s);
                  CHECK_EQ(SaslMechanism::GSSAPI, server.negotiated_mechanism());
                  CHECK_EQ("testuser", server.authenticated_user());
                }),
      std::bind(RunGSSAPINegotiationClient, std::placeholders::_1,
                [](const Status& s, SaslClient& client) {
                  CHECK_OK(s);
                  CHECK_EQ(SaslMechanism::GSSAPI, client.negotiated_mechanism());
                }));
}

#ifndef __APPLE__
// Test invalid SASL negotiations using the GSSAPI (kerberos) mechanism over a socket.
// This test is ignored on macOS because the system Kerberos implementation
// (Heimdal) caches the non-existence of client credentials, which causes futher
// tests to fail.
TEST_F(TestSaslRpc, TestGSSAPIInvalidNegotiation) {
  MiniKdc kdc;
  ASSERT_OK(kdc.Start());

  // Try to negotiate with no krb5 credentials on either side. It should fail on both
  // sides.
  RunNegotiationTest(
      std::bind(RunGSSAPINegotiationServer, std::placeholders::_1,
                [](const Status& s, SaslServer& server) {
                  // The client notices there are no credentials and
                  // doesn't send any failure message to the server.
                  // Instead, it just disconnects.
                  //
                  // TODO(todd): it might be preferable to have the server
                  // fail to start if it has no valid keytab.
                  CHECK(s.IsNetworkError());
                }),
      std::bind(RunGSSAPINegotiationClient, std::placeholders::_1,
                [](const Status& s, SaslClient& client) {
                  CHECK(s.IsNotAuthorized());
#ifndef KRB5_VERSION_LE_1_10
                  CHECK_GT(s.ToString().find("No Kerberos credentials available"), 0);
#endif
                }));


  // Create the server principal and keytab.
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));

  // Try to negotiate with no krb5 credentials on the client. It should fail on both
  // sides.
  RunNegotiationTest(
      std::bind(RunGSSAPINegotiationServer, std::placeholders::_1,
                [](const Status& s, SaslServer& server) {
                  // The client notices there are no credentials and
                  // doesn't send any failure message to the server.
                  // Instead, it just disconnects.
                  CHECK(s.IsNetworkError());
                }),
      std::bind(RunGSSAPINegotiationClient, std::placeholders::_1,
                [](const Status& s, SaslClient& client) {
                  CHECK(s.IsNotAuthorized());
#ifndef KRB5_VERSION_LE_1_10
                  CHECK_EQ(s.message().ToString(), "No Kerberos credentials available");
#endif
                }));

  // Create and kinit as a client user.
  ASSERT_OK(kdc.CreateUserPrincipal("testuser"));
  ASSERT_OK(kdc.Kinit("testuser"));
  ASSERT_OK(kdc.SetKrb5Environment());

  // Change the server's keytab file so that it has inappropriate
  // credentials.
  // Authentication should now fail.
  ASSERT_OK(kdc.CreateServiceKeytab("otherservice/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));

  RunNegotiationTest(
      std::bind(RunGSSAPINegotiationServer, std::placeholders::_1,
                [](const Status& s, SaslServer& server) {
                  CHECK(s.IsNotAuthorized());
#ifndef KRB5_VERSION_LE_1_10
                  ASSERT_STR_CONTAINS(s.ToString(),
                                      "No key table entry found matching kudu/127.0.0.1");
#endif
                }),
      std::bind(RunGSSAPINegotiationClient, std::placeholders::_1,
                [](const Status& s, SaslClient& client) {
                  CHECK(s.IsNotAuthorized());
#ifndef KRB5_VERSION_LE_1_10
                  ASSERT_STR_CONTAINS(s.ToString(),
                                      "No key table entry found matching kudu/127.0.0.1");
#endif
                }));
}
#endif

#ifndef __APPLE__
// Test that the pre-flight check for servers requiring Kerberos provides
// nice error messages for missing or bad keytabs.
//
// This is ignored on macOS because the system Kerberos implementation does not
// fail the preflight check when the keytab is inaccessible, probably because
// the preflight check passes a 0-length token.
TEST_F(TestSaslRpc, TestPreflight) {
  // Try pre-flight with no keytab.
  Status s = SaslServer::PreflightCheckGSSAPI(kSaslAppName);
  ASSERT_FALSE(s.ok());
#ifndef KRB5_VERSION_LE_1_10
  ASSERT_STR_MATCHES(s.ToString(), "Key table file.*not found");
#endif
  // Try with a valid krb5 environment and keytab.
  MiniKdc kdc;
  ASSERT_OK(kdc.Start());
  ASSERT_OK(kdc.SetKrb5Environment());
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));

  ASSERT_OK(SaslServer::PreflightCheckGSSAPI(kSaslAppName));

  // Try with an inaccessible keytab.
  CHECK_ERR(chmod(kt_path.c_str(), 0000));
  s = SaslServer::PreflightCheckGSSAPI(kSaslAppName);
  ASSERT_FALSE(s.ok());
#ifndef KRB5_VERSION_LE_1_10
  ASSERT_STR_MATCHES(s.ToString(), "error accessing keytab: Permission denied");
#endif
  CHECK_ERR(unlink(kt_path.c_str()));

  // Try with a keytab that has the wrong credentials.
  ASSERT_OK(kdc.CreateServiceKeytab("wrong-service/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));
  s = SaslServer::PreflightCheckGSSAPI(kSaslAppName);
  ASSERT_FALSE(s.ok());
#ifndef KRB5_VERSION_LE_1_10
  ASSERT_STR_MATCHES(s.ToString(), "No key table entry found matching kudu/.*");
#endif
}
#endif

////////////////////////////////////////////////////////////////////////////////

static void RunTimeoutExpectingServer(Socket* conn) {
  SaslServer sasl_server(kSaslAppName, conn);
  CHECK_OK(sasl_server.EnablePlain());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  Status s = sasl_server.Negotiate();
  ASSERT_TRUE(s.IsNetworkError()) << "Expected client to time out and close the connection. Got: "
      << s.ToString();
}

static void RunTimeoutNegotiationClient(Socket* sock) {
  SaslClient sasl_client(kSaslAppName, sock);
  CHECK_OK(sasl_client.EnablePlain("test", "test"));
  CHECK_OK(sasl_client.Init(kSaslAppName));
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  sasl_client.set_deadline(deadline);
  Status s = sasl_client.Negotiate();
  ASSERT_TRUE(s.IsTimedOut()) << "Expected timeout! Got: " << s.ToString();
  CHECK_OK(sock->Shutdown(true, true));
}

// Ensure that the client times out.
TEST_F(TestSaslRpc, TestClientTimeout) {
  RunNegotiationTest(RunTimeoutExpectingServer, RunTimeoutNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////

static void RunTimeoutNegotiationServer(Socket* sock) {
  SaslServer sasl_server(kSaslAppName, sock);
  CHECK_OK(sasl_server.EnablePlain());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  sasl_server.set_deadline(deadline);
  Status s = sasl_server.Negotiate();
  ASSERT_TRUE(s.IsTimedOut()) << "Expected timeout! Got: " << s.ToString();
  CHECK_OK(sock->Close());
}

static void RunTimeoutExpectingClient(Socket* conn) {
  SaslClient sasl_client(kSaslAppName, conn);
  CHECK_OK(sasl_client.EnablePlain("test", "test"));
  CHECK_OK(sasl_client.Init(kSaslAppName));
  Status s = sasl_client.Negotiate();
  ASSERT_TRUE(s.IsNetworkError()) << "Expected server to time out and close the connection. Got: "
      << s.ToString();
}

// Ensure that the server times out.
TEST_F(TestSaslRpc, TestServerTimeout) {
  RunNegotiationTest(RunTimeoutNegotiationServer, RunTimeoutExpectingClient);
}

////////////////////////////////////////////////////////////////////////////////

// This suite of tests ensure that applications that embed the Kudu client are
// able to externally handle the initialization of SASL. See KUDU-1749 and
// IMPALA-4497 for context.
//
// The tests are a bit tricky because the initialization of SASL is static state
// that we can't easily clear/reset between test cases. So, each test invokes
// itself as a subprocess with the appropriate --gtest_filter line as well as a
// special flag to indicate that it is the test child running.
class TestDisableInit : public KuduTest {
 protected:
  // Run the lambda 'f' in a newly-started process, capturing its stderr
  // into 'stderr'.
  template<class TestFunc>
  void DoTest(const TestFunc& f, string* stderr = nullptr) {
    if (FLAGS_is_test_child) {
      f();
      return;
    }

    // Invoke the currently-running test case in a new subprocess.
    string filter_flag = strings::Substitute("--gtest_filter=$0.$1",
                                             CURRENT_TEST_CASE_NAME(), CURRENT_TEST_NAME());
    string executable_path;
    CHECK_OK(env_->GetExecutablePath(&executable_path));
    string stdout;
    Status s = Subprocess::Call({ executable_path, "test", filter_flag, "--is_test_child" },
                                "" /* stdin */,
                                &stdout,
                                stderr);
    ASSERT_TRUE(s.ok()) << "Test failed: " << stdout;
  }
};

// Test disabling SASL but not actually properly initializing it before usage.
TEST_F(TestDisableInit, TestDisableSasl_NotInitialized) {
  DoTest([]() {
      CHECK_OK(DisableSaslInitialization());
      Status s = SaslInit("kudu");
      ASSERT_STR_CONTAINS(s.ToString(), "was disabled, but SASL was not externally initialized");
    });
}

// Test disabling SASL with proper initialization by some other app.
TEST_F(TestDisableInit, TestDisableSasl_Good) {
  DoTest([]() {
      rpc::internal::SaslSetMutex();
      sasl_client_init(NULL);
      CHECK_OK(DisableSaslInitialization());
      ASSERT_OK(SaslInit("kudu"));
    });
}

// Test a client which inits SASL itself but doesn't remember to disable Kudu's
// SASL initialization.
TEST_F(TestDisableInit, TestMultipleSaslInit) {
  string stderr;
  DoTest([]() {
      rpc::internal::SaslSetMutex();
      sasl_client_init(NULL);
      ASSERT_OK(SaslInit("kudu"));
    }, &stderr);
  // If we are the parent, we should see the warning from the child that it automatically
  // skipped initialization because it detected that it was already initialized.
  if (!FLAGS_is_test_child) {
    ASSERT_STR_CONTAINS(stderr, "Skipping initialization");
  }
}

// We are not able to detect mutexes not being set with the macOS version of libsasl.
#ifndef __APPLE__
// Test disabling SASL but not remembering to initialize the SASL mutex support. This
// should succeed but generate a warning.
TEST_F(TestDisableInit, TestDisableSasl_NoMutexImpl) {
  string stderr;
  DoTest([]() {
      sasl_client_init(NULL);
      CHECK_OK(DisableSaslInitialization());
      ASSERT_OK(SaslInit("kudu"));
    }, &stderr);
  // If we are the parent, we should see the warning from the child.
  if (!FLAGS_is_test_child) {
    ASSERT_STR_CONTAINS(stderr, "not provided with a mutex implementation");
  }
}

// Test a client which inits SASL itself but doesn't remember to disable Kudu's
// SASL initialization.
TEST_F(TestDisableInit, TestMultipleSaslInit_NoMutexImpl) {
  string stderr;
  DoTest([]() {
      sasl_client_init(NULL);
      ASSERT_OK(SaslInit("kudu"));
    }, &stderr);
  // If we are the parent, we should see the warning from the child that it automatically
  // skipped initialization because it detected that it was already initialized.
  if (!FLAGS_is_test_child) {
    ASSERT_STR_CONTAINS(stderr, "Skipping initialization");
    ASSERT_STR_CONTAINS(stderr, "not provided with a mutex implementation");
  }
}
#endif

} // namespace rpc
} // namespace kudu
