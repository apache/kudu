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
#include "kudu/rpc/constants.h"
#include "kudu/rpc/sasl_client.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/sasl_server.h"
#include "kudu/security/mini_kdc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"

using std::string;
using std::thread;

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
  server.EnableAnonymous();
  ASSERT_OK(server.Init(kSaslAppName));
  SaslClient client(kSaslAppName, nullptr);
  client.EnableAnonymous();
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

static void RunAnonNegotiationServer(Socket* conn) {
  SaslServer sasl_server(kSaslAppName, conn);
  CHECK_OK(sasl_server.EnableAnonymous());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  CHECK_OK(sasl_server.Negotiate());
}

static void RunAnonNegotiationClient(Socket* conn) {
  SaslClient sasl_client(kSaslAppName, conn);
  CHECK_OK(sasl_client.EnableAnonymous());
  CHECK_OK(sasl_client.Init(kSaslAppName));
  CHECK_OK(sasl_client.Negotiate());
}

// Test SASL negotiation using the ANONYMOUS mechanism over a socket.
TEST_F(TestSaslRpc, TestAnonNegotiation) {
  RunNegotiationTest(RunAnonNegotiationServer, RunAnonNegotiationClient);
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
                  CHECK_GT(s.ToString().find("No Kerberos credentials available"), 0);
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
                  CHECK_EQ(s.message(), "No Kerberos credentials available");
                }));


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
      std::bind(RunGSSAPINegotiationClient, std::placeholders::_1,
                [](const Status& s, SaslClient& client) {
                  CHECK_OK(s);
                  CHECK_EQ(SaslMechanism::GSSAPI, client.negotiated_mechanism());
                }));


  // Change the server's keytab file so that it has inappropriate
  // credentials.
  // Authentication should now fail.
  ASSERT_OK(kdc.CreateServiceKeytab("otherservice/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));

  RunNegotiationTest(
      std::bind(RunGSSAPINegotiationServer, std::placeholders::_1,
                [](const Status& s, SaslServer& server) {
                  CHECK(s.IsNotAuthorized());
                  ASSERT_STR_CONTAINS(s.ToString(),
                                      "No key table entry found matching kudu/127.0.0.1");
                }),
      std::bind(RunGSSAPINegotiationClient, std::placeholders::_1,
                [](const Status& s, SaslClient& client) {
                  CHECK(s.IsNotAuthorized());
                  ASSERT_STR_CONTAINS(s.ToString(),
                                      "No key table entry found matching kudu/127.0.0.1");
                }));

}

// Test that the pre-flight check for servers requiring Kerberos provides
// nice error messages for missing or bad keytabs.
TEST_F(TestSaslRpc, TestPreflight) {
  // Try pre-flight with no keytab.
  Status s = SaslServer::PreflightCheckGSSAPI(kSaslAppName);
  ASSERT_STR_MATCHES(s.ToString(), "Key table file.*not found");

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
  ASSERT_STR_MATCHES(s.ToString(), "error accessing keytab: Permission denied");
  CHECK_ERR(unlink(kt_path.c_str()));

  // Try with a keytab that has the wrong credentials.
  ASSERT_OK(kdc.CreateServiceKeytab("wrong-service/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/));
  s = SaslServer::PreflightCheckGSSAPI(kSaslAppName);
  ASSERT_STR_MATCHES(s.ToString(), "No key table entry found matching kudu/.*");
}

////////////////////////////////////////////////////////////////////////////////

static void RunTimeoutExpectingServer(Socket* conn) {
  SaslServer sasl_server(kSaslAppName, conn);
  CHECK_OK(sasl_server.EnableAnonymous());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  Status s = sasl_server.Negotiate();
  ASSERT_TRUE(s.IsNetworkError()) << "Expected client to time out and close the connection. Got: "
      << s.ToString();
}

static void RunTimeoutNegotiationClient(Socket* sock) {
  SaslClient sasl_client(kSaslAppName, sock);
  CHECK_OK(sasl_client.EnableAnonymous());
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
  CHECK_OK(sasl_server.EnableAnonymous());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  sasl_server.set_deadline(deadline);
  Status s = sasl_server.Negotiate();
  ASSERT_TRUE(s.IsTimedOut()) << "Expected timeout! Got: " << s.ToString();
  CHECK_OK(sock->Close());
}

static void RunTimeoutExpectingClient(Socket* conn) {
  SaslClient sasl_client(kSaslAppName, conn);
  CHECK_OK(sasl_client.EnableAnonymous());
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

} // namespace rpc
} // namespace kudu
