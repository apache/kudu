// Copyright (c) 2013, Cloudera, inc

#include "rpc/rpc-test-base.h"

#include <string>

#include <boost/thread.hpp>
#include <gtest/gtest.h>
#include <sasl/sasl.h>

#include "gutil/gscoped_ptr.h"
#include "rpc/constants.h"
#include "rpc/auth_store.h"
#include "rpc/sasl_client.h"
#include "rpc/sasl_common.h"
#include "rpc/sasl_server.h"
#include "util/net/sockaddr.h"
#include "util/net/socket.h"

using std::string;

namespace kudu {
namespace rpc {

class TestSaslRpc : public RpcTestBase {
 public:
  virtual void SetUp() {
    RpcTestBase::SetUp();
    ASSERT_STATUS_OK(SaslInit(kSaslAppName));
  }
};

// Test basic initialization of the objects.
TEST_F(TestSaslRpc, TestBasicInit) {
  SaslServer server(kSaslAppName, -1);
  ASSERT_STATUS_OK(server.Init(kSaslAppName));
  SaslClient client(kSaslAppName, -1);
  ASSERT_STATUS_OK(client.Init(kSaslAppName));
}

// A "Callable" that takes a Socket* param, for use with starting a thread.
// Can be used for SaslServer or SaslClient threads.
typedef void (*socket_callable_t)(Socket*);

// Call Accept() on the socket, then pass the connection to the server runner
static void RunAcceptingDelegator(Socket* acceptor, socket_callable_t server_runner) {
  Socket conn;
  Sockaddr remote;
  CHECK_OK(acceptor->Accept(&conn, &remote, 0));
  server_runner(&conn);
}

// Set up a socket and run a SASL negotiation.
static void RunNegotiationTest(socket_callable_t server_runner, socket_callable_t client_runner) {
  Socket server_sock;
  server_sock.Init(0);
  ASSERT_STATUS_OK(server_sock.BindAndListen(Sockaddr(), 1));
  Sockaddr server_bind_addr;
  ASSERT_STATUS_OK(server_sock.GetSocketAddress(&server_bind_addr));
  boost::thread server(RunAcceptingDelegator, &server_sock, server_runner);

  Socket client_sock;
  client_sock.Init(0);
  ASSERT_STATUS_OK(client_sock.Connect(server_bind_addr));
  boost::thread client(client_runner, &client_sock);

  LOG(INFO) << "Waiting for test threads to terminate...";
  client.join();
  LOG(INFO) << "Client thread terminated.";
  server.join();
  LOG(INFO) << "Server thread terminated.";
}

////////////////////////////////////////////////////////////////////////////////

static void RunAnonNegotiationServer(Socket* conn) {
  SaslServer sasl_server(kSaslAppName, conn->GetFd());
  CHECK_OK(sasl_server.Init(kSaslAppName));
  sasl_server.EnableAnonymous();
  CHECK_OK(sasl_server.Negotiate());
}

static void RunAnonNegotiationClient(Socket* conn) {
  SaslClient sasl_client(kSaslAppName, conn->GetFd());
  CHECK_OK(sasl_client.Init(kSaslAppName));
  sasl_client.EnableAnonymous();
  CHECK_OK(sasl_client.Negotiate());
}

// Test SASL negotiation using the ANONYMOUS mechanism over a socket.
TEST_F(TestSaslRpc, TestAnonNegotiation) {
  RunNegotiationTest(RunAnonNegotiationServer, RunAnonNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////

static void RunPlainNegotiationServer(Socket* conn) {
  SaslServer sasl_server(kSaslAppName, conn->GetFd());
  gscoped_ptr<AuthStore> authstore(new AuthStore());
  authstore->Add("danger", "burrito");
  CHECK_OK(sasl_server.Init(kSaslAppName));
  sasl_server.EnablePlain(authstore.Pass());
  CHECK_OK(sasl_server.Negotiate());
}

static void RunPlainNegotiationClient(Socket* conn) {
  SaslClient sasl_client(kSaslAppName, conn->GetFd());
  CHECK_OK(sasl_client.Init(kSaslAppName));
  sasl_client.EnablePlain("danger", "burrito");
  CHECK_OK(sasl_client.Negotiate());
}

// Test SASL negotiation using the PLAIN mechanism over a socket.
TEST_F(TestSaslRpc, TestPlainNegotiation) {
  RunNegotiationTest(RunPlainNegotiationServer, RunPlainNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////

static void RunPlainFailingNegotiationServer(Socket* conn) {
  SaslServer sasl_server(kSaslAppName, conn->GetFd());
  gscoped_ptr<AuthStore> authstore(new AuthStore());
  authstore->Add("danger", "burrito");
  CHECK_OK(sasl_server.Init(kSaslAppName));
  sasl_server.EnablePlain(authstore.Pass());
  Status s = sasl_server.Negotiate();
  ASSERT_TRUE(s.IsNotAuthorized()) << "Expected auth failure! Got: " << s.ToString();
}

static void RunPlainFailingNegotiationClient(Socket* conn) {
  SaslClient sasl_client(kSaslAppName, conn->GetFd());
  CHECK_OK(sasl_client.Init(kSaslAppName));
  sasl_client.EnablePlain("unknown", "burrito");
  Status s = sasl_client.Negotiate();
  ASSERT_TRUE(s.IsNotAuthorized()) << "Expected auth failure! Got: " << s.ToString();
}

// Test SASL negotiation using the PLAIN mechanism over a socket.
TEST_F(TestSaslRpc, TestPlainFailingNegotiation) {
  RunNegotiationTest(RunPlainFailingNegotiationServer, RunPlainFailingNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace rpc
} // namespace kudu
