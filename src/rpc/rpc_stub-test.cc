// Copyright (c) 2013, Cloudera, inc

#include <gtest/gtest.h>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

#include "rpc/rtest.proxy.h"
#include "rpc/rtest.service.h"
#include "rpc/rpc-test-base.h"
#include "util/countdown_latch.h"
#include "util/test_util.h"
#include "util/user.h"

DEFINE_bool(is_panic_test_child, false, "Used by TestRpcPanic");

using boost::ptr_vector;

namespace kudu {
namespace rpc {

class RpcStubTest : public RpcTestBase {
 public:
  virtual void SetUp() {
    RpcTestBase::SetUp();
    StartTestServerWithGeneratedCode(&server_addr_);
    client_messenger_ = CreateMessenger("Client");
  }
 protected:
  Sockaddr server_addr_;
  shared_ptr<Messenger> client_messenger_;
};

TEST_F(RpcStubTest, TestSimpleCall) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  AddRequestPB req;
  req.set_x(10);
  req.set_y(20);
  AddResponsePB resp;
  ASSERT_STATUS_OK(p.Add(req, &resp, &controller));
  ASSERT_EQ(30, resp.result());
}

// Test calls which are rather large.
// This test sends many of them at once using the async API and then
// waits for them all to return. This is meant to ensure that the
// IO threads can deal with read/write calls that don't succeed
// in sending the entire data in one go.
TEST_F(RpcStubTest, TestBigCallData) {
  const int kNumSentAtOnce = 20;
  const size_t kMessageSize = 5 * 1024 * 1024;
  string data;
  data.resize(kMessageSize);

  CalculatorServiceProxy p(client_messenger_, server_addr_);

  EchoRequestPB req;
  req.set_data(data);

  ptr_vector<EchoResponsePB> resps;
  ptr_vector<RpcController> controllers;

  CountDownLatch latch(kNumSentAtOnce);
  for (int i = 0; i < kNumSentAtOnce; i++) {
    EchoResponsePB *resp = new EchoResponsePB;
    resps.push_back(resp);
    RpcController *controller = new RpcController;
    controllers.push_back(controller);

    p.EchoAsync(req, resp, controller,
                boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));
  }

  latch.Wait();

  BOOST_FOREACH(RpcController &c, controllers) {
    ASSERT_STATUS_OK(c.status());
  }
}

TEST_F(RpcStubTest, TestRespondDeferred) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(1000);
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_STATUS_OK(p.Sleep(req, &resp, &controller));
}

// Test that the default user credentials are propagated to the server.
TEST_F(RpcStubTest, TestDefaultCredentialsPropagated) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  string expected;
  ASSERT_STATUS_OK(GetLoggedInUser(&expected));

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_STATUS_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_EQ(expected, resp.credentials().real_user());
  ASSERT_FALSE(resp.credentials().has_effective_user());
}

// Test that the user can specify other credentials.
TEST_F(RpcStubTest, TestCustomCredentialsPropagated) {
  const char* const kFakeUserName = "some fake user";
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  UserCredentials creds;
  creds.set_real_user(kFakeUserName);
  p.set_user_credentials(creds);

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_STATUS_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_EQ(kFakeUserName, resp.credentials().real_user());
  ASSERT_FALSE(resp.credentials().has_effective_user());
}

// Test that the user's remote address is accessible to the server.
TEST_F(RpcStubTest, TestRemoteAddress) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_STATUS_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_STR_CONTAINS(resp.address(), "127.0.0.1:");
}

////////////////////////////////////////////////////////////
// Tests for error cases
////////////////////////////////////////////////////////////

// Test sending a PB parameter with a missing field, where the client
// thinks it has sent a full PB. (eg due to version mismatch)
TEST_F(RpcStubTest, TestCallWithInvalidParam) {
  Proxy p(client_messenger_, server_addr_, CalculatorService::static_service_name());

  AddRequestPartialPB req;
  req.set_x(rand());
  // AddRequestPartialPB is missing the 'y' field.
  AddResponsePB resp;
  RpcController controller;
  Status s = p.SyncRequest("Add", req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Invalid argument: Invalid parameter for call Add: y");
}

// Wrapper around AtomicIncrement, since AtomicIncrement returns the 'old'
// value, and our callback needs to be a void function.
static void DoIncrement(Atomic32* count) {
  base::subtle::Barrier_AtomicIncrement(count, 1);
}

// Test sending a PB parameter with a missing field on the client side.
// This also ensures that the async callback is only called once
// (regression test for a previously-encountered bug).
TEST_F(RpcStubTest, TestCallWithMissingPBFieldClientSide) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  AddRequestPB req;
  req.set_x(10);
  // Request is missing the 'y' field.
  AddResponsePB resp;
  Atomic32 callback_count = 0;
  p.AddAsync(req, &resp, &controller, boost::bind(&DoIncrement, &callback_count));
  while (NoBarrier_Load(&callback_count) == 0) {
    usleep(10);
  }
  usleep(100);
  ASSERT_EQ(1, NoBarrier_Load(&callback_count));
  ASSERT_STR_CONTAINS(controller.status().ToString(),
                      "Invalid argument: RPC argument missing required fields: y");
}

// Test sending a call which isn't implemented by the server.
TEST_F(RpcStubTest, TestCallMissingMethod) {
  Proxy p(client_messenger_, server_addr_, CalculatorService::static_service_name());

  Status s = DoTestSyncCall(p, "DoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid method: DoesNotExist");
}

TEST_F(RpcStubTest, TestApplicationError) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  SleepRequestPB req;
  SleepResponsePB resp;
  req.set_sleep_micros(1);
  req.set_return_app_error(true);
  Status s = p.Sleep(req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError());
  EXPECT_EQ("Remote error: Got some error", s.ToString());
  EXPECT_EQ("message: \"Got some error\"\n"
            "[kudu.rpc_test.CalculatorError.app_error_ext] {\n"
            "  extra_error_data: \"some application-specific error data\"\n"
            "}\n", controller.error_response()->DebugString());
}

TEST_F(RpcStubTest, TestRpcPanic) {
  if (!FLAGS_is_panic_test_child) {
    // This is a poor man's death test. We call this same
    // test case, but set the above flag, and verify that
    // it aborted. gtest death tests don't work here because
    // there are already threads started up.
    string exe = strings::Substitute("/proc/$0/exe", getpid());
    string cmdline = exe +
      " --is_panic_test_child" +
      " --gtest_filter=RpcStubTest.TestRpcPanic";

    int ret = system(cmdline.c_str());
    CHECK(WIFSIGNALED(ret));
    CHECK_EQ(WTERMSIG(ret), SIGABRT);
    return;
  } else {
    // Make an RPC which causes the server to abort.
    CalculatorServiceProxy p(client_messenger_, server_addr_);
    RpcController controller;
    PanicRequestPB req;
    PanicResponsePB resp;
    p.Panic(req, &resp, &controller);
  }
}

} // namespace rpc
} // namespace kudu
