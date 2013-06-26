// Copyright (c) 2013, Cloudera, inc

#include <gtest/gtest.h>

#include "rpc/rtest.proxy.h"
#include "rpc/rtest.service.h"
#include "rpc/rpc-test-base.h"
#include "util/test_util.h"

namespace kudu {
namespace rpc {

class RpcStubTest : public RpcTestBase {
 protected:
};

TEST_F(RpcStubTest, TestSimpleCall) {
  Sockaddr server_addr;
  StartTestServerWithGeneratedCode(&server_addr);

  shared_ptr<Messenger> client_messenger = CreateMessenger("Client");
  CalculatorServiceProxy p(client_messenger, server_addr);

  RpcController controller;
  AddRequestPB req;
  req.set_x(10);
  req.set_y(20);
  AddResponsePB resp;
  ASSERT_STATUS_OK(p.Add(req, &resp, &controller));
  ASSERT_EQ(30, resp.result());
}

TEST_F(RpcStubTest, TestRespondDeferred) {
  Sockaddr server_addr;
  StartTestServerWithGeneratedCode(&server_addr);

  shared_ptr<Messenger> client_messenger = CreateMessenger("Client");
  CalculatorServiceProxy p(client_messenger, server_addr);

  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(1000);
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_STATUS_OK(p.Sleep(req, &resp, &controller));
}


// Test sending a PB parameter with a missing field
TEST_F(RpcStubTest, TestCallWithInvalidParam) {
  Sockaddr server_addr;
  StartTestServerWithGeneratedCode(&server_addr);

  shared_ptr<Messenger> client_messenger = CreateMessenger("Client");
  Proxy p(client_messenger, server_addr);

  AddRequestPartialPB req;
  req.set_x(rand());
  // AddRequestPartialPB is missing the 'y' field.
  AddResponsePB resp;
  RpcController controller;
  Status s = p.SyncRequest("Add", req, &resp, &controller);
  ASSERT_TRUE(s.IsRuntimeError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Invalid argument: Invalid parameter for call Add: y");
}

// Test sending a call which isn't implemented by the server.
TEST_F(RpcStubTest, TestCallMissingMethod) {
  Sockaddr server_addr;
  StartTestServerWithGeneratedCode(&server_addr);

  shared_ptr<Messenger> client_messenger = CreateMessenger("Client");
  Proxy p(client_messenger, server_addr);

  Status s = DoTestSyncCall(p, "DoesNotExist");
  ASSERT_TRUE(s.IsRuntimeError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid method: DoesNotExist");
}

}
}
