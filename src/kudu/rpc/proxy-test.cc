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

#include "kudu/rpc/proxy.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/notification.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(dns_addr_resolution_override);

using std::shared_ptr;
using std::string;
using std::thread;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rpc {

class Messenger;

namespace {

constexpr uint16_t kPort = 1111;
constexpr const char* kFakeHost = "fakehost";
const HostPort kFakeHostPort(kFakeHost, kPort);

void SendRequestAsync(const ResponseCallback& cb,
                      Proxy* p, RpcController* controller, SleepResponsePB* resp) {
  SleepRequestPB req;
  req.set_sleep_micros(100 * 1000); // 100ms
  p->AsyncRequest(GenericCalculatorService::kSleepMethodName, req, resp, controller, cb);
}

Status SendRequest(Proxy* p) {
  SleepRequestPB req;
  req.set_sleep_micros(100 * 1000); // 100ms
  SleepResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(10000));
  return p->SyncRequest(GenericCalculatorService::kSleepMethodName, req, &resp, &controller);
}

} // anonymous namespace

class RpcProxyTest : public RpcTestBase {
};

TEST_F(RpcProxyTest, TestProxyRetriesWhenRequestLeavesScope) {
  DnsResolver dns_resolver(1, 1024 * 1024);
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("client_messenger", &client_messenger));

  // We're providing a fake hostport to encourage retries of DNS resolution,
  // which will attempt to send the request payload after the request protobuf
  // has already left scope.
  Proxy p(client_messenger, kFakeHostPort, &dns_resolver,
          CalculatorService::static_service_name());
  p.Init();

  SleepResponsePB resp;
  {
    Notification note;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    SendRequestAsync([&note] () { note.Notify(); }, &p, &controller, &resp);
    note.WaitForNotification();
    Status s = controller.status();
    ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  }

  // Now try a successful call.
  string ip = GetBindIpForDaemon(/*index*/2, kDefaultBindMode);
  Sockaddr addr;
  ASSERT_OK(addr.ParseString(ip, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&addr));
  FLAGS_dns_addr_resolution_override = Substitute("$0=$1", kFakeHost, addr.ToString());

  Notification note;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(10000));
  SendRequestAsync([&note] () { note.Notify(); }, &p, &controller, &resp);
  note.WaitForNotification();
  ASSERT_OK(controller.status());
}

// Test that proxies initialized with a DnsResolver return errors when
// receiving a non-transient error.
TEST_F(RpcProxyTest, TestProxyReturnsOnNonTransientError) {
  SKIP_IF_SLOW_NOT_ALLOWED();  // This test waits for a timeout.

  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("client_messenger", &client_messenger));
  DnsResolver dns_resolver(1, 1024 * 1024);
  Proxy p(client_messenger, kFakeHostPort, &dns_resolver,
          CalculatorService::static_service_name());
  p.Init();
  Status s = SendRequest(&p);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // If we do resolve to an address that turns out to be bogus, we should
  // time out when negotiating.
  FLAGS_dns_addr_resolution_override = Substitute("$0=1.1.1.1", kFakeHost);
  s = SendRequest(&p);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
}

// Test that ensures a proxy initialized with an address will use that address.
TEST_F(RpcProxyTest, TestProxyUsesInitialAddr) {
  string ip1 = GetBindIpForDaemon(/*index*/1, kDefaultBindMode);
  Sockaddr server_addr;
  ASSERT_OK(server_addr.ParseString(ip1, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr));

  // Despite our proxy being configured with a fake host, our request should
  // still go through since we call Init() with a valid address.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("client_messenger", &client_messenger));
  DnsResolver dns_resolver(1, 1024 * 1024);
  Proxy p(client_messenger, kFakeHostPort, &dns_resolver,
          CalculatorService::static_service_name());
  p.Init(server_addr);
  ASSERT_OK(SendRequest(&p));

  server_messenger_.reset();
  service_pool_.reset();

  // With our server down, the request should fail.
  Status s = SendRequest(&p);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Once we bring up a new server and allow our proxy to resolve it, the
  // request should succeed.
  string ip2 = GetBindIpForDaemon(/*index*/2, kDefaultBindMode);
  Sockaddr second_addr;
  ASSERT_OK(second_addr.ParseString(ip2, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&second_addr));
  FLAGS_dns_addr_resolution_override = Substitute("$0=$1", kFakeHost, second_addr.ToString());
  ASSERT_OK(SendRequest(&p));
}

TEST_F(RpcProxyTest, TestNonResolvingProxyIgnoresInit) {
  string ip = GetBindIpForDaemon(/*index*/1, kDefaultBindMode);
  Sockaddr server_addr;
  ASSERT_OK(server_addr.ParseString(ip, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr));

  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("client_messenger", &client_messenger));
  DnsResolver dns_resolver(1, 1024 * 1024);
  HostPort hp(ip, kPort);
  Proxy p(client_messenger, hp, &dns_resolver, CalculatorService::static_service_name());

  // Call Init() with a fake address. Because this proxy isn't configured for
  // address re-resolution, the new address is ignored.
  Sockaddr fake_addr;
  ASSERT_OK(fake_addr.ParseString("1.1.1.1", kPort));
  p.Init(fake_addr);

  // We should thus have no trouble sending a request.
  ASSERT_OK(SendRequest(&p));
}

// Start a proxy with a DNS resolver that maps a hostname to the address bound
// by the server. Then restart the server but bind to a different address, and
// update the DNS resolver to map the same hostname to the different address.
// The proxy should eventually be usable.
TEST_F(RpcProxyTest, TestProxyReresolvesAddress) {
  string ip1 = GetBindIpForDaemon(/*index*/1, kDefaultBindMode);
  Sockaddr server_addr;
  ASSERT_OK(server_addr.ParseString(ip1, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr));
  FLAGS_dns_addr_resolution_override = Substitute("$0=$1", kFakeHost, server_addr.ToString());

  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("client_messenger", &client_messenger));
  DnsResolver dns_resolver(1, 1024 * 1024);
  Proxy p(client_messenger, kFakeHostPort, &dns_resolver,
          CalculatorService::static_service_name());
  p.Init();
  ASSERT_OK(SendRequest(&p));

  string ip2 = GetBindIpForDaemon(/*index*/2, kDefaultBindMode);
  Sockaddr second_addr;
  ASSERT_OK(second_addr.ParseString(ip2, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&second_addr));
  FLAGS_dns_addr_resolution_override = Substitute("$0=$1", kFakeHost, second_addr.ToString());
  ASSERT_OK(SendRequest(&p));
}

TEST_F(RpcProxyTest, TestProxyReresolvesAddressFromThreads) {
  constexpr const int kNumThreads = 4;

  string ip1 = GetBindIpForDaemon(/*index*/1, kDefaultBindMode);
  Sockaddr server_addr;
  ASSERT_OK(server_addr.ParseString(ip1, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr));
  FLAGS_dns_addr_resolution_override = Substitute("$0=$1", kFakeHost, server_addr.ToString());

  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("client_messenger", &client_messenger));
  DnsResolver dns_resolver(1, 1024 * 1024);
  Proxy p(client_messenger, kFakeHostPort, &dns_resolver,
          CalculatorService::static_service_name());
  p.Init();
  ASSERT_OK(SendRequest(&p));

  string ip2 = GetBindIpForDaemon(/*index*/2, kDefaultBindMode);
  Sockaddr second_addr;
  ASSERT_OK(second_addr.ParseString(ip2, kPort));
  ASSERT_OK(StartTestServerWithGeneratedCode(&second_addr));
  FLAGS_dns_addr_resolution_override = Substitute("$0=$1", kFakeHost, second_addr.ToString());

  vector<Status> errors(kNumThreads);
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      errors[i] = SendRequest(&p);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& e : errors) {
    EXPECT_OK(e);
  }
}

} // namespace rpc
} // namespace kudu
