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

#include "kudu/server/rpc_server.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(rpc_server_allow_ephemeral_ports);

namespace kudu {

class RpcServerAdvertisedAddressesTest : public KuduTest {
 public:

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_rpc_server_allow_ephemeral_ports = true;

    RpcServerOptions opts;
    {
      string bind = use_bind_addresses();
      if (!bind.empty()) {
        opts.rpc_bind_addresses = std::move(bind);
      } else {
        opts.rpc_bind_addresses = "127.0.0.1";
      }
      string advertised = use_advertised_addresses();
      if (!advertised.empty()) {
        opts.rpc_advertised_addresses = std::move(advertised);
      }
      string proxy_advertised = use_proxy_advertised_addresses();
      if (!proxy_advertised.empty()) {
        opts.rpc_proxy_advertised_addresses = std::move(proxy_advertised);
      }
      string proxied = use_proxied_addresses();
      if (!proxied.empty()) {
        opts.rpc_proxied_addresses = std::move(proxied);
      }
    }
    server_.reset(new RpcServer(opts));
    unique_ptr<MetricRegistry> metric_registry(new MetricRegistry);
    scoped_refptr<MetricEntity> metric_entity =
        METRIC_ENTITY_server.Instantiate(metric_registry.get(), "test");
    rpc::MessengerBuilder builder("test");
    shared_ptr<rpc::Messenger> messenger;
    builder.set_metric_entity(metric_entity);
    ASSERT_OK(builder.Build(&messenger));
    ASSERT_OK(server_->Init(messenger));
    ASSERT_OK(server_->Bind());
  }

 protected:
  // Overridden by subclasses.
  virtual string use_bind_addresses() const { return ""; }
  virtual string use_advertised_addresses() const { return ""; }
  virtual string use_proxy_advertised_addresses() const { return ""; }
  virtual string use_proxied_addresses() const { return ""; }

  Status GetAddresses(vector<Sockaddr>* bound_addrs,
                      vector<Sockaddr>* advertised_addrs) {
    RETURN_NOT_OK(server_->GetBoundAddresses(bound_addrs));
    return server_->GetAdvertisedAddresses(advertised_addrs);
  }

  void GetProxyAddresses(vector<Sockaddr>* proxied_addrs,
                         vector<HostPort>* proxy_advertised_addrs) {
    *proxied_addrs = server_->GetRpcProxiedAddresses();
    *proxy_advertised_addrs = server_->GetProxyAdvertisedHostPorts();
  }

  unique_ptr<RpcServer> server_;
};

class AdvertisedOnlyWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
};

TEST_F(AdvertisedOnlyWebserverTest, OnlyAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
}

class BoundOnlyWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  string use_bind_addresses() const override { return "127.0.0.1"; }
};

TEST_F(BoundOnlyWebserverTest, OnlyBoundAddresses) {
  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("127.0.0.1", advertised_addrs[0].host());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(advertised_addrs[0].port(), bound_addrs[0].port());
}

class BothBoundAndAdvertisedWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
  string use_bind_addresses() const override { return "127.0.0.1"; }
};

TEST_F(BothBoundAndAdvertisedWebserverTest, BothBoundAndAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
}

class ProxiedRpcAddressesTest : public RpcServerAdvertisedAddressesTest {
 public:
  void SetUp() override {
    ASSERT_OK(GetRandomPort("127.0.0.1", &rpc_bind_port_));
    ASSERT_OK(GetRandomPort("127.0.0.1", &rpc_proxied_port_));
    RpcServerAdvertisedAddressesTest::SetUp();
  }

 protected:
  string use_bind_addresses() const override {
    return Substitute("127.0.0.1:$0", rpc_bind_port_);
  }
  string use_proxy_advertised_addresses() const override {
    return "1.2.3.4:888";
  }
  string use_proxied_addresses() const override {
    return Substitute("127.0.0.1:$0", rpc_proxied_port_);
  }
  uint16_t rpc_bind_port() const {
    return rpc_bind_port_;
  }
  uint16_t rpc_proxied_port() const {
    return rpc_proxied_port_;
  }

 private:
  uint16_t rpc_bind_port_ = 0;
  uint16_t rpc_proxied_port_ = 0;
};

TEST_F(ProxiedRpcAddressesTest, Basic) {
  vector<Sockaddr> proxied_addrs;
  vector<HostPort> proxy_advertised_addrs;
  GetProxyAddresses(&proxied_addrs, &proxy_advertised_addrs);

  ASSERT_EQ(1, proxy_advertised_addrs.size());
  ASSERT_EQ("1.2.3.4", proxy_advertised_addrs[0].host());
  ASSERT_EQ(888, proxy_advertised_addrs[0].port());

  ASSERT_EQ(1, proxied_addrs.size());
  ASSERT_EQ("127.0.0.1", proxied_addrs[0].host());
  ASSERT_EQ(rpc_proxied_port(), proxied_addrs[0].port());

  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ("127.0.0.1", advertised_addrs[0].host());
  ASSERT_EQ(rpc_bind_port(), advertised_addrs[0].port());

  ASSERT_EQ(2, bound_addrs.size());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(rpc_bind_port(), bound_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[1].host());
  ASSERT_EQ(rpc_proxied_port(), bound_addrs[1].port());
}

// The advertised and proxy advertised addresses are independent and
// both can be set for an RPC server.
class ProxiedAndAdvertisedRpcAddressesTest : public ProxiedRpcAddressesTest {
 public:
  string use_advertised_addresses() const override {
      return "2.3.4.5:2345";
  }
};

TEST_F(ProxiedAndAdvertisedRpcAddressesTest, Basic) {
  vector<Sockaddr> proxied_addrs;
  vector<HostPort> proxy_advertised_addrs;
  GetProxyAddresses(&proxied_addrs, &proxy_advertised_addrs);

  ASSERT_EQ(1, proxy_advertised_addrs.size());
  ASSERT_EQ("1.2.3.4", proxy_advertised_addrs[0].host());
  ASSERT_EQ(888, proxy_advertised_addrs[0].port());

  ASSERT_EQ(1, proxied_addrs.size());
  ASSERT_EQ("127.0.0.1", proxied_addrs[0].host());
  ASSERT_EQ(rpc_proxied_port(), proxied_addrs[0].port());

  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ("2.3.4.5", advertised_addrs[0].host());
  ASSERT_EQ(2345, advertised_addrs[0].port());

  ASSERT_EQ(2, bound_addrs.size());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(rpc_bind_port(), bound_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[1].host());
  ASSERT_EQ(rpc_proxied_port(), bound_addrs[1].port());
}

// This is similar to ProxiedRpcAddressesTest, but binds to ephemeral ports
// given 0 as the port number to bind to.
class ProxiedRpcAddressesWildcardPortTest : public ProxiedRpcAddressesTest {
 public:
  void SetUp() override {
    RpcServerAdvertisedAddressesTest::SetUp();
  }
};

TEST_F(ProxiedRpcAddressesWildcardPortTest, Basic) {
  vector<Sockaddr> proxied_addrs;
  vector<HostPort> proxy_advertised_addrs;
  GetProxyAddresses(&proxied_addrs, &proxy_advertised_addrs);

  ASSERT_EQ(1, proxy_advertised_addrs.size());
  ASSERT_EQ("1.2.3.4", proxy_advertised_addrs[0].host());
  ASSERT_EQ(888, proxy_advertised_addrs[0].port());

  ASSERT_EQ(1, proxied_addrs.size());
  ASSERT_EQ("127.0.0.1", proxied_addrs[0].host());
  ASSERT_NE(0, proxied_addrs[0].port());

  vector<Sockaddr> bound_addrs;
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ("127.0.0.1", advertised_addrs[0].host());
  ASSERT_NE(0, advertised_addrs[0].port());

  ASSERT_EQ(2, bound_addrs.size());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_NE(0, bound_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[1].host());
  ASSERT_NE(0, bound_addrs[1].port());

  // The bound endpoints include the proxied RPC endpoints as well.
  // Not comparing the IP addresses since they are both loopbacks (127.0.0.1).
  ASSERT_TRUE(
      proxied_addrs[0].port() == bound_addrs[0].port() ||
      proxied_addrs[0].port() == bound_addrs[1].port());

  // The bound endpoints include the advertised endpoints as well.
  // Not comparing the IP addresses since they are both loopbacks (127.0.0.1).
  ASSERT_TRUE(
      advertised_addrs[0].port() == bound_addrs[0].port() ||
      advertised_addrs[0].port() == bound_addrs[1].port());

  // The advertised endpoints and proxied endpoints are different.
  ASSERT_NE(advertised_addrs[0].port(), proxied_addrs[0].port());
}

} // namespace kudu
