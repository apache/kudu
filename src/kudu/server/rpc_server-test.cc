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
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/rpc/messenger.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

DECLARE_bool(rpc_server_allow_ephemeral_ports);

namespace kudu {

class RpcServerAdvertisedAddressesTest : public KuduTest {
 public:

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_rpc_server_allow_ephemeral_ports = true;

    RpcServerOptions opts;
    string bind = use_bind_addresses();
    if (!bind.empty()) {
      opts.rpc_bind_addresses = bind;
    } else {
      opts.rpc_bind_addresses = "127.0.0.1";
    }
    string advertised = use_advertised_addresses();
    if (!advertised.empty()) {
      opts.rpc_advertised_addresses = advertised;
    }
    server_.reset(new RpcServer(opts));
    unique_ptr<MetricRegistry> metric_registry(new MetricRegistry());
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

  void GetAddresses(vector<Sockaddr>* bound_addrs,
                    vector<Sockaddr>* advertised_addrs) {
    ASSERT_OK(server_->GetBoundAddresses(bound_addrs));
    ASSERT_OK(server_->GetAdvertisedAddresses(advertised_addrs));
  }

  unique_ptr<RpcServer> server_;
};

class AdvertisedOnlyWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
};

class BoundOnlyWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  string use_bind_addresses() const override { return "127.0.0.1"; }
};

class BothBoundAndAdvertisedWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
  string use_bind_addresses() const override { return "127.0.0.1"; }
};

TEST_F(AdvertisedOnlyWebserverTest, OnlyAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
}

TEST_F(BoundOnlyWebserverTest, OnlyBoundAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("127.0.0.1", advertised_addrs[0].host());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(advertised_addrs[0].port(), bound_addrs[0].port());
}

TEST_F(BothBoundAndAdvertisedWebserverTest, BothBoundAndAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
}

} // namespace kudu
