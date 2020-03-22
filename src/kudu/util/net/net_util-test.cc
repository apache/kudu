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

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

class NetUtilTest : public KuduTest {
 protected:
  Status DoParseBindAddresses(const string& input, string* result) {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(ParseAddressList(input, kDefaultPort, &addrs));
    std::sort(addrs.begin(), addrs.end());

    vector<string> addr_strs;
    for (const Sockaddr& addr : addrs) {
      addr_strs.push_back(addr.ToString());
    }
    *result = JoinStrings(addr_strs, ",");
    return Status::OK();
  }

  static const uint16_t kDefaultPort = 7150;
};

TEST(SockaddrTest, Test) {
  Sockaddr addr;
  ASSERT_OK(addr.ParseString("1.1.1.1:12345", 12345));
  ASSERT_EQ(12345, addr.port());
  ASSERT_EQ("1.1.1.1", addr.host());
}

TEST_F(NetUtilTest, TestParseAddresses) {
  string ret;
  ASSERT_OK(DoParseBindAddresses("0.0.0.0:12345", &ret));
  ASSERT_EQ("0.0.0.0:12345", ret);

  ASSERT_OK(DoParseBindAddresses("0.0.0.0", &ret));
  ASSERT_EQ("0.0.0.0:7150", ret);

  ASSERT_OK(DoParseBindAddresses("0.0.0.0:12345, 0.0.0.0:12346", &ret));
  ASSERT_EQ("0.0.0.0:12345,0.0.0.0:12346", ret);

  // Test some invalid addresses.
  Status s = DoParseBindAddresses("0.0.0.0:xyz", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "invalid port");

  s = DoParseBindAddresses("0.0.0.0:100000", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "invalid port");

  s = DoParseBindAddresses("0.0.0.0:", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "invalid port");
}

TEST_F(NetUtilTest, TestParseAddressesWithScheme) {
  vector<HostPort> hostports;
  const uint16_t kDefaultPort = 12345;

  EXPECT_OK(HostPort::ParseStringsWithScheme("", kDefaultPort, &hostports));
  EXPECT_TRUE(hostports.empty());

  EXPECT_OK(HostPort::ParseStringsWithScheme(",,,", kDefaultPort, &hostports));
  EXPECT_TRUE(hostports.empty());

  EXPECT_OK(HostPort::ParseStringsWithScheme("http://foo-bar-baz:1234", kDefaultPort, &hostports));
  EXPECT_EQ(vector<HostPort>({ HostPort("foo-bar-baz", 1234) }), hostports);

  EXPECT_OK(HostPort::ParseStringsWithScheme("http://foo-bar-baz:1234/path",
                                             kDefaultPort, &hostports));
  EXPECT_EQ(vector<HostPort>({ HostPort("foo-bar-baz", 1234) }), hostports);

  EXPECT_OK(HostPort::ParseStringsWithScheme("http://abc:1234,xyz", kDefaultPort, &hostports));
  EXPECT_EQ(vector<HostPort>({ HostPort("abc", 1234), HostPort("xyz", kDefaultPort) }),
            hostports);

  // Test some invalid addresses.
  Status s = HostPort::ParseStringsWithScheme("abc:1234/path", kDefaultPort, &hostports);
  EXPECT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "invalid port");

  s = HostPort::ParseStringsWithScheme("://scheme:12", kDefaultPort, &hostports);
  EXPECT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "invalid scheme format");

  s = HostPort::ParseStringsWithScheme("http:///path", kDefaultPort, &hostports);
  EXPECT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "invalid address format");

  s = HostPort::ParseStringsWithScheme("http://abc:1234,://scheme,xyz",
                                       kDefaultPort, &hostports);
  EXPECT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "invalid scheme format");
}

TEST_F(NetUtilTest, TestResolveAddresses) {
  HostPort hp("localhost", 12345);
  vector<Sockaddr> addrs;
  ASSERT_OK(hp.ResolveAddresses(&addrs));
  ASSERT_TRUE(!addrs.empty());
  for (const Sockaddr& addr : addrs) {
    LOG(INFO) << "Address: " << addr.ToString();
    EXPECT_TRUE(HasPrefixString(addr.ToString(), "127."));
    EXPECT_TRUE(HasSuffixString(addr.ToString(), ":12345"));
    EXPECT_TRUE(addr.IsAnyLocalAddress());
  }

  ASSERT_OK(hp.ResolveAddresses(nullptr));
}

TEST_F(NetUtilTest, TestWithinNetwork) {
  Sockaddr addr;
  Network network;

  ASSERT_OK(addr.ParseString("10.0.23.0:12345", 0));
  ASSERT_OK(network.ParseCIDRString("10.0.0.0/8"));
  EXPECT_TRUE(network.WithinNetwork(addr));

  ASSERT_OK(addr.ParseString("172.28.3.4:0", 0));
  ASSERT_OK(network.ParseCIDRString("172.16.0.0/12"));
  EXPECT_TRUE(network.WithinNetwork(addr));

  ASSERT_OK(addr.ParseString("192.168.0.23", 0));
  ASSERT_OK(network.ParseCIDRString("192.168.1.14/16"));
  EXPECT_TRUE(network.WithinNetwork(addr));

  ASSERT_OK(addr.ParseString("8.8.8.8:0", 0));
  ASSERT_OK(network.ParseCIDRString("0.0.0.0/0"));
  EXPECT_TRUE(network.WithinNetwork(addr));

  ASSERT_OK(addr.ParseString("192.169.0.23", 0));
  ASSERT_OK(network.ParseCIDRString("192.168.0.0/16"));
  EXPECT_FALSE(network.WithinNetwork(addr));
}

// Ensure that we are able to do a reverse DNS lookup on various IP addresses.
// The reverse lookups should never fail, but may return numeric strings.
TEST_F(NetUtilTest, TestReverseLookup) {
  string host;
  Sockaddr addr;
  HostPort hp;
  ASSERT_OK(addr.ParseString("0.0.0.0:12345", 0));
  EXPECT_EQ(12345, addr.port());
  ASSERT_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  EXPECT_NE("0.0.0.0", hp.host());
  EXPECT_NE("", hp.host());
  EXPECT_EQ(12345, hp.port());

  ASSERT_OK(addr.ParseString("127.0.0.1:12345", 0));
  ASSERT_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  EXPECT_EQ("127.0.0.1", hp.host());
  EXPECT_EQ(12345, hp.port());
}

TEST_F(NetUtilTest, TestLsof) {
  Socket s;
  ASSERT_OK(s.Init(0));

  Sockaddr addr; // wildcard
  ASSERT_OK(s.BindAndListen(addr, 1));

  ASSERT_OK(s.GetSocketAddress(&addr));
  ASSERT_NE(addr.port(), 0);
  vector<string> lsof_lines;
  TryRunLsof(addr, &lsof_lines);
  SCOPED_TRACE(JoinStrings(lsof_lines, "\n"));

  ASSERT_GE(lsof_lines.size(), 3);
  ASSERT_STR_CONTAINS(lsof_lines[2], "net_util-test");
}

TEST_F(NetUtilTest, TestGetFQDN) {
  string fqdn;
  ASSERT_OK(GetFQDN(&fqdn));
  LOG(INFO) << "fqdn is " << fqdn;
}

TEST_F(NetUtilTest, TestGetRandomPort) {
  uint16_t port;
  ASSERT_OK(GetRandomPort(&port));
  LOG(INFO) << "Random port is " << port;
}

} // namespace kudu
