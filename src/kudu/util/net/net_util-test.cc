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

#include "kudu/util/net/net_util.h"

#include <sys/socket.h>

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(fail_dns_resolution);
DECLARE_string(fail_dns_resolution_hostports);

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

class NetUtilTest : public KuduTest {
 protected:
  static Status DoParseBindAddresses(const string& input, string* result) {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(ParseAddressList(input, kDefaultPort, &addrs));
    std::sort(addrs.begin(), addrs.end(), Sockaddr::BytewiseLess);

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

TEST_F(NetUtilTest, TestInjectFailureToResolveAddresses) {
  HostPort hp1("localhost", 12345);
  HostPort hp2("localhost", 12346);
  FLAGS_fail_dns_resolution_hostports = hp1.ToString();
  FLAGS_fail_dns_resolution = true;

  // With a list of bad hostports specified, check that resolution fails as
  // expected.
  vector<Sockaddr> addrs;
  Status s = hp1.ResolveAddresses(&addrs);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Addresses not in the list should resolve fine.
  ASSERT_TRUE(addrs.empty());
  ASSERT_OK(hp2.ResolveAddresses(&addrs));
  ASSERT_FALSE(addrs.empty());

  // With both in the list, resolution should fail.
  FLAGS_fail_dns_resolution_hostports = Substitute("$0,$1", hp1.ToString(), hp2.ToString());
  addrs.clear();
  s = hp1.ResolveAddresses(&addrs);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  s = hp2.ResolveAddresses(&addrs);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // When a list isn't specified, all resolution fails.
  FLAGS_fail_dns_resolution_hostports = "";
  s = hp1.ResolveAddresses(&addrs);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  s = hp2.ResolveAddresses(&addrs);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
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
  vector<HostPort> hps;
  ASSERT_OK(addr.ParseString("0.0.0.0:12345", 0));
  EXPECT_EQ(12345, addr.port());
  ASSERT_OK(HostPortsFromAddrs({ addr }, &hps));
  EXPECT_NE("0.0.0.0", hps[0].host());
  EXPECT_NE("", hps[0].host());
  EXPECT_EQ(12345, hps[0].port());

  hps.clear();
  ASSERT_OK(addr.ParseString("127.0.0.1:12345", 0));
  ASSERT_OK(HostPortsFromAddrs({ addr }, &hps));
  EXPECT_EQ("127.0.0.1", hps[0].host());
  EXPECT_EQ(12345, hps[0].port());
}

TEST_F(NetUtilTest, TestLsof) {
  Sockaddr addr = Sockaddr::Wildcard();
  Socket s;
  ASSERT_OK(s.Init(addr.family(), 0));

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
  ASSERT_OK(GetRandomPort("127.0.0.1", &port));
  LOG(INFO) << "Random port is " << port;
}

TEST_F(NetUtilTest, TestSockaddr) {
  auto addr1 = Sockaddr::Wildcard();
  addr1.set_port(1000);
  auto addr2 = Sockaddr::Wildcard();
  addr2.set_port(2000);
  ASSERT_EQ(1000, addr1.port());
  ASSERT_EQ(2000, addr2.port());
  ASSERT_EQ(string("0.0.0.0:1000"), addr1.ToString());
  ASSERT_EQ(string("0.0.0.0:2000"), addr2.ToString());
  Sockaddr addr3(addr1);
  ASSERT_EQ(string("0.0.0.0:1000"), addr3.ToString());
}

TEST_F(NetUtilTest, TestSockaddrEquality) {
  Sockaddr uninitialized_1;
  Sockaddr uninitialized_2;
  ASSERT_TRUE(uninitialized_1 == uninitialized_2);

  Sockaddr wildcard = Sockaddr::Wildcard();
  ASSERT_FALSE(wildcard == uninitialized_1);
  ASSERT_FALSE(uninitialized_1 == wildcard);

  Sockaddr wildcard_2 = Sockaddr::Wildcard();
  ASSERT_TRUE(wildcard == wildcard_2);
  ASSERT_TRUE(wildcard_2 == wildcard);

  Sockaddr ip_port;
  ASSERT_OK(ip_port.ParseString("127.0.0.1:12345", 0));
  ASSERT_FALSE(ip_port == uninitialized_1);
  ASSERT_FALSE(ip_port == wildcard);
  ASSERT_TRUE(ip_port == ip_port);

  Sockaddr copy = ip_port;
  ASSERT_TRUE(ip_port == copy);
}

TEST_F(NetUtilTest, TestUnixSockaddr) {
  Sockaddr addr;
  ASSERT_OK(addr.ParseUnixDomainPath("/foo/bar"));
  ASSERT_EQ(addr.family(), AF_UNIX);
  ASSERT_EQ(addr.UnixDomainPath(), "/foo/bar");
  ASSERT_EQ(addr.ToString(), "unix:/foo/bar");
  ASSERT_EQ(Sockaddr::UnixAddressType::kPath, addr.unix_address_type());

  Sockaddr addr2;
  ASSERT_OK(addr2.ParseUnixDomainPath("@my-abstract"));
  ASSERT_EQ(addr2.family(), AF_UNIX);
  ASSERT_EQ(addr2.UnixDomainPath(), "@my-abstract");
  ASSERT_EQ(addr2.ToString(), "unix:@my-abstract");
  ASSERT_EQ(Sockaddr::UnixAddressType::kAbstractNamespace, addr2.unix_address_type());

  ASSERT_TRUE(addr == addr);
  ASSERT_TRUE(addr2 == addr2);
  ASSERT_FALSE(addr == addr2);
  ASSERT_FALSE(addr2 == addr);
  ASSERT_FALSE(addr == Sockaddr::Wildcard());
  ASSERT_FALSE(Sockaddr::Wildcard() == addr);
  ASSERT_FALSE(addr == Sockaddr());
  ASSERT_FALSE(Sockaddr() == addr);
}

TEST_F(NetUtilTest, IsAddrOneOfBasic) {
  // The address should match itself.
  Sockaddr a;
  ASSERT_OK(a.ParseString("10.0.23.1:123", 0));

  // The address should not match an empty set of pattern addresses.
  ASSERT_FALSE(IsAddrOneOf(a, {}));

  ASSERT_TRUE(IsAddrOneOf(a, { a }));
  ASSERT_TRUE(IsAddrOneOf(a, { a, a }));

  Sockaddr a0;
  ASSERT_OK(a0.ParseString("10.0.23.1:123", 0));
  ASSERT_TRUE(IsAddrOneOf(a, { a0 }));
  ASSERT_TRUE(IsAddrOneOf(a0, { a }));

  // Make a copy of Sockaddr and make sure it behaves as expected.
  Sockaddr b = a;
  ASSERT_TRUE(IsAddrOneOf(b, { a }));
  ASSERT_TRUE(IsAddrOneOf(a, { b }));

  Sockaddr n;
  ASSERT_OK(n.ParseString("10.0.23.100:234", 0));
  ASSERT_TRUE(IsAddrOneOf(a, { b, n }));
  ASSERT_FALSE(IsAddrOneOf(n, { a, b }));

  // Same IP address, different port.
  Sockaddr c;
  ASSERT_OK(c.ParseString("10.0.23.1:1234", 0));
  ASSERT_FALSE(IsAddrOneOf(c, { a }));
  ASSERT_FALSE(IsAddrOneOf(a, { c }));
  ASSERT_FALSE(IsAddrOneOf(a, { c, c }));
  ASSERT_TRUE(IsAddrOneOf(a, { c, a, c }));
  ASSERT_TRUE(IsAddrOneOf(a, { c, c, a }));

  // Different IP addresses, same port.
  Sockaddr d;
  ASSERT_OK(d.ParseString("1.23.0.10:1234", 0));
  ASSERT_FALSE(IsAddrOneOf(d, { c }));
  ASSERT_FALSE(IsAddrOneOf(c, { d }));
  ASSERT_FALSE(IsAddrOneOf(c, { d, d }));
  ASSERT_TRUE(IsAddrOneOf(c, { d, d, c }));

  // Different IP addresses and ports.
  ASSERT_FALSE(IsAddrOneOf(a, { c }));
  ASSERT_FALSE(IsAddrOneOf(c, { a }));
  ASSERT_FALSE(IsAddrOneOf(a, { c, c }));
  ASSERT_TRUE(IsAddrOneOf(a, { c, c, a }));
}

TEST_F(NetUtilTest, IsAddrOneOfWildcardPort) {
  // The address should match itself.
  Sockaddr ap0;
  ASSERT_OK(ap0.ParseString("10.0.23.1", 0));
  ASSERT_EQ(0, ap0.port());

  Sockaddr a0;
  ASSERT_OK(a0.ParseString("10.0.23.1:0", 0));
  // Sanity check.
  ASSERT_EQ(0, a0.port());
  ASSERT_EQ(a0, ap0);

  Sockaddr ap123;
  ASSERT_OK(ap123.ParseString("10.0.23.1:123", 0));
  ASSERT_EQ(123, ap123.port());
  ASSERT_TRUE(IsAddrOneOf(ap123, { ap0 }));
  ASSERT_TRUE(IsAddrOneOf(ap123, { a0 }));
  ASSERT_TRUE(IsAddrOneOf(ap123, { ap0, a0 }));
  ASSERT_TRUE(IsAddrOneOf(ap123, { ap0, ap123 }));
  ASSERT_TRUE(IsAddrOneOf(ap123, { a0, ap123 }));

  // The port is wildcard, but the address is different.
  Sockaddr bp0;
  ASSERT_OK(bp0.ParseString("1.13.0.10", 0));
  ASSERT_EQ(0, bp0.port());
  ASSERT_FALSE(IsAddrOneOf(ap123, { bp0 }));
  ASSERT_FALSE(IsAddrOneOf(ap123, { bp0, bp0 }));
  ASSERT_TRUE(IsAddrOneOf(ap123, { bp0, ap123 }));
  ASSERT_TRUE(IsAddrOneOf(ap123, { bp0, ap0 }));
}

TEST_F(NetUtilTest, IsAddrOneOfWildcardAddr) {
  // Wildcards with particular port numbers.
  auto wp123 = Sockaddr::Wildcard();
  wp123.set_port(123);

  auto wp234 = Sockaddr::Wildcard();
  wp234.set_port(234);

  // The same port as in the wildcard.
  Sockaddr a123;
  ASSERT_OK(a123.ParseString("10.0.23.1:123", 0));
  ASSERT_TRUE(IsAddrOneOf(a123, { wp123 }));
  ASSERT_TRUE(IsAddrOneOf(a123, { wp123, wp234 }));
  ASSERT_TRUE(IsAddrOneOf(a123, { wp234, wp123 }));

  // Different port from the wildcard.
  Sockaddr a234;
  ASSERT_OK(a234.ParseString("10.0.23.1:234", 0));
  ASSERT_FALSE(IsAddrOneOf(a234, { wp123 }));
  ASSERT_FALSE(IsAddrOneOf(a123, { wp234 }));
  ASSERT_FALSE(IsAddrOneOf(a234, { wp123, a123 }));
  ASSERT_FALSE(IsAddrOneOf(a123, { wp234, a234 }));

  // A mix of matching wildcard address and non-matching address.
  ASSERT_TRUE(IsAddrOneOf(a234, { wp234, a123 }));
  ASSERT_TRUE(IsAddrOneOf(a234, { wp123, a234 }));
  ASSERT_TRUE(IsAddrOneOf(a234, { wp123, wp234, a234 }));

  const auto wpn = Sockaddr::Wildcard();
  auto wp0 = Sockaddr::Wildcard();
  wp0.set_port(0);
  // Sanity check.
  ASSERT_EQ(wpn, wp0);

  // Wildcard with no port number set (i.e. port number 0) matches any address.
  ASSERT_TRUE(IsAddrOneOf(a123, { wpn }));
  ASSERT_TRUE(IsAddrOneOf(a234, { wpn }));
  ASSERT_TRUE(IsAddrOneOf(a123, { wp234, wpn }));
  ASSERT_TRUE(IsAddrOneOf(a234, { wp123, wpn }));
}

} // namespace kudu
