// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>

#include <boost/foreach.hpp>
#include <algorithm>
#include <vector>

#include "gutil/strings/join.h"
#include "gutil/strings/util.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"
#include "util/test_util.h"

namespace kudu {

class NetUtilTest : public KuduTest {
 protected:
  Status DoParseBindAddresses(const string& input, string* result) {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(ParseAddressList(input, kDefaultPort, &addrs));
    std::sort(addrs.begin(), addrs.end());

    vector<string> addr_strs;
    BOOST_FOREACH(const Sockaddr& addr, addrs) {
      addr_strs.push_back(addr.ToString());
    }
    *result = JoinStrings(addr_strs, ",");
    return Status::OK();
  }

  static const uint16_t kDefaultPort = 7150;
};

TEST_F(NetUtilTest, TestParseAddresses) {
  string ret;
  ASSERT_STATUS_OK(DoParseBindAddresses("0.0.0.0:12345", &ret));
  ASSERT_EQ("0.0.0.0:12345", ret);

  ASSERT_STATUS_OK(DoParseBindAddresses("0.0.0.0", &ret));
  ASSERT_EQ("0.0.0.0:7150", ret);

  ASSERT_STATUS_OK(DoParseBindAddresses("0.0.0.0:12345, 0.0.0.0:12346", &ret));
  ASSERT_EQ("0.0.0.0:12345,0.0.0.0:12346", ret);

  // Test some invalid addresses.
  Status s = DoParseBindAddresses("0.0.0.0:xyz", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid port");

  s = DoParseBindAddresses("0.0.0.0:100000", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid port");

  s = DoParseBindAddresses("0.0.0.0:", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid port");
}

TEST_F(NetUtilTest, TestResolveAddresses) {
  HostPort hp("localhost", 12345);
  vector<Sockaddr> addrs;
  ASSERT_STATUS_OK(hp.ResolveAddresses(&addrs));
  ASSERT_TRUE(!addrs.empty());
  BOOST_FOREACH(const Sockaddr& addr, addrs) {
    LOG(INFO) << "Address: " << addr.ToString();
    EXPECT_TRUE(HasPrefixString(addr.ToString(), "127."));
    EXPECT_TRUE(HasSuffixString(addr.ToString(), ":12345"));
    EXPECT_TRUE(addr.IsAnyLocalAddress());
  }

  ASSERT_STATUS_OK(hp.ResolveAddresses(NULL));
}
} // namespace kudu
