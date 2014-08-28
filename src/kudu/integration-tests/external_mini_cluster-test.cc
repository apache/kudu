// Copyright (c) 2014, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/test_util.h"

namespace kudu {

class EMCTest : public KuduTest {
 public:
  EMCTest() {
    // Hard-coded RPC ports for the masters. This is safe, as this unit test
    // runs under a resource lock (see CMakeLists.txt in this directory).
    // TODO we should have a generic method to obtain n free ports.
    master_quorum_ports_ = boost::assign::list_of(11010)(11011)(11012);
  }

 protected:
  std::vector<uint16_t> master_quorum_ports_;
};

TEST_F(EMCTest, TestBasicOperation) {
  ExternalMiniClusterOptions opts;
  opts.num_masters = master_quorum_ports_.size();
  opts.num_tablet_servers = 3;
  opts.master_rpc_ports = master_quorum_ports_;

  ExternalMiniCluster cluster(opts);
  ASSERT_STATUS_OK(cluster.Start());

  // Verify that the masters have bound their RPC and HTTP ports.
  for (int i = 0; i < opts.num_masters; i++) {
    SCOPED_TRACE(i);
    ExternalMaster* master = CHECK_NOTNULL(cluster.master(i));
    HostPort master_rpc = master->bound_rpc_hostport();
    EXPECT_TRUE(HasPrefixString(master_rpc.ToString(), "127.0.0.1:")) << master_rpc.ToString();

    HostPort master_http = master->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(master_http.ToString(), "127.0.0.1:")) << master_http.ToString();
  }

  // Verify each of the tablet servers.
  for (int i = 0; i < opts.num_tablet_servers; i++) {
    SCOPED_TRACE(i);
    ExternalTabletServer* ts = CHECK_NOTNULL(cluster.tablet_server(i));
    HostPort ts_rpc = ts->bound_rpc_hostport();
    EXPECT_TRUE(HasPrefixString(ts_rpc.ToString(), "127.0.0.1:")) << ts_rpc.ToString();

    HostPort ts_http = ts->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(ts_http.ToString(), "127.0.0.1:")) << ts_http.ToString();
  }

  cluster.Shutdown();
}

} // namespace kudu
