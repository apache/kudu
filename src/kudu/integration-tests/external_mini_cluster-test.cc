// Copyright (c) 2014, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/test_util.h"

namespace kudu {

class EMCTest : public KuduTest {};

TEST_F(EMCTest, TestBasicOperation) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 3;

  ExternalMiniCluster cluster(opts);
  ASSERT_STATUS_OK(cluster.Start());

  // Verify that the master has bound its RPC and HTTP ports.
  HostPort master_rpc = cluster.master()->bound_rpc_hostport();
  EXPECT_TRUE(HasPrefixString(master_rpc.ToString(), "127.0.0.1:")) << master_rpc.ToString();

  HostPort master_http = cluster.master()->bound_http_hostport();
  EXPECT_TRUE(HasPrefixString(master_http.ToString(), "127.0.0.1:")) << master_http.ToString();

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
