// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>

#include "gutil/strings/join.h"
#include "tserver/tablet_server.h"
#include "tserver/tserver.proxy.h"
#include "util/net/sockaddr.h"
#include "util/status.h"
#include "util/test_util.h"
#include "rpc/messenger.h"

using std::string;
using std::tr1::shared_ptr;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;

namespace kudu {
namespace tserver {

class TabletServerTest : public KuduTest {
 protected:

  // Start a tablet server running on the loopback interface and
  // an ephemeral port. Sets *addr to the address of the started
  // server.
  void StartTestServer(Sockaddr *addr, gscoped_ptr<TabletServer>* ret) {
    // Start server on loopback.
    TabletServerOptions opts;
    opts.rpc_bind_addresses = "127.0.0.1:0";

    gscoped_ptr<TabletServer> server(new TabletServer(opts));
    ASSERT_STATUS_OK(server->Init());
    ASSERT_STATUS_OK(server->Start());

    // Find the ephemeral address of the server.
    vector<Sockaddr> addrs;
    server->GetBoundAddresses(&addrs);
    ASSERT_TRUE(!addrs.empty());

    *addr = addrs[0];
    ret->swap(server);
  }

  void CreateClientProxy(Sockaddr &addr, gscoped_ptr<TabletServerServiceProxy>* proxy) {
    if (!client_messenger_) {
      MessengerBuilder bld("Client");
      ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    }
    proxy->reset(new TabletServerServiceProxy(client_messenger_, addr));
  }

  shared_ptr<Messenger> client_messenger_;
};

TEST_F(TabletServerTest, TestPingServer) {
  Sockaddr addr;
  gscoped_ptr<TabletServer> server;
  ASSERT_NO_FATAL_FAILURE(StartTestServer(&addr, &server));
  gscoped_ptr<TabletServerServiceProxy> proxy;
  ASSERT_NO_FATAL_FAILURE(CreateClientProxy(addr, &proxy));

  // Ping the server.
  PingRequestPB req;
  PingResponsePB resp;
  RpcController controller;
  ASSERT_STATUS_OK(proxy->Ping(req, &resp, &controller));
}

} // namespace tserver
} // namespace kudu
