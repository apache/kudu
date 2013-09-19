// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>

#include "gutil/strings/join.h"
#include "master/master_server.h"
#include "master/master.proxy.h"
#include "server/rpc_server.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
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
namespace master {

class MasterServerTest : public KuduTest {
 protected:

  // Start a master server running on the loopback interface and
  // an ephemeral port. Sets *addr to the address of the started
  // server.
  void StartTestServer(Sockaddr *addr) {
    // Start server on loopback.
    MasterServerOptions opts;
    opts.rpc_opts.rpc_bind_addresses = "127.0.0.1:0";
    opts.webserver_opts.port = 0;
    // TODO: refactor this stuff into a MiniMaster class, like MiniTabletServer.

    gscoped_ptr<MasterServer> server(new MasterServer(opts));
    ASSERT_STATUS_OK(server->Init());
    ASSERT_STATUS_OK(server->Start());

    // Find the ephemeral address of the server.
    vector<Sockaddr> addrs;
    server->rpc_server()->GetBoundAddresses(&addrs);
    ASSERT_TRUE(!addrs.empty());

    *addr = addrs[0];
    server_.swap(server);
  }

  void CreateClientProxy(Sockaddr &addr, gscoped_ptr<MasterServerServiceProxy>* proxy) {
    if (!client_messenger_) {
      MessengerBuilder bld("Client");
      ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    }
    proxy->reset(new MasterServerServiceProxy(client_messenger_, addr));
  }

  void SetUp() {
    Sockaddr addr;
    ASSERT_NO_FATAL_FAILURE(StartTestServer(&addr));
    ASSERT_NO_FATAL_FAILURE(CreateClientProxy(addr, &proxy_));
  }

  shared_ptr<Messenger> client_messenger_;
  gscoped_ptr<MasterServer> server_;
  gscoped_ptr<MasterServerServiceProxy> proxy_;
};

TEST_F(MasterServerTest, TestPingServer) {
  // Ping the server.
  PingRequestPB req;
  PingResponsePB resp;
  RpcController controller;
  ASSERT_STATUS_OK(proxy_->Ping(req, &resp, &controller));
}

static void MakeHostPortPB(const string& host, uint32_t port, HostPortPB* pb) {
  pb->set_host(host);
  pb->set_port(port);
}

TEST_F(MasterServerTest, TestRegisterAndHeartbeat) {
  TSToMasterCommonPB common;
  common.mutable_ts_instance()->set_permanent_uuid("my-ts-uuid");
  common.mutable_ts_instance()->set_instance_seqno(1);

  // Try a heartbeat. The server hasn't heard of us, so should ask us
  // to re-register.
  {
    RpcController rpc;
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    req.mutable_common()->CopyFrom(common);
    ASSERT_STATUS_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_TRUE(resp.needs_reregister());
    ASSERT_TRUE(resp.needs_full_tablet_report());
  }

  vector<shared_ptr<TSDescriptor> > descs;
  server_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(0, descs.size()) << "Should not have registered anything";

  // Register the fake TS, without sending any tablet report.
  TSRegistrationPB fake_reg;
  MakeHostPortPB("localhost", 1000, fake_reg.add_rpc_addresses());
  MakeHostPortPB("localhost", 2000, fake_reg.add_http_addresses());

  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    ASSERT_STATUS_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_TRUE(resp.needs_full_tablet_report());
  }

  descs.clear();
  server_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should have registered the TS";
  TSRegistrationPB reg;
  descs[0]->GetRegistration(&reg);
  ASSERT_EQ(fake_reg.DebugString(), reg.DebugString()) << "Master got different registration";

  // Now send a tablet report
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(false);
    ASSERT_STATUS_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
  }

  descs.clear();
  server_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should still only have one TS registered";
}

} // namespace master
} // namespace kudu
