// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>

#include "gutil/strings/join.h"
#include "master/master.h"
#include "master/master.proxy.h"
#include "master/mini_master.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "server/rpc_server.h"
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

class MasterTest : public KuduTest {
 protected:
  void SetUp() {
    KuduTest::SetUp();

    // Start master
    mini_master_.reset(new MiniMaster(Env::Default(), GetTestPath("Master")));
    ASSERT_STATUS_OK(mini_master_->Start());
    master_ = mini_master_->master();

    // Create a client proxy to it.
    MessengerBuilder bld("Client");
    ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr()));
  }


  shared_ptr<Messenger> client_messenger_;
  gscoped_ptr<MiniMaster> mini_master_;
  Master* master_;
  gscoped_ptr<MasterServiceProxy> proxy_;
};

TEST_F(MasterTest, TestPingServer) {
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

TEST_F(MasterTest, TestRegisterAndHeartbeat) {
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
  master_->ts_manager()->GetAllDescriptors(&descs);
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
  master_->ts_manager()->GetAllDescriptors(&descs);
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
    tr->set_sequence_number(0);
    ASSERT_STATUS_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
  }

  descs.clear();
  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should still only have one TS registered";
}

TEST_F(MasterTest, TestTabletLocations) {
  const string kTabletId = "fake-tablet-id";
  const string kTabletServerId = "my-ts-uuid";
  TSToMasterCommonPB common;
  common.mutable_ts_instance()->set_permanent_uuid(kTabletServerId);
  common.mutable_ts_instance()->set_instance_seqno(1);

  // Register the fake TS, including a single tablet.
  TSRegistrationPB fake_reg;
  MakeHostPortPB("localhost", 1000, fake_reg.add_rpc_addresses());
  MakeHostPortPB("localhost", 2000, fake_reg.add_http_addresses());
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(false);
    tr->set_sequence_number(0);
    tr->add_updated_tablets()->set_tablet_id(kTabletId);

    ASSERT_STATUS_OK(proxy_->TSHeartbeat(req, &resp, &rpc));
  }

  // Look up the tablet locations -- should return the server we just
  // registered.
  {
    GetTabletLocationsRequestPB req;
    GetTabletLocationsResponsePB resp;
    RpcController rpc;
    req.add_tablet_ids(kTabletId);

    ASSERT_STATUS_OK(proxy_->GetTabletLocations(req, &resp, &rpc));
    SCOPED_TRACE(resp.DebugString());

    // Verify the tablet location.
    ASSERT_EQ(1, resp.tablet_locations().size());
    EXPECT_EQ("tablet_id: \"fake-tablet-id\"\n"
              "replicas {\n"
              "  ts_info {\n"
              "    permanent_uuid: \"my-ts-uuid\"\n"
              "    rpc_addresses {\n"
              "      host: \"localhost\"\n"
              "      port: 1000\n"
              "    }\n"
              "  }\n"
              "}\n",
              resp.tablet_locations(0).DebugString());
  }
}

} // namespace master
} // namespace kudu
