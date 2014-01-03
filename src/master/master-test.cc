// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>

#include "gutil/strings/join.h"
#include "master/master.h"
#include "master/master.proxy.h"
#include "master/master-test-util.h"
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
  const char *kTsUUID = "my-ts-uuid";

  TSToMasterCommonPB common;
  common.mutable_ts_instance()->set_permanent_uuid(kTsUUID);
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

  shared_ptr<TSDescriptor> ts_desc;
  Status s = master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc);
  ASSERT_TRUE(s.IsNotFound());

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

  ASSERT_STATUS_OK(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));
  ASSERT_EQ(ts_desc, descs[0]);

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

  ASSERT_STATUS_OK(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));
  ASSERT_EQ(ts_desc, descs[0]);
}

TEST_F(MasterTest, TestCatalog) {
  const char *kTableName = "testtb";
  const Schema kTableSchema(boost::assign::list_of
                            (ColumnSchema("key", UINT32))
                            (ColumnSchema("v1", UINT64))
                            (ColumnSchema("v2", STRING)),
                            1);
  // Create a table
  {
    CreateTableRequestPB req;
    CreateTableResponsePB resp;
    RpcController controller;

    req.set_name(kTableName);
    req.add_pre_split_keys("k1");
    req.add_pre_split_keys("k2");

    ASSERT_STATUS_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
    ASSERT_STATUS_OK(proxy_->CreateTable(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // List tables, should show just the created one
  {
    ListTablesRequestPB req;
    ListTablesResponsePB resp;
    RpcController controller;
    ASSERT_STATUS_OK(proxy_->ListTables(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(1, resp.tables_size());
    ASSERT_EQ(kTableName, resp.tables(0).name());
  }

  // Delete the table
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    ASSERT_STATUS_OK(proxy_->DeleteTable(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }

  // List tables, show show no table
  {
    ListTablesRequestPB req;
    ListTablesResponsePB resp;
    RpcController controller;
    ASSERT_STATUS_OK(proxy_->ListTables(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(0, resp.tables_size());
  }

  // Re-create the table
  {
    CreateTableRequestPB req;
    CreateTableResponsePB resp;
    RpcController controller;

    req.set_name(kTableName);
    req.add_pre_split_keys("k1");
    req.add_pre_split_keys("k2");

    ASSERT_STATUS_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
    ASSERT_STATUS_OK(proxy_->CreateTable(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error());
  }
}

} // namespace master
} // namespace kudu
