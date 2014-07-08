// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>

#include "client/client.h"
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
using kudu::client::KuduClient;
using kudu::client::KuduClientOptions;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;

namespace kudu {
namespace master {

class MasterTest : public KuduTest {
 protected:
  virtual void SetUp() OVERRIDE {
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

  virtual void TearDown() OVERRIDE {
    mini_master_->Shutdown();
    KuduTest::TearDown();
  }

  void DoListTables(ListTablesResponsePB* resp);
  void CreateTable(const string& table_name,
                   const KuduSchema& schema);


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

// Test that shutting down a MiniMaster without starting it does not
// SEGV.
TEST_F(MasterTest, TestShutdownWithoutStart) {
  MiniMaster m(Env::Default(), "/xxxx");
  m.Shutdown();
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

  // Ensure that the ListTabletServers shows the faked server.
  {
    ListTabletServersRequestPB req;
    ListTabletServersResponsePB resp;
    RpcController rpc;
    ASSERT_STATUS_OK(proxy_->ListTabletServers(req, &resp, &rpc));
    LOG(INFO) << resp.DebugString();
    ASSERT_EQ(1, resp.servers_size());
    ASSERT_EQ("my-ts-uuid", resp.servers(0).instance_id().permanent_uuid());
    ASSERT_EQ(1, resp.servers(0).instance_id().instance_seqno());
  }
}

// Create a table
void MasterTest::CreateTable(const string& table_name,
                             const KuduSchema& schema) {
  shared_ptr<KuduClient> client;
  KuduClientOptions opts;
  opts.master_server_addr = mini_master_->bound_rpc_addr().ToString();
  opts.messenger = client_messenger_;
  ASSERT_STATUS_OK(KuduClient::Create(opts, &client));

  vector<string> keys;
  keys.push_back("k1");
  keys.push_back("k2");

  ASSERT_STATUS_OK(client->CreateTable(
                     table_name, schema,
                     kudu::client::KuduCreateTableOptions()
                        .WithSplitKeys(keys)
                        .WaitAssignment(false)));
}

void MasterTest::DoListTables(ListTablesResponsePB* resp) {
  ListTablesRequestPB req;
  RpcController controller;
  ASSERT_STATUS_OK(proxy_->ListTables(req, resp, &controller));
  SCOPED_TRACE(resp->DebugString());
  ASSERT_FALSE(resp->has_error());
}

TEST_F(MasterTest, TestCatalog) {
  const char *kTableName = "testtb";
  const KuduSchema kTableSchema(boost::assign::list_of
                            (KuduColumnSchema("key", UINT32))
                            (KuduColumnSchema("v1", UINT64))
                            (KuduColumnSchema("v2", STRING)),
                            1);

  ASSERT_NO_FATAL_FAILURE(CreateTable(kTableName, kTableSchema));

  ListTablesResponsePB tables;
  ASSERT_NO_FATAL_FAILURE(DoListTables(&tables));
  ASSERT_EQ(1, tables.tables_size());
  ASSERT_EQ(kTableName, tables.tables(0).name());


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

  // List tables, should show no table
  ASSERT_NO_FATAL_FAILURE(DoListTables(&tables));
  ASSERT_EQ(0, tables.tables_size());

  // Re-create the table
  ASSERT_NO_FATAL_FAILURE(CreateTable(kTableName, kTableSchema));

  // Restart the master, verify the table still shows up.
  ASSERT_STATUS_OK(mini_master_->Restart());

  ASSERT_NO_FATAL_FAILURE(DoListTables(&tables));
  ASSERT_EQ(1, tables.tables_size());
  ASSERT_EQ(kTableName, tables.tables(0).name());
}

} // namespace master
} // namespace kudu
