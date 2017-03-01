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

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <thread>
#include <vector>

#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/generated/version_defines.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master-test-util.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/util/version_info.h"

using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::pair;
using std::shared_ptr;
using std::string;
using std::thread;
using strings::Substitute;

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_double(sys_catalog_fail_during_write);

namespace kudu {
namespace master {

class MasterTest : public KuduTest {
 protected:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // In this test, we create tables to test catalog manager behavior,
    // but we have no tablet servers. Typically this would be disallowed.
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;

    // Start master
    mini_master_.reset(new MiniMaster(Env::Default(), GetTestPath("Master"), 0));
    ASSERT_OK(mini_master_->Start());
    master_ = mini_master_->master();
    ASSERT_OK(master_->WaitUntilCatalogManagerIsLeaderAndReadyForTests(MonoDelta::FromSeconds(5)));

    // Create a client proxy to it.
    MessengerBuilder bld("Client");
    ASSERT_OK(bld.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr()));
  }

  virtual void TearDown() OVERRIDE {
    mini_master_->Shutdown();
    KuduTest::TearDown();
  }

  void DoListTables(const ListTablesRequestPB& req, ListTablesResponsePB* resp);
  void DoListAllTables(ListTablesResponsePB* resp);
  Status CreateTable(const string& table_name,
                     const Schema& schema);
  Status CreateTable(const string& table_name,
                     const Schema& schema,
                     const vector<KuduPartialRow>& split_rows,
                     const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds);

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
  ASSERT_OK(proxy_->Ping(req, &resp, &controller));
}

static void MakeHostPortPB(const string& host, uint32_t port, HostPortPB* pb) {
  pb->set_host(host);
  pb->set_port(port);
}

// Test that shutting down a MiniMaster without starting it does not
// SEGV.
TEST_F(MasterTest, TestShutdownWithoutStart) {
  MiniMaster m(Env::Default(), "/xxxx", 0);
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
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_TRUE(resp.leader_master());
    ASSERT_TRUE(resp.needs_reregister());
    ASSERT_TRUE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  vector<shared_ptr<TSDescriptor> > descs;
  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(0, descs.size()) << "Should not have registered anything";

  shared_ptr<TSDescriptor> ts_desc;
  ASSERT_FALSE(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));

  // Register the fake TS, without sending any tablet report.
  ServerRegistrationPB fake_reg;
  MakeHostPortPB("localhost", 1000, fake_reg.add_rpc_addresses());
  MakeHostPortPB("localhost", 2000, fake_reg.add_http_addresses());
  fake_reg.set_software_version(VersionInfo::GetShortVersionString());

  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should have registered the TS";
  ServerRegistrationPB reg;
  descs[0]->GetRegistration(&reg);
  ASSERT_EQ(SecureDebugString(fake_reg), SecureDebugString(reg))
      << "Master got different registration";

  ASSERT_TRUE(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));
  ASSERT_EQ(ts_desc, descs[0]);

  // If the tablet server somehow lost the response to its registration RPC, it would
  // attempt to register again. In that case, we shouldn't reject it -- we should
  // just respond the same.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  // If we send the registration RPC while the master isn't the leader, it
  // shouldn't ask for a full tablet report.
  {
    CatalogManager::ScopedLeaderDisablerForTests o(master_->catalog_manager());
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  // Send a full tablet report, but with the master as a follower. The
  // report will be ignored.
  {
    CatalogManager::ScopedLeaderDisablerForTests o(master_->catalog_manager());
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(false);
    tr->set_sequence_number(0);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  // Now send a full report with the master as leader. The master will process
  // it; this is reflected in the response.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(false);
    tr->set_sequence_number(0);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_TRUE(resp.has_tablet_report());
  }

  // Having sent a full report, an incremental report will also be processed.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(true);
    tr->set_sequence_number(0);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_TRUE(resp.has_tablet_report());
  }

  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should still only have one TS registered";

  ASSERT_TRUE(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));
  ASSERT_EQ(ts_desc, descs[0]);

  // Ensure that the ListTabletServers shows the faked server.
  {
    ListTabletServersRequestPB req;
    ListTabletServersResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy_->ListTabletServers(req, &resp, &rpc));
    LOG(INFO) << SecureDebugString(resp);
    ASSERT_EQ(1, resp.servers_size());
    ASSERT_EQ("my-ts-uuid", resp.servers(0).instance_id().permanent_uuid());
    ASSERT_EQ(1, resp.servers(0).instance_id().instance_seqno());
  }

  // Ensure that /dump-entities endpoint also shows the faked server.
  {
    EasyCurl c;
    faststring buf;
    string addr = mini_master_->bound_http_addr().ToString();
    ASSERT_OK(c.FetchURL(Substitute("http://$0/dump-entities", addr), &buf))
    rapidjson::Document doc;
    doc.Parse<0>(buf.ToString().c_str());
    const rapidjson::Value& tablet_servers = doc["tablet_servers"];
    ASSERT_EQ(tablet_servers.Size(), 1);
    const rapidjson::Value& tablet_server = tablet_servers[rapidjson::SizeType(0)];
    ASSERT_STREQ("localhost:1000",
        tablet_server["rpc_addrs"][rapidjson::SizeType(0)].GetString());
    ASSERT_STREQ("http://localhost:2000",
        tablet_server["http_addrs"][rapidjson::SizeType(0)].GetString());
    ASSERT_STREQ("my-ts-uuid", tablet_server["uuid"].GetString());
    ASSERT_TRUE(tablet_server["millis_since_heartbeat"].GetInt64() >= 0);
    ASSERT_EQ(true, tablet_server["live"].GetBool());
    ASSERT_STREQ(VersionInfo::GetShortVersionString().c_str(),
        tablet_server["version"].GetString());
  }

  // Ensure that trying to re-register with a different version is OK.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    // This string should never match the actual VersionInfo string, although
    // the numeric portion will match.
    req.mutable_registration()->set_software_version(Substitute("kudu $0 (rev SOME_NON_GIT_HASH)",
                                                                KUDU_VERSION_STRING));
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));
  }

  // Ensure that trying to re-register with a different port fails.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_registration()->mutable_rpc_addresses(0)->set_port(1001);
    Status s = proxy_->TSHeartbeat(req, &resp, &rpc);
    ASSERT_TRUE(s.IsRemoteError());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Tablet server my-ts-uuid is attempting to re-register "
                        "with a different host/port.");
  }
}

Status MasterTest::CreateTable(const string& table_name,
                               const Schema& schema) {
  KuduPartialRow split1(&schema);
  RETURN_NOT_OK(split1.SetInt32("key", 10));

  KuduPartialRow split2(&schema);
  RETURN_NOT_OK(split2.SetInt32("key", 20));

  return CreateTable(table_name, schema, { split1, split2 }, {});
}

Status MasterTest::CreateTable(const string& table_name,
                               const Schema& schema,
                               const vector<KuduPartialRow>& split_rows,
                               const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds) {

  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name(table_name);
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());
  for (const KuduPartialRow& row : split_rows) {
    encoder.Add(RowOperationsPB::SPLIT_ROW, row);
  }
  for (const pair<KuduPartialRow, KuduPartialRow>& bound : bounds) {
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, bound.first);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, bound.second);
  }

  if (!bounds.empty()) {
    controller.RequireServerFeature(MasterFeatures::RANGE_PARTITION_BOUNDS);
  }

  RETURN_NOT_OK(proxy_->CreateTable(req, &resp, &controller));
  if (resp.has_error()) {
    RETURN_NOT_OK(StatusFromPB(resp.error().status()));
  }
  return Status::OK();
}

void MasterTest::DoListTables(const ListTablesRequestPB& req, ListTablesResponsePB* resp) {
  RpcController controller;
  ASSERT_OK(proxy_->ListTables(req, resp, &controller));
  SCOPED_TRACE(SecureDebugString(*resp));
  ASSERT_FALSE(resp->has_error());
}

void MasterTest::DoListAllTables(ListTablesResponsePB* resp) {
  ListTablesRequestPB req;
  DoListTables(req, resp);
}

TEST_F(MasterTest, TestCatalog) {
  const char *kTableName = "testtb";
  const char *kOtherTableName = "tbtest";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  ListTablesResponsePB tables;
  ASSERT_NO_FATAL_FAILURE(DoListAllTables(&tables));
  ASSERT_EQ(1, tables.tables_size());
  ASSERT_EQ(kTableName, tables.tables(0).name());

  // Delete the table
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());
  }

  // List tables, should show no table
  ASSERT_NO_FATAL_FAILURE(DoListAllTables(&tables));
  ASSERT_EQ(0, tables.tables_size());

  // Re-create the table
  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  // Restart the master, verify the table still shows up.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->
      WaitUntilCatalogManagerIsLeaderAndReadyForTests(MonoDelta::FromSeconds(5)));

  ASSERT_NO_FATAL_FAILURE(DoListAllTables(&tables));
  ASSERT_EQ(1, tables.tables_size());
  ASSERT_EQ(kTableName, tables.tables(0).name());

  // Test listing tables with a filter.
  ASSERT_OK(CreateTable(kOtherTableName, kTableSchema));

  {
    ListTablesRequestPB req;
    req.set_name_filter("test");
    DoListTables(req, &tables);
    ASSERT_EQ(2, tables.tables_size());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter("tb");
    DoListTables(req, &tables);
    ASSERT_EQ(2, tables.tables_size());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter(kTableName);
    DoListTables(req, &tables);
    ASSERT_EQ(1, tables.tables_size());
    ASSERT_EQ(kTableName, tables.tables(0).name());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter("btes");
    DoListTables(req, &tables);
    ASSERT_EQ(1, tables.tables_size());
    ASSERT_EQ(kOtherTableName, tables.tables(0).name());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter("randomname");
    DoListTables(req, &tables);
    ASSERT_EQ(0, tables.tables_size());
  }
}

TEST_F(MasterTest, TestCreateTableCheckRangeInvariants) {
  const char *kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32), ColumnSchema("val", INT32) }, 1);

  // No duplicate split rows.
  {
    KuduPartialRow split1(&kTableSchema);
    ASSERT_OK(split1.SetInt32("key", 1));
    KuduPartialRow split2(&kTableSchema);
    ASSERT_OK(split2.SetInt32("key", 2));
    Status s = CreateTable(kTableName, kTableSchema, { split1, split1, split2 }, {});
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "duplicate split row");
  }

  // No empty split rows.
  {
    KuduPartialRow split1(&kTableSchema);
    ASSERT_OK(split1.SetInt32("key", 1));
    KuduPartialRow split2(&kTableSchema);
    Status s = CreateTable(kTableName, kTableSchema, { split1, split2 }, {});
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Invalid argument: split rows must contain a value for at "
                        "least one range partition column");
  }

  // No non-range columns.
  {
    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 1));
    ASSERT_OK(split.SetInt32("val", 1));
    Status s = CreateTable(kTableName, kTableSchema, { split }, {});
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Invalid argument: split rows may only contain values "
                        "for range partitioned columns: val")
  }

  { // Overlapping bounds.
    KuduPartialRow a_lower(&kTableSchema);
    KuduPartialRow a_upper(&kTableSchema);
    KuduPartialRow b_lower(&kTableSchema);
    KuduPartialRow b_upper(&kTableSchema);
    ASSERT_OK(a_lower.SetInt32("key", 0));
    ASSERT_OK(a_upper.SetInt32("key", 100));
    ASSERT_OK(b_lower.SetInt32("key", 50));
    ASSERT_OK(b_upper.SetInt32("key", 150));
    Status s = CreateTable(kTableName, kTableSchema, { }, { { a_lower, a_upper },
                                                            { b_lower, b_upper } });
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "Invalid argument: overlapping range partition");
  }
  { // Split row out of bounds (above).
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 150));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 200));

    Status s = CreateTable(kTableName, kTableSchema, { split },
                           { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "Invalid argument: split out of bounds");
  }
  { // Split row out of bounds (below).
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 150));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", -120));

    Status s = CreateTable(kTableName, kTableSchema, { split },
                           { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "Invalid argument: split out of bounds");
  }
  { // Lower bound greater than upper bound.
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 150));
    ASSERT_OK(bound_upper.SetInt32("key", 0));

    Status s = CreateTable(kTableName, kTableSchema, { }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Invalid argument: range partition lower bound must be "
                        "less than or equal to the upper bound");
  }
  { // Lower bound equals upper bound.
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 0));

    Status s = CreateTable(kTableName, kTableSchema, { }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Invalid argument: range partition lower bound must be "
                        "less than or equal to the upper bound");
  }
  { // Split equals lower bound
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 10));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 0));

    Status s = CreateTable(kTableName, kTableSchema, { split }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "Invalid argument: split matches lower or upper bound");
  }
  { // Split equals upper bound
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 10));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 10));

    Status s = CreateTable(kTableName, kTableSchema, { split }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "Invalid argument: split matches lower or upper bound");
  }
}

TEST_F(MasterTest, TestCreateTableInvalidKeyType) {
  const char *kTableName = "testtb";

  const DataType types[] = { BOOL, FLOAT, DOUBLE };
  for (DataType type : types) {
    const Schema kTableSchema({ ColumnSchema("key", type) }, 1);
    Status s = CreateTable(kTableName, kTableSchema, vector<KuduPartialRow>(), {});
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
        "key column may not have type of BOOL, FLOAT, or DOUBLE");
  }
}

// Regression test for KUDU-253/KUDU-592: crash if the schema passed to CreateTable
// is invalid.
TEST_F(MasterTest, TestCreateTableInvalidSchema) {
  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name("table");
  for (int i = 0; i < 2; i++) {
    ColumnSchemaPB* col = req.mutable_schema()->add_columns();
    col->set_name("col");
    col->set_type(INT32);
    col->set_is_key(true);
  }

  ASSERT_OK(proxy_->CreateTable(req, &resp, &controller));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ("code: INVALID_ARGUMENT message: \"Duplicate column name: col\"",
            SecureShortDebugString(resp.error().status()));
}

// Test that, if the client specifies mismatched read and write defaults,
// we return an error.
TEST_F(MasterTest, TestCreateTableMismatchedDefaults) {
  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name("table");

  ColumnSchemaPB* col = req.mutable_schema()->add_columns();
  col->set_name("key");
  col->set_type(INT32);
  col->set_is_key(true);

  col = req.mutable_schema()->add_columns();
  col->set_name("col");
  col->set_type(BINARY);
  col->set_is_nullable(true);
  req.mutable_schema()->mutable_columns(1)->set_read_default_value("hello");
  req.mutable_schema()->mutable_columns(1)->set_write_default_value("bye");

  ASSERT_OK(proxy_->CreateTable(req, &resp, &controller));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ("code: INVALID_ARGUMENT message: \"column \\'col\\' has "
            "mismatched read/write defaults\"",
            SecureShortDebugString(resp.error().status()));
}

// Regression test for KUDU-253/KUDU-592: crash if the GetTableLocations RPC call is
// invalid.
TEST_F(MasterTest, TestInvalidGetTableLocations) {
  const string kTableName = "test";
  Schema schema({ ColumnSchema("key", INT32) }, 1);
  ASSERT_OK(CreateTable(kTableName, schema));
  {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    // Set the "start" key greater than the "end" key.
    req.set_partition_key_start("zzzz");
    req.set_partition_key_end("aaaa");
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ("code: INVALID_ARGUMENT message: "
              "\"start partition key is greater than the end partition key\"",
              SecureShortDebugString(resp.error().status()));
  }
}

// Tests that if the master is shutdown while a table visitor is active, the
// shutdown waits for the visitor to finish, avoiding racing and crashing.
TEST_F(MasterTest, TestShutdownDuringTableVisit) {
  ASSERT_OK(master_->catalog_manager()->ElectedAsLeaderCb());

  // Master will now shut down, potentially racing with
  // CatalogManager::VisitTablesAndTabletsTask.
}

// Tests that the catalog manager handles spurious calls to ElectedAsLeaderCb()
// (i.e. those without a term change) correctly by ignoring them. If they
// aren't ignored, a concurrent GetTableLocations() call may trigger a
// use-after-free.
TEST_F(MasterTest, TestGetTableLocationsDuringRepeatedTableVisit) {
  const char* kTableName = "test";
  Schema schema({ ColumnSchema("key", INT32) }, 1);
  ASSERT_OK(CreateTable(kTableName, schema));

  AtomicBool done(false);

  // Hammers the master with GetTableLocations() calls.
  thread t([&]() {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(kTableName);

    while (!done.Load()) {
      RpcController controller;
      CHECK_OK(proxy_->GetTableLocations(req, &resp, &controller));
    }
  });

  // Call ElectedAsLeaderCb() repeatedly. If these spurious calls aren't
  // ignored, the concurrent GetTableLocations() calls may crash the master.
  for (int i = 0; i < 100; i++) {
    master_->catalog_manager()->ElectedAsLeaderCb();
  }
  done.Store(true);
  t.join();
}

// The catalog manager had a bug wherein GetTableSchema() interleaved with
// CreateTable() could expose intermediate uncommitted state to clients. This
// test ensures that bug does not regress.
TEST_F(MasterTest, TestGetTableSchemaIsAtomicWithCreateTable) {
  const char* kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  CountDownLatch started(1);
  AtomicBool done(false);

  // Kick off a thread that hammers the master with GetTableSchema() calls,
  // checking that the results make sense.
  thread t([&]() {
    GetTableSchemaRequestPB req;
    GetTableSchemaResponsePB resp;
    req.mutable_table()->set_table_name(kTableName);

    started.CountDown();
    while (!done.Load()) {
      RpcController controller;

      CHECK_OK(proxy_->GetTableSchema(req, &resp, &controller));
      SCOPED_TRACE(SecureDebugString(resp));

      // There are two possible outcomes:
      //
      // 1. GetTableSchema() happened before CreateTable(): we expect to see a
      //    TABLE_NOT_FOUND error.
      // 2. GetTableSchema() happened after CreateTable(): we expect to see the
      //    full table schema.
      //
      // Any other outcome is an error.
      if (resp.has_error()) {
        CHECK_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code());
      } else {
        Schema receivedSchema;
        CHECK_OK(SchemaFromPB(resp.schema(), &receivedSchema));
        CHECK(kTableSchema.Equals(receivedSchema)) <<
            strings::Substitute("$0 not equal to $1",
                                kTableSchema.ToString(), receivedSchema.ToString());
        CHECK_EQ(kTableName, resp.table_name());
      }
    }
  });

  // Only create the table after the thread has started.
  started.Wait();
  EXPECT_OK(CreateTable(kTableName, kTableSchema));

  done.Store(true);
  t.join();
}

// Verifies that on-disk master metadata is self-consistent and matches a set
// of expected contents.
//
// Sample usage:
//
//   MasterMetadataVerifier v(live, dead);
//   sys_catalog->VisitTables(&v);
//   sys_catalog->VisitTablets(&v);
//   ASSERT_OK(v.Verify());
//
class MasterMetadataVerifier : public TableVisitor,
                               public TabletVisitor {
 public:
  MasterMetadataVerifier(const unordered_set<string>& live_table_names,
                         const multiset<string>& dead_table_names)
    : live_table_names_(live_table_names),
      dead_table_names_(dead_table_names) {
  }

  virtual Status VisitTable(const std::string& table_id,
                             const SysTablesEntryPB& metadata) OVERRIDE {
     InsertOrDie(&visited_tables_by_id_, table_id,
                 { table_id, metadata.name(), metadata.state() });
     return Status::OK();
   }

  virtual Status VisitTablet(const std::string& table_id,
                             const std::string& tablet_id,
                             const SysTabletsEntryPB& metadata) OVERRIDE {
    InsertOrDie(&visited_tablets_by_id_, tablet_id,
                { tablet_id, table_id, metadata.state() });
    return Status::OK();
  }

  Status Verify() {
    RETURN_NOT_OK(VerifyTables());
    RETURN_NOT_OK(VerifyTablets());
    return Status::OK();
  }

 private:
  Status VerifyTables() {
    unordered_set<string> live_visited_table_names;
    multiset<string> dead_visited_table_names;

    for (const auto& entry : visited_tables_by_id_) {
      const Table& table = entry.second;
      switch (table.state) {
        case SysTablesEntryPB::RUNNING:
        case SysTablesEntryPB::ALTERING:
          InsertOrDie(&live_visited_table_names, table.name);
          break;
        case SysTablesEntryPB::REMOVED:
          // InsertOrDie() doesn't work on multisets, where the returned
          // element is not an std::pair.
          dead_visited_table_names.insert(table.name);
          break;
        default:
          return Status::Corruption(
              Substitute("Table $0 has unexpected state $1",
                         table.id,
                         SysTablesEntryPB::State_Name(table.state)));
      }
    }

    if (live_visited_table_names != live_table_names_) {
      return Status::Corruption("Live table name set mismatch");
    }

    if (dead_visited_table_names != dead_table_names_) {
      return Status::Corruption("Dead table name set mismatch");
    }
    return Status::OK();
  }

  Status VerifyTablets() {
    // Each table should be referenced by exactly this number of tablets.
    const int kNumExpectedReferences = 3;

    // Build table ID --> table map for use in verification below.
    unordered_map<string, const Table*> tables_by_id;
    for (const auto& entry : visited_tables_by_id_) {
      InsertOrDie(&tables_by_id, entry.second.id, &entry.second);
    }

    map<string, int> referenced_tables;
    for (const auto& entry : visited_tablets_by_id_) {
      const Tablet& tablet = entry.second;
      switch (tablet.state) {
        case SysTabletsEntryPB::PREPARING:
        case SysTabletsEntryPB::CREATING:
        case SysTabletsEntryPB::DELETED:
        {
          const Table* table = FindPtrOrNull(tables_by_id, tablet.table_id);
          if (!table) {
            return Status::Corruption(Substitute(
                "Tablet $0 belongs to non-existent table $1",
                tablet.id, tablet.table_id));
          }
          string table_state_str = SysTablesEntryPB_State_Name(table->state);
          string tablet_state_str = SysTabletsEntryPB_State_Name(tablet.state);

          // PREPARING or CREATING tablets must be members of RUNNING or
          // ALTERING tables.
          //
          // DELETED tablets must be members of REMOVED tables.
          if (((tablet.state == SysTabletsEntryPB::PREPARING ||
                tablet.state == SysTabletsEntryPB::CREATING) &&
               (table->state != SysTablesEntryPB::RUNNING &&
                table->state != SysTablesEntryPB::ALTERING)) ||
              (tablet.state == SysTabletsEntryPB::DELETED &&
               table->state != SysTablesEntryPB::REMOVED)) {
            return Status::Corruption(
                Substitute("Unexpected states: table $0=$1, tablet $2=$3",
                           table->id, table_state_str,
                           tablet.id, tablet_state_str));
          }

          referenced_tables[tablet.table_id]++;
          break;
        }
        default:
          return Status::Corruption(
              Substitute("Tablet $0 has unexpected state $1",
                         tablet.id,
                         SysTabletsEntryPB::State_Name(tablet.state)));
      }
    }

    for (const auto& entry : referenced_tables) {
      if (entry.second != kNumExpectedReferences) {
        return Status::Corruption(
            Substitute("Table $0 has bad reference count ($1 instead of $2)",
                       entry.first, entry.second, kNumExpectedReferences));
      }
    }
    return Status::OK();
  }

  // Names of tables that are thought to be created and never deleted.
  const unordered_set<string> live_table_names_;

  // Names of tables that are thought to be deleted. A table with a given name
  // could be deleted more than once.
  const multiset<string> dead_table_names_;

  // Table ID to table map populated during VisitTables().
  struct Table {
    string id;
    string name;
    SysTablesEntryPB::State state;
  };
  unordered_map<string, Table> visited_tables_by_id_;

  // Tablet ID to tablet map populated during VisitTablets().
  struct Tablet {
    string id;
    string table_id;
    SysTabletsEntryPB::State state;
  };
  unordered_map<string, Tablet> visited_tablets_by_id_;
};

TEST_F(MasterTest, TestMasterMetadataConsistentDespiteFailures) {
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  // When generating random table names, we use a uniform distribution so
  // as to generate the occasional name collision; the test should cope.
  const int kUniformBound = 25;

  // Ensure some portion of the attempted operations fail.
  FLAGS_sys_catalog_fail_during_write = 0.25;
  int num_injected_failures = 0;

  // Tracks all "live" tables (i.e. created and not yet deleted).
  vector<string> table_names;

  // Tracks all deleted tables. A given name may have been deleted more
  // than once.
  multiset<string> deleted_table_names;
  Random r(SeedRandom());

  // Spend some time hammering the master with create/alter/delete operations.
  MonoDelta time_to_run = MonoDelta::FromSeconds(AllowSlowTests() ? 10 : 1);
  MonoTime deadline = MonoTime::Now() + time_to_run;
  while (MonoTime::Now() < deadline) {
    int next_action = r.Uniform(3);
    switch (next_action) {
      case 0:
      {
        // Create a new table with a random name and three tablets.
        //
        // No name collision checking, so this table may already exist.
        CreateTableRequestPB req;
        CreateTableResponsePB resp;
        RpcController controller;

        req.set_name(Substitute("table-$0", r.Uniform(kUniformBound)));
        ASSERT_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
        RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());
        KuduPartialRow row(&kTableSchema);
        ASSERT_OK(row.SetInt32("key", 10));
        encoder.Add(RowOperationsPB::SPLIT_ROW, row);
        ASSERT_OK(row.SetInt32("key", 20));
        encoder.Add(RowOperationsPB::SPLIT_ROW, row);

        ASSERT_OK(proxy_->CreateTable(req, &resp, &controller));
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          ASSERT_TRUE(s.IsAlreadyPresent() || s.IsRuntimeError()) << s.ToString();
          if (s.IsRuntimeError()) {
            ASSERT_STR_CONTAINS(s.ToString(),
                                SysCatalogTable::kInjectedFailureStatusMsg);
            num_injected_failures++;
          }
        } else {
          table_names.push_back(req.name());
        }
        break;
      }
      case 1:
      {
        // Rename a random table to some random name.
        //
        // No name collision checking, so the new table name may already exist.
        int num_tables = table_names.size();
        if (num_tables == 0) {
          break;
        }
        int table_idx = r.Uniform(num_tables);
        AlterTableRequestPB req;
        AlterTableResponsePB resp;
        RpcController controller;

        req.mutable_table()->set_table_name(table_names[table_idx]);
        req.set_new_table_name(Substitute("table-$0", r.Uniform(kUniformBound)));
        ASSERT_OK(proxy_->AlterTable(req, &resp, &controller));
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          ASSERT_TRUE(s.IsAlreadyPresent() || s.IsRuntimeError()) << s.ToString();
          if (s.IsRuntimeError()) {
            ASSERT_STR_CONTAINS(s.ToString(),
                                SysCatalogTable::kInjectedFailureStatusMsg);
            num_injected_failures++;
          }
        } else {
          table_names[table_idx] = req.new_table_name();
        }
        break;
      }
      case 2:
      {
        // Delete a random table.
        int num_tables = table_names.size();
        if (num_tables == 0) {
          break;
        }
        int table_idx = r.Uniform(num_tables);
        DeleteTableRequestPB req;
        DeleteTableResponsePB resp;
        RpcController controller;

        req.mutable_table()->set_table_name(table_names[table_idx]);
        ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
          ASSERT_STR_CONTAINS(s.ToString(),
                              SysCatalogTable::kInjectedFailureStatusMsg);
          num_injected_failures++;
        } else {
          deleted_table_names.insert(table_names[table_idx]);
          table_names[table_idx] = table_names.back();
          table_names.pop_back();
        }
        break;
      }
      default:
        LOG(FATAL) << "Cannot reach here!";
    }
  }

  // Injected failures are random, but given the number of operations we did,
  // we should expect to have seen at least one.
  ASSERT_GE(num_injected_failures, 1);

  // Restart the catalog manager to ensure that it can survive reloading the
  // metadata we wrote to disk.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());

  // Reload the metadata again, this time verifying its consistency.
  unordered_set<string> live_table_names(table_names.begin(),
                                         table_names.end());
  MasterMetadataVerifier verifier(live_table_names, deleted_table_names);
  SysCatalogTable* sys_catalog =
      mini_master_->master()->catalog_manager()->sys_catalog();
  ASSERT_OK(sys_catalog->VisitTables(&verifier));
  ASSERT_OK(sys_catalog->VisitTablets(&verifier));
  ASSERT_OK(verifier.Verify());
}

TEST_F(MasterTest, TestConcurrentCreateOfSameTable) {
  const char* kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  // Kick off a bunch of threads all trying to create the same table.
  vector<thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&]() {
      CreateTableRequestPB req;
      CreateTableResponsePB resp;
      RpcController controller;

      req.set_name(kTableName);
      CHECK_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
      CHECK_OK(proxy_->CreateTable(req, &resp, &controller));
      SCOPED_TRACE(SecureDebugString(resp));

      // There are three expected outcomes:
      //
      // 1. This thread won the CreateTable() race: no error.
      // 2. This thread lost the CreateTable() race: TABLE_NOT_FOUND error
      //    with ServiceUnavailable status.
      // 3. This thread arrived after the CreateTable() race was already over:
      //    TABLE_ALREADY_PRESENT error with AlreadyPresent status.
      if (resp.has_error()) {
        Status s = StatusFromPB(resp.error().status());
        string failure_msg = Substitute("Unexpected response: $0",
                                        SecureDebugString(resp));
        switch (resp.error().code()) {
          case MasterErrorPB::TABLE_NOT_FOUND:
            CHECK(s.IsServiceUnavailable()) << failure_msg;
            break;
          case MasterErrorPB::TABLE_ALREADY_PRESENT:
            CHECK(s.IsAlreadyPresent()) << failure_msg;
            break;
          default:
            FAIL() << failure_msg;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(MasterTest, TestConcurrentRenameOfSameTable) {
  const char* kOldName = "testtb";
  const char* kNewName = "testtb-new";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);
  ASSERT_OK(CreateTable(kOldName, kTableSchema));

  // Kick off a bunch of threads all trying to rename the same table.
  vector<thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&]() {
      AlterTableRequestPB req;
      AlterTableResponsePB resp;
      RpcController controller;

      req.mutable_table()->set_table_name(kOldName);
      req.set_new_table_name(kNewName);
      CHECK_OK(proxy_->AlterTable(req, &resp, &controller));
      SCOPED_TRACE(SecureDebugString(resp));

      // There are two expected outcomes:
      //
      // 1. This thread won the AlterTable() race: no error.
      // 2. This thread lost the AlterTable() race: TABLE_NOT_FOUND error
      //    with NotFound status.
      if (resp.has_error()) {
        Status s = StatusFromPB(resp.error().status());
        string failure_msg = Substitute("Unexpected response: $0",
                                        SecureDebugString(resp));
        CHECK_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code()) << failure_msg;
        CHECK(s.IsNotFound()) << failure_msg;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(MasterTest, TestConcurrentCreateAndRenameOfSameTable) {
  const char* kOldName = "testtb";
  const char* kNewName = "testtb-new";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);
  ASSERT_OK(CreateTable(kOldName, kTableSchema));

  AtomicBool create_success(false);
  AtomicBool rename_success(false);
  vector<thread> threads;
  for (int i = 0; i < 10; i++) {
    if (i % 2) {
      threads.emplace_back([&]() {
        CreateTableRequestPB req;
        CreateTableResponsePB resp;
        RpcController controller;

        req.set_name(kNewName);
        RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());

        KuduPartialRow split1(&kTableSchema);
        CHECK_OK(split1.SetInt32("key", 10));
        encoder.Add(RowOperationsPB::SPLIT_ROW, split1);

        KuduPartialRow split2(&kTableSchema);
        CHECK_OK(split2.SetInt32("key", 20));
        encoder.Add(RowOperationsPB::SPLIT_ROW, split2);

        CHECK_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
        CHECK_OK(proxy_->CreateTable(req, &resp, &controller));
        SCOPED_TRACE(SecureDebugString(resp));

        // There are three expected outcomes:
        //
        // 1. This thread finished well before the others: no error.
        // 2. This thread raced with another thread: TABLE_NOT_FOUND error with
        //    ServiceUnavailable status.
        // 3. This thread finished well after the others: TABLE_ALREADY_PRESENT
        //    error with AlreadyPresent status.
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          string failure_msg = Substitute("Unexpected response: $0",
                                          SecureDebugString(resp));
          switch (resp.error().code()) {
            case MasterErrorPB::TABLE_NOT_FOUND:
              CHECK(s.IsServiceUnavailable()) << failure_msg;
              break;
            case MasterErrorPB::TABLE_ALREADY_PRESENT:
              CHECK(s.IsAlreadyPresent()) << failure_msg;
              break;
            default:
              FAIL() << failure_msg;
          }
        } else {
          // Creating the table should only succeed once.
          CHECK(!create_success.Exchange(true));
        }
      });
    } else {
      threads.emplace_back([&]() {
        AlterTableRequestPB req;
        AlterTableResponsePB resp;
        RpcController controller;

        req.mutable_table()->set_table_name(kOldName);
        req.set_new_table_name(kNewName);
        CHECK_OK(proxy_->AlterTable(req, &resp, &controller));
        SCOPED_TRACE(SecureDebugString(resp));

        // There are three expected outcomes:
        //
        // 1. This thread finished well before the others: no error.
        // 2. This thread raced with CreateTable(): TABLE_NOT_FOUND error with
        //    ServiceUnavailable status (if raced during reservation stage)
        //    or TABLE_ALREADY_PRESENT error with AlreadyPresent status (if
        //    raced after reservation stage).
        // 3. This thread raced with AlterTable() or finished well after the
        //    others: TABLE_NOT_FOUND error with NotFound status.
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          string failure_msg = Substitute("Unexpected response: $0",
                                          SecureDebugString(resp));
          switch (resp.error().code()) {
            case MasterErrorPB::TABLE_NOT_FOUND:
              CHECK(s.IsServiceUnavailable() || s.IsNotFound()) << failure_msg;
              break;
            case MasterErrorPB::TABLE_ALREADY_PRESENT:
              CHECK(s.IsAlreadyPresent()) << failure_msg;
              break;
            default:
              FAIL() << failure_msg;
          }
        } else {
          // Renaming the table should only succeed once.
          CHECK(!rename_success.Exchange(true));
        }
      });
    }
  }

  for (auto& t : threads) {
    t.join();
  }

  // At least one of rename or create should have failed; if both succeeded
  // there must be some sort of race.
  CHECK(!rename_success.Load() || !create_success.Load());

  unordered_set<string> live_tables;
  live_tables.insert(kNewName);
  if (create_success.Load()) {
    live_tables.insert(kOldName);
  }
  MasterMetadataVerifier verifier(live_tables, {});
  SysCatalogTable* sys_catalog =
      mini_master_->master()->catalog_manager()->sys_catalog();
  ASSERT_OK(sys_catalog->VisitTables(&verifier));
  ASSERT_OK(sys_catalog->VisitTablets(&verifier));
  ASSERT_OK(verifier.Verify());
}

// Unit tests for the ConnectToMaster() RPC:
// should issue authentication tokens and the master CA cert.
TEST_F(MasterTest, TestConnectToMaster) {
  ConnectToMasterRequestPB req;
  ConnectToMasterResponsePB resp;
  RpcController rpc;
  ASSERT_OK(proxy_->ConnectToMaster(req, &resp, &rpc));
  SCOPED_TRACE(resp.DebugString());

  EXPECT_EQ(consensus::LEADER, resp.role()) << "should be leader";
  ASSERT_EQ(1, resp.ca_cert_der_size()) << "should have one cert";
  EXPECT_GT(resp.ca_cert_der(0).size(), 100) << "CA cert should be at least 100 bytes";
  ASSERT_TRUE(resp.has_authn_token()) << "should return an authn token";
  // Using 512 bit RSA key and SHA256 digest results in 64 byte signature.
  EXPECT_EQ(64, resp.authn_token().signature().size());
  ASSERT_TRUE(resp.authn_token().has_signing_key_seq_num());
  EXPECT_GT(resp.authn_token().signing_key_seq_num(), -1);

  security::TokenPB token;
  ASSERT_TRUE(token.ParseFromString(resp.authn_token().token_data()));
  ASSERT_TRUE(token.authn().has_username());
}

// Test that the master signs its on server certificate when it becomes the leader,
// and also that it loads TSKs into the messenger's verifier.
TEST_F(MasterTest, TestSignOwnCertAndLoadTSKs) {
  AssertEventually([&]() {
      ASSERT_TRUE(master_->tls_context().has_signed_cert());
      ASSERT_GT(master_->messenger()->token_verifier().GetMaxKnownKeySequenceNumber(), -1);
    });
}

} // namespace master
} // namespace kudu
