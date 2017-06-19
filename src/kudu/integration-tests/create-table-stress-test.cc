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

#include <glog/stl_logging.h>
#include <gtest/gtest.h>
#include <memory>
#include <thread>

#include "kudu/client/client.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster.h"
#include "kudu/master/master-test-util.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableCreator;
using kudu::itest::CreateTabletServerMap;
using kudu::itest::TabletServerMap;
using kudu::master::MasterServiceProxy;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;

DECLARE_int32(heartbeat_interval_ms);
DECLARE_bool(log_preallocate_segments);
DEFINE_int32(num_test_tablets, 60, "Number of tablets for stress test");

using std::shared_ptr;
using std::thread;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {

const char* kTableName = "test_table";

class CreateTableStressTest : public KuduTest {
 public:
  CreateTableStressTest() {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("v1")->Type(KuduColumnSchema::INT64)->NotNull();
    b.AddColumn("v2")->Type(KuduColumnSchema::STRING)->NotNull();
    CHECK_OK(b.Build(&schema_));
  }

  virtual void SetUp() OVERRIDE {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    // Don't preallocate log segments, since we're creating thousands
    // of tablets here. If each preallocates 64M or so, we use
    // a ton of disk space in this test, and it fails on normal
    // sized /tmp dirs.
    // TODO: once we collapse multiple tablets into shared WAL files,
    // this won't be necessary.
    FLAGS_log_preallocate_segments = false;

    KuduTest::SetUp();
    MiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());

    ASSERT_OK(KuduClientBuilder()
                     .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr_str())
                     .Build(&client_));

    ASSERT_OK(MessengerBuilder("stress-test-msgr")
              .set_num_reactors(1)
              .set_max_negotiation_threads(1)
              .Build(&messenger_));
    master_proxy_.reset(new MasterServiceProxy(messenger_,
                                               cluster_->mini_master()->bound_rpc_addr()));
    ASSERT_OK(CreateTabletServerMap(master_proxy_, messenger_, &ts_map_));
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
    STLDeleteValues(&ts_map_);
  }

  void CreateBigTable(const string& table_name, int num_tablets);

 protected:
  client::sp::shared_ptr<KuduClient> client_;
  unique_ptr<InternalMiniCluster> cluster_;
  KuduSchema schema_;
  std::shared_ptr<Messenger> messenger_;
  shared_ptr<MasterServiceProxy> master_proxy_;
  TabletServerMap ts_map_;
};

void CreateTableStressTest::CreateBigTable(const string& table_name, int num_tablets) {
  vector<const KuduPartialRow*> split_rows;
  int num_splits = num_tablets - 1; // 4 tablets == 3 splits.
  // Let the "\x8\0\0\0" keys end up in the first split; start splitting at 1.
  for (int i = 1; i <= num_splits; i++) {
    KuduPartialRow* row = schema_.NewRow();
    CHECK_OK(row->SetInt32(0, i));
    split_rows.push_back(row);
  }

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(table_name)
            .schema(&schema_)
            .set_range_partition_columns({ "key" })
            .split_rows(split_rows)
            .num_replicas(3)
            .wait(false)
            .Create());
}

TEST_F(CreateTableStressTest, CreateAndDeleteBigTable) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping slow test";
    return;
  }
  string table_name = "test_table";
  ASSERT_NO_FATAL_FAILURE(CreateBigTable(table_name, FLAGS_num_test_tablets));
  master::GetTableLocationsResponsePB resp;
  ASSERT_OK(WaitForRunningTabletCount(cluster_->mini_master(), table_name,
                                      FLAGS_num_test_tablets, &resp));
  LOG(INFO) << "Created table successfully!";
  // Use std::cout instead of log, since these responses are large and log
  // messages have a max size.
  std::cout << "Response:\n" << SecureDebugString(resp);
  std::cout << "CatalogManager state:\n";
  cluster_->mini_master()->master()->catalog_manager()->DumpState(&std::cerr);

  LOG(INFO) << "Deleting table...";
  ASSERT_OK(client_->DeleteTable(table_name));

  // The actual removal of the tablets is asynchronous, so we loop for a bit
  // waiting for them to get removed.
  LOG(INFO) << "Waiting for tablets to be removed";
  vector<string> tablet_ids;
  for (int i = 0; i < 1000; i++) {
    ASSERT_OK(itest::ListRunningTabletIds(ts_map_.begin()->second,
                                          MonoDelta::FromSeconds(10),
                                          &tablet_ids));
    if (tablet_ids.empty()) break;
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  ASSERT_TRUE(tablet_ids.empty()) << "Tablets remained: " << tablet_ids;
}

TEST_F(CreateTableStressTest, RestartMasterDuringCreation) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping slow test";
    return;
  }

  string table_name = "test_table";
  ASSERT_NO_FATAL_FAILURE(CreateBigTable(table_name, FLAGS_num_test_tablets));

  for (int i = 0; i < 3; i++) {
    SleepFor(MonoDelta::FromMicroseconds(500));
    LOG(INFO) << "Restarting master...";
    cluster_->mini_master()->Shutdown();
    ASSERT_OK(cluster_->mini_master()->Restart());
    ASSERT_OK(cluster_->mini_master()->master()->
        WaitUntilCatalogManagerIsLeaderAndReadyForTests(MonoDelta::FromSeconds(5)));
    LOG(INFO) << "Master restarted.";
  }

  master::GetTableLocationsResponsePB resp;
  Status s = WaitForRunningTabletCount(cluster_->mini_master(), table_name,
                                       FLAGS_num_test_tablets, &resp);
  if (!s.ok()) {
    cluster_->mini_master()->master()->catalog_manager()->DumpState(&std::cerr);
    CHECK_OK(s);
  }
}

TEST_F(CreateTableStressTest, TestGetTableLocationsOptions) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping slow test";
    return;
  }

  string table_name = "test_table";
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 1. Creating big table " << table_name << " ...";
  LOG_TIMING(INFO, "creating big table") {
    ASSERT_NO_FATAL_FAILURE(CreateBigTable(table_name, FLAGS_num_test_tablets));
  }

  master::GetTableLocationsRequestPB req;
  master::GetTableLocationsResponsePB resp;

  // Make sure the table is completely created before we start poking.
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 2. Waiting for creation of big table "
            << table_name << " to complete...";
  LOG_TIMING(INFO, "waiting for creation of big table") {
    ASSERT_OK(WaitForRunningTabletCount(cluster_->mini_master(), table_name,
                                       FLAGS_num_test_tablets, &resp));
  }

  master::CatalogManager* catalog =
      cluster_->mini_master()->master()->catalog_manager();
  master::CatalogManager::ScopedLeaderSharedLock l(catalog);
  ASSERT_OK(l.first_failed_status());

  // Test asking for 0 tablets, should fail
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 3. Asking for zero tablets...";
  LOG_TIMING(INFO, "asking for zero tablets") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(0);
    Status s = catalog->GetTableLocations(&req, &resp);
    ASSERT_STR_CONTAINS(s.ToString(), "must be greater than 0");
  }

  // Ask for one, get one, verify
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 4. Asking for one tablet...";
  LOG_TIMING(INFO, "asking for one tablet") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(1);
    ASSERT_OK(catalog->GetTableLocations(&req, &resp));
    ASSERT_EQ(resp.tablet_locations_size(), 1);
    // empty since it's the first
    ASSERT_EQ(resp.tablet_locations(0).partition().partition_key_start(), "");
    ASSERT_EQ(resp.tablet_locations(0).partition().partition_key_end(), string("\x80\0\0\1", 4));
  }

  int half_tablets = FLAGS_num_test_tablets / 2;
  // Ask for half of them, get that number back
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 5. Asking for half the tablets...";
  LOG_TIMING(INFO, "asking for half the tablets") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(half_tablets);
    ASSERT_OK(catalog->GetTableLocations(&req, &resp));
    ASSERT_EQ(half_tablets, resp.tablet_locations_size());
  }

  // Ask for all of them, get that number back
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 6. Asking for all the tablets...";
  LOG_TIMING(INFO, "asking for all the tablets") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(FLAGS_num_test_tablets);
    ASSERT_OK(catalog->GetTableLocations(&req, &resp));
    ASSERT_EQ(FLAGS_num_test_tablets, resp.tablet_locations_size());
  }

  LOG(INFO) << "========================================================";
  LOG(INFO) << "Tables and tablets:";
  LOG(INFO) << "========================================================";
  std::vector<scoped_refptr<master::TableInfo> > tables;
  catalog->GetAllTables(&tables);
  for (const scoped_refptr<master::TableInfo>& table_info : tables) {
    LOG(INFO) << "Table: " << table_info->ToString();
    std::vector<scoped_refptr<master::TabletInfo> > tablets;
    table_info->GetAllTablets(&tablets);
    for (const scoped_refptr<master::TabletInfo>& tablet_info : tablets) {
      master::TabletMetadataLock l_tablet(tablet_info.get(), master::TabletMetadataLock::READ);
      const master::SysTabletsEntryPB& metadata = tablet_info->metadata().state().pb;
      LOG(INFO) << "  Tablet: " << tablet_info->ToString()
                << " { start_key: "
                << ((metadata.partition().has_partition_key_start())
                    ? metadata.partition().partition_key_start() : "<< none >>")
                << ", end_key: "
                << ((metadata.partition().has_partition_key_end())
                    ? metadata.partition().partition_key_end() : "<< none >>")
                << ", running = " << tablet_info->metadata().state().is_running() << " }";
    }
    ASSERT_EQ(FLAGS_num_test_tablets, tablets.size());
  }
  LOG(INFO) << "========================================================";

  // Get a single tablet in the middle, make sure we get that one back

  unique_ptr<KuduPartialRow> row(schema_.NewRow());
  ASSERT_OK(row->SetInt32(0, half_tablets - 1));
  string start_key_middle;
  ASSERT_OK(row->EncodeRowKey(&start_key_middle));

  LOG(INFO) << "Start key middle: " << start_key_middle;
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 7. Asking for single middle tablet...";
  LOG_TIMING(INFO, "asking for single middle tablet") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(1);
    req.set_partition_key_start(start_key_middle);
    ASSERT_OK(catalog->GetTableLocations(&req, &resp));
    ASSERT_EQ(1, resp.tablet_locations_size()) << "Response: [" << SecureDebugString(resp) << "]";
    ASSERT_EQ(start_key_middle, resp.tablet_locations(0).partition().partition_key_start());
  }
}

// Creates tables and reloads on-disk metadata concurrently to test for races
// between the two operations.
TEST_F(CreateTableStressTest, TestConcurrentCreateTableAndReloadMetadata) {
  AtomicBool stop(false);

  thread reload_metadata_thread([&]() {
    while (!stop.Load()) {
      CHECK_OK(cluster_->mini_master()->master()->catalog_manager()->
          VisitTablesAndTablets());

      // Give table creation a chance to run.
      SleepFor(MonoDelta::FromMilliseconds(1 + rand() % 10));
    }
  });

  for (int num_tables_created = 0; num_tables_created < 20;) {
    string table_name = Substitute("test-$0", num_tables_created);
    LOG(INFO) << "Creating table " << table_name;
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    Status s = table_creator->table_name(table_name)
                  .schema(&schema_)
                  .set_range_partition_columns({ "key" })
                  .num_replicas(3)
                  .wait(false)
                  .Create();
    if (s.IsTimedOut()) {
      // The master was busy reloading its metadata, replying with
      // ServiceUnavailable on CreateTable() requests. The client transparently
      // retried (randomized exponential back-off) until the timeout elapsed.
      //
      // It's hard to find some universal constant for timeout which would work
      // in any testing environment instead of simply retrying here. That's
      // because the client uses exponential-with-random back-off strategy
      // while the metadata is being reloaded very often. So, from one side
      // we want to have more or less random interaction between the metadata
      // reloading and the simultaneous table creations, but from the other side
      // it's hard do deduce the universal timeout constant and we prefer to
      // not introduce test flakiness.
      //
      // TODO(aserbin): update the test keeping its racy essence but making it
      //                cleaner regarding this timeout&retry workaround.
      continue;
    }
    ASSERT_OK(s);
    num_tables_created++;
  }
  stop.Store(true);
  reload_metadata_thread.join();
}

} // namespace kudu
