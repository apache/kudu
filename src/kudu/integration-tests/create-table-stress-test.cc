// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "kudu/client/client.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/master-test-util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using std::tr1::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::rpc::RpcController;
using kudu::master::MasterServiceProxy;

DECLARE_int32(heartbeat_interval_ms);
DECLARE_bool(log_preallocate_segments);
DEFINE_int32(num_test_tablets, 100, "Number of tablets for stress test");
DECLARE_bool(use_hybrid_clock);
DECLARE_int32(max_clock_sync_error_usec);

namespace kudu {

const char* kTableName = "test_table";

// Temporarily disabled while working on KUDU-234.
class CreateTableStressTest : public KuduTest {
 public:
  CreateTableStressTest()
    : schema_(boost::assign::list_of
              (KuduColumnSchema("key", KuduColumnSchema::UINT32))
              (KuduColumnSchema("v1", KuduColumnSchema::UINT64))
              (KuduColumnSchema("v2", KuduColumnSchema::STRING)),
              1) {
  }

  virtual void SetUp() OVERRIDE {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    // Use the hybrid clock for TS tests
    FLAGS_use_hybrid_clock = true;

    // Increase the max error tolerance to 10 seconds.
    FLAGS_max_clock_sync_error_usec = 10000000;

    // Don't preallocate log segments, since we're creating thousands
    // of tablets here. If each preallocates 64M or so, we use
    // a ton of disk space in this test, and it fails on normal
    // sized /tmp dirs.
    // TODO: once we collapse multiple tablets into shared WAL files,
    // this won't be necessary.
    FLAGS_log_preallocate_segments = false;

    KuduTest::SetUp();
    cluster_.reset(new MiniCluster(env_.get(), MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());

    ASSERT_OK(KuduClientBuilder()
                     .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr_str())
                     .Build(&client_));
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
  }

  void CreateBigTable(const string& table_name, int num_tablets);

 protected:
  shared_ptr<KuduClient> client_;
  gscoped_ptr<MiniCluster> cluster_;
  KuduSchema schema_;
};

void CreateTableStressTest::CreateBigTable(const string& table_name, int num_tablets) {
  vector<string> keys;
  int num_splits = num_tablets - 1; // 4 tablets == 3 splits.
  // Let the "k_0" keys end up in the first split; start splitting at 1.
  for (int i = 1; i <= num_splits; i++) {
    keys.push_back(StringPrintf("k_%05d", i));
  }

  ASSERT_OK(client_->NewTableCreator()
                   ->table_name(table_name)
                   .schema(&schema_)
                   .split_keys(keys)
                   .wait(false)
                   .Create());
}

TEST_F(CreateTableStressTest, CreateBigTable) {
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
  std::cout << "Response:\n" << resp.DebugString();
  std::cout << "CatalogManager state:\n";
  cluster_->mini_master()->master()->catalog_manager()->DumpState(&std::cerr);
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
    ASSERT_OK(cluster_->mini_master()->Restart());
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

  // Test asking for 0 tablets, should fail
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 3. Asking for zero tablets...";
  LOG_TIMING(INFO, "asking for zero tablets") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(0);
    Status s = cluster_->mini_master()->master()->catalog_manager()->GetTableLocations(&req, &resp);
    ASSERT_STR_CONTAINS(s.ToString(), "must be greater than 0");
  }

  // Ask for one, get one, verify
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 4. Asking for one tablet...";
  LOG_TIMING(INFO, "asking for one tablet") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(1);
    ASSERT_OK(cluster_->mini_master()->master()->catalog_manager()->GetTableLocations(&req, &resp));
    ASSERT_EQ(resp.tablet_locations_size(), 1);
    // empty since it's the first
    ASSERT_EQ(resp.tablet_locations(0).start_key(), "");
    ASSERT_EQ(resp.tablet_locations(0).end_key(), "k_00001");
  }

  int half_tablets = FLAGS_num_test_tablets / 2;
  // Ask for half of them, get that number back
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 5. Asking for half the tablets...";
  LOG_TIMING(INFO, "asking for half the tablets") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(half_tablets);
    ASSERT_OK(cluster_->mini_master()->master()->catalog_manager()->GetTableLocations(&req, &resp));
    ASSERT_EQ(half_tablets, resp.tablet_locations_size());
  }

  // Ask for all of them, get that number back
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 6. Asking for all the tablets...";
  LOG_TIMING(INFO, "asking for all the tablets") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(FLAGS_num_test_tablets);
    ASSERT_OK(cluster_->mini_master()->master()->catalog_manager()->GetTableLocations(&req, &resp));
    ASSERT_EQ(FLAGS_num_test_tablets, resp.tablet_locations_size());
  }

  LOG(INFO) << "========================================================";
  LOG(INFO) << "Tables and tablets:";
  LOG(INFO) << "========================================================";
  std::vector<scoped_refptr<master::TableInfo> > tables;
  cluster_->mini_master()->master()->catalog_manager()->GetAllTables(&tables);
  BOOST_FOREACH(const scoped_refptr<master::TableInfo>& table_info, tables) {
    LOG(INFO) << "Table: " << table_info->ToString();
    std::vector<scoped_refptr<master::TabletInfo> > tablets;
    table_info->GetAllTablets(&tablets);
    BOOST_FOREACH(const scoped_refptr<master::TabletInfo>& tablet_info, tablets) {
      master::TabletMetadataLock l_tablet(tablet_info.get(), master::TabletMetadataLock::READ);
      const master::SysTabletsEntryPB& metadata = tablet_info->metadata().state().pb;
      LOG(INFO) << "  Tablet: " << tablet_info->ToString()
                << " { start_key: "
                << ((metadata.has_start_key()) ? metadata.start_key() : "<< none >>")
                << ", end_key: "
                << ((metadata.has_end_key()) ? metadata.end_key() : "<< none >>") << ", "
                << "running = " << tablet_info->metadata().state().is_running() << " }";
    }
    ASSERT_EQ(FLAGS_num_test_tablets, tablets.size());
  }
  LOG(INFO) << "========================================================";

  // Get a single tablet in the middle, make sure we get that one back
  string start_key_middle = StringPrintf("k_%05d", half_tablets - 1);
  LOG(INFO) << "Start key middle: " << start_key_middle;
  LOG(INFO) << CURRENT_TEST_NAME() << ": Step 7. Asking for single middle tablet...";
  LOG_TIMING(INFO, "asking for single middle tablet") {
    req.Clear();
    resp.Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(1);
    req.set_start_key(start_key_middle);
    ASSERT_OK(cluster_->mini_master()->master()->catalog_manager()->GetTableLocations(&req, &resp));
    ASSERT_EQ(1, resp.tablet_locations_size()) << "Response: [" << resp.DebugString() << "]";
    ASSERT_EQ(start_key_middle, resp.tablet_locations(0).start_key());
  }
}

} // namespace kudu
