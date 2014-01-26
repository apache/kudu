// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "client/client.h"
#include "common/schema.h"
#include "common/wire_protocol.h"
#include "integration-tests/mini_cluster.h"
#include "master/master.proxy.h"
#include "master/mini_master.h"
#include "master/master-test-util.h"
#include "rpc/messenger.h"
#include "util/test_util.h"

using std::tr1::shared_ptr;
using kudu::client::CreateTableOptions;
using kudu::client::KuduClient;
using kudu::client::KuduClientOptions;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::master::MasterServiceProxy;

DECLARE_int32(heartbeat_interval_ms);
DECLARE_bool(log_preallocate_segments);

namespace kudu {

const char* kTableName = "test_table";
const int kNumTablets = 100;

class CreateTableStressTest : public KuduTest {
 public:
  CreateTableStressTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("v1", UINT64))
              (ColumnSchema("v2", STRING)),
              1) {
  }

  virtual void SetUp() {
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
    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 1));
    ASSERT_STATUS_OK(cluster_->Start());

    ASSERT_STATUS_OK(MessengerBuilder("Client").Build(&msgr_));
    KuduClientOptions opts;
    opts.master_server_addr = cluster_->mini_master()->bound_rpc_addr().ToString();
    opts.messenger = msgr_;
    ASSERT_STATUS_OK(KuduClient::Create(opts, &client_));
  }

  virtual void TearDown() {
    ASSERT_STATUS_OK(cluster_->Shutdown());
  }

  void CreateBigTable(const string& table_name);

 protected:
  shared_ptr<KuduClient> client_;
  shared_ptr<Messenger> msgr_;
  gscoped_ptr<MiniCluster> cluster_;
  Schema schema_;
};

void CreateTableStressTest::CreateBigTable(const string& table_name) {
  vector<string> keys;
  int num_splits = kNumTablets - 1; // 1 split = 2 tablets
  for (int i = num_splits; i >= 0; i--) {
    keys.push_back(StringPrintf("k_%05d", i));
  }

  ASSERT_STATUS_OK(client_->CreateTable(
                     table_name, schema_,
                     kudu::client::CreateTableOptions().WithSplitKeys(keys)));
}


TEST_F(CreateTableStressTest, CreateBigTable) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping slow test";
    return;
  }
  string table_name = "test_table";
  ASSERT_NO_FATAL_FAILURE(CreateBigTable(table_name));
  master::GetTableLocationsResponsePB resp;
  ASSERT_STATUS_OK(WaitForRunningTabletCount(cluster_->mini_master(), table_name,
                                             kNumTablets, &resp));
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
  ASSERT_NO_FATAL_FAILURE(CreateBigTable(table_name));

  for (int i = 0; i < 3; i++) {
    usleep(500);
    ASSERT_STATUS_OK(cluster_->mini_master()->Restart());
  }

  master::GetTableLocationsResponsePB resp;
  Status s = WaitForRunningTabletCount(cluster_->mini_master(), table_name,
                                       kNumTablets, &resp);
  if (!s.ok()) {
    cluster_->mini_master()->master()->catalog_manager()->DumpState(&std::cerr);
    CHECK_OK(s);
  }
}

} // namespace kudu
