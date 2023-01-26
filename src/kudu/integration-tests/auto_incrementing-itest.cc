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
//
// Integration test for flexible partitioning (eg buckets, range partitioning
// of PK subsets, etc).

#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/env_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduDelete;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::env_util::ListFilesInDir;
using kudu::rpc::RpcController;
using kudu::tserver::NewScanRequestPB;
using kudu::tserver::ScanResponsePB;
using kudu::tserver::ScanRequestPB;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace itest {

static const char* const kTableName = "test-table";
static const int kNumTabletServers = 3;
static const int kNumRows = 200;

class AutoIncrementingItest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
  }

  // Create a table with a single range partition.
  Status CreateTableWithPartition() {
    KuduSchemaBuilder b;
    b.AddColumn("c0")->Type(KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey();
    RETURN_NOT_OK(b.Build(&kudu_schema_));

    int lower_bound = 0;
    int upper_bound = 400;
    unique_ptr<KuduPartialRow> lower(kudu_schema_.NewRow());
    unique_ptr<KuduPartialRow> upper(kudu_schema_.NewRow());

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK(lower->SetInt32("c0", lower_bound));
    RETURN_NOT_OK(upper->SetInt32("c0", upper_bound));

    return(table_creator->table_name(kTableName)
           .schema(&kudu_schema_)
           .set_range_partition_columns({"c0"})
           .add_range_partition(lower.release(), upper.release())
           .num_replicas(3)
           .Create());
  }

  // Insert data into the above created table.
  Status InsertData(int start_c0_value, int end_c0_value, int sleep_millis = 0) {
    shared_ptr<KuduSession> session(client_->NewSession());
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table_));
    for (int i = start_c0_value; i < end_c0_value; i++) {
      unique_ptr<KuduInsert> insert(table_->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      RETURN_NOT_OK(row->SetInt32("c0", i));
      RETURN_NOT_OK(session->Apply(insert.release()));
      SleepFor(MonoDelta::FromMilliseconds(sleep_millis));
    }
    return Status::OK();
  }

  // Delete row based on the row values passed.
  Status DeleteRow(int c0_val, int auto_incrementing_id) {
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(15 * 1000);
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table_));

    unique_ptr<KuduDelete> del(table_->NewDelete());
    auto* row = del->mutable_row();
    RETURN_NOT_OK(row->SetInt32(0, c0_val));
    RETURN_NOT_OK(row->SetInt64(1, auto_incrementing_id));
    RETURN_NOT_OK(session->Apply(del.release()));
    return Status::OK();
  }

  // Return a scan response from the tablet on the given tablet server.
  Status ScanTablet(int ts, const string& tablet_id, vector<string>* results) {
    ScanResponsePB resp;
    RpcController rpc;
    ScanRequestPB req;

    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(tablet_id);
    scan->set_read_mode(READ_AT_SNAPSHOT);
    scan->set_order_mode(ORDERED);

    Schema schema = Schema({ ColumnSchema("c0", INT32),
                             ColumnSchema(Schema::GetAutoIncrementingColumnName(),
                                          INT64, false,false, true),
                           },2);
    RETURN_NOT_OK(SchemaToColumnPBs(schema, scan->mutable_projected_columns()));
    RETURN_NOT_OK(cluster_->tserver_proxy(ts)->Scan(req, &resp, &rpc));
    tserver::TabletServerTestBase::StringifyRowsFromResponse(schema, rpc, &resp, results);
    return Status::OK();
  }

  Status TestSetup(string* tablet_uuid) {
    cluster::ExternalMiniClusterOptions opts;
    // We ensure quick rolling and GC'ing of the wal segments and quick flushes to data.
    // This is to make sure that when we are bootstrapping, we are not looking at the
    // prior segments where the auto incrementing counter is present (INSERT ops).
    opts.extra_tserver_flags = {
        "--log_segment_size_bytes_for_tests=100",
        "--log_max_segments_to_retain=1",
        "--maintenance_manager_polling_interval_ms=10",
        "--maintenance_manager_num_threads=4",
        "--flush_threshold_secs=1",
        "--flush_threshold_mb=0",
        "--log_compression_codec=no_compression",
    };
    opts.num_tablet_servers = kNumTabletServers;
    cluster_.reset(new cluster::ExternalMiniCluster(std::move(opts)));
    RETURN_NOT_OK(cluster_->Start());
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client_));

    // Create a table and insert data.
    RETURN_NOT_OK(CreateTableWithPartition());
    RETURN_NOT_OK(InsertData(0, kNumRows));

    // Get the tablet UUID.
    rpc::RpcController rpc;
    tserver::ListTabletsRequestPB req;
    tserver::ListTabletsResponsePB resp;
    RETURN_NOT_OK(cluster_->tserver_proxy(0)->ListTablets(req, &resp, &rpc));
    CHECK(resp.status_and_schema_size() == 1);
    *tablet_uuid = resp.status_and_schema(0).tablet_status().tablet_id();
    return Status::OK();
  }

 protected:
  unique_ptr<cluster::ExternalMiniCluster> cluster_;
  KuduSchema kudu_schema_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> table_;
};

TEST_F(AutoIncrementingItest, BasicInserts) {
  cluster::ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTabletServers;
  cluster_.reset(new cluster::ExternalMiniCluster(std::move(opts)));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

  // Create a table and insert data.
  ASSERT_OK(CreateTableWithPartition());
  ASSERT_OK(InsertData(0, kNumRows));

  // Scan all the tablet replicas and validate the results.
  for (int j = 0; j < kNumTabletServers; j++) {
    auto server = cluster_->tserver_proxy(j);
    rpc::RpcController rpc;
    tserver::ListTabletsRequestPB req;
    tserver::ListTabletsResponsePB resp;
    server->ListTablets(req, &resp, &rpc);
    ASSERT_EQ(1, resp.status_and_schema_size());
    vector<string> results;
    ASSERT_OK(ScanTablet(j, resp.status_and_schema(0).tablet_status().tablet_id(), &results));
    for (int i = 0; i < kNumRows; i++) {
      ASSERT_EQ(Substitute("(int32 c0=$0, int64 $1=$2)", i,
                           Schema::GetAutoIncrementingColumnName(), i + 1), results[i]);
    }
  }
}

TEST_F(AutoIncrementingItest, BootstrapWithNoWals) {
  string tablet_uuid;
  TestSetup(&tablet_uuid);

  // Delete the first 100 rows to make sure at least latest 2 wals
  // are not insert operations.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(DeleteRow(i, i + 1));
  }
  vector<string> wal_dir_files;

  // Ensure the wals are GC'd in all the tablet servers.
  int i = 0;
  while (i < kNumTabletServers) {
    ASSERT_OK(ListFilesInDir(env_,
                             JoinPathSegments(cluster_->tablet_server(0)->wal_dir(),
                                              strings::Substitute("wals/$0", tablet_uuid)),
                             &wal_dir_files));
    ASSERT_FALSE(wal_dir_files.empty());
    if (wal_dir_files.size() > 3) {
      // We are still yet to GC the wal segments, check back the server after 50ms.
      SleepFor(MonoDelta::FromMilliseconds(50));
      continue;
    }
    i++;
  }

  // Restart the cluster.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());

  // Insert new data and validate the auto incrementing column values.
  ASSERT_OK(InsertData(kNumRows, kNumRows * 2));
  for (int j = 0; j < kNumTabletServers; j++) {
    vector<string> results;
    ASSERT_OK(ScanTablet(j, tablet_uuid, &results));
    LOG(INFO) << "Results size: " << results.size();
    for (int i = 0; i < results.size(); i++) {
      ASSERT_EQ(Substitute("(int32 c0=$0, int64 $1=$2)", i + 100,
                           Schema::GetAutoIncrementingColumnName(), i + 100 + 1), results[i]);

    }
  }
}


TEST_F(AutoIncrementingItest, BootstrapNoWalsNoData) {
  string tablet_uuid;
  TestSetup(&tablet_uuid);

  // Delete all the rows.
  for (int i = 0; i < kNumRows; i++) {
    ASSERT_OK(DeleteRow(i, i + 1));
  }

  // Ensure the wals are GC'd in all the tablet servers and there are no
  // rows present.
  vector<string> wal_dir_files;
  int i = 0;
  while (i < kNumTabletServers) {
    ASSERT_OK(ListFilesInDir(env_,
                             JoinPathSegments(cluster_->tablet_server(0)->wal_dir(),
                                              strings::Substitute("wals/$0", tablet_uuid)),
                             &wal_dir_files));
    ASSERT_FALSE(wal_dir_files.empty());
    vector<string> results;
    ASSERT_OK(ScanTablet(i, tablet_uuid, &results));
    if (wal_dir_files.size() > 3 || !results.empty()) {
      // We are still yet to GC the wal segments or flush the deletes,
      // check back the server after 50ms.
      SleepFor(MonoDelta::FromMilliseconds(50));
      continue;
    }
    i++;
  }

  // Restart the cluster.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());

  // There could still be a WAL entry with a DELETE OP which is not persisted to the data.
  // directory. This might cause the auto incrementing counter to be not set to 0 when fetching
  // from the data directories. We restart servers to force elections which in turn
  // writes more WAL entries which causes these flushes.
  for (int i = 0; i < kNumTabletServers; i++) {
    cluster_->tablet_server(i)->Shutdown();
    ASSERT_OK(cluster_->tablet_server(i)->Restart());
  }

  // Insert new data and verify auto_incrementing_id starts from 1.
  ASSERT_OK(InsertData(kNumRows, kNumRows * 2));
  for (int j = 0; j < kNumTabletServers; j++) {
    vector<string> results;
    ASSERT_OK(ScanTablet(j, tablet_uuid, &results));
    ASSERT_EQ(200, results.size());
    for (int i = 0; i < results.size(); i++) {
      ASSERT_EQ(Substitute("(int32 c0=$0, int64 $1=$2)", i + kNumRows,
                           Schema::GetAutoIncrementingColumnName(), i + 1), results[i]);
    }
  }
}

TEST_F(AutoIncrementingItest, BootstrapWalsDiverge) {
  cluster::ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTabletServers;
  cluster_.reset(new cluster::ExternalMiniCluster(std::move(opts)));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

  // Create a table.
  ASSERT_OK(CreateTableWithPartition());

  // Get the tablet server hosting the leader replica.
  int leader_ts_index = -1;
  string tablet_uuid;
  for (int i = 0; i < kNumTabletServers; i++) {
    rpc::RpcController rpc;
    tserver::ListTabletsRequestPB req;
    tserver::ListTabletsResponsePB resp;
    cluster_->tserver_proxy(i)->ListTablets(req, &resp, &rpc);
    ASSERT_EQ(1, resp.status_and_schema_size());
    if (tablet_uuid.empty()) {
      tablet_uuid = resp.status_and_schema(0).tablet_status().tablet_id();
    }
    if (resp.status_and_schema(0).role() == consensus::RaftPeerPB::LEADER) {
      leader_ts_index = i;
      break;
    }
  }
  ASSERT_NE(-1, leader_ts_index);

  // Shutdown the tablet servers in a separate thread but sleep for 100ms
  // to ensure there is data being written to the server.
  std::thread shutdown_thread([&]() {
    // Ensure at least a couple of rows are attempted to be
    // written before shutting down.
    SleepFor(MonoDelta::FromMilliseconds(100));
    // Shutdown the tablet servers hosting followers while the data is still being written.
    for (int i = 0; i < kNumTabletServers; i++) {
      if (i != leader_ts_index) {
        cluster_->tablet_server(i)->Shutdown();
      }
    }
  });
  // Sleep between each write for less than Raft heartbeat interval.
  // This is to ensure we won't write all the data before the servers are
  // shutdown and we would have at least a few WAL non-committed replicate ops.
  Status s = InsertData(0, kNumRows, 5);
  shutdown_thread.join();
  ASSERT_TRUE(!s.ok());

  // Write 200 rows at the rate of 1 row every 5ms which are sent to the leader replica. After
  // 100ms of starting to insert data, we shutdown the followers and at this point the write
  // request is expected to 900ms more. Since the leader would mark the followers as
  // unavailable after 3 lost hearbeats (1500ms), there will for sure be a situation where the
  // leader has sent a write op and hasn't gotten the response from majority-1 number of
  // followers. In this case the write op is not marked committed in the leader replica. All
  // the writes including this are considered failed.
  cluster_->tablet_server(leader_ts_index)->Shutdown();
  for (int i = 0; i < kNumTabletServers; i++) {
    if (i != leader_ts_index) {
      ASSERT_OK(cluster_->tablet_server(i)->Restart());
    }
  }

  // Insert more data.
  ASSERT_OK(InsertData(kNumRows, kNumRows * 2));

  // Start back the tablet server and ensure all the replicas contain the same data.
  // We scan the replica on leader_ts_index last to give it reasonable time to start
  // serving scans.
  ASSERT_OK(cluster_->tablet_server(leader_ts_index)->Restart());
  int j = (leader_ts_index + 1) % kNumTabletServers;
  vector<vector<string>> results;
  for (int i = 0; i < kNumTabletServers; i++) {
    vector<string> result;
    ASSERT_OK(ScanTablet(j, tablet_uuid, &result));
    results.emplace_back(result);
    j = ++j % kNumTabletServers;
  }
  ASSERT_EQ(kNumTabletServers, results.size());
  for (int i = 0; i < kNumTabletServers - 1; i++) {
    ASSERT_EQ(results[i].size(), results[i + 1].size());
    for (int k = 0; k < results[i].size(); k++) {
      ASSERT_EQ(results[i][k], results[i + 1][k]);
    }
  }
}
} // namespace itest
} // namespace kudu
