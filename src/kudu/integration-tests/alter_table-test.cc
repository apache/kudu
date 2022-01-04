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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_histogram(log_gc_duration);
METRIC_DECLARE_entity(tablet);

DECLARE_bool(enable_maintenance_manager);
DECLARE_bool(log_inject_latency);
DECLARE_bool(scanner_allow_snapshot_scans_with_logical_timestamps);
DECLARE_bool(use_hybrid_clock);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(log_inject_latency_ms_mean);
DECLARE_int32(log_segment_size_mb);
DECLARE_int32(tablet_copy_download_file_inject_latency_ms);

using kudu::client::CountTableRows;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDelete;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduUpdate;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::itest::TabletServerMap;
using kudu::itest::TServerDetails;
using kudu::master::AlterTableRequestPB;
using kudu::master::AlterTableResponsePB;
using kudu::tablet::TabletReplica;
using kudu::tablet::Tablet;
using std::atomic;
using std::map;
using std::pair;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class AlterTableTest : public KuduTest {
 public:
  AlterTableTest()
      : stop_threads_(false),
        next_idx_(0),
        update_ops_cnt_(0) {

    KuduSchemaBuilder b;
    b.AddColumn("c0")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("c1")->Type(KuduColumnSchema::INT32)->NotNull();
    CHECK_OK(b.Build(&schema_));

    FLAGS_use_hybrid_clock = false;
    FLAGS_scanner_allow_snapshot_scans_with_logical_timestamps = true;
  }

  void SetUp() override {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    KuduTest::SetUp();

    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_tservers();
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());

    CHECK_OK(KuduClientBuilder()
        .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr_str())
        .default_admin_operation_timeout(MonoDelta::FromSeconds(60))
        .Build(&client_));

    // Add a table, make sure it reports itself.
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    CHECK_OK(table_creator->table_name(kTableName)
             .schema(&schema_)
             .set_range_partition_columns({ "c0" })
             .num_replicas(num_replicas())
             .Create());

    if (num_replicas() > 0) {
      tablet_replica_ = LookupLeaderTabletReplica();
      CHECK(tablet_replica_);
    }
    LOG(INFO) << "Tablet successfully located";
  }

  void TearDown() override {
    tablet_replica_.reset();
    cluster_->Shutdown();
  }

  scoped_refptr<TabletReplica> LookupLeaderTabletReplica() {
    static const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
    for (int i = 0; i < num_tservers(); ++i) {
      vector<scoped_refptr<TabletReplica>> replicas;
      cluster_->mini_tablet_server(i)->server()->tablet_manager()->GetTabletReplicas(&replicas);
      if (replicas.empty()) {
        continue;
      }

      TabletServerMap tablet_servers;
      ValueDeleter deleter(&tablet_servers);
      CHECK_OK(CreateTabletServerMap(
          cluster_->master_proxy(), cluster_->messenger(), &tablet_servers));

      TServerDetails* leader = nullptr;
      std::string tablet_id = replicas[0]->tablet_id();
      CHECK_OK(FindTabletLeader(tablet_servers, tablet_id, kTimeout, &leader));
      replicas.clear();
      cluster_->mini_tablet_server_by_uuid(leader->uuid())->
          server()->tablet_manager()->GetTabletReplicas(&replicas);
      for (const auto& replica : replicas) {
        if (replica->tablet_id() == tablet_id) {
          return replica;
        }
      }
      CHECK(false) << "leader tablet replica must has been found in prev steps.";
    }
    return nullptr;
  }

  void ShutdownTS() {
    // Drop the tablet_replica_ reference since the tablet replica becomes invalid once
    // we shut down the server. Additionally, if we hold onto the reference,
    // we'll end up calling the destructor from the test code instead of the
    // normal location, which can cause crashes, etc.
    tablet_replica_.reset();
    if (cluster_->mini_tablet_server(0)->server() != nullptr) {
      cluster_->mini_tablet_server(0)->Shutdown();
    }
  }

  void RestartTabletServer(int idx = 0) {
    tablet_replica_.reset();
    if (cluster_->mini_tablet_server(idx)->server()) {
      cluster_->mini_tablet_server(idx)->Shutdown();
      ASSERT_OK(cluster_->mini_tablet_server(idx)->Restart());
    } else {
      ASSERT_OK(cluster_->mini_tablet_server(idx)->Start());
    }

    ASSERT_OK(cluster_->mini_tablet_server(idx)->WaitStarted());
    if (idx == 0) {
      tablet_replica_ = LookupLeaderTabletReplica();
    }
  }

  Status WaitAlterTableCompletion(const std::string& table_name, int attempts) {
    int wait_time = 1000;
    for (int i = 0; i < attempts; ++i) {
      bool in_progress;
      RETURN_NOT_OK(client_->IsAlterTableInProgress(table_name, &in_progress));
      if (!in_progress) {
        return Status::OK();
      }

      SleepFor(MonoDelta::FromMicroseconds(wait_time));
      wait_time = std::min(wait_time * 5 / 4, 1000000);
    }

    return Status::TimedOut("AlterTable not completed within the timeout");
  }

  Status AddNewI32Column(const string& table_name,
                         const string& column_name,
                         int32_t default_value) {
    return AddNewI32Column(table_name, column_name, default_value,
                           MonoDelta::FromSeconds(60));
  }

  Status AddNewI32Column(const string& table_name,
                         const string& column_name,
                         int32_t default_value,
                         const MonoDelta& timeout) {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_name));
    table_alterer->AddColumn(column_name)->Type(KuduColumnSchema::INT32)->
        NotNull()->Default(KuduValue::FromInt(default_value));
    return table_alterer->timeout(timeout)->Alter();
  }

  Status SetReplicationFactor(const string& table_name,
                              int32_t replication_factor) {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_name));
    table_alterer->data_->set_replication_factor_to_ = replication_factor;
    return table_alterer->timeout(MonoDelta::FromSeconds(60))->Alter();
  }

  enum class VerifyRowCount {
    kEnable = 0,
    kDisable
  };
  void VerifyTabletReplicaCount(int32_t replication_factor, VerifyRowCount verify_row_count) {
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(replication_factor, tablet_replica_->consensus()->CommittedConfig().peers().size());

      scoped_refptr<TabletReplica> first_node_replica;
      uint64_t first_count = 0;
      int actual_replica_count = 0;
      for (int i = 0; i < num_tservers(); i++) {
        vector<scoped_refptr<TabletReplica>> cur_node_replicas;
        cluster_->mini_tablet_server(i)->server()->
          tablet_manager()->GetTabletReplicas(&cur_node_replicas);
        if (cur_node_replicas.empty()) continue;
        ASSERT_EQ(1, cur_node_replicas.size());
        const auto& cur_node_replica = cur_node_replicas[0];
        if (!cur_node_replica->tablet()) continue;

        ASSERT_OK(cur_node_replica->CheckRunning());
        if (!first_node_replica) {
          first_node_replica = cur_node_replica;
          if (verify_row_count == VerifyRowCount::kEnable) {
            ASSERT_OK(first_node_replica->CountLiveRows(&first_count));
          }
        } else {
          ASSERT_EQ(first_node_replica->tablet()->tablet_id(),
            cur_node_replica->tablet()->tablet_id());
          ASSERT_TRUE(first_node_replica->tablet()->schema()->Equals(
            *(cur_node_replica->tablet()->schema()).get()));
          if (verify_row_count == VerifyRowCount::kEnable) {
            uint64_t cur_count = 0;
            ASSERT_OK(cur_node_replica->CountLiveRows(&cur_count));
            ASSERT_EQ(first_count, cur_count);
          }
        }
        ++actual_replica_count;
      }
      ASSERT_EQ(replication_factor, actual_replica_count);
    });
  }

  enum VerifyPattern {
    C1_MATCHES_INDEX,
    C1_IS_DEADBEEF,
    C1_DOESNT_EXIST
  };

  void VerifyRows(int start_row, int num_rows, VerifyPattern pattern);

  void InsertRows(int start_row, int num_rows);
  void DeleteRow(int row_key);

  Status InsertRowsSequential(const string& table_name, int start_row, int num_rows);

  void UpdateRow(int32_t row_key, const map<string, int32_t>& updates);

  void ScanToStrings(vector<string>* rows);

  void InserterThread();
  void UpdaterThread();
  void ScannerThread();

  Status CreateSplitTable(const string& table_name) {
    vector<const KuduPartialRow*> split_rows;
    for (int32_t i = 1; i < 10; i++) {
      KuduPartialRow* row = schema_.NewRow();
      CHECK_OK(row->SetInt32(0, i * 100));
      split_rows.push_back(row);
    }
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    return table_creator->table_name(table_name)
        .schema(&schema_)
        .set_range_partition_columns({ "c0" })
        .num_replicas(num_replicas())
        .split_rows(split_rows)
        .Create();
#pragma GCC diagnostic pop
  }

  Status CreateTable(const string& table_name,
                     const KuduSchema& schema,
                     const vector<string>& range_partition_columns,
                     vector<unique_ptr<KuduPartialRow>> split_rows,
                     vector<pair<unique_ptr<KuduPartialRow>, unique_ptr<KuduPartialRow>>> bounds);

  void CheckMaintenancePriority(int32_t expect_priority) {
    for (auto op : tablet_replica_->maintenance_ops_) {
      ASSERT_EQ(op->priority(), expect_priority);
    }
    for (auto op : tablet_replica_->tablet()->maintenance_ops_) {
      ASSERT_EQ(op->priority(), expect_priority);
    }
  }

 protected:
  virtual int num_replicas() const { return 1; }
  virtual int num_tservers() const { return 1; }

  static const char* const kTableName;

  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;

  KuduSchema schema_;

  scoped_refptr<TabletReplica> tablet_replica_;

  atomic<bool> stop_threads_;

  // The index of the next row to be inserted by the InserterThread.
  // The UpdaterThread uses this to figure out which rows can be safely updated.
  atomic<uint32_t> next_idx_;

  // Number of update operations issues by the UpdaterThread so far.
  atomic<uint32_t> update_ops_cnt_;
};

// Subclass which creates three servers and a replicated cluster.
class ReplicatedAlterTableTest : public AlterTableTest {
 protected:
  int num_replicas() const override { return 3; }
  int num_tservers() const override { return num_replicas() + 1; }
};

const char* const AlterTableTest::kTableName = "fake-table";

// Simple test to verify that the "alter table" command sent and executed
// on the TS handling the tablet of the altered table.
// TODO: create and verify multiple tablets when the client will support that.
TEST_F(AlterTableTest, TestTabletReports) {
  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());
  ASSERT_OK(AddNewI32Column(kTableName, "new-i32", 0));
  ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());
}

// Verify that adding an existing column will return an "already present" error
TEST_F(AlterTableTest, TestAddExistingColumn) {
  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());

  {
    Status s = AddNewI32Column(kTableName, "c1", 0);
    ASSERT_TRUE(s.IsAlreadyPresent());
    ASSERT_STR_CONTAINS(s.ToString(), "The column already exists: c1");
  }

  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());
}

// Verify that adding a NOT NULL column without defaults will return an error.
//
// This doesn't use the KuduClient because it's trying to make an invalid request.
// Our APIs for the client are designed such that it's impossible to send such
// a request.
TEST_F(AlterTableTest, TestAddNotNullableColumnWithoutDefaults) {
  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());

  {
    AlterTableRequestPB req;
    AlterTableResponsePB resp;

    req.mutable_table()->set_table_name(kTableName);
    AlterTableRequestPB::Step *step = req.add_alter_schema_steps();
    step->set_type(AlterTableRequestPB::ADD_COLUMN);
    ColumnSchemaToPB(ColumnSchema("c2", INT32),
                     step->mutable_add_column()->mutable_schema());

    master::CatalogManager* catalog =
        cluster_->mini_master()->master()->catalog_manager();
    master::CatalogManager::ScopedLeaderSharedLock l(catalog);
    ASSERT_OK(l.first_failed_status());
    Status s = catalog->AlterTableRpc(req, &resp, /*rpc=*/nullptr);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "column `c2`: NOT NULL columns must have a default");
  }

  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());
}

// Adding a nullable column with no default value should be equivalent
// to a NULL default.
TEST_F(AlterTableTest, TestAddNullableColumnWithoutDefault) {
  InsertRows(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AddColumn("new")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(table_alterer->Alter());
  }

  InsertRows(1, 1);

  vector<string> rows;
  ScanToStrings(&rows);
  ASSERT_EQ(2, rows.size());
  EXPECT_EQ("(int32 c0=0, int32 c1=0, int32 new=NULL)", rows[0]);
  EXPECT_EQ("(int32 c0=16777216, int32 c1=1, int32 new=NULL)", rows[1]);
}

// Rename a primary key column
TEST_F(AlterTableTest, TestRenamePrimaryKeyColumn) {
  InsertRows(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("c0")->RenameTo("primaryKeyRenamed");
    table_alterer->AlterColumn("c1")->RenameTo("secondColumn");
    ASSERT_OK(table_alterer->Alter());
  }

  InsertRows(1, 1);

  vector<string> rows;
  ScanToStrings(&rows);
  ASSERT_EQ(2, rows.size());
  EXPECT_EQ("(int32 primaryKeyRenamed=0, int32 secondColumn=0)", rows[0]);
  EXPECT_EQ("(int32 primaryKeyRenamed=16777216, int32 secondColumn=1)", rows[1]);

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("primaryKeyRenamed")->RenameTo("pk");
    table_alterer->AlterColumn("secondColumn")->RenameTo("sc");
    ASSERT_OK(table_alterer->Alter());
  }

  InsertRows(2, 1);

  rows.clear();
  ScanToStrings(&rows);
  ASSERT_EQ(3, rows.size());
  EXPECT_EQ("(int32 pk=0, int32 sc=0)", rows[0]);
  EXPECT_EQ("(int32 pk=16777216, int32 sc=1)", rows[1]);
  EXPECT_EQ("(int32 pk=33554432, int32 sc=2)", rows[2]);
}

// Test altering a column to add a default value, change a default value, and
// remove a default value.
TEST_F(AlterTableTest, TestAddChangeRemoveColumnDefaultValue) {
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AddColumn("newInt32")->Type(KuduColumnSchema::INT32);
    table_alterer->AddColumn("newString")->Type(KuduColumnSchema::STRING);
    ASSERT_OK(table_alterer->Alter());
  }

  InsertRows(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("newInt32")->Default(KuduValue::FromInt(12345));
    table_alterer->AlterColumn("newString")->Default(KuduValue::CopyString("taco"));
    ASSERT_OK(table_alterer->Alter());
  }

  InsertRows(1, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  vector<string> rows;
  ScanToStrings(&rows);
  ASSERT_EQ(2, rows.size());
  EXPECT_EQ("(int32 c0=0, int32 c1=0, int32 newInt32=NULL, string newString=NULL)", rows[0]);
  EXPECT_EQ("(int32 c0=16777216, int32 c1=1, int32 newInt32=12345, string newString=\"taco\")",
            rows[1]);

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("newInt32")->Default(KuduValue::FromInt(54321));
    table_alterer->AlterColumn("newString")->Default(KuduValue::CopyString("ocat"));
    ASSERT_OK(table_alterer->Alter());
  }

  InsertRows(2, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  ScanToStrings(&rows);
  ASSERT_EQ(3, rows.size());
  EXPECT_EQ("(int32 c0=0, int32 c1=0, int32 newInt32=NULL, string newString=NULL)", rows[0]);
  EXPECT_EQ("(int32 c0=16777216, int32 c1=1, int32 newInt32=12345, string newString=\"taco\")",
            rows[1]);
  EXPECT_EQ("(int32 c0=33554432, int32 c1=2, int32 newInt32=54321, string newString=\"ocat\")",
            rows[2]);

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("newInt32")->RemoveDefault();
    table_alterer->AlterColumn("newString")->RemoveDefault();
    // Add an extra rename step
    table_alterer->AlterColumn("newString")->RenameTo("newNewString");
    ASSERT_OK(table_alterer->Alter());
  }

  InsertRows(3, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  ScanToStrings(&rows);
  ASSERT_EQ(4, rows.size());
  EXPECT_EQ("(int32 c0=0, int32 c1=0, int32 newInt32=NULL, string newNewString=NULL)", rows[0]);
  EXPECT_EQ("(int32 c0=16777216, int32 c1=1, int32 newInt32=12345, string newNewString=\"taco\")",
            rows[1]);
  EXPECT_EQ("(int32 c0=33554432, int32 c1=2, int32 newInt32=54321, string newNewString=\"ocat\")",
            rows[2]);
  EXPECT_EQ("(int32 c0=50331648, int32 c1=3, int32 newInt32=NULL, string newNewString=NULL)",
            rows[3]);

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->DropColumn("newInt32");
    table_alterer->DropColumn("newNewString");
    ASSERT_OK(table_alterer->Alter());
  }
}

// Test for a bug seen when an alter table creates an empty RLE block and the
// block is subsequently scanned.
TEST_F(AlterTableTest, TestAlterEmptyRLEBlock) {
  // Add an RLE-encoded column
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AddColumn("rle")->Type(KuduColumnSchema::INT32)
        ->Encoding(client::KuduColumnStorageAttributes::RLE);
    ASSERT_OK(table_alterer->Alter());
  }

  // Insert some rows
  InsertRows(0, 3);

  // Now alter the column
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("rle")->RenameTo("new_rle");
    ASSERT_OK(table_alterer->Alter());
  }

  // Now scan the table, which would trigger a CHECK failure
  // on trying to seek to position 0 in an empty RLE block
  {
    vector<string> rows;
    ScanToStrings(&rows);
    EXPECT_EQ("(int32 c0=0, int32 c1=0, int32 new_rle=NULL)", rows[0]);
    EXPECT_EQ("(int32 c0=16777216, int32 c1=1, int32 new_rle=NULL)", rows[1]);
    EXPECT_EQ("(int32 c0=33554432, int32 c1=2, int32 new_rle=NULL)", rows[2]);
  }
}

// Verify that, if a tablet server is down when an alter command is issued,
// it will eventually receive the command when it restarts.
TEST_F(AlterTableTest, TestAlterOnTSRestart) {
  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());

  ShutdownTS();

  // Send the Alter request
  {
    Status s = AddNewI32Column(kTableName, "new-32", 10,
                               MonoDelta::FromMilliseconds(500));
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Verify that the Schema is the old one
  KuduSchema schema;
  bool alter_in_progress = false;
  ASSERT_OK(client_->GetTableSchema(kTableName, &schema));
  ASSERT_EQ(schema_, schema);
  ASSERT_OK(client_->IsAlterTableInProgress(kTableName, &alter_in_progress));
  ASSERT_TRUE(alter_in_progress);

  // Restart the TS and wait for the new schema
  RestartTabletServer();
  ASSERT_OK(WaitAlterTableCompletion(kTableName, 50));
  ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());
}

// Verify that nothing is left behind on cluster shutdown with pending async tasks
TEST_F(AlterTableTest, TestShutdownWithPendingTasks) {
  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());

  ShutdownTS();

  // Send the Alter request
  {
    Status s = AddNewI32Column(kTableName, "new-i32", 10,
                               MonoDelta::FromMilliseconds(500));
    ASSERT_TRUE(s.IsTimedOut());
  }
}

// Verify that the new schema is applied/reported even when
// the TS is going down with the alter operation in progress.
// On TS restart the master should:
//  - get the new schema state, and mark the alter as complete
//  - get the old schema state, and ask the TS again to perform the alter.
TEST_F(AlterTableTest, TestRestartTSDuringAlter) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());

  Status s = AddNewI32Column(kTableName, "new-i32", 10,
                             MonoDelta::FromMilliseconds(1));
  ASSERT_TRUE(s.IsTimedOut());

  // Restart the TS while alter is running
  for (int i = 0; i < 3; i++) {
    SleepFor(MonoDelta::FromMicroseconds(500));
    RestartTabletServer();
  }

  // Wait for the new schema
  ASSERT_OK(WaitAlterTableCompletion(kTableName, 50));
  ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());
}

TEST_F(AlterTableTest, TestGetSchemaAfterAlterTable) {
  ASSERT_OK(AddNewI32Column(kTableName, "new-i32", 10));

  KuduSchema s;
  ASSERT_OK(client_->GetTableSchema(kTableName, &s));
}

void AlterTableTest::InsertRows(int start_row, int num_rows) {
  shared_ptr<KuduSession> session = client_->NewSession();
  shared_ptr<KuduTable> table;
  CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  session->SetTimeoutMillis(15 * 1000);
  CHECK_OK(client_->OpenTable(kTableName, &table));

  // Insert a bunch of rows with the current schema
  for (int i = start_row; i < start_row + num_rows; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    // Endian-swap the key so that we spew inserts randomly
    // instead of just a sequential write pattern. This way
    // compactions may actually be triggered.
    int32_t key = bswap_32(i);
    CHECK_OK(insert->mutable_row()->SetInt32(0, key));

    if (table->schema().num_columns() > 1) {
      CHECK_OK(insert->mutable_row()->SetInt32(1, i));
    }

    CHECK_OK(session->Apply(insert.release()));

    if (i % 50 == 0) {
      FlushSessionOrDie(session);
    }
  }

  FlushSessionOrDie(session);
}

void AlterTableTest::DeleteRow(int row_key) {
  shared_ptr<KuduSession> session = client_->NewSession();
  shared_ptr<KuduTable> table;
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(15 * 1000);
  CHECK_OK(client_->OpenTable(kTableName, &table));

  unique_ptr<KuduDelete> del(table->NewDelete());
  CHECK_OK(del->mutable_row()->SetInt32(0, bswap_32(row_key)));
  CHECK_OK(session->Apply(del.release()));
  FlushSessionOrDie(session);
}

Status AlterTableTest::InsertRowsSequential(const string& table_name, int start_row, int num_rows) {
  shared_ptr<KuduSession> session = client_->NewSession();
  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  session->SetTimeoutMillis(15 * 1000);
  RETURN_NOT_OK(client_->OpenTable(table_name, &table));

  // Insert a bunch of rows with the current schema
  for (int i = start_row; i < start_row + num_rows; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    RETURN_NOT_OK(insert->mutable_row()->SetInt32(0, i));
    if (table->schema().num_columns() > 1) {
      RETURN_NOT_OK(insert->mutable_row()->SetInt32(1, i));
    }
    RETURN_NOT_OK(session->Apply(insert.release()));
  }
  Status s = session->Flush();
  if (!s.ok()) {
    vector<KuduError*> errors;
    ElementDeleter d(&errors);
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    for (auto* error : errors) {
      LOG(WARNING) << error->status().ToString();
    }
  }
  return s;
}

void AlterTableTest::UpdateRow(int32_t row_key,
                               const map<string, int32_t>& updates) {
  shared_ptr<KuduSession> session = client_->NewSession();
  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(kTableName, &table));
  CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  session->SetTimeoutMillis(15 * 1000);
  unique_ptr<KuduUpdate> update(table->NewUpdate());
  int32_t key = bswap_32(row_key); // endian swap to match 'InsertRows'
  CHECK_OK(update->mutable_row()->SetInt32(0, key));
  typedef map<string, int32_t>::value_type entry;
  for (const entry& e : updates) {
    CHECK_OK(update->mutable_row()->SetInt32(e.first, e.second));
  }
  CHECK_OK(session->Apply(update.release()));
  FlushSessionOrDie(session);
}

void AlterTableTest::ScanToStrings(vector<string>* rows) {
  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(kTableName, &table));
  CHECK_OK(ScanTableToStrings(table.get(), rows));
  std::sort(rows->begin(), rows->end());
}

// Verify that the 'num_rows' starting with 'start_row' fit the given pattern.
// Note that the 'start_row' here is not a row key, but the pre-transformation row
// key (InsertRows swaps endianness so that we random-write instead of sequential-write)
void AlterTableTest::VerifyRows(int start_row, int num_rows, VerifyPattern pattern) {
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  KuduScanner scanner(table.get());
  // TODO(KUDU-791): we should probably be able to use a snapshot read here,
  // but alter-table isn't all the way tied into the consistency-related code.
  ASSERT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
  ASSERT_OK(scanner.Open());

  int verified = 0;
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));

    for (const KuduScanBatch::RowPtr row : batch) {
      int32_t key = 0;
      ASSERT_OK(row.GetInt32(0, &key));
      int32_t row_idx = bswap_32(key);
      if (row_idx < start_row || row_idx >= start_row + num_rows) {
        // Outside the range we're verifying
        continue;
      }
      verified++;

      if (pattern == C1_DOESNT_EXIST) {
        continue;
      }

      int32_t c1 = 0;
      ASSERT_OK(row.GetInt32(1, &c1));

      switch (pattern) {
        case C1_MATCHES_INDEX:
          ASSERT_EQ(row_idx, c1);
          break;
        case C1_IS_DEADBEEF:
          ASSERT_EQ(0xdeadbeef, c1);
          break;
        default:
          LOG(FATAL);
      }
    }
  }
  ASSERT_EQ(verified, num_rows);
}

Status AlterTableTest::CreateTable(const string& table_name,
                                   const KuduSchema& schema,
                                   const vector<string>& range_partition_columns,
                                   vector<unique_ptr<KuduPartialRow>> split_rows,
                                   vector<pair<unique_ptr<KuduPartialRow>,
                                               unique_ptr<KuduPartialRow>>> bounds) {
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(table_name)
                .schema(&schema)
                .set_range_partition_columns(range_partition_columns)
                .num_replicas(1);

  for (auto& split_row : split_rows) {
    table_creator->add_range_partition_split(split_row.release());
  }

  for (auto& bound : bounds) {
    table_creator->add_range_partition(bound.first.release(), bound.second.release());
  }

  return table_creator->Create();
}

// Test inserting/updating some data, dropping a column, and adding a new one
// with the same name. Data should not "reappear" from the old column.
//
// This is a regression test for KUDU-461.
TEST_F(AlterTableTest, TestDropAndAddNewColumn) {
  // Reduce flush threshold so that we get both on-disk data
  // for the alter as well as in-MRS data.
  // This also increases chances of a race.
  FLAGS_flush_threshold_mb = 3;

  const int kNumRows = AllowSlowTests() ? 100000 : 1000;
  InsertRows(0, kNumRows);

  LOG(INFO) << "Verifying initial pattern";
  NO_FATALS(VerifyRows(0, kNumRows, C1_MATCHES_INDEX));

  LOG(INFO) << "Dropping and adding back c1";
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer->DropColumn("c1")
            ->Alter());

  ASSERT_OK(AddNewI32Column(kTableName, "c1", 0xdeadbeef));

  LOG(INFO) << "Verifying that the new default shows up";
  NO_FATALS(VerifyRows(0, kNumRows, C1_IS_DEADBEEF));
}

// Tests that a renamed table can still be altered. This is a regression test, we used to not carry
// over column ids after a table rename.
TEST_F(AlterTableTest, TestRenameTableAndAdd) {
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  string new_name = "someothername";
  ASSERT_OK(table_alterer->RenameTo(new_name)
            ->Alter());

  ASSERT_OK(AddNewI32Column(new_name, "new", 0xdeadbeef));
}

TEST_F(AlterTableTest, TestRenameToSameName) {
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  Status s = table_alterer->RenameTo(kTableName)->Alter();
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
}

// Test restarting a tablet server several times after various
// schema changes.
// This is a regression test for KUDU-462.
TEST_F(AlterTableTest, TestBootstrapAfterAlters) {
  vector<string> rows;

  ASSERT_OK(AddNewI32Column(kTableName, "c2", 12345));
  InsertRows(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  InsertRows(1, 1);

  UpdateRow(0, { {"c1", 10001} });
  UpdateRow(1, { {"c1", 10002} });

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c1=10001, int32 c2=12345)", rows[0]);
  ASSERT_EQ("(int32 c0=16777216, int32 c1=10002, int32 c2=12345)", rows[1]);

  LOG(INFO) << "Dropping c1";
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer->DropColumn("c1")->Alter());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c2=12345)", rows[0]);
  ASSERT_EQ("(int32 c0=16777216, int32 c2=12345)", rows[1]);

  // Test that restart doesn't fail when trying to replay updates or inserts
  // with the dropped column.
  NO_FATALS(RestartTabletServer());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c2=12345)", rows[0]);
  ASSERT_EQ("(int32 c0=16777216, int32 c2=12345)", rows[1]);

  // Add back a column called 'c2', but should not materialize old data.
  ASSERT_OK(AddNewI32Column(kTableName, "c1", 20000));
  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c2=12345, int32 c1=20000)", rows[0]);
  ASSERT_EQ("(int32 c0=16777216, int32 c2=12345, int32 c1=20000)", rows[1]);

  NO_FATALS(RestartTabletServer());
  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c2=12345, int32 c1=20000)", rows[0]);
  ASSERT_EQ("(int32 c0=16777216, int32 c2=12345, int32 c1=20000)", rows[1]);
}

TEST_F(AlterTableTest, TestCompactAfterUpdatingRemovedColumn) {
  // Disable maintenance manager, since we manually flush/compact
  // in this test.
  FLAGS_enable_maintenance_manager = false;

  vector<string> rows;

  ASSERT_OK(AddNewI32Column(kTableName, "c2", 12345));
  InsertRows(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  InsertRows(1, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());


  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c1=0, int32 c2=12345)", rows[0]);
  ASSERT_EQ("(int32 c0=16777216, int32 c1=1, int32 c2=12345)", rows[1]);

  // Add a delta for c1.
  UpdateRow(0, { {"c1", 54321} });

  // Drop c1.
  LOG(INFO) << "Dropping c1";
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer->DropColumn("c1")->Alter());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c2=12345)", rows[0]);

  // Compact
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
}

// Test which major-compacts a column for which there are updates in
// a delta file, but where the column has been removed.
TEST_F(AlterTableTest, TestMajorCompactDeltasAfterUpdatingRemovedColumn) {
  // Disable maintenance manager, since we manually flush/compact
  // in this test.
  FLAGS_enable_maintenance_manager = false;

  vector<string> rows;

  ASSERT_OK(AddNewI32Column(kTableName, "c2", 12345));
  InsertRows(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c1=0, int32 c2=12345)", rows[0]);

  // Add a delta for c1.
  UpdateRow(0, { {"c1", 54321} });

  // Make sure the delta is in a delta-file.
  ASSERT_OK(tablet_replica_->tablet()->FlushBiggestDMS());

  // Drop c1.
  LOG(INFO) << "Dropping c1";
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer->DropColumn("c1") ->Alter());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c2=12345)", rows[0]);

  // Major Compact Deltas
  ASSERT_OK(tablet_replica_->tablet()->CompactWorstDeltas(
                tablet::RowSet::MAJOR_DELTA_COMPACTION));

  // Check via debug dump that the data was properly compacted, including deltas.
  // We expect to see neither deltas nor base data for the deleted column.
  rows.clear();
  tablet_replica_->tablet()->DebugDump(&rows);
  ASSERT_EQ("Dumping tablet:\n"
            "---------------------------\n"
            "MRS memrowset:\n"
            "RowSet RowSet(0):\n"
            "RowIdxInBlock: 0; Base: (int32 c0=0, int32 c2=12345); Undo Mutations: [@4(DELETE)]; "
                "Redo Mutations: [];",
            JoinStrings(rows, "\n"));

}

// Test which major-compacts a column for which we have updates
// in a DeltaFile, but for which we didn't originally flush any
// CFile in the base data (because the the RowSet was flushed
// prior to the addition of the column).
TEST_F(AlterTableTest, TestMajorCompactDeltasIntoMissingBaseData) {
  // Disable maintenance manager, since we manually flush/compact
  // in this test.
  FLAGS_enable_maintenance_manager = false;

  vector<string> rows;

  InsertRows(0, 2);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  // Add the new column after the Flush, so it has no base data.
  ASSERT_OK(AddNewI32Column(kTableName, "c2", 12345));

  // Add a delta for c2.
  UpdateRow(0, { {"c2", 54321} });

  // Make sure the delta is in a delta-file.
  ASSERT_OK(tablet_replica_->tablet()->FlushBiggestDMS());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c1=0, int32 c2=54321)", rows[0]);
  ASSERT_EQ("(int32 c0=16777216, int32 c1=1, int32 c2=12345)", rows[1]);

  // Major Compact Deltas
  ASSERT_OK(tablet_replica_->tablet()->CompactWorstDeltas(
                tablet::RowSet::MAJOR_DELTA_COMPACTION));

  // Check via debug dump that the data was properly compacted, including deltas.
  // We expect to see the updated value materialized into the base data for the first
  // row, the default value materialized for the second, and a proper UNDO to undo
  // the update on the first row.
  rows.clear();
  tablet_replica_->tablet()->DebugDump(&rows);
  ASSERT_EQ("Dumping tablet:\n"
            "---------------------------\n"
            "MRS memrowset:\n"
            "RowSet RowSet(0):\n"
            "RowIdxInBlock: 0; Base: (int32 c0=0, int32 c1=0, int32 c2=54321); Undo Mutations: "
                "[@6(SET c2=12345), @3(DELETE)]; Redo Mutations: [];\n"
            "RowIdxInBlock: 1; Base: (int32 c0=16777216, int32 c1=1, int32 c2=12345); "
                "Undo Mutations: [@4(DELETE)]; Redo Mutations: [];",
            JoinStrings(rows, "\n"));
}

// Test which major-compacts a column for which there we have updates
// in a DeltaFile, but for which there is no corresponding CFile
// in the base data. Unlike the above test, in this case, we also drop
// the column again before running the major delta compaction.
TEST_F(AlterTableTest, TestMajorCompactDeltasAfterAddUpdateRemoveColumn) {
  // Disable maintenance manager, since we manually flush/compact
  // in this test.
  FLAGS_enable_maintenance_manager = false;

  vector<string> rows;

  InsertRows(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  // Add the new column after the Flush(), so that no CFile for this
  // column is present in the base data.
  ASSERT_OK(AddNewI32Column(kTableName, "c2", 12345));

  // Add a delta for c2.
  UpdateRow(0, { {"c2", 54321} });

  // Make sure the delta is in a delta-file.
  ASSERT_OK(tablet_replica_->tablet()->FlushBiggestDMS());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c1=0, int32 c2=54321)", rows[0]);

  // Drop c2.
  LOG(INFO) << "Dropping c2";
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer->DropColumn("c2")->Alter());

  NO_FATALS(ScanToStrings(&rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(int32 c0=0, int32 c1=0)", rows[0]);

  // Major Compact Deltas
  ASSERT_OK(tablet_replica_->tablet()->CompactWorstDeltas(
                tablet::RowSet::MAJOR_DELTA_COMPACTION));

  // Check via debug dump that the data was properly compacted, including deltas.
  // We expect to see neither deltas nor base data for the deleted column.
  rows.clear();
  tablet_replica_->tablet()->DebugDump(&rows);
  ASSERT_EQ("Dumping tablet:\n"
            "---------------------------\n"
            "MRS memrowset:\n"
            "RowSet RowSet(0):\n"
            "RowIdxInBlock: 0; Base: (int32 c0=0, int32 c1=0); Undo Mutations: [@3(DELETE)]; "
                "Redo Mutations: [];",
            JoinStrings(rows, "\n"));
}

// Test that, if we have history of previous versions of a row stored as REINSERTs,
// and those REINSERTs were written prior to adding a new column, that reading the
// old versions of the row will return the default value for the new column.
// See KUDU-1760.
TEST_F(AlterTableTest, TestReadHistoryAfterAlter) {
  FLAGS_enable_maintenance_manager = false;

  InsertRows(0, 1);
  DeleteRow(0);
  InsertRows(0, 1);
  DeleteRow(0);
  InsertRows(0, 1);
  uint64_t ts1 = client_->GetLatestObservedTimestamp();
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(AddNewI32Column(kTableName, "c2", 12345));

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  // TODO(KUDU-1813): why do we need the '- 1' below? this seems odd that the
  // "latest observed" timestamp is one higher than the thing we wrote, and the
  // Delete then is assigned that timestamp.
  ASSERT_OK(scanner.SetSnapshotRaw(ts1 - 1));
  vector<string> row_strings;
  ASSERT_OK(client::ScanToStrings(&scanner, &row_strings));
  ASSERT_EQ(1, row_strings.size());
  ASSERT_EQ("(int32 c0=0, int32 c1=0, int32 c2=12345)", row_strings[0]);
}

// Thread which inserts rows into the table.
// After each batch of rows is inserted, next_idx_ is updated
// to communicate how much data has been written (and should now be
// updateable)
void AlterTableTest::InserterThread() {
  shared_ptr<KuduSession> session = client_->NewSession();
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(15 * 1000);

  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(kTableName, &table));
  uint32_t i = 0;
  while (!stop_threads_) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    // Endian-swap the key so that we spew inserts randomly
    // instead of just a sequential write pattern. This way
    // compactions may actually be triggered.
    int32_t key = bswap_32(i++);
    CHECK_OK(insert->mutable_row()->SetInt32(0, key));
    CHECK_OK(insert->mutable_row()->SetInt32(1, i));
    CHECK_OK(session->Apply(insert.release()));

    if (i % 50 == 0) {
      FlushSessionOrDie(session);
      next_idx_ = i;
    }
  }

  FlushSessionOrDie(session);
  next_idx_ = i;
}

// Thread which follows behind the InserterThread and generates random
// updates across the previously inserted rows.
void AlterTableTest::UpdaterThread() {
  shared_ptr<KuduSession> session = client_->NewSession();
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(15 * 1000);

  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(kTableName, &table));

  Random rng(1);
  uint32_t i = 0;
  while (!stop_threads_) {
    const uint32_t max = next_idx_;
    if (max == 0) {
      // Inserter hasn't inserted anything yet, so we have nothing to update.
      SleepFor(MonoDelta::FromMicroseconds(100));
      continue;
    }
    // Endian-swap the key to match the way the InserterThread generates
    // keys to insert.
    uint32_t key = bswap_32(rng.Uniform(max - 1));
    unique_ptr<KuduUpdate> update(table->NewUpdate());
    CHECK_OK(update->mutable_row()->SetInt32(0, key));
    CHECK_OK(update->mutable_row()->SetInt32(1, i));
    CHECK_OK(session->Apply(update.release()));
    ++update_ops_cnt_;

    if (i++ % 50 == 0) {
      FlushSessionOrDie(session);
    }
  }

  FlushSessionOrDie(session);
}

// Thread which loops reading data from the table.
// No verification is performed.
void AlterTableTest::ScannerThread() {
  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(kTableName, &table));
  while (!stop_threads_) {
    KuduScanner scanner(table.get());
    uint32_t inserted_at_scanner_start = next_idx_;
    CHECK_OK(scanner.Open());
    int count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }

    LOG(INFO) << "Scanner saw " << count << " rows";
    // We may have gotten more rows than we expected, because inserts
    // kept going while we set up the scan. But, we should never get
    // fewer.
    CHECK_GE(count, inserted_at_scanner_start)
      << "We didn't get as many rows as expected";
  }
}

// Test altering a table while also sending a lot of writes,
// checking for races between the two.
TEST_F(AlterTableTest, TestAlterUnderWriteLoad) {
  // Increase chances of a race between flush and alter.
  FLAGS_flush_threshold_mb = 3;

  vector<thread> threads;
  threads.reserve(3);
  threads.emplace_back([this]() { this->InserterThread(); });
  threads.emplace_back([this]() { this->UpdaterThread(); });
  threads.emplace_back([this]() { this->ScannerThread(); });

  SCOPED_CLEANUP({
    stop_threads_ = true;
    for (auto& t : threads) {
      t.join();
    }
  });

  // Add columns until we reach 10.
  for (int i = 2; i < 10; i++) {
    if (AllowSlowTests()) {
      // In slow test mode, let more writes accumulate in between
      // alters, so that we get enough writes to cause flushes,
      // compactions, etc.
      SleepFor(MonoDelta::FromSeconds(3));
    }

    ASSERT_OK(AddNewI32Column(kTableName, Substitute("c$0", i), i));
  }

  // A sanity check: the updater should have generate at least one update
  // given the parameters the test is running with.
  CHECK_GE(update_ops_cnt_, 0U);
}

TEST_F(AlterTableTest, TestInsertAfterAlterTable) {
  const char *kSplitTableName = "split-table";

  // Create a new table with 10 tablets.
  //
  // With more tablets, there's a greater chance that the TS will heartbeat
  // after some but not all tablets have finished altering.
  ASSERT_OK(CreateSplitTable(kSplitTableName));

  // Add a column, and immediately try to insert a row including that
  // new column.
  ASSERT_OK(AddNewI32Column(kSplitTableName, "new-i32", 10));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kSplitTableName, &table));
  unique_ptr<KuduInsert> insert(table->NewInsert());
  ASSERT_OK(insert->mutable_row()->SetInt32("c0", 1));
  ASSERT_OK(insert->mutable_row()->SetInt32("c1", 1));
  ASSERT_OK(insert->mutable_row()->SetInt32("new-i32", 1));
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(15000);
  ASSERT_OK(session->Apply(insert.release()));
  Status s = session->Flush();
  if (!s.ok()) {
    ASSERT_EQ(1, session->CountPendingErrors());
    vector<KuduError*> errors;
    ElementDeleter d(&errors);
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    ASSERT_FALSE(overflow);
    ASSERT_EQ(1, errors.size());
    ASSERT_OK(errors[0]->status()); // will fail
  }
}

// Issue a bunch of alter tables in quick succession. Regression for a bug
// seen in an earlier implementation of "alter table" where these could
// conflict with each other.
TEST_F(AlterTableTest, TestMultipleAlters) {
  const char *kSplitTableName = "split-table";
  const size_t kNumNewCols = 10;
  const int32_t kDefaultValue = 10;

  // Create a new table with 10 tablets.
  //
  // With more tablets, there's a greater chance that the TS will heartbeat
  // after some but not all tablets have finished altering.
  ASSERT_OK(CreateSplitTable(kSplitTableName));

  // Issue a bunch of new alters without waiting for them to finish.
  for (int i = 0; i < kNumNewCols; i++) {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kSplitTableName));
    table_alterer->AddColumn(Substitute("new_col$0", i))
      ->Type(KuduColumnSchema::INT32)->NotNull()
      ->Default(KuduValue::FromInt(kDefaultValue));
    ASSERT_OK(table_alterer->wait(false)->Alter());
  }

  // Now wait. This should block on all of them.
  WaitAlterTableCompletion(kSplitTableName, 50);

  // All new columns should be present.
  KuduSchema new_schema;
  ASSERT_OK(client_->GetTableSchema(kSplitTableName, &new_schema));
  ASSERT_EQ(kNumNewCols + schema_.num_columns(), new_schema.num_columns());
}

TEST_F(AlterTableTest, TestAlterRangePartitioning) {
  unique_ptr<KuduTableAlterer> table_alterer;

  // Create initial table with single range partition covering the entire key
  // space, and two hash buckets.
  string table_name = "test-alter-range-partitioning";
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(table_name)
                          .schema(&schema_)
                          .set_range_partition_columns({ "c0" })
                          .add_hash_partitions({ "c0" }, 2)
                          .num_replicas(1)
                          .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));

  // Insert some rows, and then drop the partition and ensure that the table is empty.
  ASSERT_OK(InsertRowsSequential(table_name, 0, 100));
  table_alterer.reset(client_->NewTableAlterer(table_name));
  ASSERT_OK(table_alterer->DropRangePartition(schema_.NewRow(),
                                              schema_.NewRow())->Alter());
  ASSERT_EQ(0, CountTableRows(table.get()));

  // Add new range partition and insert rows.
  unique_ptr<KuduPartialRow> lower(schema_.NewRow());
  unique_ptr<KuduPartialRow> upper(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 0));
  ASSERT_OK(upper->SetInt32("c0", 100));
  table_alterer.reset(client_->NewTableAlterer(table_name));
  ASSERT_OK(table_alterer->AddRangePartition(lower.release(), upper.release())->Alter());
  ASSERT_OK(InsertRowsSequential(table_name, 0, 100));
  ASSERT_EQ(100, CountTableRows(table.get()));

  // Replace the range partition with a different one.
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 0));
  ASSERT_OK(upper->SetInt32("c0", 100));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 49));
  ASSERT_OK(upper->SetInt32("c0", 149));
  table_alterer->AddRangePartition(lower.release(), upper.release(),
                                   KuduTableCreator::EXCLUSIVE_BOUND,
                                   KuduTableCreator::INCLUSIVE_BOUND);
  ASSERT_OK(table_alterer->wait(false)->Alter());
  ASSERT_OK(InsertRowsSequential(table_name, 50, 100));
  ASSERT_EQ(100, CountTableRows(table.get()));

  // Replace the range partition with the same one.
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  ASSERT_OK(upper->SetInt32("c0", 150));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  ASSERT_OK(upper->SetInt32("c0", 150));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  ASSERT_OK(table_alterer->Alter());
  ASSERT_EQ(0, CountTableRows(table.get()));
  ASSERT_OK(InsertRowsSequential(table_name, 50, 75));
  ASSERT_EQ(75, CountTableRows(table.get()));

  // Alter table partitioning + alter table schema
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 200));
  ASSERT_OK(upper->SetInt32("c0", 300));
  table_name += "-renamed";
  table_alterer->AddRangePartition(lower.release(), upper.release())
               ->RenameTo(table_name)
               ->AddColumn("c2")->Type(KuduColumnSchema::INT32);
  ASSERT_OK(table_alterer->Alter());
  ASSERT_OK(InsertRowsSequential(table_name, 200, 100));
  ASSERT_EQ(175, CountTableRows(table.get()));
  ASSERT_OK(client_->OpenTable(table_name, &table));
  ASSERT_EQ(3, table->schema().num_columns());

  // Drop all range partitions + alter table schema. This also serves to test
  // specifying range bounds with a subset schema (since a column was
  // previously added).
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  ASSERT_OK(upper->SetInt32("c0", 150));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 200));
  ASSERT_OK(upper->SetInt32("c0", 300));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  table_alterer->AddColumn("c3")->Type(KuduColumnSchema::STRING);
  ASSERT_OK(table_alterer->Alter());
  ASSERT_EQ(0, CountTableRows(table.get()));
}

TEST_F(AlterTableTest, TestAlterRangePartitioningInvalid) {
  unique_ptr<KuduTableAlterer> table_alterer;
  Status s;

  // Create initial table with single range partition covering [0, 100).
  string table_name = "test-alter-range-partitioning-invalid";
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  unique_ptr<KuduPartialRow> lower(schema_.NewRow());
  unique_ptr<KuduPartialRow> upper(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 0));
  ASSERT_OK(upper->SetInt32("c0", 100));
  ASSERT_OK(table_creator->table_name(table_name)
                          .schema(&schema_)
                          .set_range_partition_columns({ "c0" })
                          .add_range_partition(lower.release(), upper.release())
                          .num_replicas(1)
                          .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));
  ASSERT_OK(InsertRowsSequential(table_name, 0, 100));
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [0, 100) <- already present (duplicate)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 0));
  ASSERT_OK(upper->SetInt32("c0", 100));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  s = table_alterer->wait(false)->Alter();
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "range partition already exists");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [50, 150) <- illegal (overlap)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  ASSERT_OK(upper->SetInt32("c0", 150));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  s = table_alterer->wait(false)->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "new range partition conflicts with existing one");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD (-50, 50] <- illegal (overlap)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", -50));
  ASSERT_OK(upper->SetInt32("c0", 50));
  table_alterer->AddRangePartition(lower.release(), upper.release(),
                                   KuduTableCreator::EXCLUSIVE_BOUND,
                                   KuduTableCreator::INCLUSIVE_BOUND);
  s = table_alterer->wait(false)->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "new range partition conflicts with existing one");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [200, 300)
  // ADD [-50, 150) <- illegal (overlap)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 200));
  ASSERT_OK(upper->SetInt32("c0", 300));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", -50));
  ASSERT_OK(upper->SetInt32("c0", 150));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  s = table_alterer->wait(false)->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "new range partition conflicts with existing one");
  ASSERT_FALSE(InsertRowsSequential(table_name, 200, 100).ok());
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [<start>, <end>)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  table_alterer->DropRangePartition(schema_.NewRow(), schema_.NewRow());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [50, 150)
  // RENAME foo
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  ASSERT_OK(upper->SetInt32("c0", 150));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  table_alterer->RenameTo("foo");
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [-50, 50)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", -50));
  ASSERT_OK(upper->SetInt32("c0", 50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [0, 100)
  // ADD  [100, 200)
  // DROP [100, 200)
  // ADD  [150, 250)
  // DROP [0, 10)    <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 0));
  ASSERT_OK(upper->SetInt32("c0", 100));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  ASSERT_OK(upper->SetInt32("c0", 200));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  ASSERT_OK(upper->SetInt32("c0", 200));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 150));
  ASSERT_OK(upper->SetInt32("c0", 250));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 0));
  ASSERT_OK(upper->SetInt32("c0", 10));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->wait(false)->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // KUDU-1750 Regression cases:

  // DROP [0, 50)  <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 0));
  ASSERT_OK(upper->SetInt32("c0", 50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [50, 100)  <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  ASSERT_OK(upper->SetInt32("c0", 100));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [100, 200)
  // DROP [100, 150) <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  ASSERT_OK(upper->SetInt32("c0", 200));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  ASSERT_OK(upper->SetInt32("c0", 150));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [100, 200)
  // DROP [150, 200)  <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  ASSERT_OK(upper->SetInt32("c0", 200));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 150));
  ASSERT_OK(upper->SetInt32("c0", 200));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [<min>, 0)
  // DROP [<min>, -50] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(upper->SetInt32("c0", 0));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(upper->SetInt32("c0", -50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [<min>, 0)
  // DROP [<min>, 50] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(upper->SetInt32("c0", 0));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(upper->SetInt32("c0", 50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [100, <max>)
  // DROP [150, <max>] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 150));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // ADD [100, <max>)
  // DROP [50, <max>] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // Setup for the next few test cases:
  // ADD [<min>, 0)
  // ADD [100, <max>)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(upper->SetInt32("c0", 0));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 100));
  table_alterer->AddRangePartition(lower.release(), upper.release());
  ASSERT_OK(table_alterer->Alter());

  // DROP [<min>, -50] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(upper->SetInt32("c0", -50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [<min>, 50] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(upper->SetInt32("c0", 50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [150, <max>] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 150));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [50, <max>] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  ASSERT_OK(lower->SetInt32("c0", 50));
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // DROP [<min>, <max>] <- illegal
  table_alterer.reset(client_->NewTableAlterer(table_name));
  lower.reset(schema_.NewRow());
  upper.reset(schema_.NewRow());
  table_alterer->DropRangePartition(lower.release(), upper.release());
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
  ASSERT_EQ(100, CountTableRows(table.get()));

  // Bad arguments (null ranges)
  table_alterer.reset(client_->NewTableAlterer(table_name));
  table_alterer->DropRangePartition(nullptr, nullptr);
  s = table_alterer->Alter();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "range partition bounds may not be null");
}

// Attempts to exhaustively check all cases of single-column range partition
// conflicts for ALTER TABLE ADD RANGE PARTITION ops involving two ranges.
//
// Also tests some cases of DROP RANGE PARTITION where possible, but the
// coverage is not exhaustive (the state space for invalid add/drop combinations
// is much bigger than for add/add combinations).
//
// Regression test for KUDU-1792
TEST_F(AlterTableTest, TestAddRangePartitionConflictExhaustive) {
  unique_ptr<KuduTableAlterer> table_alterer;

  // CREATE TABLE t (c0 INT PRIMARY KEY)
  // PARTITION BY
  //    RANGE (c0) ();
  string table_name = "test-alter-range-partitioning-invalid-unbounded";
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(table_name)
                          .schema(&schema_)
                          .set_range_partition_columns({ "c0" })
                          .num_replicas(1)
                          .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));

  // Drop the default UNBOUNDED tablet in order to start with a table with no ranges.
  table_alterer.reset(client_->NewTableAlterer(table_name));
  ASSERT_OK(table_alterer->DropRangePartition(schema_.NewRow(), schema_.NewRow())
                         ->wait(true)->Alter());

  // Turns an optional value into a row for the table.
  auto fill_row = [&] (boost::optional<int32_t> value) {
    unique_ptr<KuduPartialRow> row(schema_.NewRow());
    if (value) {
      CHECK_OK(row->SetInt32("c0", *value));
    }
    return row;
  };

  // Attempts to add a range partition to the table with the specified bounds.
  auto add_range_partition = [&] (boost::optional<int32_t> lower_bound,
                                  boost::optional<int32_t> upper_bound) {
    table_alterer.reset(client_->NewTableAlterer(table_name));
    return table_alterer->AddRangePartition(fill_row(lower_bound).release(),
                                            fill_row(upper_bound).release())
                        ->wait(false)
                        ->Alter();
  };

  // Attempts to drop a range partition to the table with the specified bounds.
  auto drop_range_partition = [&] (boost::optional<int32_t> lower_bound,
                                   boost::optional<int32_t> upper_bound) {
    table_alterer.reset(client_->NewTableAlterer(table_name));
    return table_alterer->DropRangePartition(fill_row(lower_bound).release(),
                                             fill_row(upper_bound).release())
                        ->wait(false)
                        ->Alter();
  };

  // Attempts to add two range partitions to the table in a single op.
  auto add_range_partitions = [&] (boost::optional<int32_t> a_lower_bound,
                                   boost::optional<int32_t> a_upper_bound,
                                   boost::optional<int32_t> b_lower_bound,
                                   boost::optional<int32_t> b_upper_bound) {
    table_alterer.reset(client_->NewTableAlterer(table_name));
    return table_alterer->AddRangePartition(fill_row(a_lower_bound).release(),
                                            fill_row(a_upper_bound).release())
                        ->AddRangePartition(fill_row(b_lower_bound).release(),
                                            fill_row(b_upper_bound).release())
                        ->wait(false)
                        ->Alter();
  };

  // Attempts to add and drop two range partitions in a single op.
  auto add_drop_range_partitions = [&] (boost::optional<int32_t> a_lower_bound,
                                        boost::optional<int32_t> a_upper_bound,
                                        boost::optional<int32_t> b_lower_bound,
                                        boost::optional<int32_t> b_upper_bound) {
    table_alterer.reset(client_->NewTableAlterer(table_name));
    return table_alterer->AddRangePartition(fill_row(a_lower_bound).release(),
                                            fill_row(a_upper_bound).release())
                        ->DropRangePartition(fill_row(b_lower_bound).release(),
                                             fill_row(b_upper_bound).release())
                        ->wait(false)
                        ->Alter();
  };

  auto bounds_to_string = [] (boost::optional<int32_t> lower_bound,
                              boost::optional<int32_t> upper_bound) -> string {
    if (!lower_bound && !upper_bound) {
      return "UNBOUNDED";
    }
    if (!lower_bound) {
      return Substitute("VALUES < $0", *upper_bound);
    }
    if (!upper_bound) {
      return Substitute("VALUES >= $0", *lower_bound);
    }
    return Substitute("$0 <= VALUES < $1", *lower_bound, *upper_bound);
  };

  // Checks that b conflicts with a, when added in that order.
  auto do_expect_range_partitions_conflict = [&] (boost::optional<int32_t> a_lower_bound,
                                                  boost::optional<int32_t> a_upper_bound,
                                                  boost::optional<int32_t> b_lower_bound,
                                                  boost::optional<int32_t> b_upper_bound) {
    SCOPED_TRACE(Substitute("b: $0", bounds_to_string(b_lower_bound, b_upper_bound)));
    SCOPED_TRACE(Substitute("a: $0", bounds_to_string(a_lower_bound, a_upper_bound)));

    // Add a then add b.
    ASSERT_OK(add_range_partition(a_lower_bound, a_upper_bound));
    auto s = add_range_partition(b_lower_bound, b_upper_bound);
    if (a_lower_bound == b_lower_bound && a_upper_bound == b_upper_bound) {
      ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "range partition already exists");
    } else {
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(),
                          "new range partition conflicts with existing one");
    }
    // Clean up by removing a.
    ASSERT_OK(drop_range_partition(a_lower_bound, a_upper_bound));

    // Add a and b in the same op.
    s = add_range_partitions(a_lower_bound, a_upper_bound,
                             b_lower_bound, b_upper_bound);
    if (a_lower_bound == b_lower_bound && a_upper_bound == b_upper_bound) {
      ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(),
                          "new range partition duplicates another newly added one");
    } else {
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(),
                          "new range partition conflicts with another newly added one");
    }

    // To get some extra coverage of DROP RANGE PARTITION, check if the two
    // ranges are not equal, and if so, check that adding one and dropping the
    // other fails.

    if (a_lower_bound != b_lower_bound || a_upper_bound != b_upper_bound) {
      // Add a then drop b.
      ASSERT_OK(add_range_partition(a_lower_bound, a_upper_bound));
      Status s = drop_range_partition(b_lower_bound, b_upper_bound);
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
      // Clean up by removing a.
      ASSERT_OK(drop_range_partition(a_lower_bound, a_upper_bound));

      // Add a and drop b in a single op.
      s = add_drop_range_partitions(a_lower_bound, a_upper_bound,
                                    b_lower_bound, b_upper_bound);
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "no range partition to drop");
    }
  };

  // Checks that two range partitions conflict.
  auto expect_range_partitions_conflict = [&] (boost::optional<int32_t> a_lower_bound,
                                               boost::optional<int32_t> a_upper_bound,
                                               boost::optional<int32_t> b_lower_bound,
                                               boost::optional<int32_t> b_upper_bound) {
    do_expect_range_partitions_conflict(a_lower_bound, a_upper_bound,
                                        b_lower_bound, b_upper_bound);
    do_expect_range_partitions_conflict(b_lower_bound, b_upper_bound,
                                        a_lower_bound, a_upper_bound);
  };

  /// Bounded / Bounded

  // [----------)
  // [----------)
  expect_range_partitions_conflict(0, 100, 0, 100);

  // [----------)
  //      [----------)
  expect_range_partitions_conflict(0, 100, 50, 150);

  // [----------)
  // [------)
  expect_range_partitions_conflict(0, 100, 0, 50);

  // [----------)
  //     [------)
  expect_range_partitions_conflict(0, 100, 50, 100);

  // [----------)
  //   [------)
  expect_range_partitions_conflict(0, 100, 25, 75);

  /// Bounded / Unbounded Above

  //  [----------)
  // [-------------->
  expect_range_partitions_conflict(0, 100, -1, boost::none);

  // [----------)
  // [-------------->
  expect_range_partitions_conflict(0, 100, 0, boost::none);

  // [----------)
  //  [------------->
  expect_range_partitions_conflict(0, 100, 1, boost::none);

  // [----------)
  //      [------------->
  expect_range_partitions_conflict(0, 100, 50, boost::none);

  // [----------)
  //           [--------->
  expect_range_partitions_conflict(0, 100, 99, boost::none);

  /// Bounded / Unbounded Below

  //        [----------)
  // <-------)
  expect_range_partitions_conflict(0, 100, boost::none, 1);

  //        [----------)
  // <------------)
  expect_range_partitions_conflict(0, 100, boost::none, 50);

  //        [----------)
  // <-----------------)
  expect_range_partitions_conflict(0, 100, boost::none, 100);

  //        [----------)
  // <-------------------)
  expect_range_partitions_conflict(0, 100, boost::none, 125);

  /// Bounded / Unbounded

  //     [----------)
  // <------------------->
  expect_range_partitions_conflict(0, 100, boost::none, boost::none);

  /// Bounded / Single Value

  // [----------)
  // |
  expect_range_partitions_conflict(0, 100, 0, 1);

  // [----------)
  //      |
  expect_range_partitions_conflict(0, 100, 25, 26);

  // [----------)
  //           |
  expect_range_partitions_conflict(0, 100, 99, 100);

  /// Unbounded Above / Unbounded Above

  //    [---------->
  // [---------->
  expect_range_partitions_conflict(0, boost::none, -10, boost::none);

  // [---------->
  // [---------->
  expect_range_partitions_conflict(0, boost::none, 0, boost::none);

  /// Unbounded Above / Unbounded Below

  // [---------->
  // <----------)
  expect_range_partitions_conflict(0, boost::none, boost::none, 100);

  //        [---------->
  // <-------)
  expect_range_partitions_conflict(0, boost::none, boost::none, 1);

  /// Unbounded Above / Unbounded

  // [---------->
  // <---------->
  expect_range_partitions_conflict(0, boost::none, boost::none, boost::none);

  /// Unbounded Above / Single Value

  // [---------->
  // |
  expect_range_partitions_conflict(0, boost::none, 0, 1);

  // [---------->
  //   |
  expect_range_partitions_conflict(0, boost::none, 100, 101);

  /// Unbounded Below / Unbounded Below

  // <----------)
  // <----------)
  expect_range_partitions_conflict(boost::none, 100, boost::none, 100);

  // <----------)
  // <-----)
  expect_range_partitions_conflict(boost::none, 100, boost::none, 50);

  /// Unbounded Below / Unbounded

  // <----------)
  // <---------->
  expect_range_partitions_conflict(boost::none, 100, boost::none, boost::none);

  /// Unbounded Below / Single Value

  // <----------)
  //       |
  expect_range_partitions_conflict(boost::none, 100, 50, 51);

  // <----------)
  //           |
  expect_range_partitions_conflict(boost::none, 100, 99, 100);

  /// Unbounded / Unbounded

  // <---------->
  // <---------->
  expect_range_partitions_conflict(boost::none, boost::none, boost::none, boost::none);

  /// Single Value / Single Value

  // |
  // |
  expect_range_partitions_conflict(0, 1, 0, 1);
}

TEST_F(ReplicatedAlterTableTest, TestReplicatedAlter) {
  const int kNumRows = 100;
  InsertRows(0, kNumRows);

  LOG(INFO) << "Verifying initial pattern";
  NO_FATALS(VerifyRows(0, kNumRows, C1_MATCHES_INDEX));

  LOG(INFO) << "Dropping and adding back c1";
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer->DropColumn("c1")->Alter());

  ASSERT_OK(AddNewI32Column(kTableName, "c1", 0xdeadbeef));

  bool alter_in_progress;
  ASSERT_OK(client_->IsAlterTableInProgress(kTableName, &alter_in_progress));
  ASSERT_FALSE(alter_in_progress);

  LOG(INFO) << "Verifying that the new default shows up";
  // TODO(KUDU-791): we should be able to assert right away, but alter-table doesn't
  // currently obey the expected consistency semantics.
  ASSERT_EVENTUALLY([&]() {
    NO_FATALS(VerifyRows(0, kNumRows, C1_IS_DEADBEEF));
  });
}

// This scenario verifies that many DDL operations (e.g., ALTER TABLE adding
// a new column) followed by dropping a tablet are processed with no issues
// even in case of slow WAL. In addition, this scenario covers the regression
// fixed with changelist 81f0dbf99ac2114a29431f49fa2ef480e7ef26c4.
TEST_F(ReplicatedAlterTableTest, AlterTableAndDropTablet) {
  SKIP_IF_SLOW_NOT_ALLOWED();

#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
  constexpr int32_t kIterNum = 10;
#else
  constexpr int32_t kIterNum = 50;
#endif

  static constexpr const char* const kTableName = "many-ranges-table";
  const auto fill_row = [&] (int32_t value) {
    unique_ptr<KuduPartialRow> row(schema_.NewRow());
    CHECK_OK(row->SetInt32("c0", value));
    return row;
  };

  {
    vector<pair<unique_ptr<KuduPartialRow>, unique_ptr<KuduPartialRow>>> bounds;
    bounds.emplace_back(fill_row(0), fill_row(1));
    ASSERT_OK(CreateTable(kTableName, schema_, {"c0"}, {}, std::move(bounds)));
  }

  for (auto i = 1; i < kIterNum; ++i) {
    unique_ptr<KuduTableAlterer> ta(client_->NewTableAlterer(kTableName));
    ASSERT_OK(ta->AddRangePartition(
        fill_row(i).release(),
        fill_row(i + 1).release())->wait(true)->Alter());
  }

  FLAGS_log_inject_latency = true;
  FLAGS_log_inject_latency_ms_mean = 250;

  for (auto i = 0; i < kIterNum; ++i) {
    unique_ptr<KuduTableAlterer> ta(client_->NewTableAlterer(kTableName));
    ta->AddColumn(Substitute("new_c$0", i))->Type(KuduColumnSchema::INT32);
    ASSERT_OK(ta->wait(false)->Alter());
  }

  for (auto i = 0; i < kIterNum; ++i) {
    unique_ptr<KuduTableAlterer> ta(client_->NewTableAlterer(kTableName));
    ASSERT_OK(ta->DropRangePartition(
        fill_row(i).release(),
        fill_row(i + 1).release())->wait(false)->Alter());
  }
  ASSERT_OK(client_->DeleteTable(kTableName));
}

TEST_F(ReplicatedAlterTableTest, AlterReplicationFactor) {
  // 1. The default replication factor is 3.
  ASSERT_EQ(0, tablet_replica_->tablet()->metadata()->schema_version());
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));

  // 2. Set replication factor to 1.
  ASSERT_OK(SetReplicationFactor(kTableName, 1));
  NO_FATALS(VerifyTabletReplicaCount(1, VerifyRowCount::kEnable));
  ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());

  // 3. Set replication factor to 3.
  ASSERT_OK(SetReplicationFactor(kTableName, 3));
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
  ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());

  // 4. Set replication factor to 3 again.
  ASSERT_OK(SetReplicationFactor(kTableName, 3));
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
  ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());

  // 5. Set replication factor to 5, while there are only 4 tservers in the cluster.
  auto s = SetReplicationFactor(kTableName, 5);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "not enough live tablet servers to alter a table with the"
                                    " requested replication factor 5; 4 tablet servers are alive");
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
  ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());

  // 6. Set replication factor to -1, it will fail.
  s = SetReplicationFactor(kTableName, -1);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "illegal replication factor -1: minimum allowed replication"
                                    " factor is 1 (controlled by --min_num_replicas)");
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
  ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());

  // 7. Set replication factor to 2, it will fail.
  s = SetReplicationFactor(kTableName, 2);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "illegal replication factor 2: replication factor must be odd");
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
  ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());

  // 8. Set replication factor to 9, it will fail.
  s = SetReplicationFactor(kTableName, 9);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "illegal replication factor 9: maximum allowed replication "
                                    "factor is 7 (controlled by --max_num_replicas)");
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
  ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());

  ASSERT_OK(client_->DeleteTable(kTableName));
}

TEST_F(ReplicatedAlterTableTest, AlterReplicationFactorWhileScanning) {
  // Delete table at first, and create table by TestWorkload later.
  ASSERT_OK(client_->DeleteTable(kTableName));

  // Write some data for scan later.
  {
    TestWorkload workload(cluster_.get());
    workload.set_table_name(kTableName);
    workload.set_num_tablets(1);
    workload.set_num_replicas(1);
    workload.set_num_write_threads(10);
    workload.Setup();
    workload.Start();
    ASSERT_EVENTUALLY([&]() {
      ASSERT_GE(workload.rows_inserted(), 30000);
    });
    workload.StopAndJoin();
  }

  // Keep scaning the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_write_threads(0);
  workload.set_num_read_threads(10);
  workload.set_read_errors_allowed(false);
  workload.Setup();
  workload.Start();

  // Set replication factor to 3.
  ASSERT_OK(SetReplicationFactor(kTableName, 3));
  ASSERT_EVENTUALLY([&] {
    tablet_replica_ = LookupLeaderTabletReplica();
    NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
    ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());
  });

  // Set replication factor to 1.
  ASSERT_OK(SetReplicationFactor(kTableName, 1));
  ASSERT_EVENTUALLY([&] {
    tablet_replica_ = LookupLeaderTabletReplica();
    NO_FATALS(VerifyTabletReplicaCount(1, VerifyRowCount::kEnable));
    ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());
  });

  workload.StopAndJoin();
}

TEST_F(ReplicatedAlterTableTest, AlterReplicationFactorWhileWriting) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Additionally, make the WAL segments smaller to encourage more frequent
  // roll-over onto WAL segments.
  FLAGS_flush_threshold_secs = 0;
  FLAGS_log_segment_size_mb = 1;

  // Make tablet replica copying slow, to make it easier to spot violations.
  FLAGS_tablet_copy_download_file_inject_latency_ms = 2000;

  // Delete table at first, and create table by TestWorkload later.
  ASSERT_OK(client_->DeleteTable(kTableName));

  // Keep writing the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_tablets(1);
  workload.set_num_replicas(1);
  workload.set_num_write_threads(10);
  workload.set_payload_bytes(1024);
  workload.set_write_timeout_millis(5 * 1000);
  workload.set_timeout_allowed(false);
  workload.set_network_error_allowed(false);
  workload.set_remote_error_allowed(false);
  workload.Setup();
  workload.Start();
  ASSERT_EVENTUALLY([&]() {
    ASSERT_GE(workload.rows_inserted(), 200000);
  });

  // Set replication factor to 3.
  ASSERT_OK(SetReplicationFactor(kTableName, 3));
  ASSERT_EVENTUALLY([&] {
    tablet_replica_ = LookupLeaderTabletReplica();
    // Not verify row count while table is being written.
    NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kDisable));
    ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());
  });

  // Stop writing and verify again.
  workload.StopAndJoin();
  ASSERT_EVENTUALLY([&] {
    tablet_replica_ = LookupLeaderTabletReplica();
    NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
    ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());
  });

  // Set replication factor to 1.
  workload.Start();
  ASSERT_OK(SetReplicationFactor(kTableName, 1));
  ASSERT_EVENTUALLY([&] {
    tablet_replica_ = LookupLeaderTabletReplica();
    NO_FATALS(VerifyTabletReplicaCount(1, VerifyRowCount::kEnable));
    ASSERT_EQ(2, tablet_replica_->tablet()->metadata()->schema_version());
  });

  workload.StopAndJoin();
}

TEST_F(ReplicatedAlterTableTest, AlterReplicationFactorAfterWALGCed) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Additionally, make the WAL segments smaller to encourage more frequent
  // roll-over onto WAL segments.
  FLAGS_flush_threshold_secs = 0;
  FLAGS_log_segment_size_mb = 1;

  ASSERT_OK(client_->DeleteTable(kTableName));
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_tablets(1);
  workload.set_num_replicas(1);
  workload.set_num_write_threads(10);
  workload.Setup();
  workload.Start();

  // Function to fetch the GC count of all tablet's WAL.
  auto get_tablet_wal_gc_count = [&] (int tserver_idx) {
    int64_t tablet_wal_gc_count = 0;
    itest::GetInt64Metric(
        HostPort(cluster_->mini_tablet_server(tserver_idx)->bound_http_addr()),
        &METRIC_ENTITY_tablet,
        "*",
        &METRIC_log_gc_duration,
        "total_count",
        &tablet_wal_gc_count);
    return tablet_wal_gc_count;
  };

  vector<int64_t> orig_gc_count(num_tservers());
  for (int tserver_idx = 0; tserver_idx < num_tservers(); tserver_idx++) {
    orig_gc_count[tserver_idx] = get_tablet_wal_gc_count(tserver_idx);
  }

  // Wait util some WALs have been GCed.
  ASSERT_EVENTUALLY([&] {
    int num_tserver_gc_updated = 0;
    for (int tserver_idx = 0; tserver_idx < num_tservers(); tserver_idx++) {
      if (get_tablet_wal_gc_count(tserver_idx) > orig_gc_count[tserver_idx]) {
        num_tserver_gc_updated++;
      }
    }
    ASSERT_GE(num_tserver_gc_updated, 1);
  });
  workload.StopAndJoin();

  // Set replication factor to 3.
  ASSERT_OK(SetReplicationFactor(kTableName, 3));
  tablet_replica_ = LookupLeaderTabletReplica();
  NO_FATALS(VerifyTabletReplicaCount(3, VerifyRowCount::kEnable));
  ASSERT_EQ(1, tablet_replica_->tablet()->metadata()->schema_version());
}

TEST_F(AlterTableTest, TestRenameStillCreatingTable) {
  const string kNewTableName = "foo";

  // Start creating the table in a separate thread.
  std::thread creator_thread([&](){
    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    CHECK_OK(creator->table_name(kNewTableName)
             .schema(&schema_)
             .set_range_partition_columns({ "c0" })
             .num_replicas(1)
             .Create());
  });
  SCOPED_CLEANUP({
    creator_thread.join();
  });

  // While the table is being created, change its name. We may have to retry a
  // few times as this races with table creation.
  //
  // If the logic that waits for table creation to finish finds the table by
  // name, this rename would cause it to fail and the test would crash.
  while (true) {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kNewTableName));
    Status s = alterer->RenameTo("foo2")->Alter();
    if (s.ok()) {
      break;
    }
    if (!s.IsNotFound()) {
      SCOPED_TRACE(s.ToString());
      FAIL();
    }
  }
}

// Regression test for KUDU-2132.
TEST_F(AlterTableTest, TestKUDU2132) {
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  table_alterer->AlterColumn("c0")->RenameTo("primaryKeyRenamed");
  table_alterer->AlterColumn("c1")->RenameTo("c0");
  table_alterer->DropColumn("c0");
  ASSERT_OK(table_alterer->Alter());
}

TEST_F(AlterTableTest, TestMaintenancePriorityAlter) {
  // Default priority level.
  NO_FATALS(CheckMaintenancePriority(0));

  // Set to some priority level.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer->AlterExtraConfig({{"kudu.table.maintenance_priority", "3"}})->Alter());
  NO_FATALS(CheckMaintenancePriority(3));

  // Set to another priority level.
  ASSERT_OK(table_alterer->AlterExtraConfig({{"kudu.table.maintenance_priority", "-1"}})->Alter());
  NO_FATALS(CheckMaintenancePriority(-1));

  // Reset to default priority level.
  ASSERT_OK(table_alterer->AlterExtraConfig({{"kudu.table.maintenance_priority", "0"}})->Alter());
  NO_FATALS(CheckMaintenancePriority(0));
}

} // namespace kudu
