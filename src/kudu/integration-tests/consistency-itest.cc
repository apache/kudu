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

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_data_block_fsync);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(scanner_gc_check_interval_us);
DECLARE_bool(use_mock_wall_clock);

using kudu::client::sp::shared_ptr;
using kudu::master::CatalogManager;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::GetTableLocationsResponsePB;
using kudu::server::HybridClock;
using kudu::tablet::TabletPeer;
using kudu::tserver::MiniTabletServer;
using kudu::tserver::TabletServer;
using std::string;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace client {


class TimestampPropagationTest : public MiniClusterITestBase {
 public:
  TimestampPropagationTest()
      : num_tablet_servers_(2),
        table_name_("timestamp_propagation_test_table"),
        key_column_name_("key"),
        key_split_value_(8) {
    // Keep unit tests fast.
    FLAGS_enable_data_block_fsync = false;

    // Using the mock clock: need to advance the clock for tablet servers.
    FLAGS_use_mock_wall_clock = true;

    // Reduce the TS<->Master heartbeat interval: this speeds up testing,
    // saving about 700ms per test.
    FLAGS_heartbeat_interval_ms = 10;
    FLAGS_scanner_gc_check_interval_us = 50 * 1000;

    KuduSchemaBuilder b;
    b.AddColumn(key_column_name_)->Type(KuduColumnSchema::INT32)
        ->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
    CHECK_OK(b.Build(&schema_));
  }

  virtual void SetUp() override {
    MiniClusterITestBase::SetUp();
    StartCluster(num_tablet_servers_);
  }

 protected:
  static void UpdateClock(HybridClock* clock, MonoDelta delta) {
    const uint64_t new_time(HybridClock::GetPhysicalValueMicros(clock->Now()) +
                            delta.ToMicroseconds());
    clock->SetMockClockWallTimeForTests(new_time);
  }

  // Creates a table with the specified name and replication factor.
  Status CreateTable(KuduClient* client,
                     const string& table_name) {
    unique_ptr<KuduPartialRow> split_row(schema_.NewRow());
    RETURN_NOT_OK(split_row->SetInt32(0, key_split_value_));

    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(table_name)
                  .schema(&schema_)
                  .add_range_partition_split(split_row.release())
                  .set_range_partition_columns({ key_column_name_ })
                  .num_replicas(1)
                  .Create());
    return Status::OK();
  }

  unique_ptr<KuduInsert> BuildTestRow(KuduTable* table, int index) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    CHECK_OK(row->SetInt32(0, index));
    CHECK_OK(row->SetInt32(1, index * 2));
    CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello %d", index))));
    return insert;
  }

  // Inserts given number of tests rows into the default test table
  // in the context of the specified session.
  Status InsertTestRows(KuduClient* client, KuduTable* table,
                        int num_rows, int first_row = 0) {
    shared_ptr<KuduSession> session = client->NewSession();
    RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    session->SetTimeoutMillis(60000);
    for (int i = first_row; i < num_rows + first_row; ++i) {
      unique_ptr<KuduInsert> insert(BuildTestRow(table, i));
      RETURN_NOT_OK(session->Apply(insert.release()));
    }
    RETURN_NOT_OK(session->Flush());
    return Status::OK();
  }

  Status GetRowCount(KuduTable* table, KuduScanner::ReadMode read_mode,
                     uint64_t ts, size_t* row_count) {
    KuduScanner scanner(table);
    RETURN_NOT_OK(scanner.SetReadMode(read_mode));
    if (read_mode == KuduScanner::READ_AT_SNAPSHOT && ts != 0) {
      RETURN_NOT_OK(scanner.SetSnapshotRaw(ts + 1));
    }
    RETURN_NOT_OK(CountRowsWithRetries(&scanner, row_count));
    return Status::OK();
  }

  Status GetTabletIdForKeyValue(int32_t key_value_begin,
                                int32_t key_value_end,
                                const string& table_name,
                                vector<string>* tablet_ids) {
    if (!tablet_ids) {
      return Status::InvalidArgument("null output container");
    }
    tablet_ids->clear();

    // Find the tablet for the first range (i.e. for the rows to be inserted).
    unique_ptr<KuduPartialRow> split_row_start(schema_.NewRow());
    RETURN_NOT_OK(split_row_start->SetInt32(0, key_value_begin));
    string partition_key_start;
    RETURN_NOT_OK(split_row_start->EncodeRowKey(&partition_key_start));

    unique_ptr<KuduPartialRow> split_row_end(schema_.NewRow());
    RETURN_NOT_OK(split_row_end->SetInt32(0, key_value_end));
    string partition_key_end;
    RETURN_NOT_OK(split_row_end->EncodeRowKey(&partition_key_end));

    GetTableLocationsRequestPB req;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start(partition_key_start);
    req.set_partition_key_end(partition_key_end);
    master::CatalogManager* catalog =
        cluster_->mini_master()->master()->catalog_manager();
    GetTableLocationsResponsePB resp;
    CatalogManager::ScopedLeaderSharedLock l(catalog);
    RETURN_NOT_OK(l.first_failed_status());
    RETURN_NOT_OK(catalog->GetTableLocations(&req, &resp));
    for (size_t i = 0; i < resp.tablet_locations_size(); ++i) {
      tablet_ids->emplace_back(resp.tablet_locations(i).tablet_id());
    }

    return Status::OK();
  }

  Status FindPeerForTablet(const string& tablet_id,
                           scoped_refptr<TabletPeer>* peer) {
    bool found = false;
    for (size_t i = 0; i < num_tablet_servers_; ++i) {
      MiniTabletServer* mts = cluster_->mini_tablet_server(i);
      TabletServer* ts = mts->server();

      scoped_refptr<TabletPeer> p;
      if (!ts->tablet_manager()->LookupTablet(tablet_id, &p)) {
        // Not this one, continue.
        continue;
      }
      peer->swap(p);
      found = true;
      break;
    }
    if (!found) {
      return Status::NotFound(
          Substitute("$0: cannot find peer for tablet"), tablet_id);
    }
    return Status::OK();
  }

  const size_t num_tablet_servers_;
  const string table_name_;
  const string key_column_name_;
  const int key_split_value_;
  KuduSchema schema_;
};

// This is a test that exposes the necessity of propagating timestamp
// between reads (scans) if consistent results are desired.
//
// Let T1, T2 be reads from the same client where T2 starts after the response
// from T1 is received and neither are assigned timestamps by the client.
// It might be the case where T2’s observed value actually precedes T1’s value
// in the row history if T1 and T2 are performed in different servers,
// as T2 can be assigned a timestamp that is lower than T1.
//
// The test is disabled because it fails due to the absense of timestamp
// propagation. Remove the 'DISABLED_' prefix, recompile and run if needed.
//
// The scenario to expose inconsistency when not propagating the timestamp
// for the scan operations:
//
//   * Set the flag to use the mock wall clock.
//
//   * Start two mini cluster tservers and create a table with two tablets
//     ({Ta, Tb} order matters), single replica.
//
//   * Advance the clock in tablet Ta's tserver by some amount.
//
//   * Write a row to the tablet Ta, discard the client.
//
//   * With a new client, read the row from the Ta tablet. Here we are using
//     READ_LATEST read mode because with READ_AT_SNAPSHOT mode the read
//     operation would block and wait for the clock to advance (which in this
//     case is possible only if doing that manually).
//     Then write a row to tablet Tb. Take note of write timestamp and
//     discard the client.
//
//   * Now scan both tablets using READ_AT_SNAPSHOT mode and the timestamp
//     of the write to the Tb tablet.
//     Since the write to the Tb tablet followed the write to the Ta tablet
//     (quite literally, we read those rows from the Ta tablet before writing
//     to the Tb tablet), we should see all the rows written.
//
//     However, that would not be the case if the scan timestamp from the Ta's
//     scan were not propagated to the Tb's write: Tb server's time is lagging
//     behind Ta server's time, and scanning at Tb's write time would not
//     include the rows inserted into Ta.
//
TEST_F(TimestampPropagationTest, DISABLED_TwoBatchesAndReadAtSnapshot) {
  uint64_t ts_a;
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    ASSERT_OK(CreateTable(client.get(), table_name_));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));

    // Find the tablet for the first range (i.e. for the rows to be inserted).
    vector<string> tablet_ids;
    ASSERT_OK(GetTabletIdForKeyValue(0, key_split_value_ - 1, // first range
                                     table_name_, &tablet_ids));
    ASSERT_EQ(1, tablet_ids.size());
    scoped_refptr<TabletPeer> peer;
    ASSERT_OK(FindPeerForTablet(tablet_ids[0], &peer));

    // Advance tablet server's clock.
    HybridClock* clock = dynamic_cast<HybridClock*>(peer->clock());
    ASSERT_NE(nullptr, clock) << "unexpected clock for tablet server";
    UpdateClock(clock, MonoDelta::FromMilliseconds(100));

    // Insert data into the first tablet (a.k.a. Ta).
    ASSERT_OK(InsertTestRows(client.get(), table.get(), key_split_value_, 0));
    size_t row_count;
    ASSERT_OK(GetRowCount(table.get(), KuduScanner::READ_LATEST, 0,
                          &row_count));
    ASSERT_EQ(key_split_value_, row_count);
    // Retrieve the latest observed timestamp.
    ts_a = client->GetLatestObservedTimestamp();
  }

  uint64_t ts_b;
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));

    // Running the prior version of the client code and using READ_AT_SNAPSHOT
    // for the scan would lead to unintentional propagating of the scan time
    // (i.e. Ta server's time) to the latest observed timestamp of the client.
    // Besides, as already mentioned in the comment for the test, the scan
    // in READ_AT_SNAPSHOT mode would block and wait for the clock to advance.
    size_t row_count;
    ASSERT_OK(GetRowCount(table.get(), KuduScanner::READ_LATEST, 0,
                          &row_count));
    // Check we see the first batch of inserted rows.
    ASSERT_EQ(key_split_value_, row_count);

    // Inserting data into the second tablet (a.k.a. Tb): using the second
    // key range partition.
    ASSERT_OK(InsertTestRows(client.get(), table.get(),
                             key_split_value_, key_split_value_));
    // Retrieve the latest observed timestamp.
    ts_b = client->GetLatestObservedTimestamp();
  }
  EXPECT_GT(ts_b, ts_a);

  // We are expecting to see all the inserted rows. It might not be the case
  // if the the timestamps were not propagated.
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));

    size_t row_count;
    ASSERT_OK(GetRowCount(table.get(), KuduScanner::READ_AT_SNAPSHOT, ts_b,
                          &row_count));
    // In total, inserted 2 * key_split_value_ rows and expecting to see
    // the total row count to reflect that: using snapshot timestamp that
    // is taken at insertng the second batch when the first batch already
    // was there.
    const size_t total_rows = 2UL * key_split_value_;
    ASSERT_EQ(total_rows, row_count);
  }
}

} // namespace client
} // namespace kudu
