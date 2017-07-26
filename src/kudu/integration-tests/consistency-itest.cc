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

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/scan_configuration.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/internal_mini_cluster.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_util.h"

DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(max_clock_sync_error_usec);
DECLARE_int32(scanner_gc_check_interval_us);
DECLARE_bool(use_mock_wall_clock);

using kudu::client::ScanConfiguration;
using kudu::client::sp::shared_ptr;
using kudu::master::CatalogManager;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::GetTableLocationsResponsePB;
using kudu::clock::HybridClock;
using kudu::tablet::TabletReplica;
using kudu::tserver::MiniTabletServer;
using kudu::tserver::TabletServer;
using std::string;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace client {


class ConsistencyITest : public MiniClusterITestBase {
 public:
  ConsistencyITest()
      : num_tablet_servers_(2),
        table_name_("timestamp_propagation_test_table"),
        key_column_name_("key"),
        key_split_value_(8) {

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

  Status GetTabletIdForKey(int32_t key_value, string* tablet_id) {
    if (!tablet_id) {
      return Status::InvalidArgument("null output string");
    }
    const int32_t key_value_begin = key_value;
    const int32_t key_value_end = key_value_begin + 1;

    unique_ptr<KuduPartialRow> split_row_start(schema_.NewRow());
    RETURN_NOT_OK(split_row_start->SetInt32(0, key_value_begin));
    string partition_key_start;
    RETURN_NOT_OK(split_row_start->EncodeRowKey(&partition_key_start));

    unique_ptr<KuduPartialRow> split_row_end(schema_.NewRow());
    RETURN_NOT_OK(split_row_end->SetInt32(0, key_value_end));
    string partition_key_end;
    RETURN_NOT_OK(split_row_end->EncodeRowKey(&partition_key_end));

    GetTableLocationsRequestPB req;
    req.mutable_table()->set_table_name(table_name_);
    req.set_partition_key_start(partition_key_start);
    req.set_partition_key_end(partition_key_end);
    master::CatalogManager* catalog =
        cluster_->mini_master()->master()->catalog_manager();
    GetTableLocationsResponsePB resp;
    CatalogManager::ScopedLeaderSharedLock l(catalog);
    RETURN_NOT_OK(l.first_failed_status());
    RETURN_NOT_OK(catalog->GetTableLocations(&req, &resp));
    if (resp.tablet_locations_size() < 1) {
      return Status::NotFound(Substitute("$0: no tablets for key", key_value));
    }
    if (resp.tablet_locations_size() > 1) {
      return Status::IllegalState(
          Substitute("$0: multiple tablet servers for key", key_value));
    }
    *tablet_id = resp.tablet_locations(0).tablet_id();

    return Status::OK();
  }

  Status FindPeerForTablet(const string& tablet_id,
                           scoped_refptr<TabletReplica>* replica) {
    bool found = false;
    for (size_t i = 0; i < num_tablet_servers_; ++i) {
      MiniTabletServer* mts = cluster_->mini_tablet_server(i);
      TabletServer* ts = mts->server();

      scoped_refptr<TabletReplica> r;
      if (!ts->tablet_manager()->LookupTablet(tablet_id, &r)) {
        // Not this one, continue.
        continue;
      }
      replica->swap(r);
      found = true;
      break;
    }
    if (!found) {
      return Status::NotFound(
          Substitute("$0: cannot find replica for tablet"), tablet_id);
    }
    return Status::OK();
  }

  Status UpdateClockForTabletHostingKey(int32_t key, const MonoDelta& offset) {
    string tablet_id;
    RETURN_NOT_OK(GetTabletIdForKey(key, &tablet_id));
    scoped_refptr<TabletReplica> r;
    RETURN_NOT_OK(FindPeerForTablet(tablet_id, &r));

    HybridClock* clock = CHECK_NOTNULL(dynamic_cast<HybridClock*>(r->clock()));
    UpdateClock(clock, offset);
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
TEST_F(ConsistencyITest, TestTimestampPropagationFromScans) {
  uint64_t ts_a;
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    ASSERT_OK(CreateTable(client.get(), table_name_));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));

    // Advance tablet server's clock hosting the first key range
    // (i.e. for the row which is about to be inserted below).
    ASSERT_OK(UpdateClockForTabletHostingKey(
        0, MonoDelta::FromMilliseconds(100)));

    // Insert data into the first tablet (a.k.a. Ta).
    const int rows_num = key_split_value_;  // fill in the partition completely
    ASSERT_OK(InsertTestRows(client.get(), table.get(), rows_num, 0));
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
    const int rows_num = key_split_value_;
    ASSERT_OK(InsertTestRows(client.get(), table.get(),
                             rows_num, key_split_value_));
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

// Make sure the client propagates the timestamp for write operations.
//
// The idea of verification is simple:
//
//   * Let's get two tablet servers, where the clock of the first server
//     is ahead of the second one.
//
//   * Create a client object.
//
//   * Using the newly created client object, insert some data into the tablet
//     hosted by the first server.
//
//   * Record the client's latest observed timestamp.
//
//   * Using the same client object, insert some data into the tablet
//     hosted by the second server.
//
//   * Get the client's latest observed timestamp: it should be strictly greater
//     than the recorded timestamp.
//
//   * Make a full table scan in the READ_AT_SNAPSHOT mode at 'ts_ref'
//     timestamp: the scan should retrieve only the first row.
//
// If the client propates the timestamps, the second server should receive
// the recorded timestamp value in write request in the 'propagated_timestamp'
// field and adjust its clock first. After that it should perform the requested
// write operation. Since a write operation should always advance the server
// clock, the resulting timestamp returned to the client should be strictly
// greater than the propagated one.
TEST_F(ConsistencyITest, TestTimestampPropagationForWriteOps) {
  const int32_t offset_usec = FLAGS_max_clock_sync_error_usec;
  // Assuming the offset is specified as a positive number.
  ASSERT_GT(offset_usec, 0);
  // Need to have at least one row in the first partition starting with key 0.
  ASSERT_LE(1, key_split_value_);

  uint64_t ts_ref;
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    ASSERT_OK(CreateTable(client.get(), table_name_));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));

    // Advance tablet server's clock hosting the first key range
    // (i.e. for the row which is about to be inserted below).
    ASSERT_OK(UpdateClockForTabletHostingKey(
        0, MonoDelta::FromMicroseconds(offset_usec)));

    // Insert 1 row into the first tablet.
    ASSERT_OK(InsertTestRows(client.get(), table.get(), 1, 0));
    // Retrieve the latest observed timestamp.
    ts_ref = client->GetLatestObservedTimestamp();

    // Insert 1 row into the second tablet.
    ASSERT_OK(InsertTestRows(client.get(), table.get(), 1, key_split_value_));
    // Retrieve the latest observed timestamp.
    const uint64_t ts = client->GetLatestObservedTimestamp();

    // If the client propagates the timestamp with write operations,
    // the timestamp received from the second server should be greater
    // than the timestamp received from the first server.
    EXPECT_GT(ts, ts_ref);
  }

  // An additional check: scan the table at the 'ts_ref' timestamp and
  // make sure only the first row is visible.
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));

    size_t row_count;
    ASSERT_OK(GetRowCount(table.get(), KuduScanner::READ_AT_SNAPSHOT,
                          ts_ref, &row_count));
    EXPECT_EQ(1, row_count);
  }
}

// This is a test for KUDU-1189. It verifies that in case of a READ_AT_SNAPSHOT
// scan with unspecified snapshot timestamp, the scanner picks timestamp from
// the first server that the data is read from. If the scan spans multiple
// tablets, the timestamp picked when scanning the first tablet is then used
// when scanning following tablets.
//
// The idea of the test is simple: have a scan spanned across two tablets
// where the clocks of the corresponding tablet servers are skewed. The sequence
// of actions is as following:
//
//   1. Create a table which spans across two tablets.
//
//   2. Run the first scenario:
//      * Advance the clock of the second tablet's server.
//      * Create a client object and with it:
//        ** Insert a row into the first tablet.
//        ** Insert a row into the second tablet.
//      * Discard the client object.
//      * Create a new client object and perform a scan at READ_AT_SNAPSHOT
//        mode, no timestamp specified.
//      * Given the tight timings on the after-the-insert scan and difference in
//        server clocks, there should only one row in the result if the snapshot
//        timestamp is taken from the first server. Otherwise, if the snapshot
//        timestamp was taken from the second server, both rows would be visible
//        for the scan.
//      * Discard the client object.
//
//   3. Run the second scenario:
//      * Advance the clock of the first tablet's server, so the clock of the
//        first tablet is ahead of the clock of the second one.
//      * Create a client object and with it:
//        ** Insert an additional row into the first tablet.
//      * Discard the client object.
//      * Create a new client object and perform a scan at READ_AT_SNAPSHOT
//        mode, no timestamp specified.
//      * All the inserted rows should be visible to the scan because we
//        expect the snapshot timestamp to be taken from the first tablet
//        server. If the snapshot timestamp was taken from the second server,
//        given the tight timings on the scan following the prior insert into
//        the first tablet and difference in server clocks, not all rows would
//        be visible the the scan.
//
TEST_F(ConsistencyITest, TestSnapshotScanTimestampReuse) {
  const int32_t offset_usec = FLAGS_max_clock_sync_error_usec / 2;
  // Assuming the offset is specified as a positive number.
  ASSERT_GT(offset_usec, 0);
  // Need to have two rows in the first partition; the values start at 0.
  ASSERT_LT(2, key_split_value_);

  // Prepare the setup: create a proper disposition for tablet servers' clocks
  // and populate the table with appropriate data.
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    ASSERT_OK(CreateTable(client.get(), table_name_));
    // Advance second partition's tablet server clock.
    ASSERT_OK(UpdateClockForTabletHostingKey(
        key_split_value_, MonoDelta::FromMicroseconds(offset_usec)));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));
    // Insert a row into the first tablet.
    ASSERT_OK(InsertTestRows(client.get(), table.get(), 1, 0));
    // Insert a row into the second tablet.
    ASSERT_OK(InsertTestRows(client.get(), table.get(), 1, key_split_value_));
  }

  // Discarding the prior client object: if using it to perform scans, due
  // to the scan timestamp propagation the lagging tablet server's clock
  // would be advanced and it was not possible to distinguish between
  // the timestamps coming from the first and the second tablet servers.

  // Now, perform the scan at READ_AT_SNAPSHOT where a timestamp is not
  // specified: make sure the snapshot timestamp is taken from the first tablet
  // server among those the data was fetched from. For this scenario, perform
  // a scan which would try to fetch all the table's data
  // (i.e. make calls to all tablet servers which host table's data).
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    // Scan the table at a snapshot: let the servers pick the timestamp.
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
    const ScanConfiguration& cfg(scanner.data_->configuration());
    ASSERT_FALSE(cfg.has_snapshot_timestamp());

    size_t row_count;
    ASSERT_OK(CountRowsWithRetries(&scanner, &row_count));

    // At this point, we have inserted 2 rows in total, where the second row
    // was inserted into the tablet which server's clock was advanced
    // (i.e. shifted into the future). We are expecting to get the timestamp
    // for the scan from the first tablet server, so the second row should not
    // be visible at that timestamp: from the second tablet server's view,
    // it was inserted after the specified timestamp. Instead, if the timestamp
    // for the scan were sampled at the second server's clock, then both rows
    // would be visible to the scan.
    ASSERT_EQ(1UL, row_count);
    ASSERT_TRUE(cfg.has_snapshot_timestamp());
  }

  // Advance the clock of the first server even further, leaving the clock
  // of the second server behind. Also, insert an additional row into the first
  // tablet.
  {
    // Find the tablet for the first range to advance its server's clock.
    ASSERT_OK(UpdateClockForTabletHostingKey(
        0, MonoDelta::FromMicroseconds(2 * offset_usec)));
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));
    // Insert an additional row into the first tablet.
    // This is to check that the timestamp is taken from the first tablet
    // server: since now the clocks of both tablet servers are ahead of the
    // timestamps of the inserted rows so far, there would be no way to tell
    // which server's clock is used for the scan using the number of rows
    // returned by the scan. In either case, there will be two rows.
    //
    // Now, once we add a new row into the first tablet, given the big time
    // margin provided by the current clock offset, we should see different
    // outcomes from the subsequent scan:
    //   * if the timestamp is taken from the first server, there should be
    //     three rows in the result
    //   * if the timestamp is taken from the second server, there should be
    //     just two rows in the result
    ASSERT_OK(InsertTestRows(client.get(), table.get(), 1, 1));
  }

  // Scan the table again and make sure the snapshot scan's timestamp is taken
  // from the first tablet server, as before. However, now the clock of the
  // first tablet server is ahead of the second tablet server's clock. If the
  // timestamp was taken from the second server, there would be 2 rows
  // in the result. The expected result is 3 rows, since the timestamp should
  // be taken from the first server.
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    // Scan the table at snapshot: let the servers pick the timestamp.
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
    const ScanConfiguration& cfg(scanner.data_->configuration());
    // Check the snapshot timestamp is unset -- it's a fresh object for a
    // READ_AT_SNAPSHOT scan where the snapshot timestamp is not specified
    // explicitly.
    ASSERT_FALSE(cfg.has_snapshot_timestamp());

    size_t row_count;
    ASSERT_OK(CountRowsWithRetries(&scanner, &row_count));

    // At this point, we have inserted 3 rows in total. Since the snapshot
    // timestamp is taken from the first server's clock, all 3 rows should be
    // visible to the scan at that timestamp. Given the tight timings on the
    // after-the-intsert scan and difference in server clocks, that would not be
    // the case if the snapshot was taken from the second server.
    ASSERT_EQ(3UL, row_count);
    // Check that the timestamp returned by the tablet server is set into the
    // scan configuration.
    ASSERT_TRUE(cfg.has_snapshot_timestamp());
  }
}

// Verify that the propagated timestamp from a serialized scan token
// makes its way into corresponding tablet servers while performing a scan
// operation built from the token.
//
// The real-world use-cases behind this test assume a Kudu client (C++/Java)
// can get scan tokens for a scan operation, serialize those and pass them
// to the other Kudu client (C++/Java). Since de-serializing a scan token
// propagates the latest observed timestamp, the latter client will have
// the latest observed timestamp set accordingly if it de-serializes those
// scan tokens into corresponding scan operations.
//
// The test scenario uses a table split into two tablets, each hosted by a
// tablet server. The clock of the first tablet server is shifted into the
// future. The first client inserts a row into the first tablet. Then it creates
// a scan token to retrieve some "related" data from the second
// tablet hosted by the second server. Now, another client receives the
// serialized scan token and runs corresponding READ_AT_SNAPSHOT scan
// with the specified timestamp to retrieve the data: it should observe
// a timestamp which is not less than the propagated timestamp
// encoded in the token.
TEST_F(ConsistencyITest, TestScanTokenTimestampPropagation) {
  const int32_t offset_usec = FLAGS_max_clock_sync_error_usec;

  // Need to have at least one row in the first partition with
  // values starting at 0.
  ASSERT_GE(key_split_value_, 1);

  {
    // Prepare the setup: create a proper disposition for tablet servers' clocks
    // and populate the table with initial data.
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    ASSERT_OK(CreateTable(client.get(), table_name_));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));

    // Insert a single row into the second tablet: it's necessary to get
    // non-empty scan in the verification phase of the test.
    ASSERT_OK(InsertTestRows(client.get(), table.get(), 1, key_split_value_));
  }

  uint64_t ts_ref;
  string scan_token;
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));

    // Advance the clock of the server hosting the first partition tablet.
    const int32_t row_key = 0;
    ASSERT_OK(UpdateClockForTabletHostingKey(
        row_key, MonoDelta::FromMicroseconds(offset_usec)));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));
    // Insert just a single row into the first tablet.
    ASSERT_OK(InsertTestRows(client.get(), table.get(), 1, row_key));
    ts_ref = client->GetLatestObservedTimestamp();

    // Create and serialize a scan token: the scan selects a row by its key
    // from the other tablet at the timestamp at which the first row was
    // inserted.
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    ASSERT_OK(builder.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        key_column_name_,
        KuduPredicate::EQUAL,
        KuduValue::FromInt(key_split_value_)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));
    ASSERT_EQ(1, tokens.size());
    ASSERT_OK(tokens[0]->Serialize(&scan_token));
  }

  // The other client: scan the second tablet using a scanner built from
  // the serialized scanner token. If the client propagates timestamp from the
  // de-serialized scan token, upon fetching a batch of rows the client
  // should observe timestamp not less than the reference propagated timestamp
  // encoded in the token.
  {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(table_name_, &table));
    KuduScanner* scanner_raw;
    ASSERT_OK(KuduScanToken::DeserializeIntoScanner(client.get(), scan_token,
                                                    &scanner_raw));
    unique_ptr<KuduScanner> scanner(scanner_raw);
    ASSERT_OK(scanner->Open());
    ASSERT_TRUE(scanner->HasMoreRows());
    size_t row_count = 0;
    while (scanner->HasMoreRows()) {
      KuduScanBatch batch;
      ASSERT_OK(scanner->NextBatch(&batch));
      row_count += batch.NumRows();
      ASSERT_LE(ts_ref, client->GetLatestObservedTimestamp());
    }
    EXPECT_EQ(1, row_count);
  }
}

} // namespace client
} // namespace kudu
