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
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/mock_ntp.h"
#include "kudu/clock/time_service.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduScanner;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::clock::HybridClock;
using kudu::tablet::Tablet;
using kudu::tablet::TabletReplica;
using kudu::tserver::MiniTabletServer;
using kudu::tserver::TabletServer;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(enable_maintenance_manager);
DECLARE_bool(enable_rowset_compaction);
DECLARE_string(time_source);
DECLARE_double(missed_heartbeats_before_rejecting_snapshot_scans);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(maintenance_manager_num_threads);
DECLARE_int32(maintenance_manager_polling_interval_ms);
DECLARE_int32(safe_time_max_lag_ms);
DECLARE_int32(scanner_ttl_ms);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_int32(undo_delta_block_gc_init_budget_millis);

DEFINE_int32(test_num_rounds, 200, "Number of rounds to loop "
                                   "RandomizedTabletHistoryGcITest.TestRandomHistoryGCWorkload");

namespace kudu {

class TabletHistoryGcITest : public MiniClusterITestBase {
 protected:
  void AddTimeToHybridClock(HybridClock* clock, MonoDelta delta) {
    uint64_t now = HybridClock::GetPhysicalValueMicros(clock->Now());
    uint64_t new_time = now + delta.ToMicroseconds();
    auto* ntp = down_cast<clock::MockNtp*>(clock->time_service());
    ntp->SetMockClockWallTimeForTests(new_time);
  }
};

// Check that attempts to scan prior to the ancient history mark fail.
TEST_F(TabletHistoryGcITest, TestSnapshotScanBeforeAHM) {
  FLAGS_tablet_history_max_age_sec = 0;

  NO_FATALS(StartCluster());

  // Create a tablet so we can scan it.
  TestWorkload workload(cluster_.get());
  workload.Setup();

  // When the tablet history max age is set to 0, it's not possible to do a
  // snapshot scan without a timestamp because it's illegal to open a snapshot
  // prior to the AHM. When a snapshot timestamp is not specified, we decide on
  // the timestamp of the snapshot before checking that it's lower than the
  // current AHM. This test verifies that scans prior to the AHM are rejected.
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(TestWorkload::kDefaultTableName, &table));
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  Status s = scanner.Open();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Snapshot timestamp is earlier than the ancient history mark");
}

// Check that the maintenance manager op to delete undo deltas actually deletes them.
TEST_F(TabletHistoryGcITest, TestUndoDeltaBlockGc) {
  // Disable rowset compaction since it also does undo block GC.
  FLAGS_enable_rowset_compaction = false;
  FLAGS_flush_threshold_secs = 0; // Flush as aggressively as possible.
  FLAGS_maintenance_manager_num_threads = 4; // Encourage concurrency.
  FLAGS_maintenance_manager_polling_interval_ms = 1; // Spin on MM for a quick test.
  FLAGS_tablet_history_max_age_sec = 1000;
  FLAGS_time_source = "mock"; // Allow moving the clock.

  NO_FATALS(StartCluster(1)); // Single-node cluster.

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(TestWorkload::kDefaultTableName, &table));
  shared_ptr<KuduSession> session = client_->NewSession();

  // Find the tablet.
  MiniTabletServer* mts = cluster_->mini_tablet_server(0);
  vector<scoped_refptr<TabletReplica>> tablet_replicas;
  mts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
  ASSERT_EQ(1, tablet_replicas.size());
  std::shared_ptr<Tablet> tablet = tablet_replicas[0]->shared_tablet();

  const int32_t kNumRows = AllowSlowTests() ? 100 : 10;

  // Insert a few rows.
  for (int32_t row_key = 0; row_key < kNumRows; row_key++) {
    unique_ptr<client::KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    ASSERT_OK_FAST(row->SetInt32(0, row_key));
    ASSERT_OK_FAST(row->SetInt32(1, 0));
    ASSERT_OK_FAST(row->SetString(2, ""));
    ASSERT_OK_FAST(session->Apply(insert.release()));
  }
  ASSERT_OK_FAST(session->Flush());

  // Update rows in a loop; wait until some undo deltas are generated.
  int32_t row_value = 0;
  while (true) {
    for (int32_t row_key = 0; row_key < kNumRows; row_key++) {
      unique_ptr<client::KuduUpdate> update(table->NewUpdate());
      KuduPartialRow* row = update->mutable_row();
      ASSERT_OK_FAST(row->SetInt32(0, row_key));
      ASSERT_OK_FAST(row->SetInt32(1, row_value));
      ASSERT_OK_FAST(session->Apply(update.release()));
    }
    ASSERT_OK_FAST(session->Flush());

    VLOG(1) << "Number of undo deltas: " << tablet->CountUndoDeltasForTests();
    VLOG(1) << "Number of redo deltas: " << tablet->CountRedoDeltasForTests();

    // Only break out of the loop once we have undos.
    if (tablet->CountUndoDeltasForTests() > 0) break;

    SleepFor(MonoDelta::FromMilliseconds(5));
    row_value++;
  }

  // Manually flush all mem stores so that we can measure the maximum disk
  // size before moving the AHM. This ensures the test isn't flaky.
  ASSERT_OK(tablet->Flush());
  ASSERT_OK(tablet->FlushAllDMSForTests());

  uint64_t measured_size_before_gc = 0;
  ASSERT_OK(Env::Default()->GetFileSizeOnDiskRecursively(cluster_->GetTabletServerFsRoot(0),
                                                         &measured_size_before_gc));

  // Move the clock so all operations are in the past. Then wait until we have
  // no more undo deltas.
  HybridClock* c = down_cast<HybridClock*>(tablet->clock().get());
  AddTimeToHybridClock(c, MonoDelta::FromSeconds(FLAGS_tablet_history_max_age_sec));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, tablet->CountUndoDeltasForTests());
  });

  // Verify that the latest values are still there.
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  ASSERT_OK(scanner.Open());
  int num_rows_scanned = 0;
  while (scanner.HasMoreRows()) {
    client::KuduScanBatch batch;
    ASSERT_OK(scanner.NextBatch(&batch));
    for (const auto& row : batch) {
      int32_t key;
      int32_t value;
      ASSERT_OK(row.GetInt32("key", &key));
      ASSERT_OK(row.GetInt32("int_val", &value));
      ASSERT_LT(key, kNumRows);
      ASSERT_EQ(row_value, value) << "key=" << key << ", int_val=" << value;
      num_rows_scanned++;
    }
  }
  ASSERT_EQ(kNumRows, num_rows_scanned);

  // Check that the tablet metrics have reasonable values.
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(tablet->metrics()->undo_delta_block_gc_init_duration->TotalCount(), 0);
    ASSERT_GT(tablet->metrics()->undo_delta_block_gc_delete_duration->TotalCount(), 0);
    ASSERT_GT(tablet->metrics()->undo_delta_block_gc_perform_duration->TotalCount(), 0);
    ASSERT_EQ(0, tablet->metrics()->undo_delta_block_gc_running->value());
    ASSERT_GT(tablet->metrics()->undo_delta_block_gc_bytes_deleted->value(), 0);
    ASSERT_EQ(0, tablet->metrics()->undo_delta_block_estimated_retained_bytes->value());

    // Check that we are now using less space.
    // We manually flush the tablet metadata here because the list of orphaned
    // blocks may take up space.
    ASSERT_OK(tablet->metadata()->Flush());
    uint64_t measured_size_after_gc = 0;
    ASSERT_OK(Env::Default()->GetFileSizeOnDiskRecursively(cluster_->GetTabletServerFsRoot(0),
                                                           &measured_size_after_gc));
    ASSERT_LT(measured_size_after_gc, measured_size_before_gc);
  });
}

// Whether a MaterializedTestRow is deleted or not.
enum IsDeleted {
  NOT_DELETED,
  DELETED
};

// Test row. Schema follows SimpleTestSchema.
struct MaterializedTestRow {
  int32_t key;
  int32_t int_val;
  string string_val;
  IsDeleted is_deleted;
};

// Randomized test that performs a repeatable sequence of events on a tablet.
class RandomizedTabletHistoryGcITest : public TabletHistoryGcITest {
 public:
  RandomizedTabletHistoryGcITest() {
    // We need to fully control compactions, flushes, and the clock.
    FLAGS_enable_maintenance_manager = false;
    FLAGS_time_source = "mock";
    FLAGS_tablet_history_max_age_sec = 100;

    // Set these really high since we're using the mock clock.
    // This allows the TimeManager to still work.
    FLAGS_safe_time_max_lag_ms = 30 * 1000 * 1000;
    FLAGS_missed_heartbeats_before_rejecting_snapshot_scans = 100.0;
  }

 protected:
  enum Actions {
    kInsert,
    kUpdate,
    kDelete,
    kReinsert,
    kFlush,
    kMergeCompaction,
    kRedoDeltaCompaction,
    kUndoDeltaBlockGc,
    kMoveTimeForward,
    kStartScan,
    kNumActions, // Count of items in this enum. Keep as last entry.
  };

  // Provide efficient sorted access to a snapshot of a table indexed by key.
  using MaterializedTestTable = std::map<int32_t, MaterializedTestRow>;

  // Timestamp value (from Timestamp::ToUint64()) -> Table snapshot.
  using MaterializedTestSnapshots = std::map<uint64_t, MaterializedTestTable>;

  using ScannerTSPair = std::pair<unique_ptr<client::KuduScanner>, Timestamp>;

  // verify_round -> { scanner, snapshot_timestamp }.
  using ScannerMap = std::multimap<int, ScannerTSPair>;

  string StringifyTestRow(const MaterializedTestRow& row) {
    return Substitute("{ $0, $1, $2, $3 }", row.key, row.int_val, row.string_val,
                      (row.is_deleted == DELETED) ? "DELETED" : "NOT_DELETED");
  }

  string StringifyTimestamp(const Timestamp& ts) {
    return Substitute("$0 ($1)", HybridClock::StringifyTimestamp(ts), ts.ToString());
  }

  MaterializedTestTable CloneLatestSnapshot() {
    return snapshots_[latest_snapshot_ts_.ToUint64()]; // Will auto-vivify on first pass.
  }

  MaterializedTestTable* GetPtrToLatestSnapshot() {
    return &snapshots_[latest_snapshot_ts_.ToUint64()];
  }

  MaterializedTestTable* GetPtrToSnapshotForTS(Timestamp ts) {
    MaterializedTestTable* table = FindFloorOrNull(snapshots_, ts.ToUint64());
    if (!table) {
      LOG(FATAL) << "There is no saved snapshot with a TS <= " << ts.ToUint64();
    }
    return table;
  }

  void SaveSnapshot(MaterializedTestTable snapshot, const Timestamp& ts) {
    VLOG(2) << "Saving snapshot at ts = " << StringifyTimestamp(ts);
    snapshots_[ts.ToUint64()] = std::move(snapshot);
    latest_snapshot_ts_ = ts;
  }

  void RegisterScanner(unique_ptr<client::KuduScanner> scanner, Timestamp snap_ts,
                       int verify_round) {
    CHECK_GE(verify_round, cur_round_);
    if (verify_round == cur_round_) {
      NO_FATALS(VerifySnapshotScan(std::move(scanner), snap_ts, verify_round));
      return;
    }
    ScannerTSPair pair(std::move(scanner), snap_ts);
    ScannerMap::value_type entry(verify_round, std::move(pair));
    scanners_.insert(std::move(entry));
  }

  void VerifyAndRemoveScanners(ScannerMap::iterator begin, ScannerMap::iterator end) {
    auto iter = begin;
    while (iter != end) {
      int verify_round = iter->first;
      unique_ptr<KuduScanner>& scanner = iter->second.first;
      Timestamp snap_ts = iter->second.second;
      NO_FATALS(VerifySnapshotScan(std::move(scanner), snap_ts, verify_round));
      auto old_iter = iter;
      ++iter;
      scanners_.erase(old_iter);
    }
  }

  void VerifyScannersForRound(int round) {
    auto iters = scanners_.equal_range(round);
    NO_FATALS(VerifyAndRemoveScanners(iters.first, iters.second));
  }

  void VerifyAllRemainingScanners() {
    VLOG(1) << "Verifying all remaining scanners";
    NO_FATALS(VerifyAndRemoveScanners(scanners_.begin(), scanners_.end()));
  }

  void VerifySnapshotScan(unique_ptr<client::KuduScanner> scanner, Timestamp snap_ts, int round) {
    LOG(INFO) << "Round " << round << ": Verifying snapshot scan for timestamp "
              << StringifyTimestamp(snap_ts);
    MaterializedTestTable* snap = GetPtrToSnapshotForTS(snap_ts);
    ASSERT_NE(snap, nullptr) << "Could not find snapshot to match timestamp " << snap_ts.ToString();
    int32_t rows_seen = 0;
    auto snap_iter = snap->cbegin();
    // Maintain a summary of mismatched keys for use in debugging.
    vector<int32_t> mismatched_keys;
    while (scanner->HasMoreRows()) {
      client::KuduScanBatch batch;
      ASSERT_OK_FAST(scanner->NextBatch(&batch));
      auto scan_iter = batch.begin();
      while (scan_iter != batch.end()) {
        // Deleted rows will show up in our verification snapshot, but not the
        // tablet scanner.
        if (snap_iter->second.is_deleted == DELETED) {
          VLOG(4) << "Row " << snap_iter->second.key << " is DELETED in our historical snapshot";
          ++snap_iter;
          continue;
        }
        int32_t key;
        int32_t int_val;
        Slice string_val;
        ASSERT_OK_FAST((*scan_iter).GetInt32(0, &key));
        ASSERT_OK_FAST((*scan_iter).GetInt32(1, &int_val));
        ASSERT_OK_FAST((*scan_iter).GetString(2, &string_val));
        // We attempt to compare both snapshots fully, even in the case of
        // failure, to help us understand what's going on when problems occur.
        EXPECT_EQ(snap_iter->second.key, key) << "Mismatch at result row number " << rows_seen;
        EXPECT_EQ(snap_iter->second.int_val, int_val) << "at row key " << key;
        EXPECT_EQ(snap_iter->second.string_val, string_val.ToString()) << "at row key " << key;
        ++rows_seen;
        if (key == snap_iter->second.key) {
          // Move both (the normal case)
          ++scan_iter;
          ++snap_iter;
        } else if (key < snap_iter->second.key) {
          mismatched_keys.push_back(key);
          ++scan_iter; // Only move scan to try to catch up.
        } else {
          mismatched_keys.push_back(snap_iter->second.key);
          ++snap_iter; // Only move snap to try to catch up.
        }
      }
    }
    // Account for trailing deleted rows in our verification snapshot.
    while (snap_iter != snap->cend()) {
      EXPECT_EQ(DELETED, snap_iter->second.is_deleted)
          << "Expected row " << snap_iter->second.key << " to be DELETED";
      if (!snap_iter->second.is_deleted) {
        mismatched_keys.push_back(snap_iter->second.key);
      }
      ++snap_iter;
    }
    ASSERT_EQ(snap->cend(), snap_iter);
    scanner->Close();
    if (::testing::Test::HasFailure()) {
      for (int32_t key : mismatched_keys) {
        LOG(ERROR) << "Mismatched key: " << key;
      }
      FAIL() << "Snapshot verification failed";
    }

    LOG(INFO) << "Snapshot verification complete. Rows seen: " << rows_seen;
  }

  MaterializedTestSnapshots snapshots_;
  std::unordered_set<int32_t> deleted_rows_;
  ScannerMap scanners_;
  HybridClock* clock_ = nullptr;

  Timestamp latest_snapshot_ts_;
  int cur_round_ = 0;
};

// Hooks to force a reupdate of missing deltas when a flush or merge compaction
// occurs.
class ReupdateHooks : public Tablet::FlushCompactCommonHooks {
 public:
  ReupdateHooks(Tablet* tablet, const Schema& schema)
      : tablet_(tablet),
        client_schema_(schema) {
  }

  Status PostWriteSnapshot() OVERRIDE {
    tablet::LocalTabletWriter writer(tablet_, &client_schema_);
    for (const MaterializedTestRow& update : updates_) {
      KuduPartialRow row(&client_schema_);
      CHECK_OK(row.SetInt32(0, update.key));
      CHECK_OK(row.SetInt32(1, update.int_val));
      CHECK_OK(row.SetStringCopy(2, update.string_val));
      CHECK_OK(writer.Update(row));
    }
    for (int32_t row_key : deletes_) {
      KuduPartialRow row(&client_schema_);
      CHECK_OK(row.SetInt32(0, row_key));
      CHECK_OK(writer.Delete(row));
    }
    for (const MaterializedTestRow& reinsert : reinserts_) {
      KuduPartialRow row(&client_schema_);
      CHECK_OK(row.SetInt32(0, reinsert.key));
      CHECK_OK(row.SetInt32(1, reinsert.int_val));
      CHECK_OK(row.SetStringCopy(2, reinsert.string_val));
      CHECK_OK(writer.Insert(row));
    }
    return Status::OK();
  }

  void set_updates(vector<MaterializedTestRow> updates) {
    updates_ = std::move(updates);
  }

  void set_deletes(vector<int32_t> deletes) {
    deletes_ = std::move(deletes);
  }

  void set_reinserts(vector<MaterializedTestRow> reinserts) {
    reinserts_ = std::move(reinserts);
  }

  void Reset() {
    updates_.clear();
    deletes_.clear();
    reinserts_.clear();
  }

 private:
  Tablet* const tablet_;
  const Schema client_schema_;

  vector<MaterializedTestRow> updates_;
  vector<int32_t> deletes_;
  vector<MaterializedTestRow> reinserts_;
};

// Randomized test that attempts to test many arbitrary history GC use cases.
TEST_F(RandomizedTabletHistoryGcITest, TestRandomHistoryGCWorkload) {
  constexpr auto kSessionTimeoutMillis = 60000;

# if !defined(THREAD_SANITIZER)
  OverrideFlagForSlowTests("test_num_rounds",
                           Substitute("$0", FLAGS_test_num_rounds * 5));
# endif

  LOG(INFO) << "Running " << FLAGS_test_num_rounds << " rounds";

  // Set high scanner TTL, since this test opens scanners and then waits for some
  // time before reading from them.
  FLAGS_scanner_ttl_ms = 1000 * 60 * 60 * 24;

  StartCluster(1); // Start InternalMiniCluster with a single tablet server.
  // Since we're using mock NTP rather than the hybrid clock, it's possible
  // that if we created a tablet now, the first timestamp assigned to a tablet
  // message would be the initial timestamp (0). For correctness of scans, it
  // is illegal to scan in this state. As such, we bump the clock up front so
  // when we create tablets, they start out with more natural, non-0 values.
  MiniTabletServer* mts = cluster_->mini_tablet_server(0);

  // Directly access the tserver so we can control compaction and the clock.
  TabletServer* ts = mts->server();
  clock_ = down_cast<HybridClock*>(ts->clock());

  // Set initial clock time to 1000 seconds past 0, which is enough so that the
  // AHM will not be negative.
  const uint64_t kInitialMicroTime = 1L * 1000 * 1000 * 1000;
  auto* ntp = down_cast<clock::MockNtp*>(clock_->time_service());
  ntp->SetMockClockWallTimeForTests(kInitialMicroTime);

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup(); // Convenient way to create a table.

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(workload.table_name(), &table));

  std::vector<scoped_refptr<TabletReplica>> replicas;
  ts->tablet_manager()->GetTabletReplicas(&replicas);
  ASSERT_EQ(1, replicas.size());
  Tablet* tablet = replicas[0]->tablet();

  LOG(INFO) << "Seeding random number generator";
  Random random(SeedRandom());

  // Save an empty snapshot at the "beginning of time";
  NO_FATALS(SaveSnapshot(MaterializedTestTable(), Timestamp(0)));

  int32_t rows_inserted = 0;

  for (cur_round_ = 0; cur_round_ < FLAGS_test_num_rounds; cur_round_++) {
    VLOG(1) << "Starting round " << cur_round_;
    NO_FATALS(VerifyScannersForRound(cur_round_));

    int action = random.Uniform(kNumActions);
    switch (action) {
      case kInsert: {
        int32_t num_rows_to_insert = random.Uniform(1000);
        VLOG(1) << "Inserting " << num_rows_to_insert << " rows";
        if (num_rows_to_insert == 0) continue;
        MaterializedTestTable snapshot = CloneLatestSnapshot();

        shared_ptr<KuduSession> session = client_->NewSession();
        session->SetTimeoutMillis(kSessionTimeoutMillis);
        ASSERT_OK_FAST(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

        for (int32_t i = 0; i < num_rows_to_insert; i++) {
          int32_t row_key = rows_inserted;
          MaterializedTestRow test_row = { row_key,
                                           static_cast<int32_t>(random.Next()),
                                           Substitute("$0", random.Next()),
                                           NOT_DELETED };
          unique_ptr<client::KuduInsert> insert(table->NewInsert());
          KuduPartialRow* row = insert->mutable_row();
          ASSERT_OK_FAST(row->SetInt32(0, test_row.key));
          ASSERT_OK_FAST(row->SetInt32(1, test_row.int_val));
          ASSERT_OK_FAST(row->SetString(2, test_row.string_val));
          ASSERT_OK_FAST(session->Apply(insert.release()));

          VLOG(2) << "Inserting row " << StringifyTestRow(test_row);
          snapshot[row_key] = std::move(test_row);
          rows_inserted++;
        }
        FlushSessionOrDie(session);
        SaveSnapshot(std::move(snapshot), clock_->Now());
        break;
      }
      case kUpdate: {
        if (rows_inserted == 0) continue;
        int32_t num_rows_to_update = random.Uniform(std::min(rows_inserted, 1000));
        VLOG(1) << "Updating up to " << num_rows_to_update << " rows";
        if (num_rows_to_update == 0) continue;

        MaterializedTestTable snapshot = CloneLatestSnapshot();

        // 5% chance to reupdate while also forcing a full compaction.
        bool force_reupdate_missed_deltas = random.OneIn(20);
        if (force_reupdate_missed_deltas) {
          VLOG(1) << "Forcing a reupdate of missed deltas";
        }

        vector<MaterializedTestRow> updates;
        for (int i = 0; i < num_rows_to_update; i++) {
          int32_t row_key = random.Uniform(rows_inserted);
          MaterializedTestRow* test_row = &snapshot[row_key];
          ASSERT_EQ(row_key, test_row->key) << "Rows inserted: " << rows_inserted
              << ", row: " << StringifyTestRow(*test_row);
          if (test_row->is_deleted == DELETED) continue;

          test_row->int_val = random.Next();
          test_row->string_val = Substitute("$0", random.Next());

          VLOG(2) << "Updating row to " << StringifyTestRow(*test_row);
          updates.push_back(*test_row);
        }

        int rows_updated = updates.size();
        if (rows_updated == 0) continue;

        if (force_reupdate_missed_deltas) {
          std::shared_ptr<ReupdateHooks> hooks =
              std::make_shared<ReupdateHooks>(tablet, GetSimpleTestSchema());
          hooks->set_updates(std::move(updates));
          tablet->SetFlushCompactCommonHooksForTests(hooks);
          ASSERT_OK(tablet->Compact(Tablet::FORCE_COMPACT_ALL));
          tablet->SetFlushCompactCommonHooksForTests(nullptr); // Clear the hook.
        } else {
          shared_ptr<KuduSession> session = client_->NewSession();
          session->SetTimeoutMillis(kSessionTimeoutMillis);
          ASSERT_OK_FAST(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

          for (const MaterializedTestRow& test_row : updates) {
            unique_ptr<client::KuduUpdate> update(table->NewUpdate());
            KuduPartialRow* row = update->mutable_row();
            ASSERT_OK_FAST(row->SetInt32(0, test_row.key));
            ASSERT_OK_FAST(row->SetInt32(1, test_row.int_val));
            ASSERT_OK_FAST(row->SetString(2, test_row.string_val));
            ASSERT_OK_FAST(session->Apply(update.release()));
          }
          FlushSessionOrDie(session);
        }
        SaveSnapshot(std::move(snapshot), clock_->Now());
        VLOG(1) << "Updated " << rows_updated << " rows";
        break;
      }
      case kDelete: {
        if (rows_inserted == 0) continue;
        int32_t num_rows_to_delete = random.Uniform(std::min(rows_inserted, 1000));
        VLOG(1) << "Deleting up to " << num_rows_to_delete << " rows";
        if (num_rows_to_delete == 0) continue;

        MaterializedTestTable snapshot = CloneLatestSnapshot();

        // 5% chance to reupdate while also forcing a full compaction.
        bool force_reupdate_missed_deltas = random.OneIn(20);
        if (force_reupdate_missed_deltas) {
          VLOG(1) << "Forcing a reupdate of missed deltas";
        }

        vector<int32_t> deletes;
        for (int i = 0; i < num_rows_to_delete; i++) {
          int32_t row_key = random.Uniform(rows_inserted);
          MaterializedTestRow* test_row = &snapshot[row_key];
          CHECK_EQ(row_key, test_row->key);

          if (test_row->is_deleted == DELETED) {
            VLOG(2) << "Row " << test_row->key << " is already deleted";
            continue;
          }

          test_row->is_deleted = DELETED;
          VLOG(2) << "Deleting row " << StringifyTestRow(*test_row);
          deletes.push_back(row_key);
        }

        int rows_deleted = deletes.size();
        if (rows_deleted == 0) continue;

        deleted_rows_.insert(deletes.begin(), deletes.end());

        if (force_reupdate_missed_deltas) {
          std::shared_ptr<ReupdateHooks> hooks =
              std::make_shared<ReupdateHooks>(tablet, GetSimpleTestSchema());
          hooks->set_deletes(std::move(deletes));
          tablet->SetFlushCompactCommonHooksForTests(hooks);
          ASSERT_OK(tablet->Compact(Tablet::FORCE_COMPACT_ALL));
          tablet->SetFlushCompactCommonHooksForTests(nullptr); // Clear the hook.
        } else {
          shared_ptr<KuduSession> session = client_->NewSession();
          session->SetTimeoutMillis(kSessionTimeoutMillis);
          ASSERT_OK_FAST(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

          for (int32_t row_key : deletes) {
            unique_ptr<client::KuduDelete> del(table->NewDelete());
            KuduPartialRow* row = del->mutable_row();
            ASSERT_OK_FAST(row->SetInt32(0, row_key));
            ASSERT_OK_FAST(session->Apply(del.release()));
          }
          FlushSessionOrDie(session);
        }
        SaveSnapshot(std::move(snapshot), clock_->Now());
        VLOG(1) << "Deleted " << rows_deleted << " rows";
        break;
      }
      case kReinsert: {
        if (rows_inserted == 0) continue;
        int32_t max_rows_to_reinsert = random.Uniform(std::min(rows_inserted, 1000));
        VLOG(1) << "Reinserting up to " << max_rows_to_reinsert << " rows";
        if (max_rows_to_reinsert == 0) continue;
        int num_deleted_rows = deleted_rows_.size();
        if (num_deleted_rows == 0) continue;


        // 5% chance to reupdate while also forcing a full compaction.
        bool force_reupdate_missed_deltas = random.OneIn(20);
        if (force_reupdate_missed_deltas) {
          VLOG(1) << "Forcing a reupdate of missed deltas";
        }

        MaterializedTestTable snapshot = CloneLatestSnapshot();

        vector<MaterializedTestRow> reinserts;
        for (int i = 0; i < max_rows_to_reinsert; i++) {
          const int32_t row_key = *std::next(deleted_rows_.begin(),
                                             random.Uniform(num_deleted_rows));
          MaterializedTestRow* test_row = &snapshot[row_key];
          ASSERT_EQ(row_key, test_row->key);

          if (test_row->is_deleted == NOT_DELETED) {
            VLOG(2) << "Row " << test_row->key << " is already reinserted";
            continue;
          }

          test_row->int_val = random.Next();
          test_row->string_val = Substitute("$0", random.Next());
          test_row->is_deleted = NOT_DELETED;

          VLOG(2) << "Reinserting row " << StringifyTestRow(*test_row);
          reinserts.push_back(*test_row);
        }

        if (reinserts.empty()) continue;

        for (const MaterializedTestRow& test_row : reinserts) {
          deleted_rows_.erase(test_row.key);
        }

        int num_reinserted = reinserts.size();

        if (force_reupdate_missed_deltas) {
          std::shared_ptr<ReupdateHooks> hooks =
              std::make_shared<ReupdateHooks>(tablet, GetSimpleTestSchema());
          hooks->set_reinserts(std::move(reinserts));
          tablet->SetFlushCompactCommonHooksForTests(hooks);
          ASSERT_OK(tablet->Compact(Tablet::FORCE_COMPACT_ALL));
          tablet->SetFlushCompactCommonHooksForTests(nullptr); // Clear the hook.
        } else {
          shared_ptr<KuduSession> session = client_->NewSession();
          session->SetTimeoutMillis(kSessionTimeoutMillis);
          ASSERT_OK_FAST(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

          for (const MaterializedTestRow& test_row : reinserts) {
            unique_ptr<client::KuduInsert> reinsert(table->NewInsert());
            KuduPartialRow* row = reinsert->mutable_row();
            ASSERT_OK_FAST(row->SetInt32(0, test_row.key));
            ASSERT_OK_FAST(row->SetInt32(1, test_row.int_val));
            ASSERT_OK_FAST(row->SetString(2, test_row.string_val));
            ASSERT_OK_FAST(session->Apply(reinsert.release()));
          }
          FlushSessionOrDie(session);
        }
        SaveSnapshot(std::move(snapshot), clock_->Now());
        VLOG(1) << "Reinserted " << num_reinserted << " rows";
        break;
      }
      case kFlush: {
        if (random.OneIn(2)) {
          VLOG(1) << "Flushing tablet";
          ASSERT_OK(tablet->Flush());
        } else {
          VLOG(1) << "Flushing biggest DMS";
          ASSERT_OK(tablet->FlushBiggestDMS());
        }
        break;
      }
      case kMergeCompaction: {
        // TODO: Randomize which rowsets get merged.
        VLOG(1) << "Running merge compaction";
        ASSERT_OK(tablet->Compact(Tablet::COMPACT_NO_FLAGS));
        break;
      }
      case kRedoDeltaCompaction: {
        // TODO: Randomize which deltas / projections get compacted.
        bool major = random.OneIn(2);
        VLOG(1) << "Running " << (major ? "major" : "minor") << " delta compaction";
        ASSERT_OK(tablet->CompactWorstDeltas(major ? tablet::RowSet::MAJOR_DELTA_COMPACTION :
                                                     tablet::RowSet::MINOR_DELTA_COMPACTION));
        break;
      }
      case kUndoDeltaBlockGc: {
        VLOG(1) << "Running UNDO delta block GC";
        ASSERT_OK(tablet->InitAncientUndoDeltas(
            MonoDelta::FromMilliseconds(FLAGS_undo_delta_block_gc_init_budget_millis), nullptr));
        int64_t blocks_deleted;
        int64_t bytes_deleted;
        ASSERT_OK(tablet->DeleteAncientUndoDeltas(&blocks_deleted, &bytes_deleted));
        // If one of these equals zero, both should equal zero.
        if (blocks_deleted == 0 || bytes_deleted == 0) {
          ASSERT_EQ(0, blocks_deleted);
          ASSERT_EQ(0, bytes_deleted);
        }
        VLOG(1) << "UNDO delta block GC deleted " << blocks_deleted
                << " blocks and " << bytes_deleted << " bytes";
        break;
      }
      case kMoveTimeForward: {
        VLOG(1) << "Moving clock forward";
        AddTimeToHybridClock(clock_, MonoDelta::FromSeconds(200));
        break;
      }
      case kStartScan: {
        int read_delay_rounds = random.Uniform(100);
        int read_round = cur_round_ + read_delay_rounds;
        bool time_travel = random.OneIn(2);
        int seconds_in_past = 0;
        if (time_travel) {
          seconds_in_past = random.Uniform(FLAGS_tablet_history_max_age_sec);
        }
        Timestamp snapshot_ts =
            HybridClock::AddPhysicalTimeToTimestamp(clock_->Now(),
                                                    MonoDelta::FromSeconds(-1L * seconds_in_past));
        VLOG(1) << "Round " << cur_round_ << ": Starting snapshot scan for " << seconds_in_past
                << " seconds in the past at " << StringifyTimestamp(snapshot_ts)
                << ") and scheduling the read for round " << read_round;

        unique_ptr<client::KuduScanner> scanner(new KuduScanner(table.get()));
        ASSERT_OK(scanner->SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
        ASSERT_OK(scanner->SetFaultTolerant());
        ASSERT_OK(scanner->SetTimeoutMillis(60 * 1000));
        ASSERT_OK(scanner->SetSnapshotRaw(snapshot_ts.ToUint64()));
        ASSERT_OK(scanner->Open());

        NO_FATALS(RegisterScanner(std::move(scanner), snapshot_ts, read_round));
        break;
      }
      default: {
        LOG(FATAL) << "Unexpected value: " << action;
      }
    }
  }

  NO_FATALS(VerifyAllRemainingScanners());
}

} // namespace kudu
