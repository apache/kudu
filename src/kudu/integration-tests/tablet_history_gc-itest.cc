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

#include <map>
#include <memory>
#include <utility>

#include "kudu/client/client-test-util.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/random.h"

using kudu::client::KuduScanner;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::server::Clock;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tserver::MiniTabletServer;
using kudu::tserver::TabletServer;
using kudu::tserver::TSTabletManager;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(use_mock_wall_clock);
DECLARE_int32(scanner_ttl_ms);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_string(block_manager);
DECLARE_bool(enable_maintenance_manager);

DEFINE_int32(test_num_rounds, 200, "Number of rounds to loop "
                                   "RandomizedTabletHistoryGcITest.TestRandomHistoryGCWorkload");

using kudu::server::HybridClock;

namespace kudu {

class TabletHistoryGcITest : public MiniClusterITestBase {
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
    FLAGS_use_mock_wall_clock = true;
    FLAGS_tablet_history_max_age_sec = 100;
  }

 protected:
  enum Actions {
    kInsert,
    kUpdate,
    kDelete,
    kFlush,
    kMergeCompaction,
    kRedoDeltaCompaction,
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

  void AddTimeToClock(MonoDelta delta) {
    uint64_t now = HybridClock::GetPhysicalValueMicros(clock_->Now());
    uint64_t new_time = now + delta.ToMicroseconds();
    clock_->SetMockClockWallTimeForTests(new_time);
  }

  void RegisterScanner(unique_ptr<client::KuduScanner> scanner, Timestamp snap_ts,
                       int verify_round) {
    CHECK_GE(verify_round, cur_round_);
    if (verify_round == cur_round_) {
      NO_FATALS(VerifySnapshotScan(std::move(scanner), std::move(snap_ts), verify_round));
      return;
    }
    ScannerTSPair pair(std::move(scanner), std::move(snap_ts));
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
  ScannerMap scanners_;
  HybridClock* clock_ = nullptr;

  Timestamp latest_snapshot_ts_;
  int32_t rows_inserted_ = 0;
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
    return Status::OK();
  }

  void set_updates(vector<MaterializedTestRow> updates) {
    updates_ = std::move(updates);
  }

  void set_deletes(vector<int32_t> deletes) {
    deletes_ = std::move(deletes);
  }

  void Reset() {
    updates_.clear();
    deletes_.clear();
  }

 private:
  enum Action {
    kUpdate,
    kDelete
  };

  Tablet* const tablet_;
  const Schema client_schema_;

  vector<MaterializedTestRow> updates_;
  vector<int32_t> deletes_;
};

// Randomized test that attempts to test many arbitrary history GC use cases.
TEST_F(RandomizedTabletHistoryGcITest, TestRandomHistoryGCWorkload) {
  OverrideFlagForSlowTests("test_num_rounds",
                           Substitute("$0", FLAGS_test_num_rounds * 10));

  LOG(INFO) << "Running " << FLAGS_test_num_rounds << " rounds";

  // Set high scanner TTL, since this test opens scanners and then waits for some
  // time before reading from them.
  FLAGS_scanner_ttl_ms = 1000 * 60 * 60 * 24;

  StartCluster(1); // Start MiniCluster with a single tablet server.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup(); // Convenient way to create a table.

  client::sp::shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(workload.table_name(), &table));

  // Directly access the Tablet so we can control compaction and the clock.
  MiniTabletServer* mts = cluster_->mini_tablet_server(0);
  TabletServer* ts = mts->server();
  clock_ = down_cast<HybridClock*>(ts->clock());
  std::vector<scoped_refptr<TabletPeer>> peers;
  ts->tablet_manager()->GetTabletPeers(&peers);
  ASSERT_EQ(1, peers.size());
  Tablet* tablet = peers[0]->tablet();

  // Set initial clock time to 1000 seconds past 0, which is enough so that the
  // AHM will not be negative.
  const uint64_t kInitialMicroTime = 1L * 1000 * 1000 * 1000;
  clock_->SetMockClockWallTimeForTests(kInitialMicroTime);

  LOG(INFO) << "Seeding random number generator";
  Random random(SeedRandom());

  // Save an empty snapshot at the "beginning of time";
  NO_FATALS(SaveSnapshot(MaterializedTestTable(), Timestamp(0)));

  for (cur_round_ = 0; cur_round_ < FLAGS_test_num_rounds; cur_round_++) {
    VLOG(1) << "Starting round " << cur_round_;
    NO_FATALS(VerifyScannersForRound(cur_round_));

    int action = random.Uniform(kNumActions);
    switch (action) {
      case kInsert: {
        // TODO: Allow for reinsert onto deleted rows after KUDU-237 has been
        // implemented.
        int32_t num_rows_to_insert = random.Uniform(1000);
        VLOG(1) << "Inserting " << num_rows_to_insert << " rows";
        if (num_rows_to_insert == 0) continue;
        MaterializedTestTable snapshot = CloneLatestSnapshot();

        client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
        session->SetTimeoutMillis(20000);
        ASSERT_OK_FAST(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));

        for (int32_t i = 0; i < num_rows_to_insert; i++) {
          int32_t row_key = rows_inserted_;
          MaterializedTestRow test_row = { row_key,
                                           static_cast<int32_t>(random.Next()),
                                           Substitute("$0", random.Next()),
                                           NOT_DELETED };
          unique_ptr<client::KuduInsert> insert(table->NewInsert());
          KuduPartialRow* row = insert->mutable_row();
          ASSERT_OK_FAST(row->SetInt32(0, test_row.key));
          ASSERT_OK_FAST(row->SetInt32(1, test_row.int_val));
          ASSERT_OK_FAST(row->SetStringCopy(2, test_row.string_val));
          ASSERT_OK_FAST(session->Apply(insert.release()));

          VLOG(2) << "Inserting row " << StringifyTestRow(test_row);
          snapshot[row_key] = std::move(test_row);
          rows_inserted_++;
        }
        FlushSessionOrDie(session);
        SaveSnapshot(std::move(snapshot), clock_->Now());
        break;
      }
      case kUpdate: {
        if (rows_inserted_ == 0) continue;
        int32_t num_rows_to_update = random.Uniform(std::min(rows_inserted_, 1000));
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
          int32_t row_key = random.Uniform(rows_inserted_);
          MaterializedTestRow test_row = snapshot[row_key];
          CHECK_EQ(row_key, test_row.key) << "Rows inserted: " << rows_inserted_
              << ", row: " << StringifyTestRow(test_row);
          if (test_row.is_deleted == DELETED) continue;

          test_row.int_val = random.Next();
          test_row.string_val = Substitute("$0", random.Next());

          VLOG(2) << "Updating row to " << StringifyTestRow(test_row);
          updates.push_back(test_row);
          snapshot[row_key] = std::move(test_row);
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
          client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
          session->SetTimeoutMillis(20000);
          ASSERT_OK_FAST(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));

          for (const MaterializedTestRow& test_row : updates) {
            unique_ptr<client::KuduUpdate> update(table->NewUpdate());
            KuduPartialRow* row = update->mutable_row();
            ASSERT_OK_FAST(row->SetInt32(0, test_row.key));
            ASSERT_OK_FAST(row->SetInt32(1, test_row.int_val));
            ASSERT_OK_FAST(row->SetStringCopy(2, test_row.string_val));
            ASSERT_OK_FAST(session->Apply(update.release()));
          }
          FlushSessionOrDie(session);
        }
        SaveSnapshot(std::move(snapshot), clock_->Now());
        VLOG(1) << "Updated " << rows_updated << " rows";
        break;
      }
      case kDelete: {
        if (rows_inserted_ == 0) continue;
        int32_t num_rows_to_delete = random.Uniform(std::min(rows_inserted_, 1000));
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
          int32_t row_key = random.Uniform(rows_inserted_);
          MaterializedTestRow test_row = snapshot[row_key];
          CHECK_EQ(row_key, test_row.key);

          if (test_row.is_deleted == DELETED) {
            VLOG(2) << "Row " << test_row.key << " is already deleted";
            continue;
          }
          VLOG(2) << "Deleting row " << row_key;
          deletes.push_back(row_key);
          test_row.is_deleted = DELETED;
          snapshot[row_key] = std::move(test_row);
        }

        int rows_deleted = deletes.size();
        if (rows_deleted == 0) continue;

        if (force_reupdate_missed_deltas) {
          std::shared_ptr<ReupdateHooks> hooks =
              std::make_shared<ReupdateHooks>(tablet, GetSimpleTestSchema());
          hooks->set_deletes(std::move(deletes));
          tablet->SetFlushCompactCommonHooksForTests(hooks);
          ASSERT_OK(tablet->Compact(Tablet::FORCE_COMPACT_ALL));
          tablet->SetFlushCompactCommonHooksForTests(nullptr); // Clear the hook.
        } else {
          client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
          session->SetTimeoutMillis(20000);
          ASSERT_OK_FAST(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));

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
      case kMoveTimeForward: {
        VLOG(1) << "Moving clock forward";
        AddTimeToClock(MonoDelta::FromSeconds(200));
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
        ASSERT_OK(scanner->SetOrderMode(KuduScanner::ORDERED));
        ASSERT_OK(scanner->SetSnapshotRaw(snapshot_ts.ToUint64()));
        ASSERT_OK(scanner->Open());

        NO_FATALS(RegisterScanner(std::move(scanner), std::move(snapshot_ts), read_round));
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
