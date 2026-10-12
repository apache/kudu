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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduColumnSchema;
using kudu::client::KuduDelete;
using kudu::client::KuduError;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduUpsert;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::itest::GetTsCounterValue;
using std::atomic;
using std::max;
using std::min;
using std::memory_order_acquire;
using std::memory_order_release;
using std::mt19937_64;
using std::numeric_limits;
using std::pair;
using std::random_device;
using std::string;
using std::thread;
using std::uniform_int_distribution;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_gauge_uint64(generic_current_allocated_bytes);
METRIC_DECLARE_gauge_int64(batch_num_rows_upper_limit);

namespace kudu {

namespace {

constexpr const char* const kTableName = "OrderedScanLargeCellsTestTable";
// These are the default values, we just want to make the test
// robust by setting them, so changes in default values wont
// ruin the test:
constexpr int kPlannedMaxCellSizeBytes = 1 << 20;  // 1 MiB.
constexpr int64_t kMemoryLimitHardBytes = 8LL << 30;  // 8 GiB.
constexpr int kMaxHugeCellSizeBytes = kPlannedMaxCellSizeBytes;
constexpr int64_t kMaxScanBatchSizeApprox = 16LL << 20;  // 16 MiB.
constexpr int kScannerBatchSizeRows = 128;
// Rows of this size alone would yield a batch size upper limit way above
// kScannerBatchSizeRows.
constexpr size_t kSmallCellSizeBytes = 1 << 10;  // 1 KiB.
// Burned in constant, not a flag:
constexpr int kMergeRowBufferDefault = 1024;

// Periodically polls the external tablet server's tcmalloc memory usage metric.
class TserverMemoryPeakTracker {
 public:
  explicit TserverMemoryPeakTracker(ExternalTabletServer* ets)
      : ets_(ets),
        sampler_([this]() {
          do {
            if (!SampleOnce()) {
              return;
            }
            SleepFor(MonoDelta::FromMilliseconds(100));
          } while (!stop_.load(memory_order_acquire));
          SampleOnce();
        }) {}

  ~TserverMemoryPeakTracker() {
    GetPeakAndStop();
  }

  // Stops the sampler (only on the first call) and returns the sampling status
  // together with the peak observed memory in bytes.
  pair<Status, int64_t> GetPeakAndStop() {
    if (!stop_.exchange(true, memory_order_release)) {
      sampler_.join();
    }
    return {status_, peak_};
  }

 private:
  bool SampleOnce() {
    int64_t v = 0;
    status_ = GetTsCounterValue(
        ets_, &METRIC_generic_current_allocated_bytes, &v);
    if (!status_.ok()) {
      return false;
    }
    peak_ = max(peak_, v);
    return true;
  }

  ExternalTabletServer* ets_;
  atomic<bool> stop_{false};
  int64_t peak_ = 0;
  Status status_;
  // Init order matters, sampler_ must be last.
  thread sampler_;
};

void FillBlobUncompressible(vector<uint8_t>* blob) {
  static mt19937_64 rng(random_device {}());
  for (size_t i = 0; i < blob->size(); i += sizeof(uint64_t)) {
    uint64_t v = rng();
    memcpy(blob->data() + i, &v, min(sizeof(v), blob->size() - i));
  }
}

}  // anonymous namespace

class OrderedScanLargeCellsITest : public ExternalMiniClusterITestBase {
 protected:
  // Creates a single-tablet, single-replica table with an INT64 PK and a
  // non-nullable BINARY column.
  void CreateTable() {
    KuduSchemaBuilder b;
    b.AddColumn("pk1")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn("s1")->Type(KuduColumnSchema::BINARY)->NotNull();
    KuduSchema schema;
    ASSERT_OK(b.Build(&schema));

    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    // Force a single tablet: empty range-partition columns, no hash partitions,
    // no split rows.
    ASSERT_OK(creator->table_name(kTableName)
                  .schema(&schema)
                  .set_range_partition_columns({})
                  .num_replicas(1)
                  .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));
  }

  // Creates a MANUAL_FLUSH session with a mutation buffer comfortably larger
  // than one batch of 1 MiB cells.
  shared_ptr<KuduSession> NewManualFlushSession() {
    shared_ptr<KuduSession> session = client_->NewSession();
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    CHECK_OK(session->SetMutationBufferSpace(64 << 20));
    return session;
  }

  // Applies (without flushing) a single UPSERT of 's1' = 'value' at pk1 = 'pk'.
  void ApplyUpsertRow(KuduSession* session, int64_t pk, const Slice& value) {
    unique_ptr<KuduUpsert> upsert(table_->NewUpsert());
    KuduPartialRow* row = upsert->mutable_row();
    CHECK_OK(row->SetInt64("pk1", pk));
    CHECK_OK(row->SetBinaryCopy("s1", value));
    CHECK_OK(session->Apply(upsert.release()));
  }

  // Upserts one row per (pk, cell_size) pair in a single flushed batch; each
  // row's 's1' blob is cell_size bytes of fresh uncompressible random data.
  // If session is null, it creates a temporary session.
  // Sets '*errors' to a per-row status indexed by position (Status::OK() where
  // the row succeeded).
  void UpsertAndFlush(
      shared_ptr<KuduSession> session,
      const vector<pair<int64_t, size_t>>& rows,
      vector<Status>* errors) {
    if (session == nullptr) {
      session = NewManualFlushSession();
    }
    // A single reused buffer is safe because ApplyUpsertRow copies the blob.
    vector<uint8_t> blob;
    for (const auto& [pk, size] : rows) {
      blob.resize(size);
      FillBlobUncompressible(&blob);
      ApplyUpsertRow(session.get(), pk, Slice(blob.data(), blob.size()));
    }
    const Status s = session->Flush();

    *errors = vector<Status>(rows.size(), Status::OK());
    if (s.ok()) {
      return;
    }
    // Flush() only signals that some op failed; map each pending per-row error
    // back to its position via the row's pk1.
    std::map<int64_t, size_t> pk_to_idx;
    for (size_t i = 0; i < rows.size(); i++) {
      pk_to_idx[rows[i].first] = i;
    }
    vector<KuduError*> pending;
    bool overflowed = false;
    session->GetPendingErrors(&pending, &overflowed);
    ASSERT_FALSE(overflowed);
    auto cleanup = MakeScopedCleanup([&]() { STLDeleteElements(&pending); });
    ASSERT_FALSE(pending.empty())
        << "Flush failed with no per-row error: " << s.ToString();
    for (const auto* e : pending) {
      int64_t pk = 0;
      ASSERT_OK(e->failed_op().row().GetInt64("pk1", &pk));

      const auto it = pk_to_idx.find(pk);
      ASSERT_TRUE(it != pk_to_idx.end())
          << "pending error for unexpected pk1 " << pk;
      (*errors)[it->second] = e->status();
    }
  }

  // UPSERT rows with random INT64 PKs and 1 MiB uncompressible blobs.
  // If 'tolerate_persistent_pressure' is true, stops without failing when a
  // write is rejected due to memory pressure; otherwise such a rejection fails
  // the test. '*rows_written' is incremented by the number of rows accepted.
  void UpsertRandomRows(int num_rows, int batch_rows,
                        bool tolerate_persistent_pressure,
                        uint64_t* rows_written) {
    mt19937_64 rng(random_device {}());
    uniform_int_distribution<int64_t> pk_dist(
        numeric_limits<int64_t>::min(),
        numeric_limits<int64_t>::max());

    shared_ptr<KuduSession> session = NewManualFlushSession();

    ASSERT_EQ(0, num_rows % batch_rows)
        << "num_rows must be divisible by batch_rows";
    for (int batch_idx = 0; batch_idx < num_rows; batch_idx += batch_rows) {
      // Every row gets a fresh 1 MiB uncompressible blob (generated inside
      // UpsertAndFlush) at a random PK.
      vector<pair<int64_t, size_t>> rows(batch_rows);
      for (int i = 0; i < batch_rows; i++) {
        rows[i] = {pk_dist(rng), kPlannedMaxCellSizeBytes};
      }
      vector<Status> errors;
      NO_FATALS(UpsertAndFlush(session, rows, &errors));

      auto real_error = [&](const Status& s) {
        const string error_msg = s.ToString();
        const bool memory_pressure =
            (s.IsServiceUnavailable() || s.IsTimedOut()) &&
            error_msg.find("memory") != string::npos;
        return !memory_pressure;
      };

      bool there_is_an_error =
          std::any_of(errors.begin(), errors.end(),
                      [](const Status& s) { return !s.ok(); });

      if (there_is_an_error) {
        for (const auto& s : errors) {
          if (s.ok()) {
            *rows_written += 1;
            continue;
          }
          ASSERT_TRUE(tolerate_persistent_pressure && !real_error(s))
              << "Error in batch: " << s.ToString();
        }
        return;
      }

      *rows_written += batch_rows;
    }
  }

  // Starts a single-tserver cluster configured for huge cells, creates the test
  // table, and enables huge cells on it (batch_sizing=auto plus an
  // override_cell_limit_bytes at the huge-cell limit). 'extra_flags' are
  // appended to the tserver flags.
  void StartHugeCellClusterAndTable(const vector<string>& extra_flags = {}) {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 1;
    opts.extra_tserver_flags = {
        Substitute("--max_huge_cell_size_bytes=$0", kMaxHugeCellSizeBytes),
        Substitute("--max_scan_batch_approx_size_bytes=$0",
                   kMaxScanBatchSizeApprox),
        Substitute("--scanner_batch_size_rows=$0", kScannerBatchSizeRows),
    };
    opts.extra_tserver_flags.insert(opts.extra_tserver_flags.end(),
                                    extra_flags.begin(), extra_flags.end());
    NO_FATALS(StartClusterWithOpts(opts));
    NO_FATALS(CreateTable());
    NO_FATALS(EnableHugeCells());
  }

  // Enables huge cells on the test table by setting 'batch_sizing=auto' and an
  // 'override_cell_limit_bytes' at the huge-cell limit in a single alter.
  // Alter() returns only once the change is applied. These properties are
  // one-way, so there is no corresponding disable helper.
  void EnableHugeCells() {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    ASSERT_OK(alterer->AlterExtraConfig(
        {{kTableBatchSizing, "auto"},
         {kTableOverrideCellLimitBytes,
          std::to_string(kMaxHugeCellSizeBytes)}})->Alter());
  }

  // Reads the single tablet's 'batch_num_rows_upper_limit' metric from
  // tserver 0. The metric reports 0 when the upper limit is unset.
  Status GetBatchSizeUpperLimit(int64_t* value) {
    return itest::GetInt64Metric(
        cluster_->tablet_server(0)->bound_http_hostport(),
        &METRIC_ENTITY_tablet, /*entity_id=*/nullptr,
        &METRIC_batch_num_rows_upper_limit, "value", value);
  }

  Status UpsertSingleRow(int64_t pk, size_t cell_size) {
    vector<Status> errors;
    UpsertAndFlush(shared_ptr<KuduSession>(), {{pk, cell_size}}, &errors);
    return errors[0];
  }

  void UpsertConsequentRows(int64_t first_pk, int num_rows, size_t cell_size) {
    vector<pair<int64_t, size_t>> rows;
    for (int i = 0; i < num_rows; i++) {
      rows.emplace_back(first_pk + i, cell_size);
    }
    vector<Status> errors;
    NO_FATALS(UpsertAndFlush(shared_ptr<KuduSession>(), rows, &errors));
    for (const auto& e : errors) {
      ASSERT_OK(e);
    }
  }

  Status DeleteConsequenRows(int64_t first_pk, int num_rows) {
    shared_ptr<KuduSession> session = NewManualFlushSession();
    for (int i = 0; i < num_rows; i++) {
      unique_ptr<KuduDelete> del(table_->NewDelete());
      RETURN_NOT_OK(del->mutable_row()->SetInt64("pk1", first_pk + i));
      RETURN_NOT_OK(session->Apply(del.release()));
    }
    return session->Flush();
  }

  shared_ptr<KuduTable> table_;
};

// KUDU-3812
// UPSERT random-PK rows and keep the second part unflushed, then run multiple
// full-table ORDERED scans with a delay between each scan, to minimize overlap.
//
// If we just start a normal Kudu cluster and do the following:
//   + Increase cell limit to 1MB.
//   + Increase rpc_max_message_size, so reads does not fail with it.
//     (also tablet_transaction_memory_limit_mb, so flag validator does not
//     fail).
//   + Create a table and start to put serious read and write load on it.
// It will quickly crash with OOM issues. It has 3 identified problems:
//   + Writes and heartbeats take way too much memory if the write buffer size
//     is increased. This is not really a problem, because we can keep the
//     default in 7 GB. And just send few rows at once.
//   + Compaction takes up way too much memory due to fixed row count.
//   + Ordered scans can take up a whole GB, that is not counted towards memory
//     tracking.
//
//
// This test is supposed to make a "repro" of the third error, and demonstrate
// that:
//   If memory is already kept near hard limit (80% of system memory by default)
//   by compaction or flushing, then this can easily cause a 8-10GB load with
//   the default 10 RPC worker threads.
//   Which means we can theoretically crash kudu-tserver on proper server
//   hardware too (e.g. 64 GB memory), not just on the bare minimum recommended
//   hardware.
// With dynamic batch sizing in place (KUDU-2874), this test measures:
//   + Scans can't significantly increase actual memory usage over the
//     configured hard limit.
//   + We can read data with large cells with default RPC options: the table has
//     'batch_sizing=auto', so the tserver auto-sizes scan batches, so as long
//     as rpc_max_message_size>=scanner_max_batch_size_bytes +
TEST_F(OrderedScanLargeCellsITest, TestRandomUpsertOrderedScan) {
  SKIP_IF_SLOW_NOT_ALLOWED();

#ifndef TCMALLOC_ENABLED
  GTEST_SKIP() << "requires tcmalloc to measure memory usage "
                  "(disabled under ASAN/TSAN)";
#endif

#if defined(NDEBUG)
  // Release builds can handle the full row count.
  constexpr int kRowsWrittenFlushingEnabled = 5000;
  constexpr int kRowsWrittenFlushingDisabled = 5000;
#else
  // But we should also test at least some load using debug build.
  constexpr int kRowsWrittenFlushingEnabled = 500;
  constexpr int kRowsWrittenFlushingDisabled = 500;
#endif
  constexpr int kRowsPerWriteBatch = 4;
  constexpr int kScanReadsNumber = 10;
  constexpr int kScannerTimeoutMilliseconds = 120 * 1000;
  // The tablet server must never exceed this much memory during the test.
  constexpr int64_t kMemoryCapBytes = 12LL << 30;  // 12 GiB.

  // Start a real 1-master + 1-tserver cluster configured for huge cells, with
  // the table's huge-cell extra config enabled. The extra flags on top of
  // the shared setup pin the memory limit and disable compactions for this
  // test.
  NO_FATALS(StartHugeCellClusterAndTable({
      // Minimum recommended memory limit for a tserver.
      Substitute("--memory_limit_hard_bytes=$0", kMemoryLimitHardBytes),
      // Keep flushes running so the ordered scan spans multiple rowsets, but
      // disable rowset and delta compactions for this test.
      "--enable_rowset_compaction=false",
      "--enable_major_delta_compaction=false",
      "--enable_minor_delta_compaction=false",
  }));

  // Track the peak tserver memory across the whole test. The regression is in
  // the ordered-scan path, but the writes must stay under the cap too, so
  // measuring the entire process (writes included) is intentional.
  TserverMemoryPeakTracker memory_peak_tracker(cluster_->tablet_server(0));

  uint64_t total_rows_written = 0;

  auto write_data = [&](const int num_rows,
                        const bool tolerate_persistent_pressure) {
    uint64_t rows_written = 0;
    NO_FATALS(UpsertRandomRows(num_rows, kRowsPerWriteBatch,
                               tolerate_persistent_pressure, &rows_written));
    total_rows_written += rows_written;
    LOG(INFO) << "Wrote " << rows_written << " rows.";
  };

  // Write the first part with background flushes enabled so some data lands on
  // disk, then disable flushes so the second part stays in the memrowset.
  NO_FATALS(write_data(kRowsWrittenFlushingEnabled, false));
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "enable_flush_memrowset", "false"));
  // Only writes until the allowed memory is filled.
  NO_FATALS(write_data(kRowsWrittenFlushingDisabled, true));

  // Scan at a snapshot so Kudu uses the merge buffer size.
  const uint64_t snapshot_ts = client_->GetLatestObservedTimestamp();

  // The goal is to measure how much additional memory usage over the memory
  // limit multiple running scan operations can cause.
  // We start the scan threads one-by-one with a delay, so they will probably
  // scan different parts of the tablet at the same time, and keep different
  // blocks in memory. (Even when a block is evicted from cache, it is kept in
  // memory until the active scan RPC call finishes.)
  // If the threads started at the same time, they might overlap a lot,
  // decreasing the number of blocks kept in memory significantly.
  const int64_t kThreadDelay = total_rows_written / kScanReadsNumber;

  // Start 1 scan thread. Whenever it reaches kThreadDelay, it releases a new
  // one to start.
  Semaphore gate(1);

  // gtest assertions are not reliably observed across threads, so each scan
  // thread records its own status here and the main thread checks them after
  // join.
  vector<Status> scan_results(kScanReadsNumber);

  auto run_scan = [&](int i) -> Status {
    gate.Acquire();
    auto release_gate = MakeScopedCleanup([&]() { gate.Release(); });

    KuduScanner scanner(table_.get());
    // SetFaultTolerant() makes the scan ordered.
    RETURN_NOT_OK(scanner.SetFaultTolerant());
    RETURN_NOT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
    RETURN_NOT_OK(scanner.SetSnapshotRaw(snapshot_ts));
    RETURN_NOT_OK(scanner.SetTimeoutMillis(kScannerTimeoutMilliseconds));
    RETURN_NOT_OK(scanner.Open());

    int64_t rows_read = 0;
    bool fired_next = false;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      RETURN_NOT_OK(scanner.NextBatch(&batch));
      rows_read += batch.NumRows();
      if (!fired_next && rows_read >= kThreadDelay) {
        fired_next = true;
        // Release now so the next thread can start.
        release_gate.run();
      }
    }
    // Random upserting can result in fewer rows than total_rows_written
    // (duplicate PKs). Just require that we read at least half of the rows
    // (probably * 0.99 would be fine too).
    if (rows_read < static_cast<int64_t>(total_rows_written) / 2) {
      return Status::Incomplete(Substitute(
          "Scan thread $0 only read $1 rows", i, rows_read));
    }
    if (!fired_next) {
      return Status::IllegalState(Substitute(
          "Scan thread $0 did not release the next thread", i));
    }
    return Status::OK();
  };

  vector<thread> scan_threads;
  scan_threads.reserve(kScanReadsNumber);
  for (int i = 0; i < kScanReadsNumber; i++) {
    scan_threads.emplace_back([&, i]() { scan_results[i] = run_scan(i); });
  }

  for (auto& st : scan_threads) {
    st.join();
  }
  for (const auto& s : scan_results) {
    ASSERT_OK(s);
  }

  const auto [sampling_status, peak] = memory_peak_tracker.GetPeakAndStop();
  ASSERT_OK(sampling_status);
  // With dynamic batch sizing (KUDU-2874) the ordered-scan path keeps peak
  // memory under the cap.
  LOG(INFO) << "Peak tracker: " << peak;
  ASSERT_LE(peak, kMemoryCapBytes);
}

// Check that --max_huge_cell_size_bytes applies once a table allows huge cells,
// and that the huge-cell properties cannot be turned back off (they are
// one-way).
TEST_F(OrderedScanLargeCellsITest, TestHugeCellSizeLimits) {
  NO_FATALS(StartHugeCellClusterAndTable());

  const auto too_large_msg = [](size_t cell_size, int limit) {
    return Substitute(
        "value too large for column 's1' ($0 bytes, maximum is $1 bytes)",
        cell_size, limit);
  };

  // StartHugeCellClusterAndTable() set an override_cell_limit_bytes at the
  // huge-cell limit, so the higher limit applies.
  const size_t huge_limit = kMaxHugeCellSizeBytes;
  ASSERT_OK(UpsertSingleRow(1, huge_limit));
  auto err = UpsertSingleRow(2, huge_limit + 1);
  ASSERT_FALSE(err.ok());
  ASSERT_STR_CONTAINS(err.ToString(),
                      too_large_msg(huge_limit + 1, kMaxHugeCellSizeBytes));

  // Huge cells cannot be turned back off: clearing the override (and/or batch
  // sizing) is rejected, because lowering the limit could brick the tablet on a
  // later WAL replay.
  {
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    const auto s = alterer->AlterExtraConfig(
        {{kTableOverrideCellLimitBytes, ""}, {kTableBatchSizing, ""}})->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }

  // The huge-cell limit therefore still applies: a huge cell is still accepted.
  ASSERT_OK(UpsertSingleRow(3, huge_limit));
}

// Test that a refused (too-large) cell does not lower the auto batch size: the
// override must be sized from the accepted cell only.
TEST_F(OrderedScanLargeCellsITest, TestRefusedCellDoesNotLowerBatchSize) {
  constexpr size_t kAcceptedCellSize = 512 * (1 << 10);  // 0.5 MiB.
  constexpr size_t kRefusedCellSize = 5 * (1 << 20);     // 5 MiB.
  static_assert(kAcceptedCellSize < static_cast<size_t>(kMaxHugeCellSizeBytes),
                "accepted cell must be within the huge-cell limit");
  static_assert(kRefusedCellSize > static_cast<size_t>(kMaxHugeCellSizeBytes),
                "refused cell must exceed the huge-cell limit");

  NO_FATALS(StartHugeCellClusterAndTable());

  int64_t upper_limit = 0;
  ASSERT_OK(GetBatchSizeUpperLimit(&upper_limit));
  ASSERT_EQ(0, upper_limit);

  // Write a 0.5 MiB and a 5 MiB row together in one batch. The 5 MiB cell is
  // refused while the 0.5 MiB one is written.
  vector<Status> errors;
  NO_FATALS(UpsertAndFlush(
      shared_ptr<KuduSession>(),
      {{2, kAcceptedCellSize}, {3, kRefusedCellSize}}, &errors));
  ASSERT_EQ(2, errors.size());
  ASSERT_OK(errors[0]);
  ASSERT_FALSE(errors[1].ok());
  ASSERT_STR_CONTAINS(
      errors[1].ToString(),
      Substitute(
          "value too large for column 's1' ($0 bytes, maximum is $1 bytes)",
          kRefusedCellSize, kMaxHugeCellSizeBytes));

  // The write op sets the upper limit after Apply; it is visible via the metric
  // by the time Flush() returns, but poll to avoid any propagation race.
  ASSERT_EVENTUALLY([&]() {
    int64_t ul = 0;
    ASSERT_OK(GetBatchSizeUpperLimit(&ul));
    ASSERT_GT(ul, 0);
  });

  // Validate the value on what we expect, not by copying the exact formula.
  // Expecting the largest power of 2 still not exceeding the desired limit.
  ASSERT_OK(GetBatchSizeUpperLimit(&upper_limit));
  ASSERT_EQ(0, upper_limit & (upper_limit - 1))
      << "not a power of 2: " << upper_limit;
  ASSERT_LE(static_cast<int64_t>(kAcceptedCellSize) * upper_limit,
            kMaxScanBatchSizeApprox);
  ASSERT_LE(kMaxScanBatchSizeApprox,
            static_cast<int64_t>(kAcceptedCellSize) * upper_limit * 2);
}

// Test that the auto batch size upper limit survives a tserver restart, even
// when the tablet metadata was not flushed (so it is restored by WAL replay).
// Writing rows with much smaller cells after the restart must not raise it:
// the limit has to stay low enough for the mix of huge and small rows.
TEST_F(OrderedScanLargeCellsITest, TestAutoBatchSizeUpperLimitSurvivesRestart) {
  // Keep the written rows in the MRS so the restart replays them through the
  // bootstrap path rather than loading them from a flushed rowset.
  NO_FATALS(StartHugeCellClusterAndTable({"--enable_flush_memrowset=false"}));

  int64_t before = 0;
  ASSERT_OK(GetBatchSizeUpperLimit(&before));
  ASSERT_EQ(0, before);

  // Write a few huge-cell rows to trigger the auto batch sizing.
  NO_FATALS(UpsertConsequentRows(1, 4, kPlannedMaxCellSizeBytes));

  ASSERT_OK(GetBatchSizeUpperLimit(&before));
  ASSERT_GT(before, 0);
  ASSERT_LT(before, kScannerBatchSizeRows);

  // Restart the tserver; the upper limit must be rebuilt by replaying the
  // writes.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ts->Shutdown();
  ASSERT_OK(ts->Restart());

  int64_t after = 0;
  ASSERT_EVENTUALLY([&]() {
    // The metric entity reappears only once the tablet is bootstrapped.
    ASSERT_OK(GetBatchSizeUpperLimit(&after));
    ASSERT_GT(after, 0);
  });
  ASSERT_EQ(before, after);

  // Small rows should not change buffer size.
  NO_FATALS(UpsertConsequentRows(5, 4, kSmallCellSizeBytes));
  ASSERT_OK(GetBatchSizeUpperLimit(&after));
  ASSERT_EQ(before, after);
}

// Test that deleting all the huge-cell rows does not reset the auto batch size.
TEST_F(OrderedScanLargeCellsITest, TestAutoBatchSizeUpperLimitStaysLowAfterDelete) {
  NO_FATALS(StartHugeCellClusterAndTable());

  NO_FATALS(UpsertConsequentRows(1, 4, kPlannedMaxCellSizeBytes));
  int64_t before = 0;
  ASSERT_OK(GetBatchSizeUpperLimit(&before));
  ASSERT_GT(before, 0);
  ASSERT_LT(before, kScannerBatchSizeRows);

  ASSERT_OK(DeleteConsequenRows(1, 4));
  NO_FATALS(UpsertConsequentRows(5, 4, kSmallCellSizeBytes));

  int64_t after = 0;
  ASSERT_OK(GetBatchSizeUpperLimit(&after));
  ASSERT_EQ(before, after);
}

}  // namespace kudu
