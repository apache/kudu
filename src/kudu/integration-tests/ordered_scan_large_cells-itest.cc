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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
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
using kudu::client::KuduError;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduUpsert;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::itest::GetTsCounterValue;
using std::atomic;
using std::max;
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

METRIC_DECLARE_gauge_uint64(generic_current_allocated_bytes);
DECLARE_int64(rpc_max_message_size);

namespace kudu {

namespace {

constexpr const char* const kTableName = "OrderedScanLargeCellsTestTable";
constexpr int kPlannedMaxCellSizeBytes = 1 << 20;  // 1 MiB.
constexpr int64_t kMemoryLimitHardBytes = 8LL << 30;  // 8 GiB.

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
  ASSERT_EQ(0, blob->size() % 8);
  static mt19937_64 rng(random_device {}());

  for (size_t i = 0; i < blob->size(); i += sizeof(uint64_t)) {
    uint64_t v = rng();
    memcpy(blob->data() + i, &v, sizeof(v));
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

    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    // Comfortably larger than one batch of 1 MiB cells.
    ASSERT_OK(session->SetMutationBufferSpace(64 << 20));

    ASSERT_EQ(0, num_rows % batch_rows)
        << "num_rows must be divisible by batch_rows";
    for (int batch_idx = 0; batch_idx < num_rows; batch_idx += batch_rows) {
      for (int in_batch = 0; in_batch < batch_rows; in_batch++) {
        unique_ptr<KuduUpsert> upsert(table_->NewUpsert());
        KuduPartialRow* row = upsert->mutable_row();
        ASSERT_OK(row->SetInt64("pk1", pk_dist(rng)));
        static vector<uint8_t> blob(kPlannedMaxCellSizeBytes);
        NO_FATALS(FillBlobUncompressible(&blob));
        ASSERT_OK(row->SetBinaryCopy("s1", Slice(blob.data(), blob.size())));
        ASSERT_OK(session->Apply(upsert.release()));
      }
      const Status s = session->Flush();
      if (!s.ok()) {
        // Flush() only signals that some op failed; the real per-row status is
        // in the pending errors. Fetch it to classify the failure.
        vector<KuduError*> errors;
        bool overflowed = false;
        session->GetPendingErrors(&errors, &overflowed);
        ASSERT_FALSE(errors.empty())
            << "Flush failed with no per-row error: " << s.ToString();
        const Status row_status = errors[0]->status();
        const string row_msg = row_status.ToString();
        STLDeleteElements(&errors);
        // A memory-pressure rejection surfaces as ServiceUnavailable
        // (tablet_service.cc / op_tracker.cc). The client retries that code
        // transparently, so once retries exhaust the deadline it instead
        // surfaces as TimedOut with the original "...memory..." text embedded.
        const bool memory_pressure =
            (row_status.IsServiceUnavailable() || row_status.IsTimedOut()) &&
            row_msg.find("memory") != string::npos;
        ASSERT_TRUE(memory_pressure)
            << "Write failed with a non-memory-pressure error: " << row_msg;
        ASSERT_TRUE(tolerate_persistent_pressure)
            << "Write rejected by memory pressure in first phase: " << row_msg;
        break;
      }
      *rows_written += batch_rows;
    }
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
// When dynamic batch sizing is implemented, this test will measure:
//   + Scans can't significantly increase actual memory usage over the
//     configured hard limit.
//   + We can read data with large cells with default options (no custom limit
//     on maximum RPC size).
// TODO(martonka): Remove "DISABLED_" when dynamic batching is ready.
TEST_F(OrderedScanLargeCellsITest, DISABLED_TestRandomUpsertOrderedScan) {
  SKIP_IF_SLOW_NOT_ALLOWED();

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
  // TODO(martonka): remove this once the batch sizing works properly.
  // Currently scan requests for ordered reads might take up this much.
  // Data size + 1 MB extra for remaining proto parts.
  constexpr int64_t kRpcLimit =
      1024LLU * kPlannedMaxCellSizeBytes + (1LLU << 20);
  constexpr int64_t kMiB = 1024LL * 1024;
  // Round up.
  constexpr int64_t kTransactionMemoryLimitMb = (kRpcLimit + kMiB - 1) / kMiB;
  // The tablet server must never exceed this much memory during the test.
  constexpr int64_t kMemoryCapBytes = 12LL << 30;  // 12 GiB.

  // TODO(martonka): remove this once the batch sizing works properly.
  // Client-side limit (for receiving large scan responses); the tserver gets
  // its own copy via extra_tserver_flags below.
  FLAGS_rpc_max_message_size = kRpcLimit;

  // Start a real 1-master + 1-tserver cluster. Server-side flags that the
  // in-process version set via FLAGS_* are now passed to the tserver process.
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 1;
  opts.extra_tserver_flags = {
      Substitute("--max_cell_size_bytes=$0", kPlannedMaxCellSizeBytes),
      // TODO(martonka): remove this once the batch sizing works properly.
      Substitute("--rpc_max_message_size=$0", kRpcLimit),
      // Not used by reads; only relevant due to the flag validator
      // ValidateOpMemoryAndRpcSize.
      Substitute("--tablet_transaction_memory_limit_mb=$0",
                 kTransactionMemoryLimitMb),
      // Minimum recommended memory limit for a tserver.
      Substitute("--memory_limit_hard_bytes=$0", kMemoryLimitHardBytes),
      // Keep flushes running so the ordered scan spans multiple rowsets, but
      // disable rowset and delta compactions for this test.
      "--enable_rowset_compaction=false",
      "--enable_major_delta_compaction=false",
      "--enable_minor_delta_compaction=false",
  };
  NO_FATALS(StartClusterWithOpts(opts));

  NO_FATALS(CreateTable());

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
  // This assertion fails currently (hence DISABLED_): the ordered-scan path
  // pushes peak memory well over the cap. It will pass once dynamic batch
  // sizing is implemented.
  // TODO(martonka): Remove comment when fixed
  ASSERT_LE(peak, kMemoryCapBytes);
}

}  // namespace kudu
