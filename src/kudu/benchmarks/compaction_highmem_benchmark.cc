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

// Standalone benchmark for high-memory rowset compaction.
//
// This file is intentionally NOT registered with ADD_KUDU_TEST. It exists to
// exercise the full intended workload — kRowsPerRowset=1000 rows per DiskRowSet
// with 64 KiB wide UNDO-delta mutations — which exceeds the tmpfs storage
// budget of shared dist-test slaves and is therefore unsuitable for CI.
//
// Run manually on a machine with ≥ 32 GiB RAM and a few hundred MB of free disk.
//
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_tracker.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/trace.h"

DECLARE_int32(flush_threshold_mb);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(flush_upper_bound_ms);
DECLARE_int64(memory_limit_hard_bytes);
DECLARE_uint32(rowset_compaction_rows_per_block);

namespace kudu {
namespace tablet {

// Benchmark fixture for high-memory rowset compaction.

class CompactionHighMemBenchmark : public KuduRowSetTest {
 public:
  CompactionHighMemBenchmark()
      : KuduRowSetTest(CreateSchema()) {
  }

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", STRING));
    CHECK_OK(builder.AddColumn("val", INT64));
    CHECK_OK(builder.AddNullableColumn("large_val", STRING));
    return builder.BuildWithoutIds();
  }

  static void SetUpTestSuite() {
    // Pin the hard memory limit to 20 GiB so the "beyond hard memory limit"
    // compaction warning fires at a predictable threshold independent of the
    // machine's installed RAM.
    FLAGS_memory_limit_hard_bytes = 20LL * 1024 * 1024 * 1024;
  }

  // Number of DiskRowSets.
  static constexpr uint32_t kNumRowsets = 10;

  // Rows per DiskRowSet. The on-disk footprint (compressed periodic payload)
  // is a few hundred MiB, safe on any developer machine but unsuitable for
  // shared dist-test tmpfs. Requires ≥ 32 GiB RAM.
  static constexpr uint32_t kRowsPerRowset = 1000;

 protected:
  // Inserts num_rowsets batches of rows_per_rowset rows each (flushing the MRS
  // after every batch) using an interleaved key pattern so that all resulting
  // DiskRowSets have overlapping key ranges and are eligible for compaction.
  void InsertInterleavedRows(uint32_t num_rowsets, uint32_t rows_per_rowset) {
    for (uint32_t batch = 0; batch < num_rowsets; batch++) {
      LocalTabletWriter writer(tablet().get(), &client_schema());
      KuduPartialRow row(&client_schema());
      for (uint32_t i = 0; i < rows_per_rowset; i++) {
        ASSERT_OK(row.SetStringCopy(
            "key", Substitute("hello $0", batch + i * num_rowsets)));
        ASSERT_OK(row.SetInt64("val", 0));
        ASSERT_OK(writer.Insert(row));
      }
      ASSERT_OK(tablet()->Flush());
    }
    ASSERT_EQ(num_rowsets, tablet()->num_rowsets());
  }

  // Updates every row in a single pass by writing both the narrow INT64 'val'
  // column and the wide STRING 'large_val' column without flushing the DMS.
  void UpdateAllRowsWithLargeValNoFlush(uint32_t num_rowsets,
                                        uint32_t rows_per_rowset,
                                        int32_t val,
                                        const std::string& large_payload) {
    for (uint32_t rowset_id = 0; rowset_id < num_rowsets; rowset_id++) {
      LocalTabletWriter writer(tablet().get(), &client_schema());
      KuduPartialRow row(&client_schema());
      for (uint32_t i = rowset_id * rows_per_rowset;
           i < (rowset_id + 1) * rows_per_rowset; i++) {
        ASSERT_OK(row.SetStringCopy("key", Substitute("hello $0", i)));
        ASSERT_OK(row.SetInt64("val", val));
        ASSERT_OK(row.SetStringCopy("large_val", large_payload));
        ASSERT_OK(writer.Upsert(row));
      }
    }
    ASSERT_EQ(num_rowsets, tablet()->num_rowsets());
  }

  // Returns the combined on-disk size of all REDO and UNDO delta files across
  // every DiskRowSet in the tablet. Called from the test body to sanity-check
  // the workload; declared as a fixture method so the call to the private
  // Tablet::GetRowSetsForTests() happens in the friend-class context.
  uint64_t TotalDeltaBytes() {
    std::vector<std::shared_ptr<RowSet>> rowsets;
    tablet()->GetRowSetsForTests(&rowsets);
    uint64_t total = 0;
    for (const auto& rs : rowsets) {
      const auto* drs = down_cast<DiskRowSet*>(rs.get());
      DiskRowSetSpace drss;
      drs->GetDiskRowSetSpaceUsage(&drss);
      total += drss.redo_deltas_size + drss.undo_deltas_size;
    }
    return total;
  }

  // Workload that generates a large number of REDO delta files through
  // frequent DMS flushes, then converts all REDOs to UNDOs via major delta
  // compaction. The resulting on-disk state (overlapping DiskRowSets, each
  // carrying many UNDO delta files) creates realistic memory pressure when
  // rowset compaction reads every delta file simultaneously.
  //
  // The method generates kFlushRounds = 2 * size_factor REDO delta files per
  // DiskRowSet, each containing kUpdatesPerFlush * kRowsPerRowset mutations.
  // After major delta compaction, the equivalent UNDO data is created.
  void GenHighMemConsumptionDeltasWithFrequentFlush(uint32_t size_factor) {
    // Drive aggressive flushing so that each update round produces exactly one
    // new on-disk REDO delta file per DiskRowSet.
    FLAGS_flush_threshold_mb = 0;
    FLAGS_flush_threshold_secs = 1;
    FLAGS_flush_upper_bound_ms = 100;

    const uint32_t kFlushRounds = 2 * size_factor;
    constexpr uint32_t kUpdatesPerFlush = 5;
    constexpr size_t kLargeValSizeBytes = 64 * 1024;

    // Periodic payload: compressible on disk yet expands to full size in
    // memory when a CFile block iterator loads its block.
    std::string kLargePayload(kLargeValSizeBytes, '\0');
    for (size_t i = 0; i < kLargeValSizeBytes; i++) {
      kLargePayload[i] = static_cast<char>((i * 193U + 37U) % 251U);
    }

    NO_FATALS(InsertInterleavedRows(kNumRowsets, kRowsPerRowset));
    ASSERT_EQ(kNumRowsets, tablet()->num_rowsets());

    for (uint32_t round = 0; round < kFlushRounds; round++) {
      for (uint32_t pass = 0; pass < kUpdatesPerFlush; pass++) {
        const int32_t val = static_cast<int32_t>(round * kUpdatesPerFlush + pass + 1);
        NO_FATALS(UpdateAllRowsWithLargeValNoFlush(
            kNumRowsets, kRowsPerRowset, val, kLargePayload));
      }
      ASSERT_OK(tablet()->FlushAllDMSForTests());
      {
        std::vector<std::shared_ptr<RowSet>> rowsets;
        tablet()->GetRowSetsForTests(&rowsets);
        ASSERT_EQ(kNumRowsets, rowsets.size());
        for (const auto& rs : rowsets) {
          const auto* drs = down_cast<DiskRowSet*>(rs.get());
          ASSERT_EQ(round + 1, drs->CountDeltaStores())
              << "REDO delta file count should equal the number of completed "
                 "flush rounds";
        }
      }
    }

    for (uint32_t i = 0; i < kNumRowsets; i++) {
      ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
    }
    {
      std::vector<std::shared_ptr<RowSet>> rowsets;
      tablet()->GetRowSetsForTests(&rowsets);
      for (const auto& rs : rowsets) {
        const auto* drs = down_cast<DiskRowSet*>(rs.get());
        ASSERT_GT(drs->delta_tracker().CountUndoDeltaStores(), 0)
            << "each DiskRowSet must have UNDO delta stores after major "
               "delta compaction";
      }
    }
  }
};

// Benchmark: rowset compaction under genuine high-memory pressure.
//
// With kRowsPerRowset=1000 and size_factor=2, peak memory during compaction
// naturally crosses the 20 GiB hard limit. No artificial threshold is required.
// Compaction of 10 rowsets × 1000 rows completes in ~40 seconds.
TEST_F(CompactionHighMemBenchmark, HighDeltaVolumeWithFrequentFlushForRowsetCompaction) {
  NO_FATALS(GenHighMemConsumptionDeltasWithFrequentFlush(/*size_factor=*/2));

  // Sanity-check that the workload produced a non-trivial on-disk delta volume.
  const uint64_t kMinDeltaBytes = 80ULL * 1024 * 1024 * kRowsPerRowset / 1000;
  ASSERT_GT(TotalDeltaBytes(), kMinDeltaBytes)
      << "expected at least " << kMinDeltaBytes
      << " bytes of combined REDO + UNDO delta data";

  // Set per-batch row count to the full dataset size so that every delta-file
  // iterator holds all of its CFile blocks in memory at once, maximising peak
  // RSS and reflecting production compaction behaviour with large rowsets.
  FLAGS_rowset_compaction_rows_per_block = kNumRowsets * kRowsPerRowset;

  StringVectorSink sink;
  ScopedRegisterSink reg(&sink);
  scoped_refptr<Trace> trace(new Trace);
  Stopwatch sw;
  sw.start();
  {
    ADOPT_TRACE(trace.get());
    ASSERT_OK(tablet()->Compact(Tablet::COMPACT_NO_FLAGS));
  }
  sw.stop();
  LOG(INFO) << Substitute("CompactRowSetsOp complete. Timing: $0 Metrics: $1",
                          sw.elapsed().ToString(),
                          trace->MetricsAsJSON());

  ASSERT_STR_MATCHES(
      JoinStrings(sink.logged_msgs(), "\n"),
      "beyond hard memory limit of.*Rowset merge compaction ops consumption:");
}

} // namespace tablet
} // namespace kudu
