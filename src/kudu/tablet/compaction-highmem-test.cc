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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.pb.h"
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
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"

DECLARE_bool(rowset_compaction_enforce_preset_factor);
DECLARE_bool(rowset_compaction_memory_estimate_enabled);
DECLARE_bool(rowset_compaction_ancient_delta_threshold_enabled);
DECLARE_double(memory_limit_compact_usage_warn_threshold_percentage);
DECLARE_double(rowset_compaction_delta_memory_factor);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(flush_upper_bound_ms);
DECLARE_int64(memory_limit_hard_bytes);
DECLARE_uint32(rowset_compaction_estimate_min_deltas_size_mb);
DECLARE_uint32(rowset_compaction_rows_per_block);

namespace kudu {
namespace tablet {

class TestHighMemCompaction : public KuduRowSetTest {
 public:
  TestHighMemCompaction()
      : KuduRowSetTest(CreateSchema()) {
  }

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", STRING));
    CHECK_OK(builder.AddColumn("val", INT64));
    CHECK_OK(builder.AddNullableColumn("nullable_val", INT32));
    // Wide column used by GenHighMemConsumptionDeltasWithFrequentFlush to make
    // each delta mutation ~1 KB on disk. This lets the test reach a large total
    // delta volume with fewer tablet operations in a short span of time.
    // Existing tests leave this column NULL so their behaviour is unchanged.
    CHECK_OK(builder.AddNullableColumn("large_val", STRING));
    return builder.BuildWithoutIds();
  }

  Status InsertOrUpsertTestRows(RowOperationsPB::Type type,
                                int64_t first_row,
                                int64_t count,
                                int32_t val) {
    LocalTabletWriter writer(tablet().get(), &client_schema());
    KuduPartialRow row(&client_schema());

    for (int64_t i = first_row; i < first_row + count; i++) {
      RETURN_NOT_OK(row.SetStringCopy("key", Substitute("hello $0", i)));
      RETURN_NOT_OK(row.SetInt64("val", val));
      if (type == RowOperationsPB::INSERT) {
        RETURN_NOT_OK(writer.Insert(row));
      } else if (type == RowOperationsPB::UPSERT) {
        RETURN_NOT_OK(writer.Upsert(row));
      } else {
        return Status::InvalidArgument(
            Substitute("unknown row operation type: $0", type));
      }
    }
    return Status::OK();
  }

  void InsertOriginalRows(int64_t num_rowsets, int64_t rows_per_rowset) {
    for (int64_t rowset_id = 0; rowset_id < num_rowsets; rowset_id++) {
      ASSERT_OK(InsertOrUpsertTestRows(RowOperationsPB::INSERT,
                                       rowset_id * rows_per_rowset,
                                       rows_per_rowset,
                                       /*val*/0));
      ASSERT_OK(tablet()->Flush());
    }
    ASSERT_EQ(num_rowsets, tablet()->num_rowsets());
  }

  void UpdateOriginalRowsNoFlush(int64_t num_rowsets, int64_t rows_per_rowset,
      int32_t val) {
    for (int64_t rowset_id = 0; rowset_id < num_rowsets; rowset_id++) {
      ASSERT_OK(InsertOrUpsertTestRows(RowOperationsPB::UPSERT,
                                       rowset_id * rows_per_rowset,
                                       rows_per_rowset,
                                       val));
    }
    ASSERT_EQ(num_rowsets, tablet()->num_rowsets());
  }

  // Inserts num_rowsets * rows_per_rowset rows in an interleaved pattern and
  // flushes the MRS after each batch, creating num_rowsets DiskRowSets that
  // have overlapping key ranges, thereby making all of the rowsets eligible
  // for compaction when compaction policy picks rowsets.
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

  // Workload to generate large sized deltas for compaction.
  // The method generates 1 MB size worth of deltas with size_factor as 1.
  // Callers can adjust the size_factor.
  // For example, to generate 5MB, set size_factor as 5.
  // Similarly, to generate 35MB, set size_factor as 35.
  void GenHighMemConsumptionDeltas(uint32_t size_factor);

  // Workload that generates a large number of REDO delta files through
  // frequent DMS flushes, then converts all REDOs to UNDOs via major delta
  // compaction. The resulting on-disk state (overlapping DiskRowSets, each
  // carrying many UNDO delta files) creates realistic memory pressure when
  // rowset compaction reads every delta file simultaneously.
  //
  // The method generates kFlushRounds = 2 * size_factor REDO delta files per
  // DiskRowSet, each containing kUpdatesPerFlush * kRowsPerRowset mutations.
  // After major delta compaction the equivalent UNDO data is created.
  // size_factor=1 generates data that requires ~6GB memory for decompressed
  // delta buffers and ~6GB memory for mutations.
  void GenHighMemConsumptionDeltasWithFrequentFlush(uint32_t size_factor);

  // Updates every row across all rowsets in a single pass, writing both the
  // narrow INT64 'val' column and the wide 'large_val' STRING column.
  void UpdateAllRowsWithLargeValNoFlush(uint32_t num_rowsets,
                                        uint32_t rows_per_rowset,
                                        int32_t val,
                                        const std::string& large_payload);

  // Enables compaction memory budgeting and then runs rowset compaction.
  // Caller can set constraints on budget and expect the results accordingly.
  // If constraints are applied, compaction may be skipped.
  void TestRowSetCompactionWithOrWithoutBudgetingConstraints(bool budgeting_constraints_applied);

  // Tests appropriate logs are printed when major compaction crosses memory threshold.
  void TestMajorCompactionCrossingMemoryThreshold();

  // Tests appropriate logs are printed when rowset compaction crosses memory threshold.
  void TestRowSetCompactionCrossingMemoryThreshold();

  static void SetUpTestSuite() {
    // Keep the memory hard limit as 1GB for deterministic results.
    // The tests in this file have a requirement of memory hard limit to be of
    // lower value in order to ensure that test expectations are met.
    // Since we have initialized memory hard limit here to 1 GB, it is going to
    // remain the same throughout the lifecyle of this binary.
    // It is important that no test in this file is expecting memory hard limit
    // set to physical memory on the node (running the test) i.e. all the tests
    // are working with the assumption that memory hard limit is limited to 1 GB.
    FLAGS_memory_limit_hard_bytes = 1024 * 1024 * 1024;

    FLAGS_rowset_compaction_ancient_delta_threshold_enabled = true;
    FLAGS_rowset_compaction_enforce_preset_factor = true;
    FLAGS_rowset_compaction_memory_estimate_enabled = true;

    // Ensure memory budgeting applies
    FLAGS_rowset_compaction_estimate_min_deltas_size_mb = 0;
  }
};

void TestHighMemCompaction::TestRowSetCompactionWithOrWithoutBudgetingConstraints(
    bool budgeting_constraints_applied) {
  // size factor as 2 generates ~2MB memory size worth of deltas
  NO_FATALS(GenHighMemConsumptionDeltas(2));

  // Run rowset compaction.
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

  if (budgeting_constraints_applied) {
    ASSERT_STR_CONTAINS(JoinStrings(sink.logged_msgs(), "\n"),
                        "removed from compaction input due to memory constraints");
  } else {
    ASSERT_STR_NOT_CONTAINS(JoinStrings(sink.logged_msgs(), "\n"),
                            "removed from compaction input due to memory constraints");
  }
}

void TestHighMemCompaction::TestMajorCompactionCrossingMemoryThreshold() {
  // Size factor as 2 generates ~2MB memory size worth of deltas.
  NO_FATALS(GenHighMemConsumptionDeltas(2));

  // Run major delta compaction.
  StringVectorSink sink;
  ScopedRegisterSink reg(&sink);
  ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  ASSERT_STR_MATCHES(JoinStrings(sink.logged_msgs(), "\n"),
                     "beyond hard memory limit of.*MajorDeltaCompaction ops consumption:");
}

void TestHighMemCompaction::TestRowSetCompactionCrossingMemoryThreshold() {
  // Size factor as 2 generates ~2MB memory size worth of deltas.
  NO_FATALS(GenHighMemConsumptionDeltas(2));

  // Run rowset compaction.
  StringVectorSink sink;
  ScopedRegisterSink reg(&sink);
  ASSERT_OK(tablet()->Compact(Tablet::COMPACT_NO_FLAGS));
  ASSERT_STR_MATCHES(JoinStrings(sink.logged_msgs(), "\n"),
                     "beyond hard memory limit of.*Rowset merge compaction ops consumption:");
}

void TestHighMemCompaction::GenHighMemConsumptionDeltas(uint32_t size_factor) {
  constexpr const uint32_t kNumRowsets = 10;
  constexpr const uint32_t kNumRowsPerRowset = 2;
  const uint32_t num_updates = 5000 * size_factor;

  NO_FATALS(InsertOriginalRows(kNumRowsets, kNumRowsPerRowset));

  // Mutate all of the rows.
  for (int i = 1; i <= num_updates; i++) {
    NO_FATALS(UpdateOriginalRowsNoFlush(kNumRowsets, kNumRowsPerRowset, i));
  }
  ASSERT_OK(tablet()->FlushAllDMSForTests());
}

void TestHighMemCompaction::UpdateAllRowsWithLargeValNoFlush(
    uint32_t num_rowsets,
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

void TestHighMemCompaction::GenHighMemConsumptionDeltasWithFrequentFlush(
    uint32_t size_factor) {
  // Set flags to drive aggressive DMS/MRS flushing. With flush_threshold_mb=0
  // and flush_threshold_secs=1, frequent and immediate flush of DMS/MRS after
  // write is possible.
  FLAGS_flush_threshold_mb = 0;
  FLAGS_flush_threshold_secs = 1;
  FLAGS_flush_upper_bound_ms = 100;

  constexpr uint32_t kNumRowsets = 10;
  constexpr uint32_t kRowsPerRowset = 1000;

  // Each of the kFlushRounds cycles performs kUpdatesPerFlush full-table
  // update passes and then flushes the DeltaMemStore. This creates exactly
  // one new on-disk REDO delta file per DiskRowSet per cycle, so after all
  // cycles each DiskRowSet has kFlushRounds separate REDO delta files.
  //
  // Each mutation writes kLargeValSizeBytes into the wide 'large_val' column so that
  // the on-disk delta entry is ~64 KB. This lets the test reach a large total delta
  // volume with kFlushRounds * kUpdatesPerFlush * kNumRowsets * kRowsPerRowset total
  // tablet operations.
  const uint32_t kFlushRounds = 2 * size_factor;
  constexpr uint32_t kUpdatesPerFlush = 5;
  constexpr size_t kLargeValSizeBytes = 64 * 1024;

  // The payload uses a periodic pattern ((i*193 + 37) % 251) rather than fully
  // random or constant bytes, giving data that is compressible on disk yet
  // expands in memory. With rowset_compaction_rows_per_block = 10000, every delta
  // iterator holds all of its CFile blocks at once, so peak RSS is driven by the
  // total *uncompressed* delta size even though the on-disk footprint stays small.
  // Fully random bytes wouldn't compress, so the on-disk size would balloon toward
  // RSS and force the compaction memory budget limit flags to be loosened; constant
  // bytes compress even better but cost more CPU/wall time per run. The periodic
  // pattern is the middle ground.
  std::string kLargePayload(kLargeValSizeBytes, '\0');
  for (size_t i = 0; i < kLargeValSizeBytes; i++) {
    kLargePayload[i] = static_cast<char>((i * 193U + 37U) % 251U);
  }

  // ---- Step 1: Create DiskRowSets with overlapping key ranges ----
  // InsertInterleavedRows uses a round-robin insertion pattern so every
  // DiskRowSet's keys span the full range ["hello 0", "hello N-1"], giving
  // genuine pairwise overlap. InsertOriginalRows would instead produce
  // contiguous ranges that only overlap through DRS 0, causing the compaction
  // policy to select at most one other rowset.
  NO_FATALS(InsertInterleavedRows(kNumRowsets, kRowsPerRowset));
  ASSERT_EQ(kNumRowsets, tablet()->num_rowsets());

  // ---- Step 2: Generate kFlushRounds REDO delta files per DiskRowSet ----
  // After each round the running REDO delta file count per rowset is verified
  // to confirm that the count increases monotonically as required.
  for (uint32_t round = 0; round < kFlushRounds; round++) {
    for (uint32_t pass = 0; pass < kUpdatesPerFlush; pass++) {
      const int32_t val = static_cast<int32_t>(round * kUpdatesPerFlush + pass + 1);
      NO_FATALS(UpdateAllRowsWithLargeValNoFlush(
          kNumRowsets, kRowsPerRowset, val, kLargePayload));
    }
    ASSERT_OK(tablet()->FlushAllDMSForTests());

    // Confirm that the REDO delta file count has grown after this flush.
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

  // ---- Step 3: Major delta compaction — apply all REDOs, create UNDOs ----
  // CompactWorstDeltas() compacts the single DiskRowSet with the highest
  // delta-compaction score. Run it kNumRowsets times so that every rowset
  // has its REDO delta files merged into the base data with corresponding
  // UNDO delta files created for the historical versions.
  for (uint32_t i = 0; i < kNumRowsets; i++) {
    ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  }
  // Verify that UNDO delta stores were created and log the total on-disk sizes.
  uint64_t total_redo_bytes = 0;
  uint64_t total_undo_bytes = 0;
  uint64_t total_base_bytes = 0;
  {
    std::vector<std::shared_ptr<RowSet>> rowsets;
    tablet()->GetRowSetsForTests(&rowsets);
    for (const auto& rs : rowsets) {
      const auto* drs = down_cast<DiskRowSet*>(rs.get());
      ASSERT_GT(drs->delta_tracker().CountUndoDeltaStores(), 0)
          << "Each DiskRowSet should have UNDO delta stores after major "
             "delta compaction";
      DiskRowSetSpace drss;
      drs->GetDiskRowSetSpaceUsage(&drss);
      total_redo_bytes += drss.redo_deltas_size;
      total_undo_bytes += drss.undo_deltas_size;
      total_base_bytes += drss.base_data_size;
    }
  }
  LOG(INFO) << Substitute(
      "Major delta compaction ran: base_data=$0 bytes, "
      "redo_deltas=$1 bytes, undo_deltas=$2 bytes, "
      "total=$3 bytes, size_factor=$4)",
      total_base_bytes, total_redo_bytes, total_undo_bytes,
      total_base_bytes + total_redo_bytes + total_undo_bytes, size_factor);
}

// This test adds workload of rowsets updates in order to
// generate some number of REDO deltas. Along with that, memory
// budgeting constraints denoted by flags are enabled in order
// to make sure that when rowset compaction is invoked, it takes
// into consideration the amount of free memory left and based on
// that proceed with the compaction because of availability of memory.
TEST_F(TestHighMemCompaction, TestRowSetCompactionProceedWithNoBudgetingConstraints) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // 1 as mem factor implies ~(2*1)MB memory requirements for all rowsets,
  // ok for compaction to proceed
  FLAGS_rowset_compaction_delta_memory_factor = 1;
  TestRowSetCompactionWithOrWithoutBudgetingConstraints(false);
}

// This test adds workload of rowsets updates in order to
// generate huge number of REDO deltas. Along with that, memory
// budgeting constraints denoted by flags are enabled in order
// to make sure that when rowset compaction is invoked, it takes
// into consideration the amount of free memory left and based on
// that skip the compaction because of lack of memory.
TEST_F(TestHighMemCompaction, TestRowSetCompactionSkipWithBudgetingConstraints) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // 1024000 mem factor implies ~(2*1024000)MB memory requirements for all rowsets,
  // forces to skip compaction
  FLAGS_rowset_compaction_delta_memory_factor = 1024000;
  TestRowSetCompactionWithOrWithoutBudgetingConstraints(true);
}

TEST_F(TestHighMemCompaction, TestMajorCompactionMemoryPressure) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Set the threshold to a value that is reliably below the minimum tracked
  // allocation during any compaction run — concretely below the initial 32 KB
  // arena footprint reported to the tracker at the start of FlushRowSetAndDeltas().
  // In ASAN builds CurrentConsumption() falls back to
  // MemTracker::GetRootTracker()->consumption() instead of tcmalloc heap
  // usage. After fixing the double-counting in delta_blocks_mem_size(), the
  // tracker value is lower and the old 3 MB constant was no longer reliably
  // exceeded. Using 16 KB as the threshold guarantees the warning fires for
  // compaction on both ASAN and non-ASAN builds.
  constexpr int64_t compaction_mem_usage_approx = 16 * 1024;

  // Set appropriate flags to ensure memory threshold checks fail.
  FLAGS_memory_limit_compact_usage_warn_threshold_percentage =
      static_cast<double>(compaction_mem_usage_approx * 100) / FLAGS_memory_limit_hard_bytes;

  TestMajorCompactionCrossingMemoryThreshold();
}

TEST_F(TestHighMemCompaction, TestRowSetCompactionMemoryPressure) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Same rationale as TestMajorCompactionMemoryPressure: use 16 KB so the
  // warning threshold is reliably crossed on both ASAN and non-ASAN builds
  // after fixing the double-counting in delta_blocks_mem_size().
  constexpr int64_t compaction_mem_usage_approx = 16 * 1024;

  // Set appropriate flags to ensure memory threshold checks fail.
  FLAGS_memory_limit_compact_usage_warn_threshold_percentage =
      static_cast<double>(compaction_mem_usage_approx * 100) / FLAGS_memory_limit_hard_bytes;

  TestRowSetCompactionCrossingMemoryThreshold();
}

// This test validates the high-memory behaviour of rowset compaction when
// delta data has been built up through many frequent DMS flushes rather than
// one large end-of-test flush.
//
// Workload summary
// ----------------
// 1. Insert kNumRowsets * kRowsPerRowset rows, flushing the MRS after each
//    batch. Because the string keys "hello 0" ... "hello N" have a lexicographic
//    order that interleaves them across batches (e.g. "hello 999" > "hello 1000"),
//    the resulting DiskRowSets have overlapping key ranges and are therefore
//    prime candidates for rowset compaction.
//
// 2. Perform kFlushRounds update-then-flush cycles. Each cycle updates every
//    row kUpdatesPerFlush times and then calls FlushAllDMSForTests(), creating
//    one new on-disk REDO delta file per DiskRowSet per cycle. After all
//    cycles, each DiskRowSet carries kFlushRounds separate REDO delta files
//    instead of one large file, mirroring what happens in production when
//    flush_threshold_mb and flush_threshold_secs are tuned for high frequency.
//
// 3. Run major delta compaction (once per DiskRowSet) to apply all REDO deltas
//    into the base data and produce corresponding UNDO delta files.
//
// 4. Run rowset compaction. The merge must open delta-file iterators for every
//    on-disk UNDO (and any remaining REDO) delta file across all rowsets being
//    merged, so the compaction operation's memory consumption grows with the
//    total number of delta files.
//
// Size target
// -----------
// With size_factor=1 (the default): ~2 flush rounds * 5 updates * 10,000
// rows ≈ 100K mutations; combined REDO + UNDO on-disk footprint ≈ 40 MB.
// To reach the 1 GB target (undo + redo + mutations + base data > 1 GB),
// set size_factor=25, which produces ~100 flush rounds and ~1 GB of delta data.
TEST_F(TestHighMemCompaction, HighDeltaVolumeWithFrequentFlushForRowsetCompaction) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // The test takes significantly longer to complete for TSAN build.
  // It also demands significant amount of additional memory for shadow space.
  // Similarly, ASAN build has memory overhead for extra padding, etc.
  // Skip the test for both builds to avoid spurious failures.
#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
  GTEST_SKIP() << "test is skipped in TSAN and ASAN builds";
#endif

  // size_factor=1 keeps the runtime manageable (≈ 30-40 seconds).
  // Raise to 25 for a full 1 GB workload.
  NO_FATALS(GenHighMemConsumptionDeltasWithFrequentFlush(/*size_factor=*/1));

  // Assert that a meaningful amount of delta data was produced. The exact
  // bytes depend on encoding and compression, but even at the minimum
  // size_factor=1 we expect well over 40 MB across all rowsets.
  {
    std::vector<std::shared_ptr<RowSet>> rowsets;
    tablet()->GetRowSetsForTests(&rowsets);
    uint64_t total_delta_bytes = 0;
    for (const auto& rs : rowsets) {
      const auto* drs = down_cast<DiskRowSet*>(rs.get());
      DiskRowSetSpace drss;
      drs->GetDiskRowSetSpaceUsage(&drss);
      total_delta_bytes += drss.redo_deltas_size + drss.undo_deltas_size;
    }
    ASSERT_GT(total_delta_bytes, 40ULL * 1024 * 1024)
        << "Expected at least 50 MB of combined REDO + UNDO delta data";
  }

  // Increase the per-batch row count from the default 100 to the full dataset
  // size (kNumRowsets * kRowsPerRowset = 10,000). With the default 100-row
  // batches, compaction is fully streaming: each batch loads its delta blocks,
  // applies mutations, and evicts those blocks before loading the next batch,
  // so peak RSS exceeds the baseline to some extent. Processing all rows in a
  // single batch forces every delta-file iterator to hold all of its CFile
  // blocks in memory simultaneously, driving up actual RSS in proportion to
  // the total uncompressed delta data size.
  FLAGS_rowset_compaction_rows_per_block = 10000;

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
