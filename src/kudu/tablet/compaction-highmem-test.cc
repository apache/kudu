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
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
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
DECLARE_int64(memory_limit_hard_bytes);
DECLARE_uint32(rowset_compaction_estimate_min_deltas_size_mb);

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

  // Workload to generate large sized deltas for compaction.
  // The method generates 1 MB size worth of deltas with size_factor as 1.
  // Callers can adjust the size_factor.
  // For example, to generate 5MB, set size_factor as 5.
  // Similarly, to generate 35MB, set size_factor as 35.
  void GenHighMemConsumptionDeltas(const uint32_t size_factor);

  // Enables compaction memory budgeting and then runs rowset compaction.
  // Caller can set constraints on budget and expect the results accordingly.
  // If constraints are applied, compaction may be skipped.
  void TestRowSetCompactionWithOrWithoutBudgetingConstraints(bool budgeting_constraints_applied);

  // Tests appropriate logs are printed when major compaction crosses memory threshold.
  void TestMajorCompactionCrossingMemoryThreshold();

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
  GenHighMemConsumptionDeltas(2);

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
  GenHighMemConsumptionDeltas(2);

  // Run major delta compaction.
  StringVectorSink sink;
  ScopedRegisterSink reg(&sink);
  scoped_refptr<Trace> trace(new Trace);
  ADOPT_TRACE(trace.get());
  ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  ASSERT_STR_CONTAINS(JoinStrings(sink.logged_msgs(), "\n"),
                      "Beyond hard memory limit of");
}

void TestHighMemCompaction::GenHighMemConsumptionDeltas(const uint32_t size_factor) {
  constexpr const uint32_t num_rowsets = 10;
  constexpr const uint32_t num_rows_per_rowset = 2;
  const uint32_t num_updates = 5000 * size_factor;

  NO_FATALS(InsertOriginalRows(num_rowsets, num_rows_per_rowset));

  // Mutate all of the rows.
  for (int i = 1; i <= num_updates; i++) {
    UpdateOriginalRowsNoFlush(num_rowsets, num_rows_per_rowset, i);
  }
  ASSERT_OK(tablet()->FlushAllDMSForTests());
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

  // Approximate memory consumed by delta compaction during the course of this test.
  // Total consumption would always exceed this usage. Since we want to be
  // certain that warning threshold limit is crossed, this value is kept low.
  const int64_t compaction_mem_usage_approx = 3 * 1024 * 1024;

  // Set appropriate flags to ensure memory threshold checks fail.
  FLAGS_memory_limit_compact_usage_warn_threshold_percentage =
      static_cast<double>(compaction_mem_usage_approx * 100) / FLAGS_memory_limit_hard_bytes;

  TestMajorCompactionCrossingMemoryThreshold();
}

} // namespace tablet
} // namespace kudu
