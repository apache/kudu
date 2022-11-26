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

#include "kudu/tablet/tablet_mm_ops.h"

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace tablet {

class KuduTabletMmOpsTest : public TabletTestBase<IntKeyTestSetup<INT64>> {
 protected:
  typedef TabletTestBase<IntKeyTestSetup<INT64> > Superclass;

  KuduTabletMmOpsTest()
  : Superclass(),
    next_time_(MonoTime::Now()) {
  }

  virtual void SetUp() OVERRIDE {
    Superclass::SetUp();
    TabletMetrics* metrics = tablet()->metrics();
    all_possible_metrics_.push_back(metrics->flush_mrs_duration);
    all_possible_metrics_.push_back(metrics->flush_dms_duration);
    all_possible_metrics_.push_back(metrics->compact_rs_duration);
    all_possible_metrics_.push_back(metrics->delta_minor_compact_rs_duration);
    all_possible_metrics_.push_back(metrics->delta_major_compact_rs_duration);
    compaction_metrics_.push_back(metrics->flush_mrs_duration);
    compaction_metrics_.push_back(metrics->flush_dms_duration);
    compaction_metrics_.push_back(metrics->compact_rs_duration);
  }

  // Functions that call MaintenanceOp::UpdateStats() first sleep for a nominal
  // amount of time, to ensure the "before" and "after" timestamps are unique
  // if the stats are modified.
  void StatsShouldChange(MaintenanceOp* op) {
    SleepFor(MonoDelta::FromMilliseconds(1));
    op->UpdateStats(&stats_);
    ASSERT_LT(next_time_, stats_.last_modified());
    next_time_ = stats_.last_modified();
  }

  void StatsShouldNotChange(MaintenanceOp* op) {
    SleepFor(MonoDelta::FromMilliseconds(1));
    op->UpdateStats(&stats_);
    ASSERT_EQ(next_time_, stats_.last_modified());
  }

  void TestFirstCall(MaintenanceOp* op) {
    // The very first call to UpdateStats() will update the stats, but
    // subsequent calls are cached.
    NO_FATALS(StatsShouldChange(op));
    NO_FATALS(StatsShouldNotChange(op));
    NO_FATALS(StatsShouldNotChange(op));
  }

  void TestAffectedMetrics(MaintenanceOp* op,
                           const std::unordered_set<
                             scoped_refptr<Histogram>,
                             ScopedRefPtrHashFunctor<Histogram>,
                             ScopedRefPtrEqualToFunctor<Histogram> >& metrics) {
    for (const scoped_refptr<Histogram>& c : all_possible_metrics_) {
      c->Increment(1); // value doesn't matter
      if (ContainsKey(metrics, c)) {
        NO_FATALS(StatsShouldChange(op));
      }
      NO_FATALS(StatsShouldNotChange(op));
      NO_FATALS(StatsShouldNotChange(op));
    }
  }

  void UpsertData() {
    // Upsert new rows.
    this->UpsertTestRows(0, 1000, 1000);
    // Upsert rows that are in the MRS.
    this->UpsertTestRows(0, 2000, 1001);

    // Flush it.
    ASSERT_OK(this->tablet()->Flush());
    // Upsert rows that are in the DRS.
    this->UpsertTestRows(0, 3000, 1002);
  }

  void TestNoAffectedMetrics(MaintenanceOp* op) {
    for (const scoped_refptr<Histogram>& c : compaction_metrics_) {
      auto min_value = c->histogram()->MinValue();
      auto mean_value = c->histogram()->MeanValue();
      auto max_value = c->histogram()->MaxValue();
      auto total_count = c->histogram()->TotalCount();
      auto total_sum = c->histogram()->TotalSum();
      UpsertData();
      op->UpdateStats(&stats_);
      ASSERT_FALSE(stats_.runnable());
      ASSERT_EQ(min_value, c->histogram()->MinValue());
      ASSERT_EQ(mean_value, c->histogram()->MeanValue());
      ASSERT_EQ(max_value, c->histogram()->MaxValue());
      ASSERT_EQ(total_count, c->histogram()->TotalCount());
      ASSERT_EQ(total_sum, c->histogram()->TotalSum());
    }
  }

  MaintenanceOpStats stats_;
  MonoTime next_time_;
  std::vector<scoped_refptr<Histogram> > all_possible_metrics_;
  std::vector<scoped_refptr<Histogram> > compaction_metrics_;
};

TEST_F(KuduTabletMmOpsTest, TestCompactRowSetsOpCacheStats) {
  CompactRowSetsOp op(tablet().get());
  ASSERT_FALSE(op.DisableCompaction());
  NO_FATALS(TestFirstCall(&op));
  auto* m = tablet()->metrics();
  NO_FATALS(TestAffectedMetrics(&op, { m->flush_mrs_duration,
                                       m->compact_rs_duration,
                                       m->undo_delta_block_gc_perform_duration }));
}

TEST_F(KuduTabletMmOpsTest, TestDisableCompactRowSetsOp) {
  CompactRowSetsOp op(tablet().get());
  TableExtraConfigPB extra_config;
  extra_config.set_disable_compaction(true);
  NO_FATALS(AlterSchema(*harness_->tablet()->schema(), std::make_optional(extra_config)));
  ASSERT_TRUE(op.DisableCompaction());
  NO_FATALS(TestNoAffectedMetrics(&op));
}

TEST_F(KuduTabletMmOpsTest, TestMinorDeltaCompactionOpCacheStats) {
  MinorDeltaCompactionOp op(tablet().get());
  ASSERT_FALSE(op.DisableCompaction());
  NO_FATALS(TestFirstCall(&op));
  NO_FATALS(TestAffectedMetrics(&op, { tablet()->metrics()->flush_mrs_duration,
                                       tablet()->metrics()->flush_dms_duration,
                                       tablet()->metrics()->compact_rs_duration,
                                       tablet()->metrics()->delta_minor_compact_rs_duration }));
}

TEST_F(KuduTabletMmOpsTest, TestDisableMinorDeltaCompactionOp) {
  MinorDeltaCompactionOp op(tablet().get());
  TableExtraConfigPB extra_config;
  extra_config.set_disable_compaction(true);
  NO_FATALS(AlterSchema(*harness_->tablet()->schema(), std::make_optional(extra_config)));
  ASSERT_TRUE(op.DisableCompaction());
  NO_FATALS(TestNoAffectedMetrics(&op));
}

TEST_F(KuduTabletMmOpsTest, TestMajorDeltaCompactionOpCacheStats) {
  MajorDeltaCompactionOp op(tablet().get());
  ASSERT_FALSE(op.DisableCompaction());
  NO_FATALS(TestFirstCall(&op));
  NO_FATALS(TestAffectedMetrics(&op, { tablet()->metrics()->flush_mrs_duration,
                                       tablet()->metrics()->flush_dms_duration,
                                       tablet()->metrics()->compact_rs_duration,
                                       tablet()->metrics()->delta_minor_compact_rs_duration,
                                       tablet()->metrics()->delta_major_compact_rs_duration }));
}

TEST_F(KuduTabletMmOpsTest, TestDisableMajorDeltaCompactionOp) {
  MajorDeltaCompactionOp op(tablet().get());
  TableExtraConfigPB extra_config;
  extra_config.set_disable_compaction(true);
  NO_FATALS(AlterSchema(*harness_->tablet()->schema(), std::make_optional(extra_config)));
  ASSERT_TRUE(op.DisableCompaction());
  NO_FATALS(TestNoAffectedMetrics(&op));
}
} // namespace tablet
} // namespace kudu
