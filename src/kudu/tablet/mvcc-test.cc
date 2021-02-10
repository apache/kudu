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

#include "kudu/tablet/mvcc.h"

#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/util/barrier.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::thread;
using std::unique_ptr;
using std::vector;

METRIC_DECLARE_entity(server);

namespace kudu {
namespace tablet {

class MvccTest : public KuduTest {
 public:
  MvccTest()
      : clock_(Timestamp::kInitialTimestamp) {
  }

  void WaitForSnapshotAtTSThread(MvccManager* mgr, Timestamp ts) {
    MvccSnapshot s;
    CHECK_OK(mgr->WaitForSnapshotWithAllApplied(ts, &s, MonoTime::Max()));
    CHECK(s.is_clean()) << "verifying postcondition";
    std::lock_guard<simple_spinlock> lock(lock_);
    result_snapshot_.reset(new MvccSnapshot(s));
  }

  bool HasResultSnapshot() {
    std::lock_guard<simple_spinlock> lock(lock_);
    return result_snapshot_ != nullptr;
  }

 protected:
  clock::LogicalClock clock_;
  simple_spinlock lock_;
  unique_ptr<MvccSnapshot> result_snapshot_;
};

TEST_F(MvccTest, TestMvccBasic) {
  MvccManager mgr;
  MvccSnapshot snap;

  // Initial state should not have any applied ops.
  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied={T|T < 1}]", snap.ToString());
  ASSERT_FALSE(snap.IsApplied(Timestamp(1)));
  ASSERT_FALSE(snap.IsApplied(Timestamp(2)));

  // Start timestamp 1
  Timestamp t = clock_.Now();
  ASSERT_EQ(1, t.value());
  ScopedOp op(&mgr, t);

  // State should still have no applied ops, since 1 is in-flight.
  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied={T|T < 1}]", snap.ToString());
  ASSERT_FALSE(snap.IsApplied(Timestamp(1)));
  ASSERT_FALSE(snap.IsApplied(Timestamp(2)));

  // Mark timestamp 1 as "applying"
  op.StartApplying();

  // This should not change the set of applied ops.
  ASSERT_FALSE(snap.IsApplied(Timestamp(1)));

  // Apply timestamp 1
  op.FinishApplying();

  // State should show 0 as applied, 1 as nonapplied.
  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied={T|T < 1 or (T in {1})}]", snap.ToString());
  ASSERT_TRUE(snap.IsApplied(Timestamp(1)));
  ASSERT_FALSE(snap.IsApplied(Timestamp(2)));
}

TEST_F(MvccTest, TestMvccMultipleInFlight) {
  MvccManager mgr;
  MvccSnapshot snap;

  Timestamp t1 = clock_.Now();
  ASSERT_EQ(1, t1.value());
  ScopedOp op1(&mgr, t1);
  Timestamp t2 = clock_.Now();
  ASSERT_EQ(2, t2.value());
  ScopedOp op2(&mgr, t2);

  // State should still have no applied ops, since both are in-flight.

  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied={T|T < 1}]", snap.ToString());
  ASSERT_FALSE(snap.IsApplied(t1));
  ASSERT_FALSE(snap.IsApplied(t2));

  // Apply timestamp 2
  op2.StartApplying();
  op2.FinishApplying();

  // State should show 2 as applied, 1 as nonapplied.
  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied="
            "{T|T < 1 or (T in {2})}]",
            snap.ToString());
  ASSERT_FALSE(snap.IsApplied(t1));
  ASSERT_TRUE(snap.IsApplied(t2));

  // Start another ops. This gets timestamp 3
  Timestamp t3 = clock_.Now();
  ASSERT_EQ(3, t3.value());
  ScopedOp op3(&mgr, t3);

  // State should show 2 as applied, 1 and 4 as nonapplied.
  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied="
            "{T|T < 1 or (T in {2})}]",
            snap.ToString());
  ASSERT_FALSE(snap.IsApplied(t1));
  ASSERT_TRUE(snap.IsApplied(t2));
  ASSERT_FALSE(snap.IsApplied(t3));

  // Apply 3
  op3.StartApplying();
  op3.FinishApplying();

  // 2 and 3 applied
  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied="
            "{T|T < 1 or (T in {2,3})}]",
            snap.ToString());
  ASSERT_FALSE(snap.IsApplied(t1));
  ASSERT_TRUE(snap.IsApplied(t2));
  ASSERT_TRUE(snap.IsApplied(t3));

  // Apply 1
  op1.StartApplying();
  op1.FinishApplying();

  // All ops are applied, adjust the new op lower bound.
  mgr.AdjustNewOpLowerBound(t3);

  // all applied
  snap = MvccSnapshot(mgr);
  ASSERT_EQ("MvccSnapshot[applied={T|T < 3 or (T in {3})}]", snap.ToString());
  ASSERT_TRUE(snap.IsApplied(t1));
  ASSERT_TRUE(snap.IsApplied(t2));
  ASSERT_TRUE(snap.IsApplied(t3));
}

TEST_F(MvccTest, TestOutOfOrderOps) {
  MetricRegistry metric_registry;
  auto metric_entity(METRIC_ENTITY_server.Instantiate(&metric_registry, "mvcc-test"));
  clock::HybridClock hybrid_clock(metric_entity);
  ASSERT_OK(hybrid_clock.Init());
  MvccManager mgr;

  // Start a normal non-commit-wait op.
  Timestamp first_ts = hybrid_clock.Now();
  ScopedOp first_op(&mgr, first_ts);

  // Take a snapshot that con
  MvccSnapshot snap_with_nothing_applied(mgr);

  // Start an op as if it were using commit-wait (i.e. started in future)
  Timestamp cw_ts = hybrid_clock.NowLatest();
  ScopedOp cw_op(&mgr, cw_ts);

  // Apply the original op
  first_op.StartApplying();
  first_op.FinishApplying();

  // Start a new op
  Timestamp second_ts = hybrid_clock.Now();
  ScopedOp second_op(&mgr, second_ts);

  // The old snapshot should not have either op
  EXPECT_FALSE(snap_with_nothing_applied.IsApplied(first_ts));
  EXPECT_FALSE(snap_with_nothing_applied.IsApplied(second_ts));

  // A new snapshot should have only the first op
  MvccSnapshot snap_with_first_applied(mgr);
  EXPECT_TRUE(snap_with_first_applied.IsApplied(first_ts));
  EXPECT_FALSE(snap_with_first_applied.IsApplied(second_ts));

  // Apply the commit-wait one once it is time.
  ASSERT_OK(hybrid_clock.WaitUntilAfter(cw_ts, MonoTime::Max()));
  cw_op.StartApplying();
  cw_op.FinishApplying();

  // A new snapshot at this point should still think that normal_op_2 is nonapplied
  MvccSnapshot snap_with_all_applied(mgr);
  EXPECT_FALSE(snap_with_all_applied.IsApplied(second_ts));
}

// Tests starting ops at a point-in-time in the past and applying them while
// adjusting the new op timestamp lower bound.
TEST_F(MvccTest, TestSafeTimeWithOutOfOrderOps) {
  MvccManager mgr;

  // Set the clock to some time in the "future".
  ASSERT_OK(clock_.Update(Timestamp(100)));

  // Start an op in the "past"
  Timestamp ts_in_the_past(50);
  ScopedOp op_in_the_past(&mgr, ts_in_the_past);
  op_in_the_past.StartApplying();

  ASSERT_EQ(Timestamp::kInitialTimestamp, mgr.GetCleanTimestamp());

  // Applying 'op_in_the_past' should not advance the new op lower
  // bound or the clean time.
  op_in_the_past.FinishApplying();

  // Now take a snapshot.
  MvccSnapshot snap_with_first_op(mgr);

  // Because we did not advance the the new op lower bound or clean time, even
  // though the only in-flight op was applied at time 50, an op at time 40
  // should still be considered nonapplied.
  ASSERT_FALSE(snap_with_first_op.IsApplied(Timestamp(40)));

  // Now advance the both clean and new op lower bound watermarks to the last
  // applied op.
  mgr.AdjustNewOpLowerBound(Timestamp(50));

  ASSERT_EQ(ts_in_the_past, mgr.GetCleanTimestamp());

  MvccSnapshot snap_with_adjusted_clean_time(mgr);

  ASSERT_TRUE(snap_with_adjusted_clean_time.IsApplied(Timestamp(40)));
}

TEST_F(MvccTest, TestScopedOp) {
  MvccManager mgr;
  MvccSnapshot snap;

  {
    ScopedOp t1(&mgr, clock_.Now());
    ScopedOp t2(&mgr, clock_.Now());

    ASSERT_EQ(1, t1.timestamp().value());
    ASSERT_EQ(2, t2.timestamp().value());

    t1.StartApplying();
    t1.FinishApplying();

    snap = MvccSnapshot(mgr);
    ASSERT_TRUE(snap.IsApplied(t1.timestamp()));
    ASSERT_FALSE(snap.IsApplied(t2.timestamp()));
  }

  // t2 going out of scope aborts it.
  snap = MvccSnapshot(mgr);
  ASSERT_TRUE(snap.IsApplied(Timestamp(1)));
  ASSERT_FALSE(snap.IsApplied(Timestamp(2)));

  // Test that an applying scoped op does not crash if it goes out of
  // scope while the MvccManager is closed.
  mgr.Close();
  {
    ScopedOp t(&mgr, clock_.Now());
    NO_FATALS(t.StartApplying());
  }
}

TEST_F(MvccTest, TestPointInTimeSnapshot) {
  MvccSnapshot snap(Timestamp(10));

  ASSERT_TRUE(snap.IsApplied(Timestamp(1)));
  ASSERT_TRUE(snap.IsApplied(Timestamp(9)));
  ASSERT_FALSE(snap.IsApplied(Timestamp(10)));
  ASSERT_FALSE(snap.IsApplied(Timestamp(11)));
}

TEST_F(MvccTest, TestMayHaveAppliedOpsAtOrAfter) {
  MvccSnapshot snap;
  snap.all_applied_before_ = Timestamp(10);
  snap.applied_timestamps_.push_back(11);
  snap.applied_timestamps_.push_back(13);
  snap.none_applied_at_or_after_ = Timestamp(14);

  ASSERT_TRUE(snap.MayHaveAppliedOpsAtOrAfter(Timestamp(9)));
  ASSERT_TRUE(snap.MayHaveAppliedOpsAtOrAfter(Timestamp(10)));
  ASSERT_TRUE(snap.MayHaveAppliedOpsAtOrAfter(Timestamp(12)));
  ASSERT_TRUE(snap.MayHaveAppliedOpsAtOrAfter(Timestamp(13)));
  ASSERT_FALSE(snap.MayHaveAppliedOpsAtOrAfter(Timestamp(14)));
  ASSERT_FALSE(snap.MayHaveAppliedOpsAtOrAfter(Timestamp(15)));

  // Test for "all applied" snapshot
  MvccSnapshot all_applied =
      MvccSnapshot::CreateSnapshotIncludingAllOps();
  ASSERT_TRUE(
      all_applied.MayHaveAppliedOpsAtOrAfter(Timestamp(1)));
  ASSERT_TRUE(
      all_applied.MayHaveAppliedOpsAtOrAfter(Timestamp(12345)));

  // And "none applied" snapshot
  MvccSnapshot none_applied =
      MvccSnapshot::CreateSnapshotIncludingNoOps();
  ASSERT_FALSE(
      none_applied.MayHaveAppliedOpsAtOrAfter(Timestamp(1)));
  ASSERT_FALSE(
      none_applied.MayHaveAppliedOpsAtOrAfter(Timestamp(12345)));

  // Test for a "clean" snapshot
  MvccSnapshot clean_snap(Timestamp(10));
  ASSERT_TRUE(clean_snap.MayHaveAppliedOpsAtOrAfter(Timestamp(9)));
  ASSERT_FALSE(clean_snap.MayHaveAppliedOpsAtOrAfter(Timestamp(10)));
}

TEST_F(MvccTest, TestMayHaveNonAppliedOpsBefore) {
  MvccSnapshot snap;
  snap.all_applied_before_ = Timestamp(10);
  snap.applied_timestamps_.push_back(11);
  snap.applied_timestamps_.push_back(13);
  snap.none_applied_at_or_after_ = Timestamp(14);

  ASSERT_FALSE(snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(9)));
  ASSERT_TRUE(snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(10)));
  ASSERT_TRUE(snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(11)));
  ASSERT_TRUE(snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(13)));
  ASSERT_TRUE(snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(14)));
  ASSERT_TRUE(snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(15)));

  // Test for "all applied" snapshot
  MvccSnapshot all_applied =
      MvccSnapshot::CreateSnapshotIncludingAllOps();
  ASSERT_FALSE(
      all_applied.MayHaveNonAppliedOpsAtOrBefore(Timestamp(1)));
  ASSERT_FALSE(
      all_applied.MayHaveNonAppliedOpsAtOrBefore(Timestamp(12345)));

  // And "none applied" snapshot
  MvccSnapshot none_applied =
      MvccSnapshot::CreateSnapshotIncludingNoOps();
  ASSERT_TRUE(
      none_applied.MayHaveNonAppliedOpsAtOrBefore(Timestamp(1)));
  ASSERT_TRUE(
      none_applied.MayHaveNonAppliedOpsAtOrBefore(
          Timestamp(12345)));

  // Test for a "clean" snapshot
  MvccSnapshot clean_snap(Timestamp(10));
  ASSERT_FALSE(clean_snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(9)));
  ASSERT_TRUE(clean_snap.MayHaveNonAppliedOpsAtOrBefore(Timestamp(10)));

  // Test for the case where we have a single op in flight. Since this is also
  // the earliest op, all_applied_before_ is equal to the op's ts, but when
  // it gets applied we can't advance all_applied_before_ past it because
  // there is no other op to advance it to. In this case we should still report
  // that there can't be any nonapplied ops before.
  MvccSnapshot snap2;
  snap2.all_applied_before_ = Timestamp(10);
  snap2.applied_timestamps_.push_back(10);

  ASSERT_FALSE(snap2.MayHaveNonAppliedOpsAtOrBefore(Timestamp(10)));
}

TEST_F(MvccTest, TestAreAllOpsAppliedForTests) {
  MvccManager mgr;

  // start several ops and take snapshots along the way
  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);
  Timestamp ts2 = clock_.Now();
  ScopedOp op2(&mgr, ts2);
  Timestamp ts3 = clock_.Now();
  ScopedOp op3(&mgr, ts3);
  mgr.AdjustNewOpLowerBound(clock_.Now());

  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(1)));
  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(2)));
  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(3)));

  // Apply op3, should all still report as having as having nonapplied ops.
  op3.StartApplying();
  op3.FinishApplying();
  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(1)));
  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(2)));
  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(3)));

  // Apply op1, first snap with in-flights should now report as all applied
  // and remaining snaps as still having nonapplied ops
  op1.StartApplying();
  op1.FinishApplying();
  ASSERT_TRUE(mgr.AreAllOpsAppliedForTests(Timestamp(1)));
  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(2)));
  ASSERT_FALSE(mgr.AreAllOpsAppliedForTests(Timestamp(3)));

  // Now they should all report as all applied.
  op2.StartApplying();
  op2.FinishApplying();
  ASSERT_TRUE(mgr.AreAllOpsAppliedForTests(Timestamp(1)));
  ASSERT_TRUE(mgr.AreAllOpsAppliedForTests(Timestamp(2)));
  ASSERT_TRUE(mgr.AreAllOpsAppliedForTests(Timestamp(3)));
}

TEST_F(MvccTest, WaitForCleanSnapshotSnapWithNoInflights) {
  MvccManager mgr;
  Timestamp to_wait_for = clock_.Now();
  mgr.AdjustNewOpLowerBound(clock_.Now());
  thread waiting_thread = thread(&MvccTest::WaitForSnapshotAtTSThread, this, &mgr, to_wait_for);

  // join immediately.
  waiting_thread.join();
  ASSERT_TRUE(HasResultSnapshot());
}

TEST_F(MvccTest, WaitForCleanSnapshotSnapBeforeSafeTimeWithInFlights) {
  MvccManager mgr;

  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);
  Timestamp ts2 = clock_.Now();
  ScopedOp op2(&mgr, ts2);
  mgr.AdjustNewOpLowerBound(ts2);
  Timestamp to_wait_for = clock_.Now();

  // Select a new op timestamp lower bound that is after all ops and after the
  // the timestamp we'll wait for.  This will cause "clean time" to move when
  // op1 and op2 finish applying.
  Timestamp future_ts = clock_.Now();
  mgr.AdjustNewOpLowerBound(future_ts);

  thread waiting_thread = thread(&MvccTest::WaitForSnapshotAtTSThread, this, &mgr, to_wait_for);

  ASSERT_FALSE(HasResultSnapshot());
  op1.StartApplying();
  op1.FinishApplying();
  ASSERT_FALSE(HasResultSnapshot());
  op2.StartApplying();
  op2.FinishApplying();
  waiting_thread.join();
  ASSERT_TRUE(HasResultSnapshot());
}

TEST_F(MvccTest, WaitForCleanSnapshotSnapAfterSafeTimeWithInFlights) {
  MvccManager mgr;
  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);
  Timestamp ts2 = clock_.Now();
  ScopedOp op2(&mgr, ts2);
  mgr.AdjustNewOpLowerBound(ts2);

  // Wait should return immediately, since we have no ops "applying" yet.
  ASSERT_OK(mgr.WaitForApplyingOpsToApply());

  op1.StartApplying();

  Status s;
  thread waiting_thread = thread([&] {
    s = mgr.WaitForApplyingOpsToApply();
  });
  while (mgr.GetNumWaitersForTests() == 0) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }
  ASSERT_EQ(mgr.GetNumWaitersForTests(), 1);

  // Aborting the other op shouldn't affect our waiter.
  op2.Abort();
  ASSERT_EQ(mgr.GetNumWaitersForTests(), 1);

  // Applying our op should wake the waiter.
  op1.FinishApplying();
  ASSERT_EQ(mgr.GetNumWaitersForTests(), 0);
  waiting_thread.join();
  ASSERT_OK(s);
}

TEST_F(MvccTest, WaitForCleanSnapshotSnapAtTimestampWithInFlights) {
  MvccManager mgr;

  // Ops with timestamp 1 through 3
  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);
  Timestamp ts2 = clock_.Now();
  ScopedOp op2(&mgr, ts2);
  Timestamp ts3 = clock_.Now();
  ScopedOp op3(&mgr, ts3);

  // Start a thread waiting for ops with ts <= 2 to finish applying
  thread waiting_thread = thread(&MvccTest::WaitForSnapshotAtTSThread, this, &mgr, ts2);
  ASSERT_FALSE(HasResultSnapshot());

  // Apply op 1 - thread should still wait.
  op1.StartApplying();
  op1.FinishApplying();
  SleepFor(MonoDelta::FromMilliseconds(1));
  ASSERT_FALSE(HasResultSnapshot());

  // Apply op 3 - thread should still wait.
  op3.StartApplying();
  op3.FinishApplying();
  SleepFor(MonoDelta::FromMilliseconds(1));
  ASSERT_FALSE(HasResultSnapshot());

  // Apply op 2 - thread should still wait.
  op2.StartApplying();
  op2.FinishApplying();
  ASSERT_FALSE(HasResultSnapshot());

  // Advance new op lower bound and the clean time, thread should continue.
  mgr.AdjustNewOpLowerBound(ts3);
  waiting_thread.join();
  ASSERT_TRUE(HasResultSnapshot());
}

TEST_F(MvccTest, TestWaitForApplyingOpsToApply) {
  MvccManager mgr;

  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);
  Timestamp ts2 = clock_.Now();
  ScopedOp op2(&mgr, ts2);
  mgr.AdjustNewOpLowerBound(ts2);

  // Wait should return immediately, since we have no ops "applying" yet.
  ASSERT_OK(mgr.WaitForApplyingOpsToApply());

  op1.StartApplying();

  thread waiting_thread = thread(&MvccManager::WaitForApplyingOpsToApply, &mgr);
  while (mgr.GetNumWaitersForTests() == 0) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }
  ASSERT_EQ(mgr.GetNumWaitersForTests(), 1);

  // Aborting the other op shouldn't affect our waiter.
  op2.Abort();
  ASSERT_EQ(mgr.GetNumWaitersForTests(), 1);

  // Applying our op should wake the waiter.
  op1.FinishApplying();
  ASSERT_EQ(mgr.GetNumWaitersForTests(), 0);
  waiting_thread.join();
}

// Test to ensure that after MVCC has been closed, it will not Wait and will
// instead return an error.
TEST_F(MvccTest, TestDontWaitAfterClose) {
  MvccManager mgr;
  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);
  mgr.AdjustNewOpLowerBound(ts1);
  op1.StartApplying();

  // Spin up a thread to wait on the applying op.
  // Lock the changing status. This is only necessary in this test to read the
  // status from the main thread, showing that, regardless of where, closing
  // MVCC will cause waiters to abort mid-wait.
  Status s;
  simple_spinlock status_lock;
  thread waiting_thread = thread([&] {
    std::lock_guard<simple_spinlock> l(status_lock);
    s = mgr.WaitForApplyingOpsToApply();
  });

  // Wait until the waiter actually gets registered.
  while (mgr.GetNumWaitersForTests() == 0) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }

  // Set that the mgr is closing. This should cause waiters to abort.
  mgr.Close();
  waiting_thread.join();
  ASSERT_STR_CONTAINS(s.ToString(), "closed");
  ASSERT_TRUE(s.IsAborted());

  // New waiters should abort immediately.
  s = mgr.WaitForApplyingOpsToApply();
  ASSERT_STR_CONTAINS(s.ToString(), "closed");
  ASSERT_TRUE(s.IsAborted());
}

// Test that if we abort an op we don't advance the new op lower bound and
// don't add the op to the applied set.
TEST_F(MvccTest, TestOpAbort) {
  MvccManager mgr;

  // Ops with timestamps 1 through 3
  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);
  Timestamp ts2 = clock_.Now();
  ScopedOp op2(&mgr, ts2);
  Timestamp ts3 = clock_.Now();
  ScopedOp op3(&mgr, ts3);
  mgr.AdjustNewOpLowerBound(ts3);

  // Now abort op1, this shouldn't move the clean time and the op shouldn't be
  // reported as applied.
  op1.Abort();
  ASSERT_EQ(Timestamp::kInitialTimestamp, mgr.GetCleanTimestamp());
  ASSERT_FALSE(mgr.cur_snap_.IsApplied(ts1));

  // Applying op3 shouldn't advance the clean time since it is not the earliest
  // in-flight, but it should advance 'new_op_timestamp_exc_lower_bound_' to 3.
  op3.StartApplying();
  op3.FinishApplying();
  ASSERT_TRUE(mgr.cur_snap_.IsApplied(ts3));
  ASSERT_EQ(ts3, mgr.new_op_timestamp_exc_lower_bound_);

  // Applying op2 should advance the clean time to 3.
  op2.StartApplying();
  op2.FinishApplying();
  ASSERT_TRUE(mgr.cur_snap_.IsApplied(ts2));
  ASSERT_EQ(ts3, mgr.GetCleanTimestamp());
}

// This tests for a bug we were observing, where a clean snapshot would not
// coalesce to the latest timestamp.
TEST_F(MvccTest, TestAutomaticCleanTimeMoveToSafeTimeOnApply) {
  MvccManager mgr;
  clock_.Update(Timestamp(20));

  ScopedOp op1(&mgr, Timestamp(10));
  ScopedOp op2(&mgr, Timestamp(15));
  mgr.AdjustNewOpLowerBound(Timestamp(15));

  op2.StartApplying();
  op2.FinishApplying();

  op1.StartApplying();
  op1.FinishApplying();
  ASSERT_EQ(mgr.cur_snap_.ToString(), "MvccSnapshot[applied={T|T < 15 or (T in {15})}]");
}

// Various death tests which ensure that we can only transition in one of the following
// valid ways:
//
// - Start() -> StartApplying() -> FinishApplying()
// - Start() -> Abort()
//
// Any other transition should fire a CHECK failure.
TEST_F(MvccTest, TestIllegalStateTransitionsCrash) {
  MvccManager mgr;
  MvccSnapshot snap;

  EXPECT_DEATH({
      mgr.StartApplyingOp(Timestamp(1));
    }, "Cannot mark timestamp 1 as APPLYING: not in the in-flight map");

  // Depending whether this is a DEBUG or RELEASE build, the error message
  // could be different for this case -- the "future timestamp" check is only
  // run in DEBUG builds.
  EXPECT_DEATH({
      mgr.FinishApplyingOp(Timestamp(1));
    },
    "Trying to apply an op with a future timestamp|"
    "Trying to remove timestamp which isn't in the in-flight set: 1");

  clock_.Update(Timestamp(20));

  EXPECT_DEATH({
      mgr.FinishApplyingOp(Timestamp(1));
    }, "Trying to remove timestamp which isn't in the in-flight set: 1");

  // Start an op, and try applying it without having moved to "Applying"
  // state.
  Timestamp t = clock_.Now();
  mgr.StartOp(t);
  EXPECT_DEATH({
      mgr.FinishApplyingOp(t);
    }, "Trying to apply an op which never entered APPLYING state");

  // Aborting should succeed, since we never moved to Applying.
  mgr.AbortOp(t);

  // Aborting a second time should fail
  EXPECT_DEATH({
      mgr.AbortOp(t);
    }, "Trying to remove timestamp which isn't in the in-flight set: 21");

  // Start a new op. This time, mark it as Applying.
  t = clock_.Now();
  mgr.StartOp(t);
  mgr.AdjustNewOpLowerBound(t);
  mgr.StartApplyingOp(t);

  // Can only call StartApplying once.
  EXPECT_DEATH({
      mgr.StartApplyingOp(t);
    }, "Cannot mark timestamp 22 as APPLYING: wrong state: 1");

  // Cannot Abort() an op once we start applying it.
  EXPECT_DEATH({
      mgr.AbortOp(t);
    }, "op with timestamp 22 cannot be aborted in state 1");

  // We can apply it successfully.
  mgr.FinishApplyingOp(t);
}

TEST_F(MvccTest, TestWaitUntilCleanDeadline) {
  MvccManager mgr;

  // Ops with timestamp 1
  Timestamp ts1 = clock_.Now();
  ScopedOp op1(&mgr, ts1);

  // Wait until the 'op1' timestamp is clean -- this won't happen because the
  // op isn't applied yet.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromMilliseconds(10);
  MvccSnapshot snap;
  Status s = mgr.WaitForSnapshotWithAllApplied(ts1, &snap, deadline);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
}

// Test for a bug related to the initialization of the MvccManager without any
// pending ops, i.e. when there are only calls to
// AdjustNewOpLowerBound().
//
// Prior to the fix we would advance clean time but not the
// 'none_applied_at_or_after_' watermark, meaning the latter would become lower
// than clean time. This had the effect on compaction of culling delta files
// even though they shouldn't be culled.
// This test makes sure that watermarks are advanced correctly and that delta
// files are culled correctly.
TEST_F(MvccTest, TestCorrectInitWithNoOps) {
  MvccManager mgr;

  MvccSnapshot snap(mgr);
  EXPECT_EQ(snap.all_applied_before_, Timestamp::kInitialTimestamp);
  EXPECT_EQ(snap.none_applied_at_or_after_, Timestamp::kInitialTimestamp);
  EXPECT_EQ(snap.applied_timestamps_.size(), 0);

  // Read the clock a few times to advance the timestamp
  for (int i = 0; i < 10; i++) {
    clock_.Now();
  }

  // Advance the new op lower bound.
  Timestamp new_ts_lower_bound = clock_.Now();
  mgr.AdjustNewOpLowerBound(new_ts_lower_bound);

  // Test that the snapshot reports that a timestamp lower than the new op
  // lower bound may have applied ops after that timestamp. Conversely, test
  // that the snapshot reports that there are no applied ops at or after the
  // new lower bound.
  MvccSnapshot snap2;
  snap2 = MvccSnapshot(mgr);
  Timestamp before_lb(new_ts_lower_bound.value() - 1);
  Timestamp after_lb(new_ts_lower_bound.value() + 1);
  EXPECT_TRUE(snap2.MayHaveAppliedOpsAtOrAfter(before_lb));
  EXPECT_FALSE(snap2.MayHaveAppliedOpsAtOrAfter(after_lb));

  EXPECT_EQ(snap2.all_applied_before_, new_ts_lower_bound);
  EXPECT_EQ(snap2.none_applied_at_or_after_, new_ts_lower_bound);
  EXPECT_EQ(snap2.applied_timestamps_.size(), 0);
}

class TransactionMvccTest : public MvccTest {
 public:
  // Simulates successfully committing the given transaction by starting an MVCC
  // op to track the commit, and setting a higher commit timestamp while that
  // op is applying. Returns the commit timestmap.
  Timestamp Commit(MvccManager* mgr, TxnMetadata* txn_meta) {
    // Start an op to begin committing.
    Timestamp ts = clock_.Now();
    ScopedOp op(mgr, ts);
    txn_meta->set_commit_mvcc_op_timestamp(ts);
    // Finalize the commit with some future timestamp.
    op.StartApplying();
    Timestamp commit_ts = Timestamp(ts.value() + 10);
    txn_meta->set_commit_timestamp(commit_ts);
    op.FinishApplying();
    return commit_ts;
  }

  // Simulates aborting a transaction by beginning to commit, and then aborting.
  void AbortAfterBeginCommit(MvccManager* mgr, TxnMetadata* txn_meta) {
    // Start an op to begin committing.
    Timestamp ts = clock_.Now();
    ScopedOp op(mgr, ts);
    txn_meta->set_commit_mvcc_op_timestamp(ts);
    // Abort the commit.
    txn_meta->set_aborted();
    op.Abort();
  }

  // Simulates aborting without starting any ops.
  static void AbortBeforeBeginCommit(TxnMetadata* txn_meta) {
    txn_meta->set_aborted();
  }
};

// Test that timestamp snapshots before and after committing correctly
// determine whether transactions are committed.
TEST_F(TransactionMvccTest, TestTimestampSnapshot) {
  MvccManager mgr;
  scoped_refptr<TxnMetadata> committed_txn_meta(new TxnMetadata);
  scoped_refptr<TxnMetadata> aborted_before_begin_commit_txn_meta(new TxnMetadata);
  scoped_refptr<TxnMetadata> aborted_after_begin_commit_txn_meta(new TxnMetadata);
  Timestamp initial_ts = clock_.Now();
  MvccSnapshot initial_snap(initial_ts);
  ASSERT_FALSE(initial_snap.IsCommitted(*committed_txn_meta.get()));
  ASSERT_FALSE(initial_snap.IsCommitted(*aborted_before_begin_commit_txn_meta.get()));
  ASSERT_FALSE(initial_snap.IsCommitted(*aborted_after_begin_commit_txn_meta.get()));

  AbortAfterBeginCommit(&mgr, aborted_before_begin_commit_txn_meta.get());
  AbortAfterBeginCommit(&mgr, aborted_after_begin_commit_txn_meta.get());
  Timestamp commit_ts = Commit(&mgr, committed_txn_meta.get());

  // Snapshots taken right before the commit timestamp should not return that
  // the transaction was committed.
  MvccSnapshot before_commit_snap(commit_ts);
  ASSERT_FALSE(before_commit_snap.IsCommitted(*committed_txn_meta.get()));
  ASSERT_FALSE(before_commit_snap.IsCommitted(*aborted_before_begin_commit_txn_meta.get()));
  ASSERT_FALSE(before_commit_snap.IsCommitted(*aborted_after_begin_commit_txn_meta.get()));

  // Snapshots taken right after the commit timestamp shouldn't return that
  // aborted transactions were committed.
  MvccSnapshot after_commit_snap(Timestamp(commit_ts.value() + 1));
  ASSERT_TRUE(after_commit_snap.IsCommitted(*committed_txn_meta.get()));
  ASSERT_FALSE(after_commit_snap.IsCommitted(*aborted_before_begin_commit_txn_meta.get()));
  ASSERT_FALSE(after_commit_snap.IsCommitted(*aborted_after_begin_commit_txn_meta.get()));
}

// Test that the latest snapshots before and after committing or aborting
// correctly determine whether transactions are committed.
TEST_F(TransactionMvccTest, TestLatestSnapshot) {
  MvccManager mgr;
  {
    scoped_refptr<TxnMetadata> txn_meta(new TxnMetadata);
    MvccSnapshot latest_before_commit(mgr);
    ASSERT_FALSE(latest_before_commit.IsCommitted(*txn_meta.get()));
    Commit(&mgr, txn_meta.get());
    MvccSnapshot latest_after_commit(mgr);
    ASSERT_FALSE(latest_before_commit.IsCommitted(*txn_meta.get()));
    ASSERT_TRUE(latest_after_commit.IsCommitted(*txn_meta.get()));
  }
  {
    scoped_refptr<TxnMetadata> txn_meta(new TxnMetadata);
    MvccSnapshot latest_before_abort(mgr);
    ASSERT_FALSE(latest_before_abort.IsCommitted(*txn_meta.get()));
    AbortAfterBeginCommit(&mgr, txn_meta.get());
    MvccSnapshot latest_after_abort(mgr);
    ASSERT_FALSE(latest_before_abort.IsCommitted(*txn_meta.get()));
    ASSERT_FALSE(latest_after_abort.IsCommitted(*txn_meta.get()));
  }
  {
    scoped_refptr<TxnMetadata> txn_meta(new TxnMetadata);
    MvccSnapshot latest_before_abort(mgr);
    ASSERT_FALSE(latest_before_abort.IsCommitted(*txn_meta.get()));
    AbortBeforeBeginCommit(txn_meta.get());
    MvccSnapshot latest_after_abort(mgr);
    ASSERT_FALSE(latest_before_abort.IsCommitted(*txn_meta.get()));
    ASSERT_FALSE(latest_after_abort.IsCommitted(*txn_meta.get()));
  }
}

enum OpType {
  kCommit,
  kAbortAfterBeginCommit,
  kAbortBeforeBeginCommit,
};

class ParamedTransactionMvccTest : public TransactionMvccTest,
                                   public ::testing::WithParamInterface<OpType> {};

// Test that snapshots taken concurrently with commit or abort do not change
// commit status.
TEST_P(ParamedTransactionMvccTest, TestConcurrentLatestSnapshots) {
  constexpr const int kNumThreads = 10;
  Barrier b(kNumThreads + 1);
  vector<thread> threads;
  vector<MvccSnapshot> snaps(kNumThreads);
  // NOTE: we really only care about bools, but vector<bool> isn't thread-safe.
  vector<int> is_committed(kNumThreads);
  scoped_refptr<TxnMetadata> txn_meta(new TxnMetadata);
  MvccManager mgr;
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      b.Wait();
      // Take a snapshot and immediately check whether the transaction is
      // committed.
      snaps[i] = MvccSnapshot(mgr);
      is_committed[i] = snaps[i].IsCommitted(*txn_meta.get());
    });
  }
  b.Wait();
  switch (GetParam()) {
    case kCommit:
      Commit(&mgr, txn_meta.get());
      break;
    case kAbortAfterBeginCommit:
      AbortAfterBeginCommit(&mgr, txn_meta.get());
      break;
    case kAbortBeforeBeginCommit:
      AbortBeforeBeginCommit(txn_meta.get());
      break;
  }
  for (auto& t : threads) {
    t.join();
  }
  // Take the collected snapshots and evaluate them again against the
  // transaction metadata. The commit status should not have changed.
  for (int i = 0; i < kNumThreads; i++) {
    ASSERT_EQ(is_committed[i], snaps[i].IsCommitted(*txn_meta.get()));
  }
}
INSTANTIATE_TEST_SUITE_P(Op, ParamedTransactionMvccTest,
                         ::testing::Values(kCommit, kAbortAfterBeginCommit,
                                           kAbortBeforeBeginCommit));

} // namespace tablet
} // namespace kudu
