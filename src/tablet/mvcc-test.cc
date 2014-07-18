// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include "server/hybrid_clock.h"
#include "server/logical_clock.h"
#include "tablet/mvcc.h"
#include "util/test_util.h"

DECLARE_int32(max_clock_sync_error_usec);

namespace kudu { namespace tablet {

using server::Clock;
using server::HybridClock;

class MvccTest : public KuduTest {
 public:
  MvccTest() :
    clock_(server::LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp)) {
    // Increase clock sync tolerance so test doesn't fail on jenkins.
    FLAGS_max_clock_sync_error_usec = 10 * 1000 * 1000;
  }

  void WaitForSnapshotAtTSThread(MvccManager* mgr, Timestamp ts) {
    MvccSnapshot s;
    mgr->WaitForCleanSnapshotAtTimestamp(ts, &s);
    mgr->WaitUntilAllCommitted(ts);
    CHECK(s.is_clean()) << "verifying postcondition";
    boost::lock_guard<simple_spinlock> lock(lock_);
    result_snapshot_.reset(new MvccSnapshot(s));
  }

  bool HasResultSnapshot() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return result_snapshot_ != NULL;
  }

 protected:
  scoped_refptr<server::Clock> clock_;

  mutable simple_spinlock lock_;
  gscoped_ptr<MvccSnapshot> result_snapshot_;
};

TEST_F(MvccTest, TestMvccBasic) {
  MvccManager mgr(clock_.get());
  MvccSnapshot snap;

  // Initial state should not have any committed transactions.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(Timestamp(1)));
  ASSERT_FALSE(snap.IsCommitted(Timestamp(2)));

  // Start timestamp 1
  Timestamp t = mgr.StartTransaction();
  ASSERT_EQ(1, t.value());

  // State should still have no committed transactions, since 1 is in-flight.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(Timestamp(1)));
  ASSERT_FALSE(snap.IsCommitted(Timestamp(2)));

  // Commit timestamp 1
  mgr.CommitTransaction(t);

  // State should show 0 as committed, 1 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 2}]", snap.ToString());
  ASSERT_TRUE(snap.IsCommitted(Timestamp(1)));
  ASSERT_FALSE(snap.IsCommitted(Timestamp(2)));
}

TEST_F(MvccTest, TestMvccMultipleInFlight) {
  MvccManager mgr(clock_.get());
  MvccSnapshot snap;

  // Start timestamp 1, timestamp 2
  Timestamp t1 = mgr.StartTransaction();
  ASSERT_EQ(1, t1.value());
  Timestamp t2 = mgr.StartTransaction();
  ASSERT_EQ(2, t2.value());

  // State should still have no committed transactions, since both are in-flight.

  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_FALSE(snap.IsCommitted(t2));

  // Commit timestamp 2
  mgr.CommitTransaction(t2);

  // State should show 2 as committed, 1 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 1 or (T in {2})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));

  // Start another transaction. This gets timestamp 3
  Timestamp t3 = mgr.StartTransaction();
  ASSERT_EQ(3, t3.value());

  // State should show 2 as committed, 1 and 4 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 1 or (T in {2})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));
  ASSERT_FALSE(snap.IsCommitted(t3));

  // Commit 3
  mgr.CommitTransaction(t3);

  // 2 and 3 committed
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 1 or (T in {2,3})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));
  ASSERT_TRUE(snap.IsCommitted(t3));

  // Commit 1
  mgr.CommitTransaction(t1);

  // all committed
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 4}]", snap.ToString());
  ASSERT_TRUE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));
  ASSERT_TRUE(snap.IsCommitted(t3));
}

TEST_F(MvccTest, TestOutOfOrderTxns) {
  scoped_refptr<Clock> hybrid_clock(new HybridClock());
  ASSERT_STATUS_OK(hybrid_clock->Init());
  MvccManager mgr(hybrid_clock);

  // Start a normal non-commit-wait txn.
  Timestamp normal_txn = mgr.StartTransaction();

  MvccSnapshot s1(mgr);

  // Start a transaction as if it were using commit-wait (i.e. started in future)
  Timestamp cw_txn = mgr.StartTransactionAtLatest();

  // Commit the original txn
  mgr.CommitTransaction(normal_txn);

  // Start a new txn
  Timestamp normal_txn_2 = mgr.StartTransaction();

  // The old snapshot should not have either txn
  EXPECT_FALSE(s1.IsCommitted(normal_txn));
  EXPECT_FALSE(s1.IsCommitted(normal_txn_2));

  // A new snapshot should have only the first transaction
  MvccSnapshot s2(mgr);
  EXPECT_TRUE(s2.IsCommitted(normal_txn));
  EXPECT_FALSE(s2.IsCommitted(normal_txn_2));

  // Commit the commit-wait one once it is time.
  ASSERT_STATUS_OK(hybrid_clock->WaitUntilAfter(cw_txn));
  mgr.CommitTransaction(cw_txn);

  // A new snapshot at this point should still think that normal_txn_2 is uncommitted
  MvccSnapshot s3(mgr);
  EXPECT_FALSE(s3.IsCommitted(normal_txn_2));
}

// Tests starting transaction at a point-in-time in the past and committing them.
// This is disconnected from the current time (whatever is returned from clock->Now())
// for replication/bootstrap.
TEST_F(MvccTest, TestOfflineTransactions) {
  MvccManager mgr(clock_.get());

  // set the clock to some time in the "future"
  ASSERT_STATUS_OK(clock_->Update(Timestamp(100)));

  // now start a transaction in the "past"
  ASSERT_STATUS_OK(mgr.StartTransactionAtTimestamp(Timestamp(50)));

  ASSERT_EQ(mgr.GetSafeTimestamp().CompareTo(Timestamp::kInitialTimestamp), 0);

  // and committing this transaction "offline" this
  // should not advance the MvccManager 'all_committed_before_'
  // watermark.
  mgr.OfflineCommitTransaction(Timestamp(50));

  // Now take a snaphsot.
  MvccSnapshot snap1;
  mgr.TakeSnapshot(&snap1);

  // Because we did not advance the watermark, even though the only
  // in-flight transaction was committed at time 50, a transaction at
  // time 40 should still be considered uncommitted.
  ASSERT_FALSE(snap1.IsCommitted(Timestamp(40)));

  // Now advance the watermark to the last committed transaction.
  mgr.OfflineAdjustSafeTime(Timestamp(50));

  ASSERT_EQ(mgr.GetSafeTimestamp().CompareTo(Timestamp(50)), 0);

  MvccSnapshot snap2;
  mgr.TakeSnapshot(&snap2);

  ASSERT_TRUE(snap2.IsCommitted(Timestamp(40)));
}

TEST_F(MvccTest, TestScopedTransaction) {
  MvccManager mgr(clock_.get());
  MvccSnapshot snap;

  {
    ScopedTransaction t1(&mgr);
    ScopedTransaction t2(&mgr);

    ASSERT_EQ(1, t1.timestamp().value());
    ASSERT_EQ(2, t2.timestamp().value());

    t1.Commit();

    mgr.TakeSnapshot(&snap);
    ASSERT_TRUE(snap.IsCommitted(t1.timestamp()));
    ASSERT_FALSE(snap.IsCommitted(t2.timestamp()));
  }

  // t2 going out of scope commits it.
  mgr.TakeSnapshot(&snap);
  ASSERT_TRUE(snap.IsCommitted(Timestamp(1)));
  ASSERT_TRUE(snap.IsCommitted(Timestamp(2)));
}

TEST_F(MvccTest, TestPointInTimeSnapshot) {
  MvccSnapshot snap(Timestamp(10));

  ASSERT_TRUE(snap.IsCommitted(Timestamp(1)));
  ASSERT_TRUE(snap.IsCommitted(Timestamp(9)));
  ASSERT_FALSE(snap.IsCommitted(Timestamp(10)));
  ASSERT_FALSE(snap.IsCommitted(Timestamp(11)));
}

TEST_F(MvccTest, TestMayHaveCommittedTransactionsAtOrAfter) {
  MvccSnapshot snap;
  snap.all_committed_before_ = Timestamp(10);
  snap.committed_timestamps_.push_back(11);
  snap.committed_timestamps_.push_back(13);
  snap.none_committed_at_or_after_ = Timestamp(14);

  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(9)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(10)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(12)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(13)));
  ASSERT_FALSE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(14)));
  ASSERT_FALSE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(15)));

  // Test for "all committed" snapshot
  MvccSnapshot all_committed = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
  ASSERT_TRUE(all_committed.MayHaveCommittedTransactionsAtOrAfter(Timestamp(1)));
  ASSERT_TRUE(all_committed.MayHaveCommittedTransactionsAtOrAfter(Timestamp(12345)));

  // And "none committed" snapshot
  MvccSnapshot none_committed = MvccSnapshot::CreateSnapshotIncludingNoTransactions();
  ASSERT_FALSE(none_committed.MayHaveCommittedTransactionsAtOrAfter(Timestamp(1)));
  ASSERT_FALSE(none_committed.MayHaveCommittedTransactionsAtOrAfter(Timestamp(12345)));

  // Test for a "clean" snapshot
  MvccSnapshot clean_snap(Timestamp(10));
  ASSERT_TRUE(clean_snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(9)));
  ASSERT_FALSE(clean_snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(10)));
}

TEST_F(MvccTest, TestMayHaveUncommittedTransactionsBefore) {
  MvccSnapshot snap;
  snap.all_committed_before_ = Timestamp(10);
  snap.committed_timestamps_.push_back(11);
  snap.committed_timestamps_.push_back(13);
  snap.none_committed_at_or_after_ = Timestamp(14);

  ASSERT_FALSE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(9)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(10)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(11)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(13)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(14)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(15)));

  // Test for "all committed" snapshot
  MvccSnapshot all_committed = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
  ASSERT_FALSE(all_committed.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(1)));
  ASSERT_FALSE(all_committed.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(12345)));

  // And "none committed" snapshot
  MvccSnapshot none_committed = MvccSnapshot::CreateSnapshotIncludingNoTransactions();
  ASSERT_TRUE(none_committed.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(1)));
  ASSERT_TRUE(none_committed.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(12345)));

  // Test for a "clean" snapshot
  MvccSnapshot clean_snap(Timestamp(10));
  ASSERT_FALSE(clean_snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(9)));
  ASSERT_TRUE(clean_snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(10)));
}

TEST_F(MvccTest, TestAreAllTransactionsCommitted) {
  MvccManager mgr(clock_.get());

  // start several transactions and take snapshots along the way
  Timestamp tx1 = mgr.StartTransaction();
  Timestamp tx2 = mgr.StartTransaction();
  Timestamp tx3 = mgr.StartTransaction();

  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(1)));
  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(2)));
  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(3)));

  // commit tx3, should all still report as having as having uncommitted
  // transactions.
  mgr.CommitTransaction(tx3);
  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(1)));
  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(2)));
  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(3)));

  // commit tx1, first snap with in-flights should now report as all committed
  // and remaining snaps as still having uncommitted transactions
  mgr.CommitTransaction(tx1);
  ASSERT_TRUE(mgr.AreAllTransactionsCommitted(Timestamp(1)));
  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(2)));
  ASSERT_FALSE(mgr.AreAllTransactionsCommitted(Timestamp(3)));

  // Now they should all report as all committed.
  mgr.CommitTransaction(tx2);
  ASSERT_TRUE(mgr.AreAllTransactionsCommitted(Timestamp(1)));
  ASSERT_TRUE(mgr.AreAllTransactionsCommitted(Timestamp(2)));
  ASSERT_TRUE(mgr.AreAllTransactionsCommitted(Timestamp(3)));
}

TEST_F(MvccTest, TestWaitUntilAllCommitted_SnapWithNoInflights) {
  MvccManager mgr(clock_.get());
  boost::thread waiting_thread = boost::thread(&MvccTest::WaitForSnapshotAtTSThread,
                                               this,
                                               &mgr,
                                               clock_->Now());

  // join immediately.
  waiting_thread.join();
  ASSERT_TRUE(HasResultSnapshot());
}

TEST_F(MvccTest, TestWaitUntilAllCommitted_SnapWithInFlights) {

  MvccManager mgr(clock_.get());

  Timestamp tx1 = mgr.StartTransaction();
  Timestamp tx2 = mgr.StartTransaction();

  boost::thread waiting_thread = boost::thread(&MvccTest::WaitForSnapshotAtTSThread,
                                               this,
                                               &mgr,
                                               clock_->Now());

  ASSERT_FALSE(HasResultSnapshot());
  mgr.CommitTransaction(tx1);
  ASSERT_FALSE(HasResultSnapshot());
  mgr.CommitTransaction(tx2);
  waiting_thread.join();
  ASSERT_TRUE(HasResultSnapshot());
}

TEST_F(MvccTest, TestWaitUntilAllCommitted_SnapAtTimestampWithInFlights) {

  MvccManager mgr(clock_.get());

  // Transactions with timestamp 1 through 3
  Timestamp tx1 = mgr.StartTransaction();
  Timestamp tx2 = mgr.StartTransaction();
  Timestamp tx3 = mgr.StartTransaction();

  // Start a thread waiting for transactions with ts <= 2 to commit
  boost::thread waiting_thread = boost::thread(&MvccTest::WaitForSnapshotAtTSThread,
                                               this,
                                               &mgr,
                                               tx2);
  ASSERT_FALSE(HasResultSnapshot());

  // Commit tx 1 - thread should still wait.
  mgr.CommitTransaction(tx1);
  usleep(1000);
  ASSERT_FALSE(HasResultSnapshot());

  // Commit tx 3 - thread should still wait.
  mgr.CommitTransaction(tx3);
  usleep(1000);
  ASSERT_FALSE(HasResultSnapshot());

  // Commit tx 2 - thread can now continue
  mgr.CommitTransaction(tx2);
  waiting_thread.join();
  ASSERT_TRUE(HasResultSnapshot());
}

} // namespace tablet
} // namespace kudu
