// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "tablet/mvcc.h"

namespace kudu { namespace tablet {


TEST(TestMvcc, TestMvccBasic) {
  MvccManager mgr;
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
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1 or (T < 2 and T not in {1})}]", snap.ToString());
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

TEST(TestMvcc, TestMvccMultipleInFlight) {
  MvccManager mgr;
  MvccSnapshot snap;

  // Start timestamp 1, timestamp 2
  Timestamp t1 = mgr.StartTransaction();
  ASSERT_EQ(1, t1.value());
  Timestamp t2 = mgr.StartTransaction();
  ASSERT_EQ(2, t2.value());

  // State should still have no committed transactions, since both are in-flight.

  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1 or (T < 3 and T not in {1,2})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_FALSE(snap.IsCommitted(t2));

  // Commit timestamp 2
  mgr.CommitTransaction(t2);

  // State should show 1 as committed, 0 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 1 or (T < 3 and T not in {1})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));

  // Start timestamp 3
  Timestamp t3 = mgr.StartTransaction();
  ASSERT_EQ(3, t3.value());

  // State should show 2 as committed, 1 and 3 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 1 or (T < 4 and T not in {1,3})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));
  ASSERT_FALSE(snap.IsCommitted(t3));

  // Commit 3
  mgr.CommitTransaction(t3);

  // 2 and 3 committed
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 1 or (T < 4 and T not in {1})}]", snap.ToString());
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

TEST(TestMvcc, TestScopedTransaction) {
  MvccManager mgr;
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

TEST(TestMvcc, TestPointInTimeSnapshot) {
  MvccSnapshot snap(Timestamp(10));

  ASSERT_TRUE(snap.IsCommitted(Timestamp(1)));
  ASSERT_TRUE(snap.IsCommitted(Timestamp(9)));
  ASSERT_FALSE(snap.IsCommitted(Timestamp(10)));
  ASSERT_FALSE(snap.IsCommitted(Timestamp(11)));
}

TEST(TestMvcc, TestMayHaveCommittedTransactionsAtOrAfter) {
  MvccSnapshot snap;
  snap.all_committed_before_timestamp_ = Timestamp(10);
  snap.timestamps_in_flight_.insert(11);
  snap.timestamps_in_flight_.insert(13);
  snap.none_committed_after_timestamp_ = Timestamp(14);

  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(9)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(10)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(12)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(13)));
  ASSERT_FALSE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(14)));
  ASSERT_FALSE(snap.MayHaveCommittedTransactionsAtOrAfter(Timestamp(15)));
}

TEST(TestMvcc, TestMayHaveUncommittedTransactionsBefore) {
  MvccSnapshot snap;
  snap.all_committed_before_timestamp_ = Timestamp(10);
  snap.timestamps_in_flight_.insert(11);
  snap.timestamps_in_flight_.insert(13);
  snap.none_committed_after_timestamp_ = Timestamp(14);

  ASSERT_FALSE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(9)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(10)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(11)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(13)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(14)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(Timestamp(15)));
}


} // namespace tablet
} // namespace kudu
