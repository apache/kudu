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
  ASSERT_FALSE(snap.IsCommitted(txid_t(1)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(2)));

  // Start txid 1
  txid_t t = mgr.StartTransaction();
  ASSERT_EQ(1, t.value());

  // State should still have no committed transactions, since 1 is in-flight.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1 or (T < 2 and T not in {1})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(txid_t(1)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(2)));

  // Commit txid 1
  mgr.CommitTransaction(t);

  // State should show 0 as committed, 1 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 2}]", snap.ToString());
  ASSERT_TRUE(snap.IsCommitted(txid_t(1)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(2)));
}

TEST(TestMvcc, TestMvccMultipleInFlight) {
  MvccManager mgr;
  MvccSnapshot snap;

  // Start txid 1, txid 2
  txid_t t1 = mgr.StartTransaction();
  ASSERT_EQ(1, t1.value());
  txid_t t2 = mgr.StartTransaction();
  ASSERT_EQ(2, t2.value());

  // State should still have no committed transactions, since both are in-flight.

  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1 or (T < 3 and T not in {1,2})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_FALSE(snap.IsCommitted(t2));

  // Commit txid 2
  mgr.CommitTransaction(t2);

  // State should show 1 as committed, 0 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 1 or (T < 3 and T not in {1})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));

  // Start txid 3
  txid_t t3 = mgr.StartTransaction();
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

    ASSERT_EQ(1, t1.txid().value());
    ASSERT_EQ(2, t2.txid().value());

    t1.Commit();

    mgr.TakeSnapshot(&snap);
    ASSERT_TRUE(snap.IsCommitted(t1.txid()));
    ASSERT_FALSE(snap.IsCommitted(t2.txid()));
  }

  // t2 going out of scope commits it.
  mgr.TakeSnapshot(&snap);
  ASSERT_TRUE(snap.IsCommitted(txid_t(1)));
  ASSERT_TRUE(snap.IsCommitted(txid_t(2)));
}

TEST(TestMvcc, TestPointInTimeSnapshot) {
  MvccSnapshot snap(txid_t(10));

  ASSERT_TRUE(snap.IsCommitted(txid_t(1)));
  ASSERT_TRUE(snap.IsCommitted(txid_t(9)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(10)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(11)));
}

TEST(TestMvcc, TestMayHaveCommittedTransactionsAtOrAfter) {
  MvccSnapshot snap;
  snap.all_committed_before_txid_ = txid_t(10);
  snap.txids_in_flight_.insert(11);
  snap.txids_in_flight_.insert(13);
  snap.none_committed_after_txid_ = txid_t(14);

  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(txid_t(9)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(txid_t(10)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(txid_t(12)));
  ASSERT_TRUE(snap.MayHaveCommittedTransactionsAtOrAfter(txid_t(13)));
  ASSERT_FALSE(snap.MayHaveCommittedTransactionsAtOrAfter(txid_t(14)));
  ASSERT_FALSE(snap.MayHaveCommittedTransactionsAtOrAfter(txid_t(15)));
}

TEST(TestMvcc, TestMayHaveUncommittedTransactionsBefore) {
  MvccSnapshot snap;
  snap.all_committed_before_txid_ = txid_t(10);
  snap.txids_in_flight_.insert(11);
  snap.txids_in_flight_.insert(13);
  snap.none_committed_after_txid_ = txid_t(14);

  ASSERT_FALSE(snap.MayHaveUncommittedTransactionsAtOrBefore(txid_t(9)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(txid_t(10)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(txid_t(11)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(txid_t(13)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(txid_t(14)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsAtOrBefore(txid_t(15)));
}


} // namespace tablet
} // namespace kudu
