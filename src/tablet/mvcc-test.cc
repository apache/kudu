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
  ASSERT_EQ("MvccSnapshot[committed={T|T < 0}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(txid_t(0)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(1)));

  // Start txid 0
  txid_t t = mgr.StartTransaction();
  ASSERT_EQ(0, t.v);

  // State should still have no committed transactions, since 0 is in-flight.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 0}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(txid_t(0)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(1)));

  // Commit txid 0
  mgr.CommitTransaction(t);

  // State should show 0 as committed, 1 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 1}]", snap.ToString());
  ASSERT_TRUE(snap.IsCommitted(txid_t(0)));
  ASSERT_FALSE(snap.IsCommitted(txid_t(1)));
}

TEST(TestMvcc, TestMvccMultipleInFlight) {
  MvccManager mgr;
  MvccSnapshot snap;

  // Start txid 0, txid 1
  txid_t t0 = mgr.StartTransaction();
  ASSERT_EQ(0, t0.v);
  txid_t t1 = mgr.StartTransaction();
  ASSERT_EQ(1, t1.v);

  // State should still have no committed transactions, since both are in-flight.

  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 0}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t0));
  ASSERT_FALSE(snap.IsCommitted(t1));

  // Commit txid 1
  mgr.CommitTransaction(t1);

  // State should show 1 as committed, 0 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 0 or (T < 2 and T not in {0})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t0));
  ASSERT_TRUE(snap.IsCommitted(t1));

  // Start txid 2
  txid_t t2 = mgr.StartTransaction();
  ASSERT_EQ(2, t2.v);

  // State should show 1 as committed, 0 and 2 as uncommitted.
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 0 or (T < 3 and T not in {0,2})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t0));
  ASSERT_TRUE(snap.IsCommitted(t1));
  ASSERT_FALSE(snap.IsCommitted(t2));

  // Commit 2
  mgr.CommitTransaction(t2);

  // 1 and 2 committed
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed="
            "{T|T < 0 or (T < 3 and T not in {0})}]", snap.ToString());
  ASSERT_FALSE(snap.IsCommitted(t0));
  ASSERT_TRUE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));

  // Commit 0
  mgr.CommitTransaction(t0);

  // all committed
  mgr.TakeSnapshot(&snap);
  ASSERT_EQ("MvccSnapshot[committed={T|T < 3}]", snap.ToString());
  ASSERT_TRUE(snap.IsCommitted(t0));
  ASSERT_TRUE(snap.IsCommitted(t1));
  ASSERT_TRUE(snap.IsCommitted(t2));
}

TEST(TestMvcc, TestScopedTransaction) {
  MvccManager mgr;
  MvccSnapshot snap;

  {
    ScopedTransaction t0(&mgr);
    ScopedTransaction t1(&mgr);

    ASSERT_EQ(0, t0.txid().v);
    ASSERT_EQ(1, t1.txid().v);

    t0.Commit();

    mgr.TakeSnapshot(&snap);
    ASSERT_TRUE(snap.IsCommitted(t0.txid()));
    ASSERT_FALSE(snap.IsCommitted(t1.txid()));
  }

  // t1 going out of scope commits it.
  mgr.TakeSnapshot(&snap);
  ASSERT_TRUE(snap.IsCommitted(txid_t(0)));
  ASSERT_TRUE(snap.IsCommitted(txid_t(1)));
}

TEST(TestMvcc, TestPointInTimeSnapshot) {
  MvccSnapshot snap(txid_t(10));

  ASSERT_TRUE(snap.IsCommitted(txid_t(0)));
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

  ASSERT_FALSE(snap.MayHaveUncommittedTransactionsBefore(txid_t(9)));
  ASSERT_FALSE(snap.MayHaveUncommittedTransactionsBefore(txid_t(10)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsBefore(txid_t(11)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsBefore(txid_t(13)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsBefore(txid_t(14)));
  ASSERT_TRUE(snap.MayHaveUncommittedTransactionsBefore(txid_t(15)));
}


} // namespace tablet
} // namespace kudu
