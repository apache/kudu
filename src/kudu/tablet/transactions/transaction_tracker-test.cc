// Copyright 2014 Cloudera, Inc.
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::vector;

namespace kudu {
namespace tablet {

class TransactionTrackerTest : public KuduTest {
 public:
  class TestTransactionDriver : public ReplicaTransactionDriver {
   public:
    explicit TestTransactionDriver(TransactionTracker* tracker)
      : ReplicaTransactionDriver(tracker, NULL, NULL, NULL) {
    }

    virtual void Init(Transaction* transaction) {
      TransactionDriver::Init(transaction);
    }

    virtual void ApplyOrCommitFailed(const Status& status) {
      txn_tracker_->Release(this);
    }

    virtual ~TestTransactionDriver() {
      prepare_finished_calls_ = 2;
    }

   private:
  };

  void RunTransactionsThread(CountDownLatch* finish_latch);

  TransactionTracker tracker_;
};

TEST_F(TransactionTrackerTest, TestGetPending) {
  ASSERT_EQ(0, tracker_.GetNumPendingForTests());
  scoped_refptr<TestTransactionDriver> driver(new TestTransactionDriver(&tracker_));
  driver->Init(new WriteTransaction(new WriteTransactionState, consensus::LEADER));

  ASSERT_EQ(1, tracker_.GetNumPendingForTests());

  vector<scoped_refptr<TransactionDriver> > pending_transactions;
  tracker_.GetPendingTransactions(&pending_transactions);
  ASSERT_EQ(1, pending_transactions.size());
  ASSERT_EQ(driver.get(), pending_transactions.front().get());

  // And mark the transaction as failed, which will cause it to unregister itself.
  driver->ApplyOrCommitFailed(Status::IllegalState(""));

  ASSERT_EQ(0, tracker_.GetNumPendingForTests());
}

// Thread which starts a bunch of transactions and later stops them all.
void TransactionTrackerTest::RunTransactionsThread(CountDownLatch* finish_latch) {
  const int kNumTransactions = 100;
  // Start a bunch of transactions.
  vector<scoped_refptr<TestTransactionDriver> > drivers;
  for (int i = 0; i < kNumTransactions; i++) {
    scoped_refptr<TestTransactionDriver> driver(new TestTransactionDriver(&tracker_));
    driver->Init(new WriteTransaction(new WriteTransactionState, consensus::LEADER));

    drivers.push_back(driver);
  }

  // Wait for the main thread to tell us to proceed.
  finish_latch->Wait();

  // Sleep a tiny bit to give the main thread a chance to get into the
  // WaitForAllToFinish() call.
  usleep(1000);

  // Finish all the transactions
  BOOST_FOREACH(const scoped_refptr<TestTransactionDriver>& driver, drivers) {
    // And mark the transaction as failed, which will cause it to unregister itself.
    driver->ApplyOrCommitFailed(Status::IllegalState(""));
  }
}

// Regression test for KUDU-384 (thread safety issue with TestWaitForAllToFinish)
TEST_F(TransactionTrackerTest, TestWaitForAllToFinish) {
  CountDownLatch finish_latch(1);
  scoped_refptr<Thread> thr;
  CHECK_OK(Thread::Create("test", "txn-thread",
                          &TransactionTrackerTest::RunTransactionsThread, this, &finish_latch,
                          &thr));

  // Wait for the txns to start.
  while (tracker_.GetNumPendingForTests() == 0) {
    usleep(1000);
  }

  // Allow the thread to proceed, and then wait for it to abort all the
  // transactions.
  finish_latch.CountDown();
  tracker_.WaitForAllToFinish();

  CHECK_OK(ThreadJoiner(thr.get()).Join());
  ASSERT_EQ(tracker_.GetNumPendingForTests(), 0);
}

} // namespace tablet
} // namespace kudu
