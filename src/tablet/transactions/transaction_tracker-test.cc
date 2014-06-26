// Copyright 2014 Cloudera, Inc.
#include <vector>

#include <gtest/gtest.h>

#include "gutil/ref_counted.h"
#include "tablet/transactions/transaction_driver.h"
#include "tablet/transactions/transaction_tracker.h"
#include "tablet/transactions/transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "util/task_executor.h"
#include "util/test_util.h"

using std::vector;

namespace kudu {
namespace tablet {

class TransactionTrackerTest : public KuduTest {
 protected:
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

  TransactionTracker tracker_;
};

TEST_F(TransactionTrackerTest, TestGetPending) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(1).Build(&executor));

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

} // namespace tablet
} // namespace kudu
