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
  TransactionTracker tracker_;
};

TEST_F(TransactionTrackerTest, TestGetPending) {
  gscoped_ptr<TaskExecutor> executor(TaskExecutor::CreateNew("test", 1));
  simple_spinlock lock;

  ASSERT_EQ(0, tracker_.GetNumPendingForTests());
  scoped_refptr<LeaderTransactionDriver> driver;
  LeaderTransactionDriver::Create(&tracker_, NULL, executor.get(), executor.get(), &lock, &driver);

  ASSERT_EQ(1, tracker_.GetNumPendingForTests());

  vector<scoped_refptr<TransactionDriver> > pending_transactions;
  tracker_.GetPendingTransactions(&pending_transactions);
  ASSERT_EQ(1, pending_transactions.size());
  ASSERT_EQ(driver.get(), pending_transactions.front().get());

  // Fake the completion of the prepare/replicate stage by tweaking the internal
  // state.
  driver->prepare_finished_calls_ = 2;
  // And mark the transaction as failed, which will cause it to unregister itself.
  driver->ApplyOrCommitFailed(Status::IllegalState(""));

  ASSERT_EQ(0, tracker_.GetNumPendingForTests());
}

} // namespace tablet
} // namespace kudu
