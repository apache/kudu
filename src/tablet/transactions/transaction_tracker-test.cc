// Copyright 2014 Cloudera, Inc.
#include <vector>

#include <gtest/gtest.h>

#include "gutil/ref_counted.h"
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

class MockLeaderWriteTransaction : public LeaderWriteTransaction {
 public:
  MockLeaderWriteTransaction(TransactionTracker *txn_tracker,
                             WriteTransactionContext* tx_ctx,
                             consensus::Consensus* consensus,
                             TaskExecutor* prepare_executor,
                             TaskExecutor* apply_executor,
                             simple_spinlock* prepare_replicate_lock)
    : LeaderWriteTransaction(txn_tracker, tx_ctx, consensus,
                             prepare_executor, apply_executor, prepare_replicate_lock) {
  }

  // Simply aborts an initialized transaction without executing it.
  Status Abort() {
    Status s = Status::Aborted("Aborted for testing before Execute()");
    prepare_finished_callback_->OnFailure(s);
    HandlePrepareFailure();
    return s;
  }
};

TEST_F(TransactionTrackerTest, TestGetPending) {
  gscoped_ptr<TaskExecutor> executor(TaskExecutor::CreateNew("test", 1));
  simple_spinlock lock;

  ASSERT_EQ(0, tracker_.GetNumPendingForTests());
  scoped_refptr<MockLeaderWriteTransaction> tx(new MockLeaderWriteTransaction(
                                                  &tracker_, new WriteTransactionContext(), NULL,
                                                  executor.get(), executor.get(), &lock));
  ASSERT_EQ(1, tracker_.GetNumPendingForTests());

  vector<scoped_refptr<Transaction> > pending_transactions;
  ASSERT_TRUE(pending_transactions.empty());
  tracker_.GetPendingTransactions(&pending_transactions);
  ASSERT_EQ(1, pending_transactions.size());
  ASSERT_EQ(tx.get(), pending_transactions.front().get());

  tx->Abort();
  ASSERT_EQ(0, tracker_.GetNumPendingForTests());
}

} // namespace tablet
} // namespace kudu
