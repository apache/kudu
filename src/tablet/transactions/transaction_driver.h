// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TRANSACTION_DRIVER_H_
#define KUDU_TABLET_TRANSACTION_DRIVER_H_

#include <string>

#include "consensus/consensus.h"
#include "gutil/ref_counted.h"
#include "tablet/transactions/transaction.h"
#include "util/status.h"
#include "util/trace.h"

namespace kudu {
class TaskExecutor;

namespace tablet {
class TransactionTracker;

// Base class for transaction drivers.
//
// TransactionDriver classes encapsulate the logic of coordinating the execution of
// a transaction. There are Leader and Replica side implementations.
//
// This class is refcounted, and subclasses must not define a public destructor.
// This class and implementations are thread safe.
class TransactionDriver : public base::RefCountedThreadSafe<TransactionDriver> {
 public:
  // Perform any non-constructor initialization. Sets the transaction
  // that will be executed.
  virtual void Init(Transaction* transaction);

  // Returns the OpId of the transaction being executed or an uninitialized
  // OpId if none has been assigned. Returns a copy and thus should not
  // be used in tight loops.
  virtual consensus::OpId GetOpId();

  // Submits the transaction for execution.
  // The returned status acknowledges any error on the submission process.
  // The transaction will be replied to asynchronously.
  virtual Status Execute() = 0;

  virtual const std::tr1::shared_ptr<FutureCallback>& commit_finished_callback();

  virtual std::string ToString() const;

  // Returns the type of the driver.
  consensus::DriverType type() const;

  // Returns the type of the transaction being executed by this driver.
  Transaction::TransactionType tx_type() const;

  // Returns the state of the transaction being executed by this driver.
  const TransactionState* state() const;

  const MonoTime& start_time() const { return start_time_; }

  Trace* trace() { return trace_.get(); }

 protected:
  TransactionDriver(TransactionTracker* txn_tracker,
                    consensus::Consensus* consensus,
                    TaskExecutor* prepare_executor,
                    TaskExecutor* apply_executor);

  virtual ~TransactionDriver() {}

  // Calls Transaction::Apply() followed by Consensus::Commit() with the
  // results from the Apply().
  virtual Status ApplyAndCommit() = 0;

  // Called when both Transaction::Apply() and Consensus::Commit() successfully
  // completed. When this is called the commit message was appended to the WAL.
  virtual void ApplyAndCommitSucceeded() = 0;

  // Called if ApplyAndCommit() failed for some reason, or if
  // Consensus::Commit() failed afterwards.
  // This method will only be called once.
  virtual void ApplyOrCommitFailed(const Status& status) = 0;

  // Returns the mutable state of the transaction being executed by
  // this driver.
  TransactionState* mutable_state();

  TransactionTracker* txn_tracker_;
  consensus::Consensus* consensus_;
  std::tr1::shared_ptr<FutureCallback> commit_finished_callback_;
  TaskExecutor* prepare_executor_;
  TaskExecutor* apply_executor_;

  Status transaction_status_;
  int prepare_finished_calls_;

  // Lock that synchronizes access to 'transaction_status_' and
  // 'prepare_finished_calls_'.
  mutable simple_spinlock lock_;

  // A copy of the transaction's OpId, set when the transaction first
  // receives one from Consensus and uninitialized until then.
  // TODO(todd): we have three separate copies of this now -- in TransactionState,
  // CommitMsg, and here... we should be able to consolidate!
  consensus::OpId op_id_copy_;

  // Lock that protects access to the driver's copy of the op_id, specifically.
  // GetOpId() is the only method expected to be called by threads outside
  // of the control of the driver, so we use a special lock to control access
  // otherwise callers would block for a long time for long running transactions.
  mutable simple_spinlock opid_lock_;

  // The transaction to be executed by this driver.
  gscoped_ptr<Transaction> transaction_;

  // Trace object for tracing any transactions started by this driver.
  scoped_refptr<Trace> trace_;

 private:
  friend class base::RefCountedThreadSafe<TransactionDriver>;

  const MonoTime start_time_;

  DISALLOW_COPY_AND_ASSIGN(TransactionDriver);
};

// Leader transaction driver.
// For how write transactions are executed see: tablet/transactions/write_transaction.h.
//
// This class is thread safe.
class LeaderTransactionDriver : public TransactionDriver {
 public:
  static void Create(Transaction* transaction,
                     TransactionTracker* txn_tracker,
                     consensus::Consensus* consensus,
                     TaskExecutor* prepare_executor,
                     TaskExecutor* apply_executor,
                     simple_spinlock* prepare_replicate_lock,
                     scoped_refptr<LeaderTransactionDriver>* driver);

  virtual Status Execute() OVERRIDE;

 protected:
  LeaderTransactionDriver(TransactionTracker* txn_tracker,
                          consensus::Consensus* consensus,
                          TaskExecutor* prepare_executor,
                          TaskExecutor* apply_executor,
                          simple_spinlock* prepare_replicate_lock);

  virtual ~LeaderTransactionDriver() OVERRIDE;

  virtual Status ApplyAndCommit() OVERRIDE;

  virtual void ApplyAndCommitSucceeded() OVERRIDE;

  virtual void ApplyOrCommitFailed(const Status& status) OVERRIDE;

 private:
  friend class base::RefCountedThreadSafe<LeaderTransactionDriver>;
  FRIEND_TEST(TransactionTrackerTest, TestGetPending);

  void PrepareOrReplicateSucceeded();

  void PrepareOrReplicateFailed(const Status& status);

  // Called when Transaction::Prepare() or Consensus::Replicate() failed,
  // after they have both completed.
  void HandlePrepareOrReplicateFailure();

  // Called between Transaction::Apply() and Consensus::Commit() if the transaction
  // has COMMIT_WAIT external consistency.
  Status CommitWait();

  // Lock that protects that, on Execute(), Transaction::Prepare() and
  // Consensus::Replicate() are submitted in one go across transactions.
  simple_spinlock* prepare_replicate_lock_;

  DISALLOW_COPY_AND_ASSIGN(LeaderTransactionDriver);
};

// Replica version of the transaction driver.
class ReplicaTransactionDriver : public TransactionDriver,
                                 public consensus::ReplicaCommitContinuation {
 public:
  static void Create(Transaction* transaction,
                     TransactionTracker* txn_tracker,
                     consensus::Consensus* consensus,
                     TaskExecutor* prepare_executor,
                     TaskExecutor* apply_executor,
                     scoped_refptr<ReplicaTransactionDriver>* driver);

  virtual void Init(Transaction* transaction) OVERRIDE;

  virtual Status Execute() OVERRIDE;

 protected:
  ReplicaTransactionDriver(TransactionTracker* txn_tracker,
                           consensus::Consensus* consensus,
                           TaskExecutor* prepare_executor,
                           TaskExecutor* apply_executor);

  virtual ~ReplicaTransactionDriver() OVERRIDE;

  virtual Status LeaderCommitted(gscoped_ptr<consensus::OperationPB> leader_commit_op) OVERRIDE;

  virtual Status ApplyAndCommit() OVERRIDE;

  virtual void ApplyAndCommitSucceeded() OVERRIDE;

  virtual void ApplyOrCommitFailed(const Status& status) OVERRIDE;

 private:
  friend class base::RefCountedThreadSafe<ReplicaTransactionDriver>;

  void PrepareFinished(const Status& status);

  void PrepareOrLeaderCommitSucceeded();

  void PrepareOrLeaderCommitFailed(const Status& status);

  void HandlePrepareOrLeaderCommitFailure();

  std::tr1::shared_ptr<Future> apply_future_;

  DISALLOW_COPY_AND_ASSIGN(ReplicaTransactionDriver);
};


}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TRANSACTION_DRIVER_H_ */
