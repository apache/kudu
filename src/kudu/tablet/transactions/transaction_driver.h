// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TRANSACTION_DRIVER_H_
#define KUDU_TABLET_TRANSACTION_DRIVER_H_

#include <string>

#include "kudu/consensus/consensus.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

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
class TransactionDriver : public RefCountedThreadSafe<TransactionDriver> {
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

  // Aborts the transaction, if possible. Since transactions are executed in
  // multiple stages by multiple executors it might not be possible to stop
  // the transaction immediately, but this will make sure it is aborted
  // at the next synchronization point.
  virtual void Abort() = 0;

  virtual const std::tr1::shared_ptr<FutureCallback>& commit_finished_callback();

  virtual std::string ToString() const;

  virtual std::string ToStringUnlocked() const;

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
  Status ApplyTask();

  virtual Status Apply() = 0;

  // Called when both Transaction::Apply() and Consensus::Commit() successfully
  // completed. When this is called the commit message was appended to the WAL.
  virtual void Finalize() = 0;

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

  // Lock that synchronizes access to the transaction's state.
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
  friend class RefCountedThreadSafe<TransactionDriver>;

  const MonoTime start_time_;

  DISALLOW_COPY_AND_ASSIGN(TransactionDriver);
};

// Leader transaction driver.
//
// Leader transaction execution is illustrated in the next diagram. (Further
// illustration of the inner workings of the consensus system can be found
// in consensus/consensus.h).
//
//                  1 Execute()
//                       +
//                       |
//             +---------v----------+
//             |                    |
//             |   2 Prepare()      |
//             |                    |
//             +---------+----------+
//                       |
//                       |
//             +---------v----------+
//       succ. |                    | fails
//             |                    |
//      +------v------+     +-------v------+
//      |             |     |              |
//      |3 Replicate()|     |  FAIL(Reply) |
//      |             |     |              |
//      +------+------+     +-------^------+
//             |                    |
//             |                    | fails
//             +--------------------+
//             |
//             |
//             +----------+ succeeds
//                        |
//             +----------v---------+
//             |                    |
//             |    4 Apply()       |
//             |                    |
//             +----------+---------+
//                        |
//                        |
//             +----------v---------+
//      succ.  |                    | fails
//             |                    |
//      +------v-------+    +-------v------+
//      |              |    |              |
//      |SUCCESS(Reply)|    | ABORT(Reply) |
//      |              |    |              |
//      +------+-------+    +-------+------+
//             |                    |
//             +----------+---------+
//                        |
//                        |
//             +----------v---------+
//             |                    |
//             |    5 Finalize()    |
//             |                    |
//             +----------+---------+
//                        |
//                        v
//                  Destroy/Cleanup
//
//  1 - Execute() is called on the LeaderTransactionDriver. The transaction's
//      prepare is queued in the 'prepare_executor_'. The method returns immediately.
//  2 - On Prepare(), messages are decoded and locks acquired. When Prepare() completes
//      successfully, the transaction is assigned a timestamp. If Prepare() fails, the
//      transaction is simply cancelled/destroyed and the client notified. If Prepare()
//      fails the transaction had no side-effects.
//  3 - If Replicate() succeeds (i.e a majority of peers ACKed the operation), it can be
//      considered committed. This is the point of no return: from here on, a commit or
//      abort message must eventually be stored in the WAL. It it succeeds Apply() is
//      called. If it fails for some reason e.g. the transaction could not be replicated
//      because a majority of peers is down, the transaction will never be committed and
//      the client is notified.
//  4 - When Apply() is called changes are made to the in-memory data structures. These
//      changes are not visible to clients yet. After Apply() completes, if successful,
//      the client is replied to immediately, locks are released[1] and changes are made
//      visible.
//      Whether successful or not, before returning, Apply() appends a CommitMsg to the
//      WAL reflecting whether the transaction was successful and which data structures
//      where mutated.
//      Changes to data structure are not allowed to be made durable though, as the CommitMsg
//      hasn't been persisted yet, but it is safe to reply to the client as the transaction
//      is now guaranteed to survive failures.
//  5 - Finalize() is called when the ApplyMsg has been made durable and performs some cleanup
//      and updates metrics.
//      In-mem data structures that contain the changes made by the transaction can now
//      be made durable.
//
// [1] - see 'Implementation Techniques for Main Memory Database Systems', DeWitt et. al.
//
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

  virtual void Abort() OVERRIDE;

 protected:
  LeaderTransactionDriver(TransactionTracker* txn_tracker,
                          consensus::Consensus* consensus,
                          TaskExecutor* prepare_executor,
                          TaskExecutor* apply_executor,
                          simple_spinlock* prepare_replicate_lock);

  virtual Status Apply() OVERRIDE;

  virtual void Finalize() OVERRIDE;

  virtual void ApplyOrCommitFailed(const Status& status) OVERRIDE;

  virtual ~LeaderTransactionDriver() OVERRIDE;

 private:
  friend class RefCountedThreadSafe<LeaderTransactionDriver>;
  FRIEND_TEST(TransactionTrackerTest, TestGetPending);

  // Leaders execute Prepare() and Start() in sequence.
  Status PrepareAndStartTask();
  Status PrepareAndStart();

  void ReplicateSucceeded();

  void ReplicateFailed(const Status& status);

  // Called when Transaction::Prepare() or Consensus::Replicate() failed,
  // after they have both completed.
  void HandlePrepareOrReplicateFailure();

  // Called between Transaction::Apply() and Consensus::Commit() if the transaction
  // has COMMIT_WAIT external consistency.
  Status CommitWait();

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

  virtual void Abort() OVERRIDE;

 protected:
  ReplicaTransactionDriver(TransactionTracker* txn_tracker,
                           consensus::Consensus* consensus,
                           TaskExecutor* prepare_executor,
                           TaskExecutor* apply_executor);

  virtual ~ReplicaTransactionDriver() OVERRIDE;

  virtual Status LeaderCommitted(gscoped_ptr<consensus::OperationPB> leader_commit_op) OVERRIDE;

  virtual Status AbortAndCommit();

  virtual Status Apply() OVERRIDE;

  virtual void Finalize() OVERRIDE;

  virtual void ApplyOrCommitFailed(const Status& status) OVERRIDE;

 private:
  friend class RefCountedThreadSafe<ReplicaTransactionDriver>;

  void PrepareFinished(const Status& status);

  // Called when either of the parallel phases finishes. If both phases
  // have finished, takes care of triggering Apply.
  void PrepareOrLeaderCommitFinishedUnlocked();

  // Starts the transaction and enqueues Apply to happen on the correct threadpool.
  void StartAndTriggerApplyUnlocked();

  void HandlePrepareOrLeaderCommitFailure();

  // Whether the PREPARE phase has finished. This is started as soon
  // as we receive a REPLICATE message from the leader.
  bool prepare_finished_;

  // Whether the leader has committed yet. It's possible that we are
  // still waiting on prepare_finished_ to become true. Only once
  // prepare is done and the leader has committed may we proceed to
  // Apply.
  bool leader_committed_;

  std::tr1::shared_ptr<Future> apply_future_;

  DISALLOW_COPY_AND_ASSIGN(ReplicaTransactionDriver);
};


}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TRANSACTION_DRIVER_H_ */
