// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TRANSACTION_H_
#define KUDU_TABLET_TRANSACTION_H_

#include <string>
#include <tr1/memory>

#include "common/timestamp.h"
#include "common/wire_protocol.h"
#include "consensus/consensus.h"
#include "gutil/ref_counted.h"
#include "util/auto_release_pool.h"
#include "util/countdown_latch.h"
#include "util/status.h"
#include "util/locks.h"
#include "util/memory/arena.h"

namespace kudu {
class FutureCallback;
class Task;
class TaskExecutor;

namespace consensus {
class CommitMsg;
class ReplicateMsg;
}

namespace tablet {
class TabletPeer;
class TransactionTracker;

// All metrics associated with a TransactionContext.
struct TransactionMetrics {
  TransactionMetrics();
  void Reset();
  int successful_inserts;
  int successful_updates;
  uint64_t commit_wait_duration_usec;
};

// A parent class for the callback that gets called when transactions
// complete.
//
// This must be set in the TransactionContext if the transaction initiator is to
// be notified of when a transaction completes. The callback belongs to the
// transaction context and is deleted along with it.
//
// NOTE: this is a concrete class so that we can use it as a default implementation
// which avoids callers having to keep checking for NULL.
class TransactionCompletionCallback {
 public:

  TransactionCompletionCallback()
      : code_(tserver::TabletServerErrorPB::UNKNOWN_ERROR) {
  }

  // Allows to set an error for this transaction and a mapping to a server level code.
  // Calling this method does not mean the transaction is completed.
  void set_error(const Status& status, tserver::TabletServerErrorPB::Code code) {
    status_ = status;
    code_ = code;
  }

  void set_error(const Status& status) {
    status_ = status;
  }

  bool has_error() const {
    return !status_.ok();
  }

  const Status& status() const { return status_; }

  const tserver::TabletServerErrorPB::Code error_code() const { return code_; }

  // Subclasses should override this.
  virtual void TransactionCompleted() {}

  virtual ~TransactionCompletionCallback() {}

 protected:
  Status status_;
  tserver::TabletServerErrorPB::Code code_;
};

// TransactionCompletionCallback implementation that can be waited on.
// Helper to make async transaction, sync.
// This is templated to accept any response PB that has a TabletServerError
// 'error' field and to set the error before performing the latch countdown.
template<class ResponsePB>
class LatchTransactionCompletionCallback : public TransactionCompletionCallback {
 public:
  explicit LatchTransactionCompletionCallback(CountDownLatch* latch,
                                              ResponsePB* response)
  : latch_(latch),
    response_(response) {}

  virtual void TransactionCompleted() {
    tserver::TabletServerErrorPB* error = response_->mutable_error();
    StatusToPB(status_, error->mutable_status());
    error->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
    latch_->CountDown();
  }

 private:
  CountDownLatch* latch_;
  ResponsePB* response_;
};


class TransactionContext {
 public:
  // Sets the ConsensusContext for this transaction, if this transaction is
  // being executed through the consensus system.
  void set_consensus_ctx(gscoped_ptr<consensus::ConsensusContext> consensus_ctx) {
    consensus_ctx_.reset(consensus_ctx.release());
    op_id_ = consensus_ctx_->id();
  }

  // Returns the ConsensusContext being used, if this transaction is being
  // executed through the consensus system or NULL if it's not.
  consensus::ConsensusContext* consensus_ctx() {
    return consensus_ctx_.get();
  }

  TabletPeer* tablet_peer() const { return tablet_peer_; }

  // Return metrics related to this transaction.
  const TransactionMetrics& metrics() const { return tx_metrics_; }

  TransactionMetrics* mutable_metrics() { return &tx_metrics_; }

  void set_completion_callback(gscoped_ptr<TransactionCompletionCallback> completion_clbk) {
    completion_clbk_.reset(completion_clbk.release());
  }

  // Returns the completion callback is there is one. or NULL otherwise
  TransactionCompletionCallback* completion_callback() { return completion_clbk_.get(); }

  // Sets a heap object to be managed by this transaction's AutoReleasePool.
  template <class T>
  T* AddToAutoReleasePool(T* t) {
    return pool_.Add(t);
  }

  // Sets an array heap object to be managed by this transaction's AutoReleasePool.
  template <class T>
  T* AddArrayToAutoReleasePool(T* t) {
    return pool_.AddArray(t);
  }

  // Return the arena associated with this transaction.
  // NOTE: this is not a thread-safe arena!
  Arena* arena() { return &arena_; }

  string ToString() {
    return "TODO transaction toString";
  }

  // Sets the timestamp for the transaction
  void set_timestamp(const Timestamp& timestamp) {
    // make sure we set the timestamp only once
    DCHECK(timestamp_ == Timestamp::kInvalidTimestamp);
    timestamp_ = timestamp;
  }

  Timestamp timestamp() {
    return timestamp_;
  }

  consensus::OpId* mutable_op_id() {
    return &op_id_;
  }

  const consensus::OpId& op_id() const {
    return op_id_;
  }

  ExternalConsistencyMode external_consistency_mode() const {
    return external_consistency_mode_;
  }

  void set_client_propagated_timestamp(const Timestamp& timestamp) {
    client_propagated_timestamp_ = timestamp;
  }

  Timestamp client_propagated_timestamp() const {
    return client_propagated_timestamp_;
  }

 protected:
  explicit TransactionContext(TabletPeer* tablet_peer)
      : tablet_peer_(tablet_peer),
        completion_clbk_(new TransactionCompletionCallback()),
        arena_(32*1024, 4*1024*1024),
        external_consistency_mode_(NO_CONSISTENCY) {
  }

  TransactionMetrics tx_metrics_;

  // The tablet peer that is coordinating this transaction.
  TabletPeer* tablet_peer_;

  // Optional callback to be called once the transaction completes.
  gscoped_ptr<TransactionCompletionCallback> completion_clbk_;

  AutoReleasePool pool_;

  // This transaction's timestamp
  Timestamp timestamp_;

  // The clock error when timestamp_ was read.
  uint64_t timestamp_error_;

  Arena arena_;

  // This OpId stores the canonical "anchor" OpId for this transaction.
  consensus::OpId op_id_;

  gscoped_ptr<consensus::ConsensusContext> consensus_ctx_;

  // The defined consistency mode for this transaction.
  ExternalConsistencyMode external_consistency_mode_;

  // The timestamp that was propagated by the client in the request
  // Only set if ExternalConsistencyMode = CLIENT_PROPAGATED
  Timestamp client_propagated_timestamp_;
};

// Base class for transactions.
// Transaction classes encapsulate the logic of executing a transaction. as well
// as any required state.
// This class is refcounted, and subclasses must not define a public destructor.
class Transaction : public base::RefCountedThreadSafe<Transaction> {
 public:
  // Starts the execution of a transaction.
  virtual Status Execute() = 0;

  // Returns the TransactionContext for this transaction.
  virtual TransactionContext* tx_ctx() = 0;

 protected:
  Transaction(TaskExecutor* prepare_executor,
              TaskExecutor* apply_executor);

  virtual ~Transaction() {}

  // Executes the prepare phase of this transaction, the actual actions
  // of this phase depend on the transaction type, but usually are limited
  // to what can be done without actually changing data structures.
  virtual Status Prepare() = 0;

  // If supported, aborts the prepare phase, and subsequently the transaction.
  // Not supported by default.
  virtual bool AbortPrepare() { return false; }

  // Called when the Prepare phase has successfully completed. For most successful
  // transactions, which interact with consensus, this method is actually
  // called twice, one when Prepare() completes and one when Consensus::Append()
  // completes.
  // Once PrepareSucceeded() completes successfully it triggers the Apply() phase.
  virtual void PrepareSucceeded() = 0;

  // For transactions that interact with consensus this method is called if either
  // the Prepare() call fails or if Consensus::Append() fails.
  virtual void PrepareFailed(const Status& status) = 0;

  // The Apply phase is a transaction is where changes to data structures are
  // usually made.
  virtual Status Apply() = 0;

  // If supported, aborts the apply phase, and subsequently the transaction.
  // Not supported by default.
  virtual bool AbortApply() { return false; }

  // Called when the Apply phase has successfully completed. Usually implementations
  // of this method perform cleanup and answer to the client.
  virtual void ApplySucceeded() = 0;

  // Called when the Apply phase failed.
  virtual void ApplyFailed(const Status& status) = 0;

  // Makes the transaction update the relevant metrics.
  virtual void UpdateMetrics() {}

  // Makes the caller thread wait until timestamp < now.earliest
  Status CommitWait();

  std::tr1::shared_ptr<FutureCallback> prepare_finished_callback_;
  std::tr1::shared_ptr<FutureCallback> commit_finished_callback_;
  TaskExecutor* prepare_executor_;
  TaskExecutor* apply_executor_;

 private:
  friend class base::RefCountedThreadSafe<Transaction>;
  DISALLOW_COPY_AND_ASSIGN(Transaction);
};

// Base class for Leader transactions.
// For how write transactions are executed see: tablet/transactions/write_transaction.h
class LeaderTransaction : public Transaction {
 public:
  LeaderTransaction(TransactionTracker *txn_tracker,
                    consensus::Consensus* consensus,
                    TaskExecutor* prepare_executor,
                    TaskExecutor* apply_executor,
                    simple_spinlock* prepare_replicate_lock);

  virtual Status Execute();

  virtual TransactionContext* tx_ctx() = 0;

  virtual ~LeaderTransaction();

 protected:
  //===========================================================================
  // Implemented inherited methods.
  //===========================================================================
  virtual void PrepareSucceeded();

  virtual void PrepareFailed(const Status& status);

  // Child classes should call this _after_ performing any action as this will
  // reply to the client and delete the transaction.
  virtual void ApplySucceeded();

  virtual void ApplyFailed(const Status& status);

  // Called when one of Prepare() or Consensus::Append() has failed,
  // after they have both completed.
  //
  // On failure one of several things might have happened:
  // 1 - None of Prepare or Append were submitted.
  // 2 - Append was submitted but Prepare failed to submit.
  // 3 - Both Append and Prepare were submitted but one of them failed
  //     afterwards.
  // 4 - Both Append and Prepare succeeded but submitting Apply failed.
  //
  // In case 1 this callback does the cleanup and answers the client. In cases
  // 2,3,4, this callback submits a commit abort message to consensus and quits.
  // The commit callback will answer the client later on.
  void HandlePrepareFailure();

  //===========================================================================
  // Additional pure virtual methods.
  //===========================================================================

  // Builds the ReplicateMsg for this transaction.
  virtual void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg) = 0;

  // Executes whatever needs to be executed when a prepare phase fails and
  // returns a commit message that describes the failure.
  virtual void PrepareFailedPreCommitHooks(gscoped_ptr<consensus::CommitMsg>* commit_msg) = 0;

  TransactionTracker *txn_tracker_;
  consensus::Consensus* consensus_;
  Atomic32 prepare_finished_calls_;
  Status prepare_status_;

  // Lock that protects that, on Execute(), Prepare() and Consensus::Append() are submitted
  // in one go across transactions.
  simple_spinlock* prepare_replicate_lock_;

 private:
  DISALLOW_COPY_AND_ASSIGN(LeaderTransaction);
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TRANSACTION_H_ */
