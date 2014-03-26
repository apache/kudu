// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_
#define KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_

#include "gutil/macros.h"
#include "tablet/transactions/transaction.h"
#include "util/task_executor.h"
#include "util/semaphore.h"

namespace kudu {

namespace consensus {
class Consensus;
}

namespace tablet {

// Transaction Context for the change config operation.
class ChangeConfigTransactionContext : public TransactionContext {
 public:
  explicit ChangeConfigTransactionContext(const tserver::ChangeConfigRequestPB* request)
    : TransactionContext(NULL),
      request_(request),
      response_(NULL) {
  }

  ChangeConfigTransactionContext(TabletPeer* tablet_peer,
                                 const tserver::ChangeConfigRequestPB* request,
                                 tserver::ChangeConfigResponsePB* response)
      : TransactionContext(tablet_peer),
        request_(request),
        response_(response) {
  }

  const tserver::ChangeConfigRequestPB* request() const { return request_; }
  tserver::ChangeConfigResponsePB* response() { return response_; }

  void acquire_config_sem(Semaphore* sem) {
    config_lock_ = boost::unique_lock<Semaphore>(*sem);
  }

  void release_config_sem() {
    if (config_lock_.owns_lock()) {
      config_lock_.unlock();
    }
  }

  void commit() {
    release_config_sem();
  }

  ~ChangeConfigTransactionContext() {
    release_config_sem();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(ChangeConfigTransactionContext);

  const tserver::ChangeConfigRequestPB *request_;
  tserver::ChangeConfigResponsePB *response_;
  boost::unique_lock<Semaphore> config_lock_;
};

// Executes the change config transaction, leader side.
class LeaderChangeConfigTransaction : public LeaderTransaction {
 public:
  LeaderChangeConfigTransaction(TransactionTracker *txn_tracker,
                                ChangeConfigTransactionContext* tx_ctx,
                                consensus::Consensus* consensus,
                                TaskExecutor* prepare_executor,
                                TaskExecutor* apply_executor,
                                simple_spinlock& prepare_replicate_lock,
                                Semaphore* config_sem);
 protected:

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg);

  // Executes a Prepare for the change config transaction, leader side.
  virtual Status Prepare();

  virtual void PrepareFailedPreCommitHooks(gscoped_ptr<consensus::CommitMsg>* commit_msg);

  // Executes an Apply for the change config transaction, leader side.
  virtual Status Apply();

  // Actually commits the transaction.
  virtual void ApplySucceeded();

  virtual ChangeConfigTransactionContext* tx_ctx() { return tx_ctx_.get(); }

 private:

  gscoped_ptr<ChangeConfigTransactionContext> tx_ctx_;
  DISALLOW_COPY_AND_ASSIGN(LeaderChangeConfigTransaction);
  Semaphore* config_sem_;
};

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_ */
