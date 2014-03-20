// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_
#define KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_

#include "gutil/macros.h"
#include "tablet/transactions/transaction.h"
#include "util/task_executor.h"

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

  void acquire_config_lock(boost::mutex* meta_lock) {
    meta_lock_.reset(new boost::lock_guard<boost::mutex>(*meta_lock));
  }

  void release_config_lock() {
    meta_lock_.reset();
  }

  void commit() {
    release_config_lock();
  }

  ~ChangeConfigTransactionContext() {
    release_config_lock();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(ChangeConfigTransactionContext);

  const tserver::ChangeConfigRequestPB *request_;
  tserver::ChangeConfigResponsePB *response_;
  gscoped_ptr<boost::lock_guard<boost::mutex> > meta_lock_;
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
                                boost::mutex* config_lock);
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
  boost::mutex* config_lock_;
};

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_ */
