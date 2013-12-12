// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_
#define KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_

#include "gutil/macros.h"
#include "tablet/transactions/transaction.h"
#include "util/task_executor.h"

namespace kudu {

class Schema;

namespace consensus {
class Consensus;
}

namespace tablet {

// Transaction Context for the AlterSchema operation.
// Keeps track of the Transaction states (request, result, ...)
class AlterSchemaTransactionContext : public TransactionContext {
 public:
  AlterSchemaTransactionContext()
    : TransactionContext(NULL),
      schema_(NULL),
      request_(NULL),
      response_(NULL) {
  }

  ~AlterSchemaTransactionContext() {
    release_tablet_lock();
  }

  AlterSchemaTransactionContext(TabletPeer* tablet_peer,
                                const tserver::AlterSchemaRequestPB* request,
                                tserver::AlterSchemaResponsePB* response)
      : TransactionContext(tablet_peer),
        schema_(NULL),
        request_(request),
        response_(response) {
  }

  const tserver::AlterSchemaRequestPB* request() const { return request_; }
  tserver::AlterSchemaResponsePB* response() { return response_; }

  void set_schema(const Schema* schema) { schema_ = schema; }
  const Schema* schema() const { return schema_; }

  void acquire_tablet_lock(percpu_rwlock& component_lock) {
    component_lock_ = boost::unique_lock<percpu_rwlock>(component_lock);
    DCHECK(component_lock_.owns_lock());
  }

  void release_tablet_lock() {
    if (component_lock_.owns_lock()) {
      component_lock_.unlock();
    }
  }

  void commit() {
    release_tablet_lock();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaTransactionContext);

  const Schema* schema_;
  const tserver::AlterSchemaRequestPB *request_;
  tserver::AlterSchemaResponsePB *response_;
  boost::unique_lock<percpu_rwlock> component_lock_;
};

// Executes the alter schema transaction, leader side.
class LeaderAlterSchemaTransaction : public LeaderTransaction {
 public:
  LeaderAlterSchemaTransaction(AlterSchemaTransactionContext* tx_ctx,
                               consensus::Consensus* consensus,
                               TaskExecutor* prepare_executor,
                               TaskExecutor* apply_executor,
                               simple_spinlock& prepare_replicate_lock);
 protected:

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg);

  // Executes a Prepare for the alter schema transaction, leader side.
  //
  // Acquires the tablet component lock for the transaction.
  virtual Status Prepare();

  // Releases the alter schema tablet lock and sets up the error in the AlterSchemaResponse
  virtual void PrepareFailedPreCommitHooks(gscoped_ptr<consensus::CommitMsg>* commit_msg);

  // Executes an Apply for the alter schema transaction, leader side.
  virtual Status Apply();

  // Actually commits the transaction.
  virtual void ApplySucceeded();

  virtual AlterSchemaTransactionContext* tx_ctx() { return tx_ctx_.get(); }

 private:

  gscoped_ptr<AlterSchemaTransactionContext> tx_ctx_;
  DISALLOW_COPY_AND_ASSIGN(LeaderAlterSchemaTransaction);
};

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_ */
