// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_
#define KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_

#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/util/task_executor.h"

namespace kudu {

class Schema;

namespace consensus {
class Consensus;
}

namespace tablet {

// Transaction Context for the AlterSchema operation.
// Keeps track of the Transaction states (request, result, ...)
class AlterSchemaTransactionState : public TransactionState {
 public:
  explicit AlterSchemaTransactionState(const tserver::AlterSchemaRequestPB* request)
      : TransactionState(NULL),
        schema_(NULL),
        request_(request),
        response_(NULL) {
  }

  ~AlterSchemaTransactionState() {
  }

  AlterSchemaTransactionState(TabletPeer* tablet_peer,
                              const tserver::AlterSchemaRequestPB* request,
                              tserver::AlterSchemaResponsePB* response)
      : TransactionState(tablet_peer),
        schema_(NULL),
        request_(request),
        response_(response) {
  }

  const tserver::AlterSchemaRequestPB* request() const { return request_; }
  tserver::AlterSchemaResponsePB* response() { return response_; }

  void set_schema(const Schema* schema) { schema_ = schema; }
  const Schema* schema() const { return schema_; }

  std::string new_table_name() const {
    return request_->new_table_name();
  }

  bool has_new_table_name() const {
    return request_->has_new_table_name();
  }

  uint32_t schema_version() const {
    return request_->schema_version();
  }

  // Note: request_ and response_ are set to NULL after this method returns.
  void commit() {
    // Make the request NULL since after this transaction commits
    // the request may be deleted at any moment.
    request_ = NULL;
    response_ = NULL;
  }

  virtual std::string ToString() const OVERRIDE;

 private:
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaTransactionState);

  const Schema* schema_;
  const tserver::AlterSchemaRequestPB *request_;
  tserver::AlterSchemaResponsePB *response_;
};

// Executes the alter schema transaction,.
class AlterSchemaTransaction : public Transaction {
 public:
  AlterSchemaTransaction(AlterSchemaTransactionState* tx_state, consensus::DriverType type);

  virtual AlterSchemaTransactionState* state() OVERRIDE { return state_.get(); }
  virtual const AlterSchemaTransactionState* state() const OVERRIDE { return state_.get(); }

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg) OVERRIDE;

  // Executes a Prepare for the alter schema transaction.
  //
  // TODO: need a schema lock?

  virtual Status Prepare() OVERRIDE;

  // Starts the AlterSchemaTransaction by assigning it a timestamp.
  virtual Status Start() OVERRIDE;

  virtual void NewCommitAbortMessage(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Executes an Apply for the alter schema transaction
  virtual Status Apply(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Actually commits the transaction.
  virtual void Finish() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

 private:
  gscoped_ptr<AlterSchemaTransactionState> state_;
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaTransaction);
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_ */
