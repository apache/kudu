// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_
#define KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_

#include <boost/thread/locks.hpp>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/trace.h"

namespace kudu {

namespace consensus {
class Consensus;
}

namespace tablet {

// Transaction Context for the change config operation.
class ChangeConfigTransactionState : public TransactionState {
 public:
  explicit ChangeConfigTransactionState(TabletPeer* tablet_peer,
                                        const consensus::ChangeConfigRequestPB* request)
    : TransactionState(tablet_peer),
      request_(request),
      response_(NULL) {
  }

  ChangeConfigTransactionState(TabletPeer* tablet_peer,
                               const consensus::ChangeConfigRequestPB* request,
                               consensus::ChangeConfigResponsePB* response)
      : TransactionState(tablet_peer),
        request_(request),
        response_(response) {
  }

  const consensus::ChangeConfigRequestPB* request() const { return request_; }
  consensus::ChangeConfigResponsePB* response() { return response_; }

  void set_old_quorum(metadata::QuorumPB quorum) {
    old_quorum_.CopyFrom(quorum);
  }

  // Returns the quorum that was configured before this transaction
  // took place. Only available after the prepare phase.
  const metadata::QuorumPB old_quorum() { return old_quorum_; }

  void acquire_config_sem(Semaphore* sem) {
    TRACE("Acquiring the config sem.");
    config_lock_ = boost::unique_lock<Semaphore>(*sem);
  }

  void release_config_sem() {
    if (config_lock_.owns_lock()) {
      config_lock_.unlock();
    }
  }

  // Note: request_ and response_ are set to NULL after this method returns.
  void commit() {
    release_config_sem();
    // Make the request NULL since after this transaction commits
    // the request may be deleted at any moment.
    request_ = NULL;
    response_ = NULL;
  }

  virtual std::string ToString() const OVERRIDE;

  ~ChangeConfigTransactionState() {
    release_config_sem();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(ChangeConfigTransactionState);

  metadata::QuorumPB old_quorum_;
  const consensus::ChangeConfigRequestPB *request_;
  consensus::ChangeConfigResponsePB *response_;
  boost::unique_lock<Semaphore> config_lock_;
};

// Executes the change config transaction.
class ChangeConfigTransaction : public Transaction {
 public:
  ChangeConfigTransaction(ChangeConfigTransactionState* tx_state,
                          consensus::DriverType type,
                          Semaphore* config_sem);

  virtual ChangeConfigTransactionState* state() OVERRIDE { return state_.get(); }
  virtual const ChangeConfigTransactionState* state() const OVERRIDE { return state_.get(); }

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg) OVERRIDE;

  // Executes a Prepare for the change config transaction.
  virtual Status Prepare() OVERRIDE;

  // Starts the ChangeConfigTransaction by assigning it a timestamp.
  virtual Status Start() OVERRIDE;

  virtual void NewCommitAbortMessage(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Executes an Apply for the change config transaction.
  virtual Status Apply(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Actually commits the transaction.
  virtual void Finish() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

 private:

  gscoped_ptr<ChangeConfigTransactionState> state_;
  DISALLOW_COPY_AND_ASSIGN(ChangeConfigTransaction);
  Semaphore* config_sem_;
};

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_CHANGE_CONFIG_TRANSACTION_H_ */
