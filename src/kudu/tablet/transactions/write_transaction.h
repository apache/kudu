// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_WRITE_TRANSACTION_H_
#define KUDU_TABLET_WRITE_TRANSACTION_H_

#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/common/row_changelist.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/lock_manager.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/tablet/transactions/write_util.h"
#include "kudu/util/task_executor.h"

namespace kudu {
struct DecodedRowOperation;
class ConstContiguousRow;
class RowwiseRowBlockPB;

namespace consensus {
class Consensus;
}

namespace tserver {
class WriteRequestPB;
class WriteResponsePB;
}

namespace tablet {
struct RowOp;
class RowSetKeyProbe;
struct TabletComponents;

// A TransactionState for a batch of inserts/mutates. This class holds and
// owns most everything related to a transaction, including:
// - A RowOp structure for each of the rows being inserted or mutated, which itself
//   contains:
//   - decoded/projected data
//   - row lock reference
//   - result of this particular insert/mutate operation, once executed
// - the Replicate and Commit PB messages
//
// All the transaction related pointers are owned by this class
// and destroyed on Reset() or by the destructor.
//
// IMPORTANT: All the acquired locks will not be released unless the TransactionState
// is either destroyed or Reset() or release_locks() is called. Beware of this
// or else there will be lock leaks.
//
// Used when logging to WAL in that we keep track of where inserts/updates
// were applied and add that information to the commit message that is stored
// on the WAL.
//
// NOTE: this class isn't thread safe.
class WriteTransactionState : public TransactionState {
 public:
  WriteTransactionState(TabletPeer* tablet_peer = NULL,
                        const tserver::WriteRequestPB *request = NULL,
                        tserver::WriteResponsePB *response = NULL);
  virtual ~WriteTransactionState();

  // Returns the result of this transaction in its protocol buffers form.
  // The transaction result holds information on exactly which memory stores
  // were mutated in the context of this transaction and can be used to
  // perform recovery.
  //
  // This releases part of the state of the transaction, and will crash
  // if called more than once.
  void ReleaseTxResultPB(TxResultPB* result) const;

  // Returns the original client request for this transaction, if there was
  // one.
  const tserver::WriteRequestPB *request() const {
    return request_;
  }

  // Returns the prepared response to the client that will be sent when this
  // transaction is completed, if this transaction was started by a client.
  tserver::WriteResponsePB *response() {
    return response_;
  }

  // Set the MVCC transaction associated with this Write operation.
  // This must be called exactly once, during the PREPARE phase.
  void set_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx);

  // Set the Tablet components that this transaction will write into.
  // Called exactly once during PREPARE
  void set_tablet_components(const scoped_refptr<const TabletComponents>& components);

  void set_schema_at_decode_time(const Schema* schema) {
    schema_at_decode_time_ = schema;
  }

  const Schema* schema_at_decode_time() const {
    return schema_at_decode_time_;
  }

  const TabletComponents* tablet_components() const {
    return tablet_components_.get();
  }

  // Commits the Mvcc transaction and releases the component lock. After
  // this method is called all the inserts and mutations will become
  // visible to other transactions.
  //
  // Note: request_ and response_ are set to NULL after this method returns.
  void commit();

  // Returns all the prepared row writes for this transaction. Usually called
  // on the apply phase to actually make changes to the tablet.
  const std::vector<RowOp*>& row_ops() const {
    return row_ops_;
  }

  std::vector<RowOp*>* mutable_row_ops() {
    return &row_ops_;
  }

  void UpdateMetricsForOp(const RowOp& op);

  // Releases all the row locks acquired by this transaction.
  void release_row_locks();

  // Resets this TransactionState, releasing all locks, destroying all prepared
  // writes, clearing the transaction result _and_ committing the current Mvcc
  // transaction.
  void Reset();

  virtual std::string ToString() const OVERRIDE;

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteTransactionState);

  // pointers to the rpc context, request and response, lifecyle
  // is managed by the rpc subsystem. These pointers maybe NULL if the
  // transaction was not initiated by an RPC call.
  const tserver::WriteRequestPB* request_;
  tserver::WriteResponsePB* response_;

  // The row operations which are decoded from the request during PREPARE
  std::vector<RowOp*> row_ops_;

  // The MVCC transaction, set up during PREPARE phase
  gscoped_ptr<ScopedTransaction> mvcc_tx_;

  // The tablet components, acquired at the same time as mvcc_tx_ is set.
  scoped_refptr<const TabletComponents> tablet_components_;

  // The Schema of the tablet when the transaction was first decoded.
  // This is verified at APPLY time to ensure we don't have races against
  // schema change.
  const Schema* schema_at_decode_time_;
};

// Executes a write transaction.
//
// Transaction execution is illustrated in the next diagram. (Further
// illustration of the inner workings of the consensus system can be found
// in consensus/consensus.h).
//
//                                 + 1) Execute()
//                                 |
//                                 |
//                                 |
//                         +-------+-------+
//                         |       |       |
// 3) Consensus::Append()  |       v       | 2) Prepare()
//                         |   (returns)   |
//                         v               v
//                  +------------------------------+
//               4) |      PrepareSucceeded()      |
//                  |------------------------------|
//                  | - continues once both        |
//                  |   phases have finished       |
//                  +--------------+---------------+
//                                 |
//                                 | Submits Apply()
//                                 v
//                     +-------------------------+
//                  5) |         Apply           |
//                     |-------------------------|
//                     | - applies transaction   |
//                     +-------------------------+
//                                 |
//                                 | 6) Calls Consensus::Commit()
//                                 v
//                                 + ApplySucceeded().
//
// 1) TabletPeer calls LeaderWriteTransaction::Execute() which creates the ReplicateMsg,
//    calls Consensus::Append() and LeaderWriteTransaction::Prepare(). Both calls execute
//    asynchronously.
//
// 2) The Prepare() call executes the sequence of steps required for a write
//    transaction, leader side. This means, decoding the client's request, and acquiring
//    all relevant row locks, as well as the component lock in shared mode.
//
// 3) Consensus::Append() completes when having replicated and persisted the client's
//    request. Depending on the consensus algorithm this might mean just writing to
//    local disk, or replicating the request across quorum replicas.
//
// 4) PrepareSucceeded() is called as a callback when both Consensus::Append() and
//    Prepare() complete. When PrepareSucceeded() is is called twice (independently
//    of who finishes first) Apply() is called, asynchronously.
//
// 5) When Apply() starts execution the RowOps within the WriteTransactionState
//    must all have been prepared (i.e row locks obtained, etc).
//    Apply() starts the mvcc transaction and calls Tablet::Apply() which performs
//    all of the actual underlying operations, saving their results (and mutation
//    records) in the fields of each RowOp struct.
//
//    After all the inserts/mutates are performed Apply() releases row
//    locks (see 'Implementation Techniques for Main Memory Database Systems',
//    DeWitt et. al.). It then readies the CommitMsg with the TXResultPB in
//    transaction context and calls ConsensusRound::Commit() which will
//    in turn trigger a commit of the consensus system.
//
// 6) After the consensus system deems the CommitMsg committed (which might
//    have different requirements depending on the consensus algorithm) the
//    ApplySucceeded callback is called and the transaction is considered
//    completed, the mvcc transaction committed (making the updates visible
//    to other transactions), the metrics updated and the transaction's
//    resources released.
// TODO rephrase the above comment to apply to generic transactions and move
//      to transaction.h
class WriteTransaction : public Transaction {
 public:
  WriteTransaction(WriteTransactionState* tx_state, consensus::DriverType type);

  virtual WriteTransactionState* state() OVERRIDE { return state_.get(); }
  virtual const WriteTransactionState* state() const OVERRIDE { return state_.get(); }

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg) OVERRIDE;

  virtual void NewCommitAbortMessage(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Executes a Prepare for a write transaction
  //
  // Decodes the operations in the request PB and acquires row locks for each of the
  // affected rows. This results in adding 'RowOp' objects for each of the operations
  // into the WriteTransactionState.
  virtual Status Prepare() OVERRIDE;

  // Actually starts the Mvcc transaction and assigns a timestamp to this transaction.
  virtual Status Start() OVERRIDE;

  // Executes an Apply for a write transaction.
  //
  // Actually applies inserts/mutates into the tablet. After these start being
  // applied, the transaction must run to completion as there is currently no
  // means of undoing an update.
  //
  // After completing the inserts/mutates, the row locks and the mvcc transaction
  // can be released, allowing other transactions to update the same rows.
  // However the component lock must not be released until the commit msg, which
  // indicates where each of the inserts/mutates were applied, is persisted to
  // stable storage. Because of this ApplyTask must enqueue a CommitTask before
  // releasing both the row locks and deleting the MvccTransaction as we need to
  // make sure that Commits that touch the same set of rows are persisted in
  // order, for recovery.
  // This, of course, assumes that commits are executed in the same order they
  // are placed in the queue (but not necessarily in the same order of the
  // original requests) which is already a requirement of the consensus
  // algorithm.
  virtual Status Apply(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Releases the row locks (Early Lock Release).
  virtual void PreCommit() OVERRIDE;

  // Actually commits the mvcc transaction and updates the metrics.
  virtual void Finish() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

 private:
  // this transaction's start time
  MonoTime start_time_;

  gscoped_ptr<WriteTransactionState> state_;

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteTransaction);
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_WRITE_TRANSACTION_H_ */
