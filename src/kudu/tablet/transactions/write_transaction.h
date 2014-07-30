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
class PreparedRowWrite;
class RowSetKeyProbe;
struct TabletComponents;

// A transaction context for a batch of inserts/mutates. This class holds and
// owns most everything related to a transaction, including the acquired locks
// (row and component), the PreparedRowWrites, the Replicate and Commit messages.
// With the exception of the rows (ConstContiguousRow), and the mutations
// (RowChangeList), all the transaction related pointers are owned by this class
// and destroyed on Reset() or by the destructor.
//
// IMPORTANT: All the acquired locks will not be released unless the transaction
// context is either destroyed or Reset() or release_locks() is called, beware of
// this when using the transaction context or there will be lock leaks.
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

  // Adds an applied insert to this TransactionState, including the
  // id of the MemRowSet to which it was applied.
  Status AddInsert(const Timestamp &tx_id,
                   int64_t mrs_id);

  // Adds a failed operation to this TransactionState, including the status
  // explaining why the operation failed.
  void AddFailedOperation(const Status &status);

  // Adds an applied mutation to this TransactionState, including the
  // tablet id, the mvcc transaction id, the mutation that was applied
  // and the delta stores that were mutated.
  Status AddMutation(const Timestamp &tx_id,
                     gscoped_ptr<OperationResultPB> result);

  // Adds a missed mutation to this TransactionState.
  // Missed mutations are the ones that are applied on Phase 2 of compaction
  // and reflect updates to the old DeltaMemStore that were not yet present
  // in the new DeltaMemStore.
  // The passed 'changelist' is copied into a protobuf and does not need to
  // be alive after this method returns.
  Status AddMissedMutation(const Timestamp& timestamp,
                           gscoped_ptr<RowwiseRowBlockPB> row_key,
                           const RowChangeList& changelist,
                           gscoped_ptr<OperationResultPB> result);

  bool is_all_success() const {
    return failed_operations_ == 0;
  }

  // Returns the result of this transaction in its protocol buffers form.
  // The transaction result holds information on exactly which memory stores
  // were mutated in the context of this transaction and can be used to
  // perform recovery.
  const TxResultPB& Result() const {
    return result_pb_;
  }

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

  const TabletComponents* tablet_components() const {
    return tablet_components_.get();
  }

  // Commits the Mvcc transaction and releases the component lock. After
  // this method is called all the inserts and mutations will become
  // visible to other transactions.
  //
  // Note: request_ and response_ are set to NULL after this method returns.
  void commit();

  // Adds a PreparedRowWrite to be managed by this transaction context, as
  // created in the prepare phase.
  void add_prepared_row(gscoped_ptr<PreparedRowWrite> row) {
    DCHECK(!mvcc_tx_) << "Must not add row locks after acquiring timestamp!";
    rows_.push_back(row.release());
  }

  // Returns all the prepared row writes for this transaction. Usually called
  // on the apply phase to actually make changes to the tablet.
  vector<PreparedRowWrite *> &rows() {
    return rows_;
  }

  // Releases all the row locks acquired by this transaction.
  void release_row_locks();

  // Resets this TransactionState, releasing all locks, destroying all prepared
  // writes, clearing the transaction result _and_ committing the current Mvcc
  // transaction.
  void Reset();

  virtual std::string ToString() const OVERRIDE;

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteTransactionState);

  TxResultPB result_pb_;
  int32_t failed_operations_;

  // pointers to the rpc context, request and response, lifecyle
  // is managed by the rpc subsystem. These pointers maybe NULL if the
  // transaction was not initiated by an RPC call.
  const tserver::WriteRequestPB* request_;
  tserver::WriteResponsePB* response_;

  // the rows and locks as transformed/acquired by the prepare task
  vector<PreparedRowWrite*> rows_;

  // The MVCC transaction, set up during PREPARE phase
  gscoped_ptr<ScopedTransaction> mvcc_tx_;

  // The tablet components, acquired at the same time as mvcc_tx_ is set.
  scoped_refptr<const TabletComponents> tablet_components_;
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
// 5) When Apply() starts execution the TransactionState must have been
//    passed all the PreparedRowWrites (each containing a row lock) and the
//    'component_lock'. Apply() starts the mvcc transaction and calls
//    Tablet::InsertUnlocked/Tablet::MutateUnlocked with each of the
//    PreparedRowWrites. Apply() keeps track of any single row errors
//    that might have occurred while inserting/mutating and sets those in
//    WriteResponse. TransactionState is passed with each insert/mutate
//    to keep track of which in-memory stores were mutated.
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
  // Acquires all the relevant row locks for the transaction, the tablet
  // component_lock in shared mode and starts an Mvcc transaction. When the task
  // is finished, the next one in the pipeline must take ownership of these.
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
  // Decodes the rows in WriteRequestPB and creates prepared row writes.
  Status CreatePreparedInsertsAndMutates(const Schema& client_schema);

  // this transaction's start time
  MonoTime start_time_;

  gscoped_ptr<WriteTransactionState> state_;

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteTransaction);
};

// A context for a single row in a transaction. Contains the row, the probe
// and the row lock for an insert, or the row_key, the probe, the mutation
// and the row lock, for a mutation.
//
// This class owns the 'probe' and the 'row_lock' but does not own the 'row', 'row_key'
// or 'mutations'. The non-owned data structures are expected to last for the lifetime
// of this class.
class PreparedRowWrite {
 public:
  enum Type {
    INSERT,
    MUTATE
  };

  const ConstContiguousRow* row() const {
    return row_;
  }

  const ConstContiguousRow* row_key() const {
    return row_key_;
  }

  const RowSetKeyProbe* probe() const {
    return probe_.get();
  }

  const RowChangeList& changelist() const {
    return changelist_;
  }

  bool has_row_lock() const {
    return row_lock_.acquired();
  }

  const Type write_type() const {
    return op_type_;
  }

  std::string ToString() const;

 private:

  friend class Tablet;

  // ctor for inserts
  PreparedRowWrite(const ConstContiguousRow* row,
                   const gscoped_ptr<RowSetKeyProbe> probe,
                   ScopedRowLock lock);

  // ctor for mutations
  PreparedRowWrite(const ConstContiguousRow* row_key,
                   const RowChangeList& mutations,
                   const gscoped_ptr<RowSetKeyProbe> probe,
                   ScopedRowLock lock);

  const ConstContiguousRow *row_;
  const ConstContiguousRow *row_key_;
  const RowChangeList changelist_;

  const gscoped_ptr<RowSetKeyProbe> probe_;
  const ScopedRowLock row_lock_;
  const Type op_type_;
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_WRITE_TRANSACTION_H_ */
