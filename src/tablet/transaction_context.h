// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TRANSACTION_CONTEXT_H_
#define KUDU_TABLET_TRANSACTION_CONTEXT_H_

#include <string>
#include <vector>

#include "common/row.h"
#include "common/row_changelist.h"
#include "common/wire_protocol.h"
#include "consensus/consensus.h"
#include "rpc/rpc_context.h"
#include "rpc/service_if.h"
#include "tablet/lock_manager.h"
#include "tablet/mvcc.h"
#include "tablet/rowset.h"
#include "tablet/tablet.pb.h"
#include "tserver/tserver.pb.h"
#include "util/auto_release_pool.h"

namespace kudu {

namespace tablet {

class TabletPeer;
class PreparedRowWrite;

// All metrics associated with a TransactionContext.
struct TransactionMetrics {
  TransactionMetrics();
  void Reset();
  int successful_inserts;
  int successful_updates;
};

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
class TransactionContext {

 public:
  TransactionContext()
      : failed_operations_(0),
        tablet_peer_(NULL),
        rpc_ctx_(NULL),
        request_(NULL),
        response_(NULL),
        component_lock_(NULL),
        mvcc_tx_(NULL),
        consensus_ctx_(NULL) {
    result_pb_.set_txid(txid_t::kInvalidTxId.v);
  }

  TransactionContext(TabletPeer* tablet_peer,
                     rpc::RpcContext *rpc_ctx,
                     const tserver::WriteRequestPB *request,
                     tserver::WriteResponsePB *response)
      : failed_operations_(0),
        tablet_peer_(tablet_peer),
        rpc_ctx_(rpc_ctx),
        request_(request),
        response_(response),
        component_lock_(NULL),
        mvcc_tx_(NULL),
        consensus_ctx_(NULL) {
    result_pb_.set_txid(txid_t::kInvalidTxId.v);
  }

  // Adds an applied insert to this TransactionContext, including the
  // id of the MemRowSet to which it was applied.
  Status AddInsert(const txid_t &tx_id,
                   int64_t mrs_id);

  // Adds a failed insert to this TransactionContext, including the status
  // explaining why the insert failed.
  void AddFailedInsert(const Status &status);

  // Adds an applied mutation to this TransactionContext, including the
  // tablet id, the mvcc transaction id, the mutation that was applied
  // and the delta stores that were mutated.
  Status AddMutation(const txid_t &tx_id,
                     gscoped_ptr<MutationResultPB> result);

  // Adds a missed mutation to this TransactionContext.
  // Missed mutations are the ones that are applied on Phase 2 of compaction
  // and reflect updates to the old DeltaMemStore that were not yet present
  // in the new DeltaMemStore.
  // The passed 'changelist' is copied into a protobuf and does not need to
  // be alive after this method returns.
  Status AddMissedMutation(const txid_t &tx_id,
                           gscoped_ptr<RowwiseRowBlockPB> row_key,
                           const RowChangeList& changelist,
                           gscoped_ptr<MutationResultPB> result);

  // Adds a failed mutation to this TransactionContext, including the status
  // explaining why it failed.
  void AddFailedMutation(const Status &status);

  // Return metrics related to this transaction.
  const TransactionMetrics& metrics() const { return tx_metrics_; }

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

  TabletPeer* tablet_peer() {
    return tablet_peer_;
  }

  // Returns the original client request for this transaction, if there was
  // one.
  const tserver::WriteRequestPB *request() {
    return request_;
  }

  // Returns the prepared response to the client that will be sent when this
  // transaction is completed, if this transaction was started by a client.
  tserver::WriteResponsePB *response() {
    return response_;
  }

  // Returns the RPCContext that triggered this transaction, if this
  // transaction was triggered by a client.
  rpc::RpcContext *rpc_context() {
    return rpc_ctx_;
  }

  // Returns the Mvcc transaction id for the ongoing transaction or
  // kInvalidTxId if no Mvcc transaction is managed by this TransactionContext.
  txid_t mvcc_txid();

  // Starts an Mvcc transaction, the ScopedTransaction will not commit until
  // commit_mvcc_tx is called. To be able to start an Mvcc transaction this
  // TransactionContext must have a hold on the MvccManager.
  txid_t start_mvcc_tx();

  // Allows to set the current Mvcc transaction externally when
  // this TransactionContext doesn't have a handle to MvccManager.
  void set_current_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx);

  // Commits the Mvcc transaction and releases the component lock. After
  // this method is called all the inserts and mutations will become
  // visible to other transactions.
  void commit();

  // Adds a PreparedRowWrite to be managed by this transaction context, as
  // created in the prepare phase.
  void add_prepared_row(gscoped_ptr<PreparedRowWrite> row) {
    rows_.push_back(row.release());
  }

  // Returns all the prepared row writes for this transaction. Usually called
  // on the apply phase to actually make changes to the tablet.
  vector<PreparedRowWrite *> &rows() {
    return rows_;
  }

  // Sets the component lock for this transaction. The lock will not be
  // unlocked unless either release_locks() or Reset() is called or this
  // TransactionContext is destroyed.
  void set_component_lock(gscoped_ptr<boost::shared_lock<rw_spinlock> > lock) {
    component_lock_.reset(lock.release());
  }

  boost::shared_lock<rw_spinlock>* component_lock() {
    return component_lock_.get();
  }

  // Releases all the row locks acquired by this transaction.
  void release_row_locks();

  // Sets the ConsensusContext for this transaction, if this transaction is
  // being executed through the consensus system.
  void set_consensus_ctx(gscoped_ptr<consensus::ConsensusContext> consensus_ctx) {
    consensus_ctx_.reset(consensus_ctx.release());
  }

  // Returns the ConsensusContext being used, if this transaction is being
  // executed through the consensus system or NULL if it's not.
  consensus::ConsensusContext* consensus_ctx() {
    return consensus_ctx_.get();
  }

  // Sets a heap object to be managed by this transaction's AutoReleasePool.
  template <class T>
  T* AddToAutoReleasePool(T* t) {
    return pool_.Add(t);
  }

  // Resets this TransactionContext, releasing all locks, destroying all prepared
  // writes, clearing the transaction result _and_ committing the current Mvcc
  // transaction.
  void Reset();

  ~TransactionContext() {
    Reset();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(TransactionContext);

  TxResultPB result_pb_;
  TransactionMetrics tx_metrics_;
  int32_t failed_operations_;

  // The tablet peer that is coordinating this transaction.
  TabletPeer* tablet_peer_;

  // pointers to the rpc context, request and response, lifecyle
  // is managed by the rpc subsystem. These pointers maybe NULL if the
  // transaction was not initiated by an RPC call.
  rpc::RpcContext* rpc_ctx_;
  const tserver::WriteRequestPB* request_;
  tserver::WriteResponsePB* response_;

  // the rows and locks as transformed/acquired by the prepare task
  vector<PreparedRowWrite*> rows_;
  // the component lock, acquired by all inserters/updaters
  gscoped_ptr<boost::shared_lock<rw_spinlock> > component_lock_;
  gscoped_ptr<ScopedTransaction> mvcc_tx_;
  gscoped_ptr<consensus::ConsensusContext> consensus_ctx_;

  AutoReleasePool pool_;
};

// Calculates type of the mutation based on the set fields and number of targets.
MutationResultPB::MutationTypePB MutationType(const MutationResultPB* result);

// A context for a single row in a transaction. Contains the row, the probe
// and the row lock for an insert, or the row_key, the probe, the mutation
// and the row lock, for a mutation.
//
// This class owns the 'probe' and the 'row_lock' but does not own the 'row', 'row_key'
// or 'mutations'. The non-owned data structures are expected to last for the lifetime
// of this class.
class PreparedRowWrite {
 public:
  const Schema *schema() const {
    return schema_;
  }

  const ConstContiguousRow* row() const {
    return row_;
  }

  const ConstContiguousRow* row_key() const {
    return row_key_;
  }

  const RowSetKeyProbe* probe() const {
    return probe_.get();
  }

  const RowChangeList* changelist() const {
    return changelist_;
  }

  const ScopedRowLock* row_lock() const {
    return row_lock_.get();
  }

  const TxOperationPB::TxOperationTypePB write_type() const {
    return op_type_;
  }

 private:

  friend class Tablet;

  // ctor for inserts
  PreparedRowWrite(const ConstContiguousRow* row,
                   const gscoped_ptr<RowSetKeyProbe> probe,
                   const gscoped_ptr<ScopedRowLock> lock);

  // ctor for mutations
  PreparedRowWrite(const ConstContiguousRow* row_key,
                   const Schema* mutations_schema,
                   const RowChangeList* mutations,
                   const gscoped_ptr<RowSetKeyProbe> probe,
                   const gscoped_ptr<tablet::ScopedRowLock> lock);

  const Schema* schema_;
  const ConstContiguousRow *row_;
  const ConstContiguousRow *row_key_;
  const RowChangeList *changelist_;

  const gscoped_ptr<RowSetKeyProbe> probe_;
  const gscoped_ptr<ScopedRowLock> row_lock_;
  const TxOperationPB::TxOperationTypePB op_type_;

};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TRANSACTION_CONTEXT_H_ */
