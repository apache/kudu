// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/write_transaction.h"

#include <vector>

#include "common/wire_protocol.h"
#include "gutil/stl_util.h"
#include "rpc/rpc_context.h"
#include "server/hybrid_clock.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tablet/tablet_metrics.h"
#include "tablet/transactions/write_util.h"
#include "tserver/tserver.pb.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using boost::shared_lock;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;
using boost::bind;

LeaderWriteTransaction::LeaderWriteTransaction(TransactionTracker *txn_tracker,
                                               WriteTransactionContext* tx_ctx,
                                               consensus::Consensus* consensus,
                                               TaskExecutor* prepare_executor,
                                               TaskExecutor* apply_executor,
                                               simple_spinlock* prepare_replicate_lock)
: LeaderTransaction(txn_tracker,
                    consensus,
                    prepare_executor,
                    apply_executor,
                    prepare_replicate_lock),
  tx_ctx_(tx_ctx) {
}

void LeaderWriteTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(WRITE_OP);
  (*replicate_msg)->mutable_write_request()->CopyFrom(*tx_ctx()->request());
}

Status LeaderWriteTransaction::Prepare() {
  TRACE("PREPARE: Starting");

  const WriteRequestPB* req = tx_ctx_->request();
  Tablet* tablet = tx_ctx_->tablet_peer()->tablet();

  // Decode everything first so that we give up if something major is wrong.
  Schema client_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(req->schema(), &client_schema),
                        "Cannot decode client schema");
  if (client_schema.has_column_ids()) {
    // TODO: we have this kind of code a lot - add a new SchemaFromPB variant which
    // does this check inline.
    Status s = Status::InvalidArgument("User requests should not have Column IDs");
    tx_ctx_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }
  Status s = CreatePreparedInsertsAndMutates(tablet,
                                             tx_ctx_.get(),
                                             client_schema,
                                             req->row_operations());

  // Now that we've prepared set the transaction timestamp (by initiating the
  // mvcc transaction). Doing this here allows us to wait the least possible
  // time if we're using commit wait.
  Timestamp timestamp = tx_ctx_->start_mvcc_tx();
  TRACE("PREPARE: finished. Timestamp: $0", server::HybridClock::GetPhysicalValue(timestamp));
  return s;
}

void LeaderWriteTransaction::PrepareFailedPreCommitHooks(gscoped_ptr<CommitMsg>* commit_msg) {
  // Release all row locks (no effect if no locks were acquired).
  tx_ctx_->release_row_locks();

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_write_response()->CopyFrom(*tx_ctx_->response());
  (*commit_msg)->mutable_result()->CopyFrom(tx_ctx_->Result());
  (*commit_msg)->set_timestamp(tx_ctx_->timestamp().ToUint64());
}

// FIXME: Since this is called as a void in a thread-pool callback,
// it seems pointless to return a Status!
Status LeaderWriteTransaction::Apply() {
  TRACE("APPLY: Starting");

  Tablet* tablet = tx_ctx_->tablet_peer()->tablet();

  int i = 0;
  Status s;
  BOOST_FOREACH(const PreparedRowWrite *row, tx_ctx_->rows()) {
    switch (row->write_type()) {
      case PreparedRowWrite::INSERT: {
        s = tablet->InsertUnlocked(tx_ctx(), row);
        break;
      }
      case PreparedRowWrite::MUTATE: {
        s = tablet->MutateRowUnlocked(tx_ctx(), row);
        break;
      }
    }
    if (PREDICT_FALSE(!s.ok())) {
      WriteResponsePB::PerRowErrorPB* error = tx_ctx_->response()->add_per_row_errors();
      error->set_row_index(i);
      StatusToPB(s, error->mutable_error());
    }
    i++;
  }

  // If the client requested COMMIT_WAIT as the external consistency mode
  // calculate the latest that the prepare timestamp could be and wait
  // until now.earliest > prepare_latest. Only after this are the locks
  // released.
  if (tx_ctx_->external_consistency_mode() == COMMIT_WAIT) {
    TRACE("APPLY: Commit Wait.");
    // If we can't commit wait and have already applied we might have consistency
    // issues if we still reply to the client that the operation was a success.
    // On the other hand we don't have rollbacks as of yet thus we can't undo the
    // the apply either, so we just CHECK_OK for now.
    CHECK_OK(CommitWait());
  }

  TRACE("APPLY: Releasing row locks");

  // Perform early lock release after we've applied all changes
  tx_ctx_->release_row_locks();

  gscoped_ptr<CommitMsg> commit(new CommitMsg());
  commit->mutable_result()->CopyFrom(tx_ctx_->Result());
  commit->set_op_type(WRITE_OP);
  commit->set_timestamp(tx_ctx_->timestamp().ToUint64());
  tx_ctx_->response()->set_write_timestamp(tx_ctx_->timestamp().ToUint64());

  TRACE("APPLY: finished, triggering COMMIT");

  RETURN_NOT_OK(tx_ctx_->consensus_ctx()->Commit(commit.Pass()));
  // NB: do not use tx_ctx_ after this point, because the commit may have
  // succeeded, in which case the context may have been torn down.
  return Status::OK();
}

void LeaderWriteTransaction::ApplySucceeded() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("WriteCommitCallback: making edits visible");
  tx_ctx()->commit();
  LeaderTransaction::ApplySucceeded();
}

void LeaderWriteTransaction::UpdateMetrics() {
  // Update tablet server metrics.
  TabletMetrics* metrics = tx_ctx_->tablet_peer()->tablet()->metrics();
  if (metrics) {
    // TODO: should we change this so it's actually incremented by the
    // Tablet code itself instead of this wrapper code?
    metrics->rows_inserted->IncrementBy(tx_ctx_->metrics().successful_inserts);
    metrics->rows_updated->IncrementBy(tx_ctx_->metrics().successful_updates);
    if (tx_ctx()->external_consistency_mode() == COMMIT_WAIT) {
      metrics->commit_wait_duration->Increment(tx_ctx_->metrics().commit_wait_duration_usec);
    }
    uint64_t op_duration_usec =
        MonoTime::Now(MonoTime::FINE).GetDeltaSince(start_time_).ToMicroseconds();
    switch (tx_ctx()->external_consistency_mode()) {
      case NO_CONSISTENCY:
        metrics->write_op_duration_no_consistency->Increment(op_duration_usec);
        break;
      case CLIENT_PROPAGATED:
        metrics->write_op_duration_client_propagated_consistency->Increment(op_duration_usec);
        break;
      case COMMIT_WAIT:
        metrics->write_op_duration_commit_wait_consistency->Increment(op_duration_usec);
        break;
    }
  }
}

Status WriteTransactionContext::AddInsert(const Timestamp &timestamp, int64_t mrs_id) {
  if (PREDICT_TRUE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->timestamp(), timestamp)
        << "tx_id doesn't match the id of the ongoing transaction";
  }
  OperationResultPB* insert = result_pb_.add_ops();
  insert->add_mutated_stores()->set_mrs_id(mrs_id);
  tx_metrics_.successful_inserts++;
  return Status::OK();
}

void WriteTransactionContext::AddFailedOperation(const Status &status) {
  OperationResultPB* insert = result_pb_.add_ops();
  StatusToPB(status, insert->mutable_failed_status());
  failed_operations_++;
}

Status WriteTransactionContext::AddMutation(const Timestamp &timestamp,
                                            gscoped_ptr<OperationResultPB> result) {
  if (PREDICT_FALSE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->timestamp(), timestamp)
        << "tx_id doesn't match the id of the ongoing transaction";
  }
  result_pb_.mutable_ops()->AddAllocated(result.release());
  tx_metrics_.successful_updates++;
  return Status::OK();
}

Timestamp WriteTransactionContext::start_mvcc_tx() {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  // if the consistency mode is set to COMMIT_WAIT instruct
  // ScopedTransaction to obtain the latest possible clock value
  // taking the error into consideration.
  if (external_consistency_mode() == COMMIT_WAIT) {
    mvcc_tx_.reset(new ScopedTransaction(tablet_peer_->tablet()->mvcc_manager(), true));
  } else {
    mvcc_tx_.reset(new ScopedTransaction(tablet_peer_->tablet()->mvcc_manager()));
  }
  set_timestamp(mvcc_tx_->timestamp());
  return mvcc_tx_->timestamp();
}

void WriteTransactionContext::set_current_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx) {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  DCHECK(external_consistency_mode() != COMMIT_WAIT);
  mvcc_tx_.reset(mvcc_tx.release());
  set_timestamp(mvcc_tx_->timestamp());
}

void WriteTransactionContext::commit() {
  if (mvcc_tx_.get() != NULL) {
    // commit the transaction
    mvcc_tx_->Commit();
  }
  mvcc_tx_.reset();
  component_lock_.reset();
  release_row_locks();
}

void WriteTransactionContext::release_row_locks() {
  // free the row locks
  STLDeleteElements(&rows_);
}

void WriteTransactionContext::Reset() {
  commit();
  result_pb_.Clear();
  tx_metrics_.Reset();
  failed_operations_ = 0;
  timestamp_ = Timestamp::kInvalidTimestamp;
}

PreparedRowWrite::PreparedRowWrite(const ConstContiguousRow* row,
                                   gscoped_ptr<RowSetKeyProbe> probe,
                                   gscoped_ptr<ScopedRowLock> lock)
    : row_(row),
      row_key_(NULL),
      probe_(probe.Pass()),
      row_lock_(lock.Pass()),
      op_type_(INSERT) {
}

PreparedRowWrite::PreparedRowWrite(const ConstContiguousRow* row_key,
                                   const RowChangeList& changelist,
                                   gscoped_ptr<RowSetKeyProbe> probe,
                                   gscoped_ptr<tablet::ScopedRowLock> lock)
    : row_(NULL),
      row_key_(row_key),
      changelist_(changelist),
      probe_(probe.Pass()),
      row_lock_(lock.Pass()),
      op_type_(MUTATE) {
}

}  // namespace tablet
}  // namespace kudu


