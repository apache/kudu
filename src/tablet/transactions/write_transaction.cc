// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/write_transaction.h"

#include <vector>

#include "common/wire_protocol.h"
#include "common/row_operations.h"
#include "gutil/stl_util.h"
#include "gutil/strings/numbers.h"
#include "gutil/walltime.h"
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

using boost::bind;
using boost::shared_lock;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::DriverType;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;
using strings::Substitute;

WriteTransaction::WriteTransaction(WriteTransactionState* state, DriverType type)
  : Transaction(state, type, Transaction::WRITE_TXN),
  state_(state) {
  start_time_ = MonoTime::Now(MonoTime::FINE);
}

void WriteTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(WRITE_OP);
  (*replicate_msg)->mutable_write_request()->CopyFrom(*state()->request());
}

void WriteTransaction::NewCommitAbortMessage(gscoped_ptr<CommitMsg>* commit_msg) {
  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  if (type() == consensus::LEADER) {
    if (state()->has_timestamp()) {
      (*commit_msg)->set_timestamp(state()->timestamp().ToUint64());
    }
    (*commit_msg)->mutable_write_response()->CopyFrom(*state_->response());
  } else {
    consensus::OperationPB* leader_op = state()->consensus_round()->leader_commit_op();
    if (leader_op->commit().has_timestamp()) {
      (*commit_msg)->set_timestamp(leader_op->commit().timestamp());
    }
    (*commit_msg)->mutable_write_response()->CopyFrom(leader_op->commit().write_response());
  }
}

Status WriteTransaction::CreatePreparedInsertsAndMutates(const Schema& client_schema) {
  TRACE("PREPARE: Acquiring component lock");

  Tablet* tablet = state()->tablet_peer()->tablet();

  // acquire the component lock. this is more like "tablet lock" and is used
  // to prevent AlterSchema and other operations that requires exclusive access
  // to the tablet.
  state()->set_component_lock(tablet->component_lock());

  TRACE("Projecting inserts");
  // Now that the schema is fixed, we can project the operations into that schema.
  vector<DecodedRowOperation> decoded_ops;
  if (state()->request()->row_operations().rows().size() > 0) {
    RowOperationsPBDecoder dec(&state()->request()->row_operations(),
                               &client_schema,
                               tablet->schema_unlocked().get(),
                               state()->arena());
    Status s = dec.DecodeOperations(&decoded_ops);
    if (!s.ok()) {
      // TODO: is MISMATCHED_SCHEMA always right here? probably not.
      state()->completion_callback()->set_error(s, TabletServerErrorPB::MISMATCHED_SCHEMA);
      return s;
    }
  }

  // Now acquire row locks and prepare everything for apply
  TRACE("PREPARE: Running prepare for $0 operations", decoded_ops.size());
  BOOST_FOREACH(const DecodedRowOperation& op, decoded_ops) {
    gscoped_ptr<PreparedRowWrite> row_write;

    switch (op.type) {
      case RowOperationsPB::INSERT:
      {
        // TODO pass 'row_ptr' to the PreparedRowWrite once we get rid of the
        // old API that has a Mutate method that receives the row as a reference.
        // TODO: allocating ConstContiguousRow is kind of a waste since it is just
        // a {schema, ptr} pair itself and probably cheaper to copy around.
        ConstContiguousRow *row = state()->AddToAutoReleasePool(
          new ConstContiguousRow(*tablet->schema_unlocked().get(), op.row_data));
        RETURN_NOT_OK(tablet->CreatePreparedInsert(state(), row, &row_write));
        break;
      }
      case RowOperationsPB::UPDATE:
      case RowOperationsPB::DELETE:
      {
        const uint8_t* row_key_ptr = op.row_data;
        const RowChangeList& mutation = op.changelist;

        // TODO pass 'row_key_ptr' to the PreparedRowWrite once we get rid of the
        // old API that has a Mutate method that receives the row as a reference.
        // TODO: allocating ConstContiguousRow is kind of a waste since it is just
        // a {schema, ptr} pair itself and probably cheaper to copy around.
        ConstContiguousRow* row_key = new ConstContiguousRow(tablet->key_schema(), row_key_ptr);
        row_key = state()->AddToAutoReleasePool(row_key);
        RETURN_NOT_OK(tablet->CreatePreparedMutate(state(), row_key, mutation, &row_write));
        break;
      }
      default:
        LOG(FATAL) << "Bad type: " << op.type;
    }

    state()->add_prepared_row(row_write.Pass());
  }
  return Status::OK();
}

Status WriteTransaction::Prepare() {
  TRACE("PREPARE: Starting");

  const WriteRequestPB* req = state_->request();

  // Decode everything first so that we give up if something major is wrong.
  Schema client_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(req->schema(), &client_schema),
                        "Cannot decode client schema");
  if (client_schema.has_column_ids()) {
    // TODO: we have this kind of code a lot - add a new SchemaFromPB variant which
    // does this check inline.
    Status s = Status::InvalidArgument("User requests should not have Column IDs");
    state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }
  Status s = CreatePreparedInsertsAndMutates(client_schema);

  // Now that we've prepared set the transaction timestamp (by initiating the
  // mvcc transaction). Doing this here allows us to wait the least possible
  // time if we're using commit wait.
  Timestamp timestamp = state_->start_mvcc_tx();
  TRACE("PREPARE: finished. Timestamp: $0", server::HybridClock::GetPhysicalValue(timestamp));
  return s;
}

// FIXME: Since this is called as a void in a thread-pool callback,
// it seems pointless to return a Status!
Status WriteTransaction::Apply(gscoped_ptr<CommitMsg>* commit_msg) {
  TRACE("APPLY: Starting");

  Tablet* tablet = state()->tablet_peer()->tablet();
  int i = 0;
  // TODO for now we're just warning on this status. This status indicates if
  // _any_ insert or mutation failed consider writing OP_ABORT if all failed.
  Status s;
  int ctr = 0;
  VLOG(2) <<  "Write Transaction at " << reinterpret_cast<size_t>(this) << ":\n";
  BOOST_FOREACH(const PreparedRowWrite *row, state()->rows()) {
    VLOG(2) << "(" << reinterpret_cast<size_t>(this) << "#"
              << ctr++ << ") " << ((row->write_type() == PreparedRowWrite::INSERT) ?
                                   "insertion" : "mutation");
    switch (row->write_type()) {
      case PreparedRowWrite::INSERT: {
        s = tablet->InsertUnlocked(state(), row);
        break;
      }
      case PreparedRowWrite::MUTATE: {
        s = tablet->MutateRowUnlocked(state(), row);
        break;
      }
    }
    if (PREDICT_FALSE(!s.ok())) {
      // Replicas disregard the per row errors, for now
      // TODO check the per-row errors against the leader's, at least in debug mode
      if (state()->response() != NULL) {
        WriteResponsePB::PerRowErrorPB* error = state()->response()->add_per_row_errors();
        error->set_row_index(i);
        StatusToPB(s, error->mutable_error());
      }
    }
    i++;
  }
  WARN_NOT_OK(s, "Some row writes failed.");

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->mutable_result()->CopyFrom(state()->Result());
  (*commit_msg)->set_op_type(WRITE_OP);
  (*commit_msg)->set_timestamp(state()->timestamp().ToUint64());

  // If this is a leader side transaction set the timestamp on the response
  // TODO(todd): can we consolidate this code into the driver? we seem to be
  // quite inconsistent whether we do this or not in the other transaction types.
  if (type() == consensus::LEADER) {
    state()->response()->set_write_timestamp(state()->timestamp().ToUint64());
    (*commit_msg)->mutable_write_response()->CopyFrom(*state()->response());
  } else {
    (*commit_msg)->mutable_write_response()->CopyFrom(
        state()->consensus_round()->leader_commit_op()->commit().write_response());
  }
  return Status::OK();
}

void WriteTransaction::PreCommit() {
  TRACE("PRECOMMIT: Releasing row locks");
  // Perform early lock release after we've applied all changes
  state()->release_row_locks();
}

void WriteTransaction::Finish() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("FINISH: making edits visible");
  state()->commit();

  TabletMetrics* metrics = state_->tablet_peer()->tablet()->metrics();
  if (metrics) {
    // TODO: should we change this so it's actually incremented by the
    // Tablet code itself instead of this wrapper code?
    metrics->rows_inserted->IncrementBy(state_->metrics().successful_inserts);
    metrics->rows_updated->IncrementBy(state_->metrics().successful_updates);

    if (type() == consensus::LEADER) {
      if (state()->external_consistency_mode() == COMMIT_WAIT) {
        metrics->commit_wait_duration->Increment(state_->metrics().commit_wait_duration_usec);
      }
      uint64_t op_duration_usec =
          MonoTime::Now(MonoTime::FINE).GetDeltaSince(start_time_).ToMicroseconds();
      switch (state()->external_consistency_mode()) {
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
}

string WriteTransaction::ToString() const {
  MonoTime now(MonoTime::Now(MonoTime::FINE));
  MonoDelta d = now.GetDeltaSince(start_time_);
  WallTime abs_time = WallTime_Now() - d.ToSeconds();
  string abs_time_formatted;
  StringAppendStrftime(&abs_time_formatted, "%Y-%m-%d %H:%M:%S", (time_t)abs_time, true);
  return Substitute("WriteTransaction [start_time=$0, state=$1]",
                    abs_time_formatted, state_->ToString());
}

Status WriteTransactionState::AddInsert(const Timestamp &timestamp, int64_t mrs_id) {
  if (PREDICT_TRUE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->timestamp(), timestamp)
        << "tx_id doesn't match the id of the ongoing transaction";
  }
  OperationResultPB* insert = result_pb_.add_ops();
  insert->add_mutated_stores()->set_mrs_id(mrs_id);
  tx_metrics_.successful_inserts++;
  return Status::OK();
}

void WriteTransactionState::AddFailedOperation(const Status &status) {
  OperationResultPB* insert = result_pb_.add_ops();
  StatusToPB(status, insert->mutable_failed_status());
  failed_operations_++;
}

Status WriteTransactionState::AddMutation(const Timestamp &timestamp,
                                            gscoped_ptr<OperationResultPB> result) {
  if (PREDICT_FALSE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->timestamp(), timestamp)
        << "tx_id doesn't match the id of the ongoing transaction";
  }
  result_pb_.mutable_ops()->AddAllocated(result.release());
  tx_metrics_.successful_updates++;
  return Status::OK();
}

Timestamp WriteTransactionState::start_mvcc_tx() {
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

void WriteTransactionState::set_current_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx) {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  DCHECK(external_consistency_mode() != COMMIT_WAIT);
  mvcc_tx_.reset(mvcc_tx.release());
  set_timestamp(mvcc_tx_->timestamp());
}

void WriteTransactionState::commit() {
  if (mvcc_tx_.get() != NULL) {
    // commit the transaction
    mvcc_tx_->Commit();
  }
  mvcc_tx_.reset();
  if (component_lock_.owns_lock()) {
    component_lock_.unlock();
  }
  release_row_locks();
}

void WriteTransactionState::release_row_locks() {
  // free the row locks
  STLDeleteElements(&rows_);
}

void WriteTransactionState::Reset() {
  commit();
  result_pb_.Clear();
  tx_metrics_.Reset();
  failed_operations_ = 0;
  timestamp_ = Timestamp::kInvalidTimestamp;
}

string WriteTransactionState::ToString() const {
  // TODO Add a Debug/Stringify to PreparedRowWrite() so that we can
  // see the information on locks held by the transactions.
  //
  // Note: a debug string of the request is not included for security
  // reasons.
  string ts_str;
  if (has_timestamp()) {
    ts_str = timestamp().ToString();
  } else {
    ts_str = "<unassigned>";
  }

  return Substitute("WriteTransactionState $0 [op_id=($1), ts=$2]",
                    this,
                    op_id().ShortDebugString(),
                    ts_str);
}

PreparedRowWrite::PreparedRowWrite(const ConstContiguousRow* row,
                                   gscoped_ptr<RowSetKeyProbe> probe,
                                   ScopedRowLock lock)
    : row_(row),
      row_key_(NULL),
      probe_(probe.Pass()),
      row_lock_(lock.Pass()),
      op_type_(INSERT) {
}

PreparedRowWrite::PreparedRowWrite(const ConstContiguousRow* row_key,
                                   const RowChangeList& changelist,
                                   gscoped_ptr<RowSetKeyProbe> probe,
                                   ScopedRowLock lock)
    : row_(NULL),
      row_key_(row_key),
      changelist_(changelist),
      probe_(probe.Pass()),
      row_lock_(lock.Pass()),
      op_type_(MUTATE) {
}

}  // namespace tablet
}  // namespace kudu


