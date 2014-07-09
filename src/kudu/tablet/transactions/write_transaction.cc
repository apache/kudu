// Copyright (c) 2013, Cloudera, inc.

#include "kudu/tablet/transactions/write_transaction.h"

#include <algorithm>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/common/row_operations.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/transactions/write_util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/trace.h"

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

Status WriteTransaction::Prepare() {
  TRACE("PREPARE: Starting");

  // Decode everything first so that we give up if something major is wrong.
  Schema client_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(state_->request()->schema(), &client_schema),
                        "Cannot decode client schema");
  if (client_schema.has_column_ids()) {
    // TODO: we have this kind of code a lot - add a new SchemaFromPB variant which
    // does this check inline.
    Status s = Status::InvalidArgument("User requests should not have Column IDs");
    state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }

  Tablet* tablet = state()->tablet_peer()->tablet();

  Status s = tablet->DecodeWriteOperations(&client_schema, state());
  if (!s.ok()) {
    // TODO: is MISMATCHED_SCHEMA always right here? probably not.
    state()->completion_callback()->set_error(s, TabletServerErrorPB::MISMATCHED_SCHEMA);
    return s;
  }

  // Now acquire row locks and prepare everything for apply
  RETURN_NOT_OK(tablet->AcquireRowLocks(state()));

  TRACE("PREPARE: finished.");
  return Status::OK();
}

Status WriteTransaction::Start() {
  state_->tablet_peer()->tablet()->StartTransaction(state_.get());
  TRACE("START. Timestamp: $0", state_->tablet_peer()->clock()->Stringify(state_->timestamp()));
  return Status::OK();
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
  return Substitute("WriteTransaction [type=$0, start_time=$1, state=$2]",
                    DriverType_Name(type()), abs_time_formatted, state_->ToString());
}

WriteTransactionState::WriteTransactionState(TabletPeer* tablet_peer,
                                             const tserver::WriteRequestPB *request,
                                             tserver::WriteResponsePB *response)
  : TransactionState(tablet_peer),
    failed_operations_(0),
    request_(request),
    response_(response),
    mvcc_tx_(NULL) {
  if (request) {
    external_consistency_mode_ = request->external_consistency_mode();
  } else {
    external_consistency_mode_ = NO_CONSISTENCY;
  }
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

void WriteTransactionState::set_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx) {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  mvcc_tx_.reset(mvcc_tx.release());
  set_timestamp(mvcc_tx_->timestamp());
}

void WriteTransactionState::set_tablet_components(
    const scoped_refptr<const TabletComponents>& components) {
  DCHECK(!tablet_components_) << "Already set";
  DCHECK(components);
  tablet_components_ = components;
}

void WriteTransactionState::commit() {
  if (mvcc_tx_.get() != NULL) {
    // commit the transaction
    mvcc_tx_->Commit();
  }
  mvcc_tx_.reset();
  release_row_locks();

  // Make the request NULL since after this transaction commits
  // the request may be deleted at any moment.
  request_ = NULL;
  response_ = NULL;
}

void WriteTransactionState::release_row_locks() {
  // free the row locks
  STLDeleteElements(&rows_);
}

WriteTransactionState::~WriteTransactionState() {
  Reset();
}

void WriteTransactionState::Reset() {
  commit();
  result_pb_.Clear();
  tx_metrics_.Reset();
  failed_operations_ = 0;
  timestamp_ = Timestamp::kInvalidTimestamp;
  tablet_components_ = NULL;
}

string WriteTransactionState::ToString() const {
  string ts_str;
  if (has_timestamp()) {
    ts_str = timestamp().ToString();
  } else {
    ts_str = "<unassigned>";
  }

  // Stringify the actual row operations (eg INSERT/UPDATE/etc)
  // NOTE: we'll eventually need to gate this by some flag if we want to avoid
  // user data escaping into the log. See KUDU-387.
  const size_t kMaxToStringify = 3;
  string rows_str = "[";
  for (int i = 0; i < std::min(rows_.size(), kMaxToStringify); i++) {
    if (i > 0) {
      rows_str.append(", ");
    }
    rows_str.append(rows_[i]->ToString());
  }
  if (rows_.size() > kMaxToStringify) {
    rows_str.append(", ...");
  }
  rows_str.append("]");

  return Substitute("WriteTransactionState $0 [op_id=($1), ts=$2, rows=$3]",
                    this,
                    op_id().ShortDebugString(),
                    ts_str,
                    rows_str);
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

string PreparedRowWrite::ToString() const {
  switch (op_type_) {
    case INSERT:
      return Substitute("INSERT $0", row_->schema()->DebugRowKey(*row_));
    case MUTATE:
      return Substitute("MUTATE $0", row_key_->schema()->DebugRowKey(*row_key_));
      break;
    default:
      LOG(FATAL);
  }
  return ""; // silence spurious "no return" warning
}

}  // namespace tablet
}  // namespace kudu


