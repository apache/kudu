// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TABLET_LOCAL_TABLET_WRITER_H
#define KUDU_TABLET_LOCAL_TABLET_WRITER_H

#include "kudu/common/row_operations.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/gutil/macros.h"

namespace kudu {
namespace tablet {

// Helper class to write directly into a local tablet, without going
// through TabletPeer, consensus, etc.
//
// This is useful for unit-testing the Tablet code paths with no consensus
// implementation or thread pools.
class LocalTabletWriter {
 public:
  explicit LocalTabletWriter(Tablet* tablet,
                             const Schema* client_schema)
    : tablet_(tablet),
      client_schema_(client_schema) {
    CHECK(!client_schema->has_column_ids());
    CHECK_OK(SchemaToPB(*client_schema, req_.mutable_schema()));
  }

  ~LocalTabletWriter() {}

  Status Insert(const KuduPartialRow& row) {
    return Write(RowOperationsPB::INSERT, row);
  }

  Status Delete(const KuduPartialRow& row) {
    return Write(RowOperationsPB::DELETE, row);
  }

  Status Update(const KuduPartialRow& row) {
    return Write(RowOperationsPB::UPDATE, row);
  }

  // Perform a write against the local tablet.
  // Returns a bad Status if the applied operation had a per-row error.
  Status Write(RowOperationsPB::Type type,
               const KuduPartialRow& row) {
    req_.mutable_row_operations()->Clear();
    RowOperationsPBEncoder encoder(req_.mutable_row_operations());

    encoder.Add(type, row);
    tx_state_.reset(new WriteTransactionState(NULL, &req_, NULL));

    RETURN_NOT_OK(tablet_->DecodeWriteOperations(client_schema_, tx_state_.get()));
    RETURN_NOT_OK(tablet_->AcquireRowLocks(tx_state_.get()));
    tablet_->StartTransaction(tx_state_.get());

    // Create a "fake" OpId and set it in the TransactionState for anchoring.
    tx_state_->mutable_op_id()->CopyFrom(consensus::MaximumOpId());
    tablet_->ApplyRowOperations(tx_state_.get());

    tx_state_->Commit();

    tx_state_->release_row_locks();
    tx_state_->ReleaseSchemaLock();

    // return the status of last op
    if (last_op_result().has_failed_status()) {
      return StatusFromPB(last_op_result().failed_status());
    }
    return Status::OK();
  }

  // Return the result of the last row operation run against the tablet.
  const OperationResultPB& last_op_result() {
    CHECK_GE(tx_state_->row_ops().size(), 1);
    return *CHECK_NOTNULL(tx_state_->row_ops().back()->result.get());
  }

 private:
  Tablet* const tablet_;
  const Schema* client_schema_;

  tserver::WriteRequestPB req_;
  gscoped_ptr<WriteTransactionState> tx_state_;

  DISALLOW_COPY_AND_ASSIGN(LocalTabletWriter);
};


} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_LOCAL_TABLET_WRITER_H */
