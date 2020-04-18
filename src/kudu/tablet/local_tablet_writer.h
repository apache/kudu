// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <memory>
#include <vector>

#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/transactions/write_transaction.h"

namespace kudu {
namespace tablet {

// Helper class to write directly into a local tablet, without going
// through TabletReplica, consensus, etc.
//
// This is useful for unit-testing the Tablet code paths with no consensus
// implementation or thread pools.
class LocalTabletWriter {
 public:
  struct RowOp {
    RowOp(RowOperationsPB::Type type,
       const KuduPartialRow* row)
      : type(type),
        row(row) {
    }

    RowOperationsPB::Type type;
    const KuduPartialRow* row;
  };

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

  Status InsertIgnore(const KuduPartialRow& row) {
    return Write(RowOperationsPB::INSERT_IGNORE, row);
  }

  Status Upsert(const KuduPartialRow& row) {
    return Write(RowOperationsPB::UPSERT, row);
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
    std::vector<RowOp> ops;
    ops.emplace_back(type, &row);
    return WriteBatch(ops);
  }

  Status WriteBatch(const std::vector<RowOp>& ops) {
    req_.mutable_row_operations()->Clear();
    RowOperationsPBEncoder encoder(req_.mutable_row_operations());

    for (const RowOp& op : ops) {
      encoder.Add(op.type, *op.row);
    }

    op_state_.reset(new WriteOpState(NULL, &req_, NULL));

    RETURN_NOT_OK(tablet_->DecodeWriteOperations(client_schema_, op_state_.get()));
    RETURN_NOT_OK(tablet_->AcquireRowLocks(op_state_.get()));
    tablet_->AssignTimestampAndStartOpForTests(op_state_.get());

    // Create a "fake" OpId and set it in the OpState for anchoring.
    op_state_->mutable_op_id()->CopyFrom(consensus::MaximumOpId());
    RETURN_NOT_OK(tablet_->ApplyRowOperations(op_state_.get()));

    op_state_->ReleaseTxResultPB(&result_);
    tablet_->mvcc_manager()->AdjustNewOpLowerBound(op_state_->timestamp());
    op_state_->CommitOrAbort(Op::COMMITTED);

    // Return the status of first failed op.
    int op_idx = 0;
    for (const OperationResultPB& result : result_.ops()) {
      if (result.has_failed_status()) {
        return StatusFromPB(result.failed_status())
          .CloneAndPrepend(ops[op_idx].row->ToString());
        break;
      }
      op_idx++;
    }

    // Update the metrics.
    TabletMetrics* metrics = tablet_->metrics();
    if (metrics) {
      metrics->rows_inserted->IncrementBy(op_idx);
    }

    return Status::OK();
  }

  // Return the result of the last row operation run against the tablet.
  const OperationResultPB& last_op_result() {
    CHECK_GE(result_.ops_size(), 1);
    return result_.ops(result_.ops_size() - 1);
  }

 private:
  Tablet* const tablet_;
  const Schema* client_schema_;

  TxResultPB result_;
  tserver::WriteRequestPB req_;
  std::unique_ptr<WriteOpState> op_state_;

  DISALLOW_COPY_AND_ASSIGN(LocalTabletWriter);
};


} // namespace tablet
} // namespace kudu
