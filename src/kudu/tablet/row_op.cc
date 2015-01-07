// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/common/wire_protocol.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet.pb.h"

namespace kudu {
namespace tablet {

RowOp::RowOp(const DecodedRowOperation& decoded_op)
  : decoded_op(decoded_op) {
}

RowOp::~RowOp() {
}

void RowOp::SetFailed(const Status& s) {
  DCHECK(!result) << result->DebugString();
  result.reset(new OperationResultPB());
  StatusToPB(s, result->mutable_failed_status());
}

void RowOp::SetInsertSucceeded(int mrs_id) {
  DCHECK(!result) << result->DebugString();
  result.reset(new OperationResultPB());
  result->add_mutated_stores()->set_mrs_id(mrs_id);
}

void RowOp::SetMutateSucceeded(gscoped_ptr<OperationResultPB> result) {
  DCHECK(!this->result) << result->DebugString();
  this->result = result.Pass();
}

string RowOp::ToString(const Schema& schema) const {
  return decoded_op.ToString(schema);
}

void RowOp::SetAlreadyFlushed() {
  DCHECK(!result) << result->DebugString();
  result.reset(new OperationResultPB());
  result->set_flushed(true);
}

} // namespace tablet
} // namespace kudu
