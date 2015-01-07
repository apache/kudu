// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TABLET_ROW_OP_H
#define KUDU_TABLET_ROW_OP_H

#include <string>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/common/row_operations.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/lock_manager.h"

namespace kudu {

class Schema;

namespace tablet {

// Structure tracking the progress of a single row operation within a WriteTransaction.
struct RowOp {
 public:
  explicit RowOp(const DecodedRowOperation& decoded_op);
  ~RowOp();

  // Functions to set the result of the mutation.
  // Only one of the following three functions must be called,
  // at most once.
  void SetFailed(const Status& s);
  void SetInsertSucceeded(int mrs_id);
  void SetMutateSucceeded(gscoped_ptr<OperationResultPB> result);
  void SetAlreadyFlushed();

  bool has_row_lock() const {
    return row_lock.acquired();
  }

  std::string ToString(const Schema& schema) const;

  // The original operation as decoded from the client request.
  DecodedRowOperation decoded_op;

  // The key probe structure contains the row key in both key-encoded and
  // ContiguousRow formats, bloom probe structure, etc. This is set during
  // the "prepare" phase.
  gscoped_ptr<RowSetKeyProbe> key_probe;

  // The row lock which has been acquired for this row. Set during the "prepare"
  // phase.
  ScopedRowLock row_lock;

  // The result of the operation, after Apply.
  gscoped_ptr<OperationResultPB> result;
};


} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_ROW_OP_H */

