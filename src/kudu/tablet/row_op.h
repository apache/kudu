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

#include <string>

#include "kudu/common/row_operations.h"

namespace google {
namespace protobuf {
class Arena;
}
}

namespace kudu {

class Schema;
class Status;

namespace tablet {
class OperationResultPB;
class RowSet;
class RowSetKeyProbe;

// Structure tracking the progress of a single row operation within a WriteTransaction.
struct RowOp {
 public:
  RowOp(google::protobuf::Arena* pb_arena, DecodedRowOperation op);
  ~RowOp() = default;

  // Functions to set the result of the mutation.
  // Only one of the following four functions must be called, at most once.
  void SetFailed(const Status& s);
  void SetInsertSucceeded(int mrs_id);
  void SetErrorIgnored();

  // REQUIRES: result must be allocated from the same protobuf::Arena associated
  // with this RowOp.
  void SetMutateSucceeded(OperationResultPB* result);

  // Sets the result of a skipped operation on bootstrap.
  // TODO(dralves) Currently this performs a copy. Might be avoided with some refactoring.
  // see TODO(dralves) in TabletBoostrap::ApplyOperations().
  void SetSkippedResult(const OperationResultPB& result);

  // In the case that this operation is being replayed from the WAL
  // during tablet bootstrap, we may need to look at the original result
  // stored in the COMMIT message to know the correct RowSet to apply it to.
  //
  // This pointer must stay live as long as this RowOp.
  void set_original_result_from_log(const OperationResultPB* orig_result) {
    orig_result_from_log = orig_result;
  }

  bool has_result() const {
    return result != nullptr;
  }

  std::string ToString(const Schema& schema) const;

  google::protobuf::Arena* const pb_arena_;

  // The original operation as decoded from the client request.
  DecodedRowOperation decoded_op;

  // If this operation is being replayed from the log, set to the original
  // result. Otherwise nullptr.
  const OperationResultPB* orig_result_from_log = nullptr;

  // The key probe structure contains the row key in both key-encoded and
  // ContiguousRow formats, bloom probe structure, etc. This is set during
  // the "prepare" phase.
  //
  // Allocated on the op state's Arena.
  RowSetKeyProbe* key_probe = nullptr;

  // Flag whether this op has already been validated as valid.
  bool valid = false;

  // Flag whether this op has already had 'present_in_rowset' filled in.
  // If false, 'present_in_rowset' must be nullptr. If true, and
  // 'present_in_rowset' is nullptr, then this indicates that the key
  // for this op does not exist in any RowSet.
  bool checked_present = false;

  // True if an ignore op was ignored due to an error.
  bool error_ignored = false;

  // The RowSet in which this op's key has been found present and alive.
  // This will be null if 'checked_present' is false, or if it has been
  // checked and found not to be alive in any RowSet.
  RowSet* present_in_rowset = nullptr;

  // The result of the operation, allocated from pb_arena_
  OperationResultPB* result = nullptr;
};


} // namespace tablet
} // namespace kudu
