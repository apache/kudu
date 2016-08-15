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

#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"

namespace kudu {

// A ColumnMaterializationContext provides a clean interface to the set of
// objects that get passed down to the column iterator and decoders during
// materialization.
//
// Example of path taken:
// MaterializingIterator -> CFileSet::Iterator -> CFileIterator -> BlockDecoder
class ColumnMaterializationContext {
 public:
  ColumnMaterializationContext(size_t col_idx,
                    const ColumnPredicate* pred,
                    ColumnBlock* block,
                    SelectionVector* sel)
    : col_idx_(col_idx),
      pred_(pred),
      block_(block),
      sel_(sel),
      decoder_eval_status_(kNotSet) {
      if (!pred_ || !sel || !block) {
        decoder_eval_status_ = kDecoderEvalNotSupported;
      }
  }

  // Column index in within the projection schema, not the underlying schema.
  const size_t col_idx() { return col_idx_; }

  // Predicate being evaluated.
  const ColumnPredicate* pred() { return pred_; }

  // Destination for copied data.
  ColumnBlock* block() { return block_; }

  // Selection vector reflecting the result of the predicate evaluation.
  // If a given bit is already cleared, that row can be skipped.
  //
  // Required non-null if used by scans, else there will be a nullptr reference
  // when creating a SelectionVectorView around it. Must be large enough to
  // cover the number of rows being scanned.
  SelectionVector* sel() { return sel_; }

  // Checked after returning from the decoder to determine whether or not the
  // block must still be evaluated (on true).
  bool DecoderEvalNotSupported() const {
    return decoder_eval_status_ == kDecoderEvalNotSupported;
  }

  // Checked by CFileIterator::Scan() to determine whether decoder-level
  // evaluation should be attempted (on true).
  bool DecoderEvalNotDisabled() const {
    return decoder_eval_status_ != kDecoderEvalNotSupported;
  }

  // A context should not switch from supporting decoder-level eval to not
  // supporting it, or vice versa.
  //
  // Going from NotSupported to Supported may lead to incorrect results if
  // previously scanned rows materialize values that should not be in the
  // results set. To prevent this, attempts to do this will result in no-ops.
  // This will disable decoder-level evaluation and yield correct values.
  //
  // Should only be called by CopyNextAndEval() of a decoder that supports
  // evaluation.
  void SetDecoderEvalSupported() {
    DCHECK(decoder_eval_status_ != kDecoderEvalNotSupported);
    if (decoder_eval_status_ == kNotSet) {
      DCHECK(sel_ != nullptr && pred_ != nullptr);
      decoder_eval_status_ = kDecoderEvalSupported;
    }
  }

  // Going from Supported to NotSupported may lead to errors if the scan
  // previously evaluated rows that were deemed to be not in the result set.
  // During batch evaluation, these rows would be missing from the buffer,
  // and this could lead to a memory error.
  //
  // Should be called before materializing a column if support has manually
  // been turned off with materializing_iterator_decoder_eval flag, or by
  // CopyNextAndEval() of a decoder that does not support evaluation.
  void SetDecoderEvalNotSupported() {
    CHECK(decoder_eval_status_ != kDecoderEvalSupported);
    decoder_eval_status_ = kDecoderEvalNotSupported;
  }

 private:
  enum DecoderEvalStatus {
    // During scan, will try to evaluate with the decoder, after which the
    // correct status will be set.
    kNotSet = 0,

    // May be set before scanning if the decoder eval flag is set to false or
    // if iterator has deltas associated with it.
    // May be set by decoder during scan if decoder eval is not supported.
    // Once set, scanning will materialize the entire column into the block,
    // leaving evaluation for after the scan.
    kDecoderEvalNotSupported,

    // Is set by a decoder during scan if decoder eval is supported.
    // Once set, the decoder is contractually bound to return with selection
    // vector filled out.
    kDecoderEvalSupported
  };

  const size_t col_idx_;

  const ColumnPredicate* pred_;

  ColumnBlock* block_;

  SelectionVector* const sel_;

  DecoderEvalStatus decoder_eval_status_;
};

} // namespace kudu
