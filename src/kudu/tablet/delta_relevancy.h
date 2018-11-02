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

#include "kudu/common/timestamp.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/mvcc.h"

namespace kudu {
namespace tablet {

// Functions that evaluate the relevancy of deltas against a snapshot or pair
// of snapshots.
//
// When constructing an iterator tree, one must specify either one or two MVCC
// snapshots. The first always serves as an upper bound, ensuring that the data
// may be further mutated by writers without affecting the results of the
// iteration. If present, the second serves as a lower bound, excluding any
// deltas that were committed before it in time.
//
// Together, the snapshots affect the behavior of deltas encountered during the
// iteration. Various criteria is used to determine whether a particular delta
// is relevant to the iteration; which criteria is used depends on the context.
//
// Selection criteria
// ==================
// The selection (or "select") criteria determines whether the particular delta
// should cause its row to be included in the iteration. It's only applicable to
// two snapshots. In detail, regardless of delta type, if the delta's timestamp
// is "between" the two snapshots (i.e. not committed in the lower bound
// snapshot but committed in the upper bound snapshot), its row should be included.
//
// Application criteria
// ====================
// The application (or "apply") criteria determines whether the logical contents
// of the UPDATE, DELETE, or REINSERT in a particular delta should be applied to
// a row block during a scan. It only considers the first snapshot. In detail:
// - For REDO deltas: if the delta's timestamp is committed in the snapshot,
//   the mutation should be applied.
// - For UNDO deltas: if the delta's timestamp is not committed in the snapshot,
//   the mutation should be applied.

// Returns whether a delta at 'delta_ts' is relevant under the apply criteria to 'snap'.
template<DeltaType Type>
inline bool IsDeltaRelevantForApply(const MvccSnapshot& snap,
                                    const Timestamp& delta_ts) {
  bool ignored;
  return IsDeltaRelevantForApply<Type>(snap, delta_ts, &ignored);
}

// A variant of IsDeltaRelevantForApply that, if the delta is not relevant,
// further checks whether any remaining deltas for this row can be skipped; this
// is an optimization and not necessary for correctness.
template<DeltaType Type>
inline bool IsDeltaRelevantForApply(const MvccSnapshot& snap,
                                    const Timestamp& delta_ts,
                                    bool* finished_row);

template<>
inline bool IsDeltaRelevantForApply<REDO>(const MvccSnapshot& snap,
                                          const Timestamp& delta_ts,
                                          bool* finished_row) {
  *finished_row = false;
  if (snap.IsCommitted(delta_ts)) {
    return true;
  }
  if (!snap.MayHaveCommittedTransactionsAtOrAfter(delta_ts)) {
    // REDO deltas are sorted first in ascending row ordinal order, then in
    // ascending timestamp order. Thus, if we know that there are no more
    // committed transactions whose timestamps are >= 'delta_ts', we know that
    // any future deltas belonging to this row aren't relevant (as per the apply
    // criteria, REDOs are relevant if they are committed in the snapshot), and
    // we can skip to the next row.
    *finished_row = true;
  }
  return false;
}

template<>
inline bool IsDeltaRelevantForApply<UNDO>(const MvccSnapshot& snap,
                                          const Timestamp& delta_ts,
                                          bool* finished_row) {
  *finished_row = false;
  if (!snap.IsCommitted(delta_ts)) {
    return true;
  }
  if (!snap.MayHaveUncommittedTransactionsAtOrBefore(delta_ts)) {
    // UNDO deltas are sorted first in ascending row ordinal order, then in
    // descending timestamp order. Thus, if we know that there are no more
    // uncommitted transactions whose timestamps are <= 'delta_ts', we know that
    // any future deltas belonging to this row aren't relevant (as per the apply
    // criteria, UNDOs are relevant if they are uncommitted in the snapshot),
    // and we can skip to the next row.
    *finished_row = true;
  }
  return false;
}

// Returns whether deltas within the time range 'delta_ts_start' to
// 'delta_ts_end' are relevant under the select criteria to 'snap_start' and 'snap_end'.
inline bool IsDeltaRelevantForSelect(const MvccSnapshot& snap_start,
                                     const MvccSnapshot& snap_end,
                                     const Timestamp& delta_ts_start,
                                     const Timestamp& delta_ts_end) {
  return !snap_start.IsCommitted(delta_ts_end) &&
      snap_end.IsCommitted(delta_ts_start);
}

// A variant of IsDeltaRelevantForSelect that operates on a single delta's
// timestamp given by 'delta_ts', and if the delta is not relevant, further
// checks whether any remaining deltas for this row can be skipped; this is an
// optimization and not necessary for correctness.
template<DeltaType Type>
inline bool IsDeltaRelevantForSelect(const MvccSnapshot& snap_start,
                                     const MvccSnapshot& snap_end,
                                     const Timestamp& delta_ts,
                                     bool* finished_row);

template<>
inline bool IsDeltaRelevantForSelect<REDO>(const MvccSnapshot& snap_start,
                                           const MvccSnapshot& snap_end,
                                           const Timestamp& delta_ts,
                                           bool* finished_row) {
  *finished_row = false;
  if (snap_start.IsCommitted(delta_ts)) {
    // No short-circuit available here; because REDO deltas for a given row are
    // sorted in ascending timestamp order, the next REDO may be uncommitted in
    // 'snap_start'.
    return false;
  }
  if (!snap_end.IsCommitted(delta_ts)) {
    if (!snap_end.MayHaveCommittedTransactionsAtOrAfter(delta_ts)) {
      // But if 'delta_ts' is not committed in 'snap_end', all future REDOs may
      // also be uncommitted in 'snap_end'.
      *finished_row = true;
    }
    return false;
  }
  return true;
}

template<>
inline bool IsDeltaRelevantForSelect<UNDO>(const MvccSnapshot& snap_start,
                                           const MvccSnapshot& snap_end,
                                           const Timestamp& delta_ts,
                                           bool* finished_row) {
  *finished_row = false;
  if (!snap_end.IsCommitted(delta_ts)) {
    // No short-circuit available here; because UNDO deltas for a given row are
    // sorted in descending timestamp order, the next UNDO may be committed in
    // 'snap_end'.
    return false;
  }
  if (snap_start.IsCommitted(delta_ts)) {
    if (!snap_start.MayHaveUncommittedTransactionsAtOrBefore(delta_ts)) {
      // But if 'delta_ts' is committed in 'snap_start', all future UNDOs may
      // also be committed in 'snap_start'.
      *finished_row = true;
    }
    return false;
  }
  return true;
}

} // namespace tablet
} // namespace kudu
