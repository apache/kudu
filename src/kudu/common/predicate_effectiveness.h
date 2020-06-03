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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/column_predicate.h"

namespace kudu {
class RowwiseIterator;
struct IteratorStats;

// Whether the column predicate can be checked for effectiveness and automatically disabled.
// Currently only Bloom filter predicate qualifies.
bool IsColumnPredicateDisableable(PredicateType type);

// Per disableable column predicate effectiveness context.
struct PredicateEffectivenessContext {
  const ColumnPredicate* pred;
  size_t rows_read;
  size_t rows_rejected;
  bool enabled;
  explicit PredicateEffectivenessContext(const ColumnPredicate* pred)
      : pred(pred),
        rows_read(0),
        rows_rejected(0),
        enabled(true) {
  }

  // Is the column predicate tracked by this context enabled.
  bool IsPredicateEnabled() const;
};

// Per iterator effectiveness context that wraps effectiveness contexts for multiple column
// predicates.
class IteratorPredicateEffectivenessContext {
 public:
  IteratorPredicateEffectivenessContext() : next_block_count_(0) {}

  // Add a disableable column predicate to track, where 'idx' is the user supplied key.
  void AddDisableablePredicate(int idx, const ColumnPredicate* pred) {
    DCHECK(IsColumnPredicateDisableable(pred->predicate_type()));
    predicate_ctxs_.emplace(idx, PredicateEffectivenessContext(pred));
  }

  void IncrementNextBlockCount() {
    next_block_count_++;
  }

  // Get the effectiveness context associated with column predicate index 'idx'.
  PredicateEffectivenessContext& operator[](int idx) {
    auto it = predicate_ctxs_.find(idx);
    CHECK(it != predicate_ctxs_.end());
    return it->second;
  }

  const PredicateEffectivenessContext& operator[](int idx) const {
    auto it = predicate_ctxs_.find(idx);
    CHECK(it != predicate_ctxs_.end());
    return it->second;
  }

  int num_predicate_ctxs() const {
    return predicate_ctxs_.size();
  }

  // Checks effectiveness of the predicates using stats collected so far and disables
  // ineffective disableable predicates.
  // Heuristic: Check every FLAGS_predicate_effectivess_num_skip_blocks blocks,
  // ratio of rows rejected by a predicate. If the rejection ratio is less than
  // FLAGS_predicate_effectivess_reject_ratio then the predicate is disabled.
  void DisableIneffectivePredicates();

  // Populate the output 'stats' vector with disabled column predicates
  // where indices of the vector correspond to the table's column indices.
  //
  // Input 'col_idx_predicates' helps map predicate index to the table's column index.
  void PopulateIteratorStatsWithDisabledPredicates(
      const std::vector<std::pair<int32_t, ColumnPredicate>>& col_idx_predicates,
      std::vector<IteratorStats>* stats) const;

 private:
  int next_block_count_;
  // Per column predicate effectiveness context where key is the index as specified
  // in AddDisableablePredicate().
  std::unordered_map<int, PredicateEffectivenessContext> predicate_ctxs_;
};

// Gets the predicate effectiveness context associated with the iterator.
//
// Only for use by tests.
const IteratorPredicateEffectivenessContext& GetIteratorPredicateEffectivenessCtxForTests(
    const std::unique_ptr<RowwiseIterator>& iter);

} // namespace kudu
