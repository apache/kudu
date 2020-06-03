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

#include "kudu/common/predicate_effectiveness.h"

#include <ostream>

#include <gflags/gflags.h>

#include "kudu/common/iterator_stats.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/flag_tags.h"

using std::pair;
using std::vector;

DEFINE_bool(predicate_effectivess_enabled, true,
            "Should ineffective predicate filtering be disabled");
TAG_FLAG(predicate_effectivess_enabled, advanced);
TAG_FLAG(predicate_effectivess_enabled, runtime);

DEFINE_double(predicate_effectivess_reject_ratio, 0.10,
              "Rejection ratio below which predicate filter is considered ineffective");
TAG_FLAG(predicate_effectivess_reject_ratio, advanced);
TAG_FLAG(predicate_effectivess_reject_ratio, runtime);

DEFINE_int32(predicate_effectivess_num_skip_blocks, 128,
             "Number of blocks to skip for every predicate effectiveness check");
TAG_FLAG(predicate_effectivess_num_skip_blocks, advanced);
TAG_FLAG(predicate_effectivess_num_skip_blocks, runtime);

namespace kudu {

bool IsColumnPredicateDisableable(PredicateType type) {
  return FLAGS_predicate_effectivess_enabled &&
         type == PredicateType::InBloomFilter;
}

bool PredicateEffectivenessContext::IsPredicateEnabled() const {
  return !FLAGS_predicate_effectivess_enabled || enabled;
}

void IteratorPredicateEffectivenessContext::DisableIneffectivePredicates() {
  if (!FLAGS_predicate_effectivess_enabled ||
      (next_block_count_ % FLAGS_predicate_effectivess_num_skip_blocks) != 0) {
    return;
  }

  for (auto& idx_and_pred_ctx : predicate_ctxs_) {
    auto& pred_ctx = idx_and_pred_ctx.second;
    DCHECK(IsColumnPredicateDisableable(pred_ctx.pred->predicate_type()));
    if (!pred_ctx.enabled) {
      // Predicate already disabled.
      continue;
    }
    DCHECK(pred_ctx.rows_read != 0);
    auto rejection_ratio = static_cast<double>(pred_ctx.rows_rejected) / pred_ctx.rows_read;
    DCHECK_LE(rejection_ratio, 1.0);
    if (rejection_ratio < FLAGS_predicate_effectivess_reject_ratio) {
      pred_ctx.enabled = false;
      VLOG(1) << "Disabling column predicate " << pred_ctx.pred->ToString();
    }
  }
}

void IteratorPredicateEffectivenessContext::PopulateIteratorStatsWithDisabledPredicates(
    const vector<pair<int32_t, ColumnPredicate>>& col_idx_predicates,
    vector<IteratorStats>* stats) const {

  for (const auto& pred_idx_and_eff_ctx : predicate_ctxs_) {
    const auto& pred_idx = pred_idx_and_eff_ctx.first;
    const auto& eff_ctx = pred_idx_and_eff_ctx.second;
    const auto& col_idx = col_idx_predicates[pred_idx].first;
    DCHECK(IsColumnPredicateDisableable(eff_ctx.pred->predicate_type()))
      << "Incorrectly tracked non-disableable predicate: " << eff_ctx.pred->ToString();
    DCHECK_LT(col_idx, stats->size());
    (*stats)[col_idx].predicates_disabled = !eff_ctx.enabled;
  }
}

} // namespace kudu
