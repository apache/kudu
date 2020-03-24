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

#include "kudu/tablet/compaction_policy.h"

#include <algorithm>
#include <limits>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/tablet/svg_dump.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/knapsack_solver.h"
#include "kudu/util/status.h"

using std::vector;
using strings::Substitute;

DEFINE_int64(budgeted_compaction_target_rowset_size, 32 * 1024 * 1024,
             "The target size in bytes for DiskRowSets produced by flushes or "
             "compactions when the budgeted compaction policy is used.");
TAG_FLAG(budgeted_compaction_target_rowset_size, advanced);
TAG_FLAG(budgeted_compaction_target_rowset_size, experimental);

DEFINE_double(compaction_approximation_ratio, 1.05f,
              "Approximation ratio allowed for optimal compaction calculation. A "
              "value of 1.05 indicates that the policy may use an approximate result "
              "if it is known to be within 5% of the optimal solution.");
TAG_FLAG(compaction_approximation_ratio, advanced);
TAG_FLAG(compaction_approximation_ratio, experimental);

DEFINE_double(compaction_minimum_improvement, 0.01f,
              "The minimum quality for a compaction to run. If a compaction does not "
              "improve the average height of DiskRowSets by at least this amount, the "
              "compaction will be considered ineligible.");
TAG_FLAG(compaction_minimum_improvement, advanced);

namespace kudu {
namespace tablet {

// A constant factor used to penalize wider solutions when there is rowset
// overlap.
// Consider this input:
//
//  |-----A----||----C----|
//  |-----B----|
//
// where A, B, and C are all 30MiB, the budget is 100MiB, and the target rowset
// size is 32MiB. Without this tweak, the decrease in tablet height from the
// compaction selection {A, B, C} is the exact same as from {A, B}, since both
// compactions would yield a tablet with average height 1. Taking the rowset
// sizes into account, {A, B, C} is favored over {A, B}. However, including
// C in the compaction is not greatly beneficial: there's 60MiB of extra I/O
// in exchange for no benefit in average height over {A, B} and no decrease in
// the number of rowsets, just a reorganization of the data into 2 rowsets of
// size 32MiB and one of size 26MiB. On the other hand, if A, B, and C were all
// 8MiB rowsets, then {A, B, C} is clearly a superior selection to {A, B}.
// Furthermore, the arrangement
//
// |- A -| |- B -| |- C -|
//
// shouldn't be penalized based on total width at all. So this penalty is
// applied only when there is overlap, and it is small so that a significant
// benefit in reducing rowset count can overwhelm it.
static const double kSupportAdjust = 1.003;

////////////////////////////////////////////////////////////
// BudgetedCompactionPolicy
////////////////////////////////////////////////////////////

BudgetedCompactionPolicy::BudgetedCompactionPolicy(int budget)
  : size_budget_mb_(budget) {
  CHECK_GT(budget, 0);
}

uint64_t BudgetedCompactionPolicy::target_rowset_size() const {
  CHECK_GT(FLAGS_budgeted_compaction_target_rowset_size, 0);
  return FLAGS_budgeted_compaction_target_rowset_size;
}

void BudgetedCompactionPolicy::SetupKnapsackInput(
    const RowSetTree& tree,
    vector<RowSetInfo>* asc_min_key,
    vector<RowSetInfo>* asc_max_key) const {
  RowSetInfo::ComputeCdfAndCollectOrdered(tree,
                                          /*rowset_total_height=*/nullptr,
                                          /*rowset_total_width=*/nullptr,
                                          asc_min_key,
                                          asc_max_key);

  if (asc_min_key->size() < 2) {
    // There must be at least two rowsets to compact.
    asc_min_key->clear();
    asc_max_key->clear();
  }
}

namespace {

struct KnapsackTraits {
  typedef const RowSetInfo* item_type;
  typedef double value_type;
  static int get_weight(item_type item) {
    return item->base_and_redos_size_mb();
  }
  static value_type get_value(item_type item) {
    return item->value();
  }
};


// Incremental calculator for lower and upper bounds on a knapsack solution,
// given a set of items. The bounds are computed by solving the
// simpler "fractional knapsack problem" -- i.e the related problem
// in which each input may be fractionally put in the knapsack, instead
// of all-or-nothing. The fractional knapsack problem has a very efficient
// solution: sort by descending density and greedily choose elements
// until the budget is reached. The last element to be chosen may be
// partially included in the knapsack.
//
// This provides an upper bound (with the last element fractionally included)
// and a lower bound (the same solution but without the fractional last element).
//
// Because this greedy solution only depends on sorting, it can be computed
// incrementally as items are considered by maintaining a min-heap, ordered
// by the density of the input elements. We need only maintain enough elements
// to satisfy the budget, making this logarithmic in the budget and linear
// in the number of elements added.
class BoundCalculator {
 public:
  explicit BoundCalculator(int max_weight)
    : current_weight_(0.0),
      current_value_(0.0),
      max_weight_(max_weight),
      top_density_(std::numeric_limits<double>::min()) {
  }

  void Add(const RowSetInfo& candidate) {
    // If we're already over the budget and 'candidate' is not denser than the
    // least dense item in the knapsack, it won't be part of the upper bound
    // fractional solution.
    if (current_weight_ >= max_weight_ && candidate.density() <= top_density_) {
      return;
    }

    const auto compareByDescendingDensity =
        [](const RowSetInfo* a, const RowSetInfo* b) {
          return a->density() > b->density();
        };

    // Push the candidate, then pop items from the heap until removing the
    // element at the top of the heap reduces the current weight to the max
    // weight or less. In other words, remove items until the heap is one item
    // too heavy.
    fractional_solution_.push_back(&candidate);
    std::push_heap(fractional_solution_.begin(),
                   fractional_solution_.end(),
                   compareByDescendingDensity);
    current_weight_ += candidate.base_and_redos_size_mb();
    current_value_ += candidate.value();
    const RowSetInfo* top = fractional_solution_.front();
    while (current_weight_ - top->base_and_redos_size_mb() > max_weight_) {
      current_weight_ -= top->base_and_redos_size_mb();
      current_value_ -= top->value();
      std::pop_heap(fractional_solution_.begin(),
                    fractional_solution_.end(),
                    compareByDescendingDensity);
      fractional_solution_.pop_back();
      top = fractional_solution_.front();
    }
    top_density_ = top->density();
  }

  // Compute the lower and upper bounds to the 0-1 knapsack problem for the
  // current state.
  std::pair<double, double> ComputeLowerAndUpperBound() const {
    int excess_weight = current_weight_ - max_weight_;
    if (excess_weight <= 0) {
      // If we've added less than the budget, our "bounds" are just including
      // all of the items.
      return { current_value_, current_value_ };
    }

    const RowSetInfo& top = *fractional_solution_.front();

    // The 0-1 knapsack problem's solution is lower bounded by both
    // - the value of the highest density N items that fit, and
    // - the value of the (N+1)th item alone, if it fits
    // This is a 2-approximation (i.e. no worse than 1/2 of the best solution).
    // See https://courses.engr.illinois.edu/cs598csc/sp2009/lectures/lecture_4.pdf
    double lower_bound = std::max(current_value_ - top.value(), top.value());

    // An upper bound for the integer problem is the solution to the fractional
    // problem since in the fractional problem we can add the fraction of the
    // densest element that uses up the excess weight:
    //
    //   fraction_to_remove = excess_weight / top.size_mb();
    //   portion_to_remove = fraction_to_remove * top.width()
    //
    // To avoid the division, we can just use the fact that density = width/size:
    double portion_of_top_to_remove = static_cast<double>(excess_weight) * top.density();
    DCHECK_GT(portion_of_top_to_remove, 0);
    double upper_bound = current_value_ - portion_of_top_to_remove;

    return {lower_bound, upper_bound};
  }

  // Return the items which make up the current lower-bound solution.
  void GetLowerBoundSolution(vector<const RowSetInfo*>* solution) {
    DCHECK(solution);
    solution->clear();
    int excess_weight = current_weight_ - max_weight_;
    if (excess_weight <= 0) {
      *solution = fractional_solution_;
      return;
    }

    // See above: there are two choices for the lower-bound estimate,
    // and we need to return the one matching the bound we computed.
    const RowSetInfo* top = fractional_solution_.front();
    if (current_value_ - top->value() > top->value()) {
      // The current solution less the top (minimum density) element.
      solution->assign(fractional_solution_.begin() + 1,
                       fractional_solution_.end());
    } else {
      solution->push_back(top);
    }
  }

  void clear() {
    fractional_solution_.clear();
    current_weight_ = 0;
    current_value_ = 0;
  }

 private:
  // Uses pointers to rather than copies to allow for fast swapping in the heap.
  vector<const RowSetInfo*> fractional_solution_;
  int current_weight_;
  double current_value_;
  const int max_weight_;
  double top_density_;
};

// If 'sum_width' is no bigger than 'union_width', there is little or no overlap
// between the rowsets relative to their total width. In this case, we don't
// want to penalize the solution value for being wide, so that small rowset
// compaction will work. If there is significant overlap, then we do want
// to penalize wider solutions. See the comment about 'kSupportAdjust' above.
double MaybePenalizeWideSolution(double sum_width, double union_width) {
  return sum_width <= union_width ? sum_width - union_width :
                                    sum_width - kSupportAdjust * union_width;
}

} // anonymous namespace

void BudgetedCompactionPolicy::RunApproximation(
    const vector<RowSetInfo>& asc_min_key,
    const vector<RowSetInfo>& asc_max_key,
    vector<double>* best_upper_bounds,
    SolutionAndValue* best_solution) const {
  DCHECK(best_upper_bounds);
  DCHECK(best_solution);

  best_upper_bounds->clear();
  best_upper_bounds->reserve(asc_min_key.size());
  BoundCalculator bound_calc(size_budget_mb_);

  // See docs/design-docs/compaction-policy.md for an explanation of the logic.
  for (const RowSetInfo& rowset_a : asc_min_key) {
    bound_calc.clear();
    double union_min = rowset_a.cdf_min_key();
    double union_max = rowset_a.cdf_max_key();
    double best_upper = 0.0;
    for (const RowSetInfo& rowset_b : asc_max_key) {
      if (rowset_b.cdf_min_key() < union_min) {
        continue;
      }
      union_max = std::max(union_max, rowset_b.cdf_max_key());
      double union_width = union_max - union_min;

      bound_calc.Add(rowset_b);
      auto bounds = bound_calc.ComputeLowerAndUpperBound();
      double lower = MaybePenalizeWideSolution(bounds.first, union_width);
      double upper = MaybePenalizeWideSolution(bounds.second, union_width);
      best_upper = std::max(upper, best_upper);
      if (lower > best_solution->value) {
        vector<const RowSetInfo*> approx_solution;
        bound_calc.GetLowerBoundSolution(&approx_solution);
        best_solution->rowsets.clear();
        for (const auto* rsi : approx_solution) {
          best_solution->rowsets.insert(rsi->rowset());
        }
        best_solution->value = lower;
      }
    }
    best_upper_bounds->push_back(best_upper);
  }
}

void BudgetedCompactionPolicy::RunExact(
    const vector<RowSetInfo>& asc_min_key,
    const vector<RowSetInfo>& asc_max_key,
    const vector<double>& best_upper_bounds,
    SolutionAndValue* best_solution) const {
  DCHECK_EQ(asc_min_key.size(), best_upper_bounds.size());
  DCHECK(best_solution);

  KnapsackSolver<KnapsackTraits> solver;
  vector<const RowSetInfo*> candidates;
  candidates.reserve(asc_min_key.size());
  for (int i = 0; i < asc_min_key.size(); i++) {
    const auto& rowset_a = asc_min_key[i];
    const auto upper_bound = best_upper_bounds[i];

    // 'upper_bound' is an upper bound on the solution value of any compaction
    // that includes 'rowset_a' as its left-most rowset. If that bound is worse
    // than the current best solution, then we can skip finding the precise
    // value of this solution.
    //
    // Here we also build in the approximation ratio as slop: the upper bound
    // doesn't need to just be better than the current solution-- it needs to be
    // better by at least the approximation ratio before we bother looking for
    // the corresponding exact solution.
    if (upper_bound <= best_solution->value * FLAGS_compaction_approximation_ratio) {
      continue;
    }

    candidates.clear();
    double union_min = rowset_a.cdf_min_key();
    double union_max = rowset_a.cdf_max_key();
    for (const RowSetInfo& rowset_b : asc_max_key) {
      if (rowset_b.cdf_min_key() < union_min) {
        continue;
      }
      candidates.push_back(&rowset_b);
    }
    if (candidates.empty()) continue;

    solver.Reset(size_budget_mb_, &candidates);

    vector<int> chosen_indexes;
    int j = 0;
    while (solver.ProcessNext()) {
      const RowSetInfo* item = candidates[j++];
      std::pair<int, double> best_with_this_item = solver.GetSolution();
      double best_value = best_with_this_item.second;

      union_max = std::max(item->cdf_max_key(), union_max);
      DCHECK_GE(union_max, union_min);
      double solution = MaybePenalizeWideSolution(best_value,
                                                  union_max - union_min);
      if (solution > best_solution->value) {
        solver.TracePath(best_with_this_item, &chosen_indexes);
        best_solution->value = solution;
      }
    }

    // If we came up with a new solution, replace.
    if (!chosen_indexes.empty()) {
      best_solution->rowsets.clear();
      for (int idx : chosen_indexes) {
        best_solution->rowsets.insert(candidates[idx]->rowset());
      }
    }
  }
}

// See docs/design-docs/compaction-policy.md for an overview of the compaction
// policy implemented in this function.
Status BudgetedCompactionPolicy::PickRowSets(
    const RowSetTree& tree,
    CompactionSelection* picked,
    double* quality,
    std::vector<std::string>* log) {
  DCHECK(picked);
  DCHECK(quality);

  vector<RowSetInfo> asc_min_key, asc_max_key;
  SetupKnapsackInput(tree, &asc_min_key, &asc_max_key);
  if (asc_max_key.empty()) {
    if (log) {
      LOG_STRING(INFO, log) << "No rowsets to compact";
    }
    *quality = 0.0;
    return Status::OK();
  }

  // The best set of rowsets chosen so far, and the value attained by that choice.
  SolutionAndValue best_solution;

  // The algorithm proceeds in two passes. The first is based on an
  // approximation of the knapsack problem, and computes upper and lower bounds
  // for the quality score of compaction selections with a specific rowset as
  // the left-most rowset. The second pass looks again over the input for any
  // cases where the upper bound tells us there is a significant improvement
  // potentially available, and only in those cases runs the actual knapsack
  // solver.

  // Both passes are structured as a loop over all pairs {rowset_a, rowset_b}
  // such that rowset_a.min_key() < rowset_b.min_key(). Given any such pair, we
  // know that a compaction including both of these pairs would result in an
  // output rowset that spans the range
  // [rowset_a.min_key(), max(rowset_a.max_key(), rowset_b.max_key())]. This
  // width is designated 'union_width' below.

  // In particular, the order in which we consider 'rowset_b' is such that, for
  // the inner loop (a fixed 'rowset_a'), the 'union_width' increases minimally
  // to only include the new 'rowset_b' and not also cover any other rowsets.
  //
  // For example:
  //
  //  |-----A----|
  //      |-----B----|
  //         |----C----|
  //       |--------D-------|
  //
  // We process 'rowset_b' in the order: A, B, C, D.
  //
  // With this iteration order, each knapsack problem builds on the previous
  // knapsack problem by adding just a single rowset, meaning that we can reuse
  // the existing solution state to incrementally update the solution, rather
  // than having to rebuild from scratch.
  //
  // Pass 1 (approximate)
  // ------------------------------------------------------------
  // First, run the whole algorithm using a fast approximation of the knapsack
  // solver to come up with lower bounds and upper bounds. The approximation
  // algorithm is a 2-approximation but in practice often yields results that
  // are very close to optimal for the sort of problems we encounter in our use
  // case.
  //
  // This pass calculates:
  // 1) 'best_upper_bounds': for each 'rowset_a', what's the upper bound for any
  //    solution including this rowset. We use this later to avoid solving the
  //    knapsack for cases where the upper bound is lower than our current best
  //    solution.
  // 2) 'best_solution' and 'best_solution->value': the best approximate
  //    solution found.
  vector<double> best_upper_bounds;
  RunApproximation(asc_min_key, asc_max_key, &best_upper_bounds, &best_solution);

  // If the best solution found above is less than some tiny threshold, we don't
  // bother searching for the exact solution, since it can have value at most
  // twice the approximate solution. Likewise, if all the upper bounds are below
  // the threshold, we short-circuit.
  if (best_solution.value * 2 <= FLAGS_compaction_minimum_improvement ||
      *std::max_element(best_upper_bounds.begin(), best_upper_bounds.end()) <=
      FLAGS_compaction_minimum_improvement) {
    VLOG(1) << "Approximation algorithm short-circuited exact compaction calculation";
    *quality = 0.0;
    return Status::OK();
  }

  // Pass 2 (precise)
  // ------------------------------------------------------------
  // Now that we've found an approximate solution and upper bounds, do another
  // pass. In cases where the upper bound indicates we could do substantially
  // better than our current best solution, we use the exact knapsack solver to
  // find the improved solution.
  RunExact(asc_min_key, asc_max_key, best_upper_bounds, &best_solution);

  // Log the input and output of the selection.
  if (VLOG_IS_ON(1) || log != nullptr) {
    LOG_STRING(INFO, log) << "Budgeted compaction selection:";
    for (RowSetInfo &cand : asc_min_key) {
      const char *checkbox = "[ ]";
      if (ContainsKey(best_solution.rowsets, cand.rowset())) {
        checkbox = "[x]";
      }
      LOG_STRING(INFO, log) << "  " << checkbox << " " << cand.ToString();
    }
    LOG_STRING(INFO, log) << "Solution value: " << best_solution.value;
  }

  *quality = best_solution.value;

  if (best_solution.value <= FLAGS_compaction_minimum_improvement) {
    VLOG(1) << Substitute("The best compaction score found ($0) is less than "
                          "the threshold for compaction ($1): not compacting.",
                          best_solution.value,
                          FLAGS_compaction_minimum_improvement);
    *quality = 0.0;
    return Status::OK();
  }

  picked->swap(best_solution.rowsets);
  DumpCompactionSVGToFile(asc_min_key, *picked);

  return Status::OK();
}

} // namespace tablet
} // namespace kudu
