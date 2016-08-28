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

#include <glog/logging.h>

#include <algorithm>
#include <utility>
#include <string>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/tablet/svg_dump.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/knapsack_solver.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::vector;

DEFINE_int32(budgeted_compaction_target_rowset_size, 32*1024*1024,
             "The target size for DiskRowSets during flush/compact when the "
             "budgeted compaction policy is used");
TAG_FLAG(budgeted_compaction_target_rowset_size, experimental);
TAG_FLAG(budgeted_compaction_target_rowset_size, advanced);

DEFINE_double(compaction_approximation_ratio, 1.05f,
              "Approximation ratio allowed for optimal compaction calculation. A "
              "value of 1.05 indicates that the policy may use an approximate result "
              "if it is known to be within 5% of the optimal solution.");
TAG_FLAG(compaction_approximation_ratio, experimental);

namespace kudu {
namespace tablet {

// Adjust the result downward slightly for wider solutions.
// Consider this input:
//
//  |-----A----||----C----|
//  |-----B----|
//
// where A, B, and C are all 1MB, and the budget is 10MB.
//
// Without this tweak, the solution {A, B, C} has the exact same
// solution value as {A, B}, since both compactions would yield a
// tablet with average height 1. Since both solutions fit within
// the budget, either would be a valid pick, and it would be up
// to chance which solution would be selected.
// Intuitively, though, there's no benefit to including "C" in the
// compaction -- it just uses up some extra IO. If we slightly
// penalize wider solutions as a tie-breaker, then we'll pick {A, B}
// here.
static const double kSupportAdjust = 1.01;

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

// Returns in min-key and max-key sorted order
void BudgetedCompactionPolicy::SetupKnapsackInput(const RowSetTree &tree,
                                                  vector<RowSetInfo>* min_key,
                                                  vector<RowSetInfo>* max_key) {
  RowSetInfo::CollectOrdered(tree, min_key, max_key);

  if (min_key->size() < 2) {
    // require at least 2 rowsets to compact
    min_key->clear();
    max_key->clear();
    return;
  }
}

namespace {

struct CompareByDescendingDensity {
  bool operator()(const RowSetInfo& a, const RowSetInfo& b) const {
    return a.density() > b.density();
  }
};

struct KnapsackTraits {
  typedef const RowSetInfo* item_type;
  typedef double value_type;
  static int get_weight(const RowSetInfo* item) {
    return item->size_mb();
  }
  static value_type get_value(const RowSetInfo* item) {
    return item->width();
  }
};

// Dereference-then-compare comparator
template<class Compare>
struct DerefCompare {
  template<class T>
  bool operator()(T* a, T* b) const {
    static const Compare comp = Compare();
    return comp(*a, *b);
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
    : total_weight_(0),
      total_value_(0),
      max_weight_(max_weight),
      topdensity_(MathLimits<double>::kNegInf) {
  }

  void Add(const RowSetInfo& candidate) {
    // No need to add if less dense than the top and have no more room
    if (total_weight_ >= max_weight_ &&
        candidate.density() <= topdensity_)
      return;

    fractional_solution_.push_back(&candidate);
    std::push_heap(fractional_solution_.begin(), fractional_solution_.end(),
                   DerefCompare<CompareByDescendingDensity>());

    total_weight_ += candidate.size_mb();
    total_value_ += candidate.width();
    const RowSetInfo* top = fractional_solution_.front();
    while (total_weight_ - top->size_mb() > max_weight_) {
      total_weight_ -= top->size_mb();
      total_value_ -= top->width();
      std::pop_heap(fractional_solution_.begin(), fractional_solution_.end(),
                    DerefCompare<CompareByDescendingDensity>());
      fractional_solution_.pop_back();
      top = fractional_solution_.front();
    }
    topdensity_ = top->density();
  }

  // Compute the lower and upper bounds to the 0-1 knapsack problem with the elements
  // added so far.
  pair<double, double> ComputeLowerAndUpperBound() const {
    int excess_weight = total_weight_ - max_weight_;
    if (excess_weight <= 0) {
      // If we've added less than the budget, our "bounds" are just including
      // all of the items.
      return { total_value_, total_value_ };
    }

    const RowSetInfo& top = *fractional_solution_.front();

    // The lower bound is either of:
    // - the highest density N items such that they all fit, OR
    // - the N+1th item, if it fits
    // This is a 2-approximation (i.e. no worse than 1/2 of the best solution).
    // See https://courses.engr.illinois.edu/cs598csc/sp2009/lectures/lecture_4.pdf
    double lower_bound = std::max(total_value_ - top.width(), top.width());
    double fraction_of_top_to_remove = static_cast<double>(excess_weight) / top.size_mb();
    DCHECK_GT(fraction_of_top_to_remove, 0);
    double upper_bound = total_value_ - fraction_of_top_to_remove * top.width();
    return {lower_bound, upper_bound};
  }

  // Return the items which make up the current lower-bound solution.
  void GetLowerBoundSolution(vector<const RowSetInfo*>* solution) {
    solution->clear();
    int excess_weight = total_weight_ - max_weight_;
    if (excess_weight <= 0) {
      *solution = fractional_solution_;
    } else {
      const RowSetInfo* top = fractional_solution_.front();

      // See above: there are two choices for the lower-bound estimate,
      // and we need to return the one matching the bound we computed.
      if (total_value_ - top->width() > top->width()) {
        // The current solution less the top (minimum density) element.
        solution->assign(fractional_solution_.begin() + 1,
                         fractional_solution_.end());
      } else {
        solution->push_back(top);
      }
    }
  }

  void clear() {
    fractional_solution_.clear();
    total_weight_ = 0;
    total_value_ = 0;
  }

 private:

  // Store pointers to RowSetInfo rather than whole copies in order
  // to allow for fast swapping in the heap.
  vector<const RowSetInfo*> fractional_solution_;
  int total_weight_;
  double total_value_;
  int max_weight_;
  double topdensity_;
};

} // anonymous namespace

void BudgetedCompactionPolicy::RunApproximation(
    const vector<RowSetInfo>& asc_min_key,
    const vector<RowSetInfo>& asc_max_key,
    vector<double>* best_upper_bounds,
    SolutionAndValue* best_solution) {
  best_upper_bounds->clear();
  best_upper_bounds->reserve(asc_min_key.size());
  BoundCalculator bound_calc(size_budget_mb_);
  for (const RowSetInfo& cc_a : asc_min_key) {
    bound_calc.clear();
    double ab_min = cc_a.cdf_min_key();
    double ab_max = cc_a.cdf_max_key();
    double best_upper = 0;
    for (const RowSetInfo& cc_b : asc_max_key) {
      if (cc_b.cdf_min_key() < ab_min) {
        continue;
      }
      ab_max = std::max(cc_b.cdf_max_key(), ab_max);
      double union_width = ab_max - ab_min;
      bound_calc.Add(cc_b);
      auto bounds = bound_calc.ComputeLowerAndUpperBound();
      double lower = bounds.first - union_width * kSupportAdjust;
      double upper = bounds.second - union_width * kSupportAdjust;
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
    SolutionAndValue* best_solution) {

  KnapsackSolver<KnapsackTraits> solver;
  vector<const RowSetInfo*> inrange_candidates;
  inrange_candidates.reserve(asc_min_key.size());
  for (int i = 0; i < asc_min_key.size(); i++) {
    const RowSetInfo& cc_a = asc_min_key[i];
    const double upper_bound = best_upper_bounds[i];

    // 'upper_bound' is an upper bound on the solution value of any compaction that includes
    // 'cc_a' as its left-most RowSet. If that bound is worse than the current best solution,
    // then we can skip finding the precise value of this solution.
    //
    // Here we also build in the approximation ratio as slop: the upper bound doesn't need
    // to just be better than the current solution, but needs to be better by at least
    // the approximation ratio before we bother looking for it.
    if (upper_bound < best_solution->value * FLAGS_compaction_approximation_ratio) {
      continue;
    }

    inrange_candidates.clear();
    double ab_min = cc_a.cdf_min_key();
    double ab_max = cc_a.cdf_max_key();
    for (const RowSetInfo& cc_b : asc_max_key) {
      if (cc_b.cdf_min_key() < ab_min) {
        // Would expand support to the left.
        // TODO: possible optimization here: binary search to skip to the first
        // cc_b with cdf_max_key() > cc_a.cdf_min_key()
        continue;
      }
      inrange_candidates.push_back(&cc_b);
    }
    if (inrange_candidates.empty()) continue;

    solver.Reset(size_budget_mb_, &inrange_candidates);
    ab_max = cc_a.cdf_max_key();

    vector<int> chosen_indexes;
    int j = 0;
    while (solver.ProcessNext()) {
      const RowSetInfo* item = inrange_candidates[j++];
      std::pair<int, double> best_with_this_item = solver.GetSolution();
      double best_value = best_with_this_item.second;

      ab_max = std::max(item->cdf_max_key(), ab_max);
      DCHECK_GE(ab_max, ab_min);
      double solution = best_value - (ab_max - ab_min) * kSupportAdjust;
      if (solution > best_solution->value) {
        solver.TracePath(best_with_this_item, &chosen_indexes);
        best_solution->value = solution;
      }
    }

    // If we came up with a new solution, replace.
    if (!chosen_indexes.empty()) {
      best_solution->rowsets.clear();
      for (int idx : chosen_indexes) {
        best_solution->rowsets.insert(inrange_candidates[idx]->rowset());
      }
    }
  }
}

// See docs/design-docs/compaction-policy.md for an overview of the compaction
// policy implemented in this function.
Status BudgetedCompactionPolicy::PickRowSets(const RowSetTree &tree,
                                             unordered_set<RowSet*>* picked,
                                             double* quality,
                                             std::vector<std::string>* log) {
  vector<RowSetInfo> asc_min_key, asc_max_key;
  SetupKnapsackInput(tree, &asc_min_key, &asc_max_key);
  if (asc_max_key.empty()) {
    if (log) {
      LOG_STRING(INFO, log) << "No rowsets to compact";
    }
    // nothing to compact.
    return Status::OK();
  }

  // The best set of rowsets chosen so far, and the value attained by that choice.
  SolutionAndValue best_solution;

  // The algorithm proceeds in two passes. The first is based on an approximation
  // of the knapsack problem, and computes some upper and lower bounds. The second
  // pass looks again over the input for any cases where the upper bound tells us
  // there is a significant improvement potentially available, and only in those
  // cases runs the actual knapsack solver.

  // Both passes are structured as a loop over all pairs {cc_a, cc_b} such that
  // cc_a.min_key() < cc_b.min_key(). Given any such pair, we know that a compaction
  // including both of these pairs would result in an output rowset that spans
  // the range [cc_a.min_key(), max(cc_a.max_key(), cc_b.max_key())]. This width
  // is designated 'union_width' below.

  // In particular, the order in which we consider 'cc_b' is such that, for the
  // inner loop (a fixed 'cc_a'), the 'union_width' increases minimally to only
  // include the new 'cc_b' and not also cover any other rowsets.
  //
  // For example:
  //
  //  |-----A----|
  //      |-----B----|
  //         |----C----|
  //       |--------D-------|
  //
  // We process in the order: A, B, C, D.
  //
  // With this iteration order, each knapsack problem builds on the previous knapsack
  // problem by adding just a single rowset, meaning that we can reuse the
  // existing solution state to incrementally update the solution, rather than having
  // to rebuild from scratch.


  // Pass 1 (approximate)
  // ------------------------------------------------------------
  // First, run the whole algorithm using a fast approximation of the knapsack solver
  // to come up with lower bounds and upper bounds. The approximation algorithm is a
  // 2-approximation but in practice often yields results that are very close to optimal
  // for the sort of problems we encounter in our use case.
  //
  // This pass calculates:
  // 1) 'best_upper_bounds': for each 'cc_a', what's the upper bound for any solution
  //     including this rowset. We use this later to avoid solving the knapsack for
  //     cases where the upper bound is lower than our current best solution.
  // 2) 'best_solution' and 'best_solution->value': the best approximate solution
  //     found.
  vector<double> best_upper_bounds;
  RunApproximation(asc_min_key, asc_max_key, &best_upper_bounds, &best_solution);

  // Pass 2 (precise)
  // ------------------------------------------------------------
  // Now that we've found an approximate solution and upper bounds, do another pass.
  // In cases where the upper bound indicates we could do substantially better than
  // our current best solution, we use the exact knapsack solver to find the improved
  // solution.
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

  if (best_solution.value <= 0) {
    VLOG(1) << "Best compaction available makes things worse. Not compacting.";
    return Status::OK();
  }

  picked->swap(best_solution.rowsets);
  DumpCompactionSVG(asc_min_key, *picked);

  return Status::OK();
}

} // namespace tablet
} // namespace kudu
