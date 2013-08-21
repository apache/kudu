// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_KNAPSACK_SOLVER_H
#define KUDU_UTIL_KNAPSACK_SOLVER_H

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <utility>
#include <vector>
#include "gutil/macros.h"

namespace kudu {

// Solver for the 0-1 knapsack problem. This uses dynamic programming
// to solve the problem exactly.
//
// Given a knapsack capacity of 'W' and a number of potential items 'n',
// this solver is O(nW) time and space.
//
// This implementation is cribbed from wikipedia. The only interesting
// bit here that doesn't directly match the pseudo-code is that we
// maintain the "taken" bitmap keeping track of which items were
// taken, so we can efficiently "trace back" the chosen items.
template<class Traits>
class KnapsackSolver {
 public:
  typedef typename Traits::item_type item_type;
  typedef typename Traits::value_type value_type;
  typedef std::pair<int, value_type> solution_type;

  KnapsackSolver() {}
  ~KnapsackSolver() {}

  // Solve a knapsack problem in one shot. Finds the set of
  // items in 'items' such that their weights add up to no
  // more than 'knapsack_capacity' and maximizes the sum
  // of their values.
  // The indexes of the chosen items are stored in 'chosen_items',
  // and the maximal value is stored in 'optimal_value'.
  void Solve(std::vector<item_type> &items,
             size_t knapsack_capacity,
             std::vector<size_t>* chosen_items,
             value_type* optimal_value);


  // The following functions are a more advanced API for solving
  // knapsack problems, allowing the caller to obtain incremental
  // results as each item is considered. See the implementation of
  // Solve() for usage.

  // Prepare to solve a knapsack problem with the given capacity and
  // item set. The vector of items must remain valid and unchanged
  // until the next call to Reset().
  void Reset(int knapsack_capacity,
             const std::vector<item_type>* items);

  // Process the next item in 'items'. Returns false if there
  // were no more items to process.
  bool ProcessNext();

  // Returns the current best solution after the most recent ProcessNext
  // call. *solution is a pair of (weight used, value obtained).
  solution_type GetSolution();

  // Trace the path of item indexes used to achieve the given best
  // solution as of the latest ProcessNext() call.
  void TracePath(const solution_type& best,
                 std::vector<size_t>* chosen_items);

 private:

  // The state kept by the DP algorithm.
  template<typename value_type>
  class KnapsackBlackboard {
   public:
    KnapsackBlackboard() :
      n_items_(0),
      n_weights_(0) {
    }

    void Resize(int n_items, int max_weight);

    value_type &at_prev_column(int weight) {
      DCHECK_GE(weight, 0);
      DCHECK_LT(weight, n_weights_);
      return prev_column_[weight];
    }

    value_type &at_cur_column(int weight) {
      DCHECK_GE(weight, 0);
      DCHECK_LT(weight, n_weights_);
      return cur_column_[weight];
    }

    void Advance() {
      cur_column_.swap(prev_column_);
    }

    void ClearCurrentColumn() {
      cur_column_.assign(cur_column_.size(), 0);
    }

    bool taken(int item, int weight) {
      DCHECK_GE(weight, 0);
      DCHECK_LT(weight, n_weights_);
      DCHECK_GE(item, 0);
      DCHECK_LT(item, n_items_);
      return item_taken_[index(item, weight)];
    }

    void mark_taken(int item, int weight) {
      item_taken_[index(item, weight)] = true;
    }

    std::pair<int, value_type> BestSolution() {
      value_type max_value = 0;
      int max_value_weight = 0;
      for (int w = 0; w < n_weights_; w++) {
        value_type val = at_cur_column(w);
        if (val > max_value) {
          max_value = val;
          max_value_weight = w;
        }
      }
      return std::make_pair(max_value_weight, max_value);
    }

   private:
    // If the dynamic programming matrix has more than this number of cells,
    // then warn.
    static const int kWarnDimension = 10000000;

    size_t index(int item, int weight) {
      return n_weights_ * item + weight;
    }

    std::vector<value_type> prev_column_;
    std::vector<value_type> cur_column_;
    std::vector<bool> item_taken_;
    size_t n_items_, n_weights_;

    DISALLOW_COPY_AND_ASSIGN(KnapsackBlackboard);
  };

  KnapsackBlackboard<value_type> bb_;
  const std::vector<item_type>* items_;
  int cur_item_idx_;
  int knapsack_capacity_;

  DISALLOW_COPY_AND_ASSIGN(KnapsackSolver);
};

template<class Traits>
inline void KnapsackSolver<Traits>::Reset(int knapsack_capacity,
                                          const std::vector<item_type>* items) {
  DCHECK_GE(knapsack_capacity, 0);
  items_ = items;
  knapsack_capacity_ = knapsack_capacity;
  bb_.Resize(items->size(), knapsack_capacity);
  bb_.ClearCurrentColumn();
  cur_item_idx_ = 0;
}

template<class Traits>
inline bool KnapsackSolver<Traits>::ProcessNext() {
  if (cur_item_idx_ >= items_->size()) {
    return false;
  }

  bb_.Advance();
  const item_type& item = (*items_)[cur_item_idx_];
  int item_weight = Traits::get_weight(item);
  value_type item_value = Traits::get_value(item);

  int j;
  for (j = 0; j < item_weight && j <= knapsack_capacity_; j++) {
    bb_.at_cur_column(j) = bb_.at_prev_column(j);
    // No need to set "taken" to false -- we clear to false at the start
    // in Resize()
  }
  for (; j <= knapsack_capacity_; j++) {
    value_type val_if_taken = bb_.at_prev_column(j - item_weight) + item_value;
    value_type val_if_skipped = bb_.at_prev_column(j);

    if (val_if_taken > val_if_skipped) {
      bb_.at_cur_column(j) = val_if_taken;
      bb_.mark_taken(cur_item_idx_, j);
    } else {
      bb_.at_cur_column(j) = val_if_skipped;
      // No need to set false -- we clear to false at the start.
    }
  }

  cur_item_idx_++;
  return true;
}

template<class Traits>
inline void KnapsackSolver<Traits>::Solve(std::vector<item_type> &items,
                                          size_t knapsack_capacity,
                                          std::vector<size_t>* chosen_items,
                                          value_type* optimal_value) {

  Reset(knapsack_capacity, &items);

  while (ProcessNext()) {
  }

  solution_type best = GetSolution();
  *optimal_value = best.second;
  TracePath(best, chosen_items);
}

template<class Traits>
inline typename KnapsackSolver<Traits>::solution_type KnapsackSolver<Traits>::GetSolution() {
  return bb_.BestSolution();
}

template<class Traits>
inline void KnapsackSolver<Traits>::TracePath(const solution_type& best,
                                              std::vector<size_t>* chosen_items) {
  chosen_items->clear();
  // Retrace back which set of items corresponded to this value.
  int w = best.first;
  chosen_items->clear();
  for (int k = cur_item_idx_ - 1; k >= 0; k--) {
    if (bb_.taken(k, w)) {
      const item_type& taken = (*items_)[k];
      chosen_items->push_back(k);
      w -= Traits::get_weight(taken);
      DCHECK_GE(w, 0);
    }
  }
}

template<class Traits>
template<class value_type>
void KnapsackSolver<Traits>::KnapsackBlackboard<value_type>::Resize(
    int n_items, int max_weight) {
  CHECK_GT(n_items, 0);
  CHECK_GE(max_weight, 0);

  // Rather than zero-indexing the weights, we size the array from
  // 0 to max_weight. This avoids having to subtract 1 every time
  // we index into the array.
  n_weights_ = max_weight + 1;

  prev_column_.resize(n_weights_);
  cur_column_.resize(n_weights_);

  size_t dimension = index(n_items, n_weights_);
  if (dimension > kWarnDimension) {
    LOG(WARNING) << "Knapsack problem " << n_items << "x" << n_weights_
                 << " is large: may be inefficient!";
  }
  item_taken_.resize(dimension);
  std::fill(item_taken_.begin(), item_taken_.end(), false);
  n_items_ = n_items;
}

} // namespace kudu
#endif
