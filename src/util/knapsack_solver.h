// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_KNAPSACK_SOLVER_H
#define KUDU_UTIL_KNAPSACK_SOLVER_H

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <vector>
#include "gutil/macros.h"

namespace kudu {

// Solver for the 0-1 knapsack problem. This uses dynamic programming
// to solve the problem exactly.
//
// Given a knapsack capacity of 'W' and a number of potential items 'n',
// this solver is O(nW) time and space.
template<class Traits>
class KnapsackSolver {
 public:
  typedef typename Traits::item_type item_type;
  typedef typename Traits::value_type value_type;

  KnapsackSolver() {}
  ~KnapsackSolver() {}

  void Solve(std::vector<item_type> &items,
             size_t knapsack_capacity,
             std::vector<size_t> *chosen_items,
             value_type *optimal_value);

 private:
  DISALLOW_COPY_AND_ASSIGN(KnapsackSolver);

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

    bool taken(int item, int weight) {
      DCHECK_GE(weight, 0);
      DCHECK_LT(weight, n_weights_);
      DCHECK_GE(item, 1);
      DCHECK_LE(item, n_items_);
      return item_taken_[index(item, weight)];
    }

    void mark_taken(int item, int weight) {
      item_taken_[index(item, weight)] = true;
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
};

template<class Traits>
inline void KnapsackSolver<Traits>::Solve(std::vector<item_type> &items,
                                          size_t knapsack_capacity,
                                          std::vector<size_t> *chosen_items,
                                          value_type *optimal_value) {
  if (knapsack_capacity == 0) {
    *optimal_value = 0;
    return;
  }

  bb_.Resize(items.size(), knapsack_capacity);

  for (int w = 0; w <= knapsack_capacity; w++) {
    bb_.at_cur_column(w) = 0;
  }

  // The dynamic-programming solution to the knapsack problem, faithfully
  // cribbed from wikipedia. The only interesting bit here that doesn't
  // directly match the pseudo-code is that we maintain the "taken" bitmap
  // keeping track of which items were taken, so we can efficiently
  // "trace back" the chosen items.
  for (int i = 1; i <= items.size(); i++) {
    bb_.Advance();

    int item_weight = Traits::get_weight(items[i - 1]);
    value_type item_value = Traits::get_value(items[i - 1]);

    int j;
    for (j = 0; j < item_weight && j <= knapsack_capacity; j++) {
      bb_.at_cur_column(j) = bb_.at_prev_column(j);
      // No need to set false -- we clear to false at the start.
    }
    for (; j <= knapsack_capacity; j++) {
      value_type val_if_taken = bb_.at_prev_column(j - item_weight) + item_value;
      value_type val_if_skipped = bb_.at_prev_column(j);

      if (val_if_taken > val_if_skipped) {
        bb_.at_cur_column(j) = val_if_taken;
        bb_.mark_taken(i, j);
      } else {
        bb_.at_cur_column(j) = val_if_skipped;
        // No need to set false -- we clear to false at the start.
      }
    }
  }

  // Determine the max value that was attained.
  value_type max_value = 0;
  int max_value_weight = 0;
  for (int w = 0; w <= knapsack_capacity; w++) {
    value_type val = bb_.at_cur_column(w);
    if (val > max_value) {
      max_value = val;
      max_value_weight = w;
    }
  }

  // Retrace back which set of items corresponded to this value.
  int w = max_value_weight;
  for (int i = items.size(); i > 0; i--) {
    if (bb_.taken(i, w)) {
      chosen_items->push_back(i - 1);
      w -= Traits::get_weight(items[i - 1]);
      DCHECK_GE(w, 0);
    }
  }

  *optimal_value = max_value;
}

template<class Traits>
template<class value_type>
void KnapsackSolver<Traits>::KnapsackBlackboard<value_type>::Resize(
    int n_items, int max_weight) {
  CHECK_GT(n_items, 0);
  CHECK_GT(max_weight, 0);

  // Rather than zero-indexing the weights, we size the array from
  // 0 to max_weight. This avoids having to subtract 1 every time
  // we index into the array.
  n_weights_ = max_weight + 1;

  prev_column_.resize(n_weights_);
  cur_column_.resize(n_weights_);

  size_t dimension = index(n_items + 1, n_weights_);
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
