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
#ifndef KUDU_TABLET_COMPACTION_POLICY_H
#define KUDU_TABLET_COMPACTION_POLICY_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

class RowSet;
class RowSetInfo;
class RowSetTree;

// A set of rowsets selected for compaction.
typedef std::unordered_set<const RowSet*> CompactionSelection;

// A CompactionPolicy is responsible for picking which rowsets in a tablet
// should be compacted together.
class CompactionPolicy {
 public:
  CompactionPolicy() = default;
  virtual ~CompactionPolicy() = default;

  // Select a set of RowSets to compact out of 'tree'.
  //
  // Callers are responsible for externally synchronizing selection within a
  // given tablet. This will only select rowsets whose compact_flush_lock
  // is unlocked, but will not itself take the lock. Hence no other threads
  // should lock or unlock the rowsets' compact_flush_lock while this method
  // is running.
  //
  // *quality is set to represent how effectively the compaction reduces the
  // tablet's average IO per write operation.
  //
  // If 'log' is not NULL, then a verbose log of the compaction selection
  // process will be appended to it.
  virtual Status PickRowSets(const RowSetTree& tree,
                             CompactionSelection* picked,
                             double* quality,
                             std::vector<std::string>* log) = 0;

  // Return the size in bytes at which flush/compact should "roll" to new files.
  // Some compaction policies may prefer to deal with small constant-size files
  // whereas others may prefer large ones.
  virtual uint64_t target_rowset_size() const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(CompactionPolicy);
};

// A compaction policy that, given a size budget for a compaction, tries to pick
// a set of RowSets that fit into that budget and minimize the future cost of
// operations on the tablet.
//
// See docs/design-docs/compaction-policy.md for details.
class BudgetedCompactionPolicy : public CompactionPolicy {
 public:
  explicit BudgetedCompactionPolicy(int size_budget_mb);

  Status PickRowSets(const RowSetTree &tree,
                     CompactionSelection* picked,
                     double* quality,
                     std::vector<std::string>* log) override;

  uint64_t target_rowset_size() const override;

 private:
  struct SolutionAndValue {
    CompactionSelection rowsets;
    double value = 0.0;
  };

  // Set up the 'asc_min_key' and 'asc_max_key' vectors used by both the
  // RunApproximation and RunExact. 'asc_min_key' will hold RowSetInfo instances
  // for rowsets from 'tree' in ascending order by min key; 'asc_max_key' will
  // hold the RowSetInfo instances in ascending order by max key.
  void SetupKnapsackInput(const RowSetTree& tree,
                          std::vector<RowSetInfo>* asc_min_key,
                          std::vector<RowSetInfo>* asc_max_key) const;

  // Runs the first pass approximate algorithm for the compaction selection
  // problem. Stores the best solution found in 'best_solution'.
  //
  // Sets best_upper_bounds[i] to an upper bound for a solution containing
  // asc_min_key[i] as its left-most rowset.
  void RunApproximation(const std::vector<RowSetInfo>& asc_min_key,
                        const std::vector<RowSetInfo>& asc_max_key,
                        std::vector<double>* best_upper_bounds,
                        SolutionAndValue* best_solution) const;


  // Runs the second pass of the algorithm.
  //
  // For each i, the algorithm first checks if best_upper_bounds[i] indicates
  // that a solution containing asc_min_key[i] as its left-most rowset might be
  // a better solution than the current 'best_solution' by at least the
  // configured approximation ratio. If so, it runs the full knapsack algorithm
  // to determine the value of that solution and, if it is indeed better,
  // replaces '*best_solution' with the new best solution.
  void RunExact(const std::vector<RowSetInfo>& asc_min_key,
                const std::vector<RowSetInfo>& asc_max_key,
                const std::vector<double>& best_upper_bounds,
                SolutionAndValue* best_solution) const;

  const size_t size_budget_mb_;
};

} // namespace tablet
} // namespace kudu
#endif
