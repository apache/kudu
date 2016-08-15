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

#include <boost/optional.hpp>
#include <string>

#include "kudu/common/schema.h"

namespace kudu {

class Arena;
class ColumnBlock;
class ColumnSchema;
class SelectionVector;
class TypeInfo;

enum class PredicateType {
  // A predicate which always evaluates to false.
  None,

  // A predicate which evaluates to true if the column value equals a known
  // value.
  Equality,

  // A predicate which evaluates to true if the column value falls within a
  // range.
  Range,

  // A predicate which evaluates to true if the value is not null.
  IsNotNull,
};

// A predicate which can be evaluated over a block of column values.
//
// Predicates over the same column can be merged to create a conjunction of the
// two constituent predicates.
//
// There are multiple types of column predicates, which have different behavior
// when merging and evaluating.
//
// A ColumnPredicate does not own the data to which it points internally,
// so its lifetime must be managed to make sure it does not reference invalid
// data. Typically the lifetime of a ColumnPredicate will be tied to a scan (on
// the client side), or a scan iterator (on the server side).
class ColumnPredicate {
 public:

  // Creates a new equality predicate on the column and value.
  //
  // The value is not copied, and must outlive the returned predicate.
  static ColumnPredicate Equality(ColumnSchema column, const void* value);

  // Creates a new range column predicate from an inclusive lower bound and
  // exclusive upper bound.
  //
  // The values are not copied, and must outlive the returned predicate.
  //
  // Either (but not both) of the bounds may be a nullptr to indicate an
  // unbounded range on that end.
  //
  // The range will be simplified into an Equality or None predicate type if
  // possible.
  static ColumnPredicate Range(ColumnSchema column, const void* lower, const void* upper);

  // Creates a new range column predicate from an inclusive lower bound and an
  // inclusive upper bound.
  //
  // The values are not copied, and must outlive the returned predicate. The
  // arena is used for allocating an incremented upper bound to transform the
  // bound to exclusive. The arena must outlive the returned predicate.
  //
  // If a normalized column predicate cannot be created, then boost::none will
  // be returned. This indicates that the predicate would cover the entire
  // column range.
  static boost::optional<ColumnPredicate> InclusiveRange(ColumnSchema column,
                                                         const void* lower,
                                                         const void* upper,
                                                         Arena* arena);

  // Creates a new range column predicate from an exclusive lower bound and an
  // exclusive upper bound.
  //
  // The values are not copied, and must outlive the returned predicate. The
  // arena is used for allocating an incremented lower bound to transform the
  // bound to inclusive. The arena must outlive the returned predicate.
  static ColumnPredicate ExclusiveRange(ColumnSchema column,
                                        const void* lower,
                                        const void* upper,
                                        Arena* arena);

  // Creates a new IS NOT NULL predicate for the column.
  static ColumnPredicate IsNotNull(ColumnSchema column);

  // Returns the type of this predicate.
  PredicateType predicate_type() const {
    return predicate_type_;
  }

  // Merge another predicate into this one.
  //
  // The other predicate must be on the same column.
  //
  // After a merge, this predicate will be the logical intersection of the
  // original predicates.
  //
  // Data is not copied from the other predicate, so its data must continue to
  // outlive the merged predicate.
  void Merge(const ColumnPredicate& other);

  // Evaluate the predicate on every row in the column block.
  //
  // This is evaluated as an 'AND' with the current contents of *sel:
  // - If the predicate evaluates to false, sets the appropriate bit in the
  //   selection vector to 0.
  // - If the predicate evaluates to true, does not make any change to the
  //   selection vector.
  //
  // On any rows where the current value of *sel is false, the predicate evaluation
  // may be skipped.
  //
  // NOTE: the evaluation result is stored into '*sel' which may or may not be the
  // same vector as block->selection_vector().
  void Evaluate(const ColumnBlock& block, SelectionVector* sel) const;

  // Evaluate the predicate on a single cell.
  bool EvaluateCell(const void *cell) const;

  // Print the predicate for debugging.
  std::string ToString() const;

  // Returns true if the column predicates are equivalent.
  //
  // Predicates over different columns are not equal.
  bool operator==(const ColumnPredicate& other) const;

  // Returns the raw lower bound value if this is a range predicate, or the
  // equality value if this is an equality predicate.
  const void* raw_lower() const {
    return lower_;
  }

  // Returns the raw upper bound if this is a range predicate.
  const void* raw_upper() const {
    return upper_;
  }

  // Returns the column schema of the column on which this predicate applies.
  const ColumnSchema& column() const {
    return column_;
  }

 private:

  friend class TestColumnPredicate;

  // Creates a new column predicate.
  ColumnPredicate(PredicateType predicate_type,
                  ColumnSchema column,
                  const void* lower,
                  const void* upper);

  // Creates a new predicate which matches no values.
  static ColumnPredicate None(ColumnSchema column);

  // Transition to a None predicate type.
  void SetToNone();

  // Simplifies this predicate if possible.
  void Simplify();

  // Merge another predicate into this Range predicate.
  void MergeIntoRange(const ColumnPredicate& other);

  // Merge another predicate into this Equality predicate.
  void MergeIntoEquality(const ColumnPredicate& other);

  // The type of this predicate.
  PredicateType predicate_type_;

  // The data type of the column. TypeInfo instances have a static lifetime.
  ColumnSchema column_;

  // The inclusive lower bound value if this is a Range predicate, or the
  // equality value if this is an Equality predicate.
  const void* lower_;

  // The exclusive upper bound value if this is a Range predicate.
  const void* upper_;
};

// Compares predicates according to selectivity. Predicates that match fewer
// rows will sort before predicates that match more rows.
//
// TODO: this could be improved with a histogram of expected values.
int SelectivityComparator(const ColumnPredicate& left, const ColumnPredicate& right);

} // namespace kudu
