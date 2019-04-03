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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/common/iterator.h"
#include "kudu/util/status.h"

namespace kudu {

class ColumnPredicate;
class ScanSpec;

// Encapsulates a rowwise-iterator along with the (encoded) lower and upper
// bounds for the rowset that the iterator belongs to.
//
// The bounds are optional because some rowsets (e.g. MRS) don't have them.
struct IterWithBounds {
  std::unique_ptr<RowwiseIterator> iter;
  boost::optional<std::pair<std::string, std::string>> encoded_bounds;
};

// Options struct for the MergeIterator.
struct MergeIteratorOptions {
  explicit MergeIteratorOptions(bool include_deleted_rows)
      : include_deleted_rows(include_deleted_rows) {}

  // Whether to include and de-duplicate ghost (deleted) rows. If true, the
  // sub-iterator schemas must also contain an IS_DELETED column or an error
  // will be returned during MergeIterator initialization.
  const bool include_deleted_rows;
};

// Constructs a MergeIterator of the given iterators.
//
// The iterators must have matching schemas and should not yet be initialized.
std::unique_ptr<RowwiseIterator> NewMergeIterator(
    MergeIteratorOptions opts,
    std::vector<IterWithBounds> iters);

// Constructs a UnionIterator of the given iterators.
//
// The iterators must have matching schemas and should not yet be initialized.
std::unique_ptr<RowwiseIterator> NewUnionIterator(std::vector<IterWithBounds> iters);

// Constructs a MaterializingIterator of the given ColumnwiseIterator.
std::unique_ptr<RowwiseIterator> NewMaterializingIterator(
    std::unique_ptr<ColumnwiseIterator> iter);

// Initializes the given '*base_iter' with the given 'spec'.
//
// If the base_iter accepts all predicates, then simply returns. Otherwise,
// swaps out *base_iter for a PredicateEvaluatingIterator which wraps the
// original iterator and accepts all predicates on its behalf.
//
// POSTCONDITION: spec->predicates().empty()
// POSTCONDITION: base_iter and its wrapper are initialized
Status InitAndMaybeWrap(std::unique_ptr<RowwiseIterator>* base_iter,
                        ScanSpec* spec);

// Gets the predicates associated with a PredicateEvaluatingIterator.
//
// Only for use by tests.
const std::vector<ColumnPredicate>& GetIteratorPredicatesForTests(
    const std::unique_ptr<RowwiseIterator>& iter);

} // namespace kudu
