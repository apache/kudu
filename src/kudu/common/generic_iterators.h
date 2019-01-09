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

#include <cstdint>
#include <memory>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {

class ColumnPredicate;
class ColumnwiseIterator;
class RowwiseIterator;
class ScanSpec;

// Constructs a MergeIterator of the given iterators.
//
// The iterators must have matching schemas and should not yet be initialized.
std::unique_ptr<RowwiseIterator> NewMergeIterator(
    std::vector<std::unique_ptr<RowwiseIterator>> iters);

// Constructs a UnionIterator of the given iterators.
//
// The iterators must have matching schemas and should not yet be initialized.
std::unique_ptr<RowwiseIterator> NewUnionIterator(
    std::vector<std::unique_ptr<RowwiseIterator>> iters);

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

// Gets the number of comparisons performed by a MergeIterator.
//
// Only for use by tests.
int64_t GetMergeIteratorNumComparisonsForTests(
    const std::unique_ptr<RowwiseIterator>& iter);

} // namespace kudu
