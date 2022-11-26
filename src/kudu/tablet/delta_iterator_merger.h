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
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/rowid.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class ColumnBlock;
class ScanSpec;
class SelectionVector;
struct ColumnId;

namespace tablet {

class Mutation;
struct RowIteratorOptions;

// DeltaIterator that simply combines together other DeltaIterators,
// applying deltas from each in order.
class DeltaIteratorMerger : public DeltaIterator {
 public:
  // Create a new DeltaIterator which combines the deltas from
  // all of the input delta stores. It is expected that the caller has sorted
  // the input stores according to the desired application order:
  // - REDO stores are ordered in increasing timestamp order.
  // - UNDO stores are ordered in decreasing timestamp order.
  //
  // If only one store is input, this will automatically return an unwrapped
  // iterator for greater efficiency.
  static Status Create(
      const std::vector<std::shared_ptr<DeltaStore>> &stores,
      const RowIteratorOptions& opts,
      std::unique_ptr<DeltaIterator>* out);

  ////////////////////////////////////////////////////////////
  // Implementations of DeltaIterator
  ////////////////////////////////////////////////////////////
  Status Init(ScanSpec* spec) override;

  Status SeekToOrdinal(rowid_t idx) override;

  Status PrepareBatch(size_t nrows, int prepare_flags) override;

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                      const SelectionVector& filter) override;

  Status ApplyDeletes(SelectionVector* sel_vec) override;

  Status SelectDeltas(SelectedDeltas* deltas) override;

  Status CollectMutations(std::vector<Mutation*>* dst, Arena* arena) override;

  Status FilterColumnIdsAndCollectDeltas(const std::vector<ColumnId>& col_ids,
                                         std::vector<DeltaKeyAndUpdate>* out,
                                         Arena* arena) override;

  bool HasNext() const override;

  bool MayHaveDeltas() const override;

  std::string ToString() const override;

  int64_t deltas_selected() const override {
    return total_deltas_selected_in_prepare_;
  }

  void set_deltas_selected(int64_t /*deltas_selected*/) override {
    LOG(DFATAL) << "Not implemented";
  }

  size_t memory_footprint() override;

 private:
  explicit DeltaIteratorMerger(std::vector<std::unique_ptr<DeltaIterator> > iters);

  // The number of deltas selected in PrepareBatch() across all iterators so
  // far. This is useful to ensure that as we iterate through each
  // DeltaIterator, we are able to define a total ordering of our deltas.
  int64_t total_deltas_selected_in_prepare_;

  std::vector<std::unique_ptr<DeltaIterator> > iters_;
};

} // namespace tablet
} // namespace kudu
