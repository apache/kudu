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
#ifndef KUDU_TABLET_DELTA_APPLIER_H
#define KUDU_TABLET_DELTA_APPLIER_H

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/common/iterator.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/status.h"

namespace kudu {

class ColumnMaterializationContext;
class ScanSpec;
class Schema;
class SelectionVector;
struct IteratorStats;

namespace tablet {

class DeltaIterator;

////////////////////////////////////////////////////////////
// Delta-applying iterators
////////////////////////////////////////////////////////////

// A DeltaApplier takes in a base ColumnwiseIterator along with a
// DeltaIterator. It is responsible for applying the updates coming
// from the delta iterator to the results of the base iterator.
class DeltaApplier final : public ColumnwiseIterator {
 public:
  Status Init(ScanSpec* spec) override;
  Status PrepareBatch(size_t* nrows) override;

  Status FinishBatch() override;

  bool HasNext() const override;

  std::string ToString() const override;

  const Schema& schema() const override;

  void GetIteratorStats(std::vector<IteratorStats>* stats) const override;

  // Initialize the selection vector for the current batch.
  // This processes DELETEs -- any deleted rows are set to 0 in 'sel_vec'.
  // All other rows are set to 1.
  Status InitializeSelectionVector(SelectionVector* sel_vec) override;

  Status MaterializeColumn(ColumnMaterializationContext* ctx) override;

 private:
  friend class DeltaTracker;

  FRIEND_TEST(TestMajorDeltaCompaction, TestCompact);

  // Construct. The base_iter and delta_iter should not be Initted.
  DeltaApplier(RowIteratorOptions opts,
               std::shared_ptr<CFileSet::Iterator> base_iter,
               std::unique_ptr<DeltaIterator> delta_iter);
  ~DeltaApplier() override;

  const RowIteratorOptions opts_;

  std::shared_ptr<CFileSet::Iterator> base_iter_;
  std::unique_ptr<DeltaIterator> delta_iter_;

  bool first_prepare_;

  DISALLOW_COPY_AND_ASSIGN(DeltaApplier);
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_DELTA_APPLIER_H */
