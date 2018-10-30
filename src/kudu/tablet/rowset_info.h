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
#ifndef KUDU_TABLET_ROWSET_INFO_H_
#define KUDU_TABLET_ROWSET_INFO_H_

#include <cstdint>
#include <string>
#include <vector>

#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/slice.h"

namespace kudu {

class KeyRange;
struct ColumnId;

namespace tablet {

class RowSet;
class RowSetTree;

// Class used to cache some computed statistics on a RowSet used
// during evaluation of budgeted compaction policy.
//
// Class is immutable.
class RowSetInfo {
 public:

  // Appends the rowsets in no order without the cdf values set.
  static void Collect(const RowSetTree& tree, std::vector<RowSetInfo>* rsvec);

  // From the rowset tree 'tree', computes the keyspace cdf and collects rowset
  // information in min-key- and max-key-sorted order into 'info_by_min_key'
  // and 'info_by_max_key', respectively. The average value of the height of the
  // rowset tree is set into 'average_height', if it is not nullptr.
  // If one of 'info_by_min_key' and 'info_by_max_key' is nullptr, the other
  // must be.
  static void ComputeCdfAndCollectOrdered(const RowSetTree& tree,
                                          double* average_height,
                                          std::vector<RowSetInfo>* info_by_min_key,
                                          std::vector<RowSetInfo>* info_by_max_key);

  // Split [start_key, stop_key) into primary key ranges by chunk size.
  //
  // If col_ids specified, then the size estimate used for 'target_chunk_size'
  // should only include these columns. This can be used if a query will
  // only scan a certain subset of the columns.
  static void SplitKeyRange(const RowSetTree& tree,
                            Slice start_key,
                            Slice stop_key,
                            const std::vector<ColumnId>& col_ids,
                            uint64 target_chunk_size,
                            std::vector<KeyRange>* ranges);

  uint64_t size_bytes(const ColumnId& col_id) const;
  uint64_t size_bytes() const { return extra_->size_bytes; }
  int size_mb() const { return size_mb_; }

  // Return the value of the CDF at the minimum key of this candidate.
  double cdf_min_key() const { return cdf_min_key_; }
  // Return the value of the CDF at the maximum key of this candidate.
  double cdf_max_key() const { return cdf_max_key_; }

  bool has_bounds() const { return extra_->has_bounds;  }
  const std::string& min_key() const { return extra_->min_key; }
  const std::string& max_key() const { return extra_->max_key; }

  // Return the "width" of the candidate rowset.
  //
  // This is an estimate of the percentage of the tablet data which lies between
  // the min and max key of this RowSet (including data not in this rowset but
  // in overlapping ones), calculated by integrating the probability
  // distribution function across this rowset's keyrange.
  double width() const {
    return cdf_max_key_ - cdf_min_key_;
  }

  double value() const {
    return value_;
  }

  double density() const { return density_; }

  RowSet* rowset() const { return extra_->rowset; }

  std::string ToString() const;

  // Return true if this candidate overlaps the other candidate in
  // the computed cdf interval. To check intersection in key space,
  // use this instance's rowset()->GetBounds().
  // The two intersection results may not agree because of floating
  // point error in the cdf calculation.
  bool Intersects(const RowSetInfo& other) const;

 private:
  explicit RowSetInfo(RowSet* rs, double init_cdf);

  static void FinalizeCDFVector(double quot, std::vector<RowSetInfo>* vec);

  // The size in MB, already clamped so that all rowsets have size at least
  // 1MB. This is cached to avoid the branch during the selection hot path.
  int size_mb_;

  double cdf_min_key_, cdf_max_key_;

  // The value of the rowset is its value as an item in the compaction knapsack
  // problem:
  // value = width + tradeoff * (1 - size in bytes / target size in bytes).
  double value_;

  // The density is the density of the rowset as an item in compaction knapsack
  // problems:
  // density = value / size in MiB
  // It doesn't matter if the size is in bytes or MiB since densities are only
  // used to compare rowsets.
  double density_;

  // We move these out of the RowSetInfo object because the std::strings are relatively
  // large objects, and we'd like the RowSetInfos to be as small as possible so that
  // the algorithm can fit mostly in CPU cache. The string bounds themselves are rarely
  // accessed in the hot part of the algorithm so it's worth out-of-lining them.
  //
  // These are ref-counted so that RowSetInfo is copyable.
  struct ExtraData : public RefCounted<ExtraData> {
    // Cached version of rowset_->OnDiskBaseDataSizeWithRedos().
    uint64_t size_bytes;

    // True if the RowSet has known bounds.
    // MemRowSets in particular do not.
    bool has_bounds;
    std::string min_key, max_key;

    // The original RowSet that this RowSetInfo was constructed from.
    RowSet* rowset;
  };
  const scoped_refptr<ExtraData> extra_;
};

} // namespace tablet
} // namespace kudu

#endif
