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
#ifndef KUDU_TABLET_LAYER_BASEDATA_H
#define KUDU_TABLET_LAYER_BASEDATA_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/vector.hpp>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/common/iterator.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/status.h"

namespace boost {
template <class T>
class optional;
}  // namespace boost

namespace kudu {

class ColumnMaterializationContext;
class MemTracker;
class ScanSpec;
class SelectionVector;
struct IteratorStats;

namespace cfile {
class BloomFileReader;
}  // namespace cfile

namespace fs {
struct IOContext;
}  // namespace fs

namespace tablet {

class RowSetKeyProbe;
struct ProbeStats;

// Set of CFiles which make up the base data for a single rowset
//
// All of these files have the same number of rows, and thus the positional
// indexes can be used to seek to corresponding entries in each.
class CFileSet :
    public std::enable_shared_from_this<CFileSet>,
    public enable_make_shared<CFileSet> {
 public:
  class Iterator;

  static Status Open(std::shared_ptr<RowSetMetadata> rowset_metadata,
                     std::shared_ptr<MemTracker> bloomfile_tracker,
                     std::shared_ptr<MemTracker> cfile_reader_tracker,
                     const fs::IOContext* io_context,
                     std::shared_ptr<CFileSet>* cfile_set);

  // Create an iterator with the given projection. 'projection' must remain valid
  // for the lifetime of the returned iterator.
  std::unique_ptr<Iterator> NewIterator(SchemaPtr projection,
                                        const fs::IOContext* io_context) const;

  Status CountRows(const fs::IOContext* io_context, rowid_t *count) const;

  // See RowSet::GetBounds
  Status GetBounds(std::string* min_encoded_key,
                   std::string* max_encoded_key) const;

  // The on-disk size, in bytes, of this cfile set's ad hoc index.
  // Returns 0 if there is no ad hoc index.
  uint64_t AdhocIndexOnDiskSize() const;

  // The on-disk size, in bytes, of this cfile set's bloomfiles.
  // Returns 0 if there are no bloomfiles.
  uint64_t BloomFileOnDiskSize() const;

  // The size on-disk of this cfile set's data, in bytes.
  // Excludes the ad hoc index and bloomfiles.
  uint64_t OnDiskDataSize() const;

  // The size on-disk of column cfile's data, in bytes.
  uint64_t OnDiskColumnDataSize(const ColumnId& col_id) const;

  // Determine the index of the given row key.
  // Sets *idx to boost::none if the row is not found.
  Status FindRow(const RowSetKeyProbe& probe,
                 const fs::IOContext* io_context,
                 boost::optional<rowid_t>* idx,
                 ProbeStats* stats) const;

  std::string ToString() const {
    return std::string("CFile base data in ") + rowset_metadata_->ToString();
  }

  // Check if the given row is present. If it is, sets *rowid to the
  // row's index.
  Status CheckRowPresent(const RowSetKeyProbe& probe, const fs::IOContext* io_context,
                         bool* present, rowid_t* rowid, ProbeStats* stats) const;

  // Return true if there exists a CFile for the given column ID.
  bool has_data_for_column_id(ColumnId col_id) const {
    return ContainsKey(readers_by_col_id_, col_id);
  }

  virtual ~CFileSet();

 protected:
  CFileSet(std::shared_ptr<RowSetMetadata> rowset_metadata,
           std::shared_ptr<MemTracker> bloomfile_tracker,
           std::shared_ptr<MemTracker> cfile_reader_tracker);

 private:
  friend class Iterator;

  DISALLOW_COPY_AND_ASSIGN(CFileSet);

  Status DoOpen(const fs::IOContext* io_context);
  Status OpenBloomReader(const fs::IOContext* io_context);
  Status LoadMinMaxKeys(const fs::IOContext* io_context);

  Status NewColumnIterator(ColumnId col_id,
                           cfile::CFileReader::CacheControl cache_blocks,
                           const fs::IOContext* io_context,
                           std::unique_ptr<cfile::CFileIterator>* iter) const;
  Status NewKeyIterator(const fs::IOContext* io_context,
                        std::unique_ptr<cfile::CFileIterator>* key_iter) const;

  // Return the CFileReader responsible for reading the key index.
  // (the ad-hoc reader for composite keys, otherwise the key column reader)
  cfile::CFileReader* key_index_reader() const;

  SchemaPtr tablet_schema() const { return rowset_metadata_->tablet_schema(); }

  std::shared_ptr<RowSetMetadata> rowset_metadata_;
  std::shared_ptr<MemTracker> bloomfile_tracker_;
  std::shared_ptr<MemTracker> cfile_reader_tracker_;

  std::string min_encoded_key_;
  std::string max_encoded_key_;

  // Map of column ID to reader. These are lazily initialized as needed.
  // We use flat_map here since it's the most memory-compact while
  // still having good performance for small maps.
  typedef boost::container::flat_map<int, std::unique_ptr<cfile::CFileReader>> ReaderMap;
  ReaderMap readers_by_col_id_;

  // A file reader for an ad-hoc index, i.e. an index that sits in its own file
  // and is not embedded with the column's data blocks. This is used when the
  // index pertains to more than one column, as in the case of composite keys.
  std::unique_ptr<cfile::CFileReader> ad_hoc_idx_reader_;
  std::unique_ptr<cfile::BloomFileReader> bloom_reader_;
};


////////////////////////////////////////////////////////////

// Column-wise iterator implementation over a set of column files.
//
// This simply ties together underlying files so that they can be batched
// together, and iterated in parallel.
class CFileSet::Iterator : public ColumnwiseIterator {
 public:

  virtual Status Init(ScanSpec *spec) OVERRIDE;

  virtual Status PrepareBatch(size_t *nrows) OVERRIDE;

  virtual Status InitializeSelectionVector(SelectionVector *sel_vec) OVERRIDE;

  Status MaterializeColumn(ColumnMaterializationContext *ctx) override;

  virtual Status FinishBatch() OVERRIDE;

  virtual bool HasNext() const OVERRIDE {
    DCHECK(initted_);
    return cur_idx_ < upper_bound_idx_;
  }

  virtual std::string ToString() const OVERRIDE {
    return std::string("rowset iterator for ") + base_data_->ToString();
  }

  const SchemaPtr schema() const OVERRIDE {
    return projection_;
  }

  // Return the ordinal index of the next row to be returned from
  // the iterator.
  rowid_t cur_ordinal_idx() const {
    return cur_idx_;
  }

  // Collect the IO statistics for each of the underlying columns.
  virtual void GetIteratorStats(std::vector<IteratorStats> *stats) const OVERRIDE;

  virtual ~Iterator();
 private:
  DISALLOW_COPY_AND_ASSIGN(Iterator);
  FRIEND_TEST(TestCFileSet, TestRangeScan);
  friend class CFileSet;

  // 'projection' must remain valid for the lifetime of this object.
  Iterator(std::shared_ptr<CFileSet const> base_data, SchemaPtr& projection,
           const fs::IOContext* io_context)
      : base_data_(std::move(base_data)),
        projection_(projection),
        initted_(false),
        cur_idx_(0),
        prepared_count_(0),
        io_context_(io_context) {}

  // Fill in col_iters_ for each of the requested columns.
  Status CreateColumnIterators(const ScanSpec* spec);

  // Look for a predicate which can be converted into a range scan using the key
  // column's index. If such a predicate exists, remove it from the scan spec and
  // store it in member fields.
  Status PushdownRangeScanPredicate(ScanSpec *spec);

  void Unprepare();

  // Prepare the given column. The column must not have been prepared yet.
  Status PrepareColumn(ColumnMaterializationContext *ctx);

  const std::shared_ptr<CFileSet const> base_data_;
  SchemaPtr projection_;

  // Iterator for the key column in the underlying data.
  std::unique_ptr<cfile::CFileIterator> key_iter_;
  std::vector<std::unique_ptr<cfile::ColumnIterator>> col_iters_;

  bool initted_;

  size_t cur_idx_;
  size_t prepared_count_;

  // The total number of rows in the file
  rowid_t row_count_;

  // Lower bound (inclusive) and upper bound (exclusive) for this iterator, in terms of
  // ordinal row indexes.
  // Both of these bounds are always set (even if there is no predicate).
  // If there is no predicate, then the bounds will be [0, row_count_]
  rowid_t lower_bound_idx_;
  rowid_t upper_bound_idx_;

  const fs::IOContext* io_context_;

  // The underlying columns are prepared lazily, so that if a column is never
  // materialized, it doesn't need to be read off disk.
  //
  // The list of columns which have been prepared (and thus must be 'finished'
  // at the end of the current batch). These are pointers into the same iterators
  // stored in 'col_iters_'.
  std::vector<cfile::ColumnIterator*> prepared_iters_;

};

} // namespace tablet
} // namespace kudu
#endif
