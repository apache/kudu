// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_LAYER_BASEDATA_H
#define KUDU_TABLET_LAYER_BASEDATA_H

#include <boost/noncopyable.hpp>
#include <string>
#include <tr1/memory>

#include "cfile/bloomfile.h"
#include "cfile/cfile_reader.h"

#include "common/iterator.h"
#include "common/schema.h"
#include "tablet/layer-interfaces.h"
#include "tablet/memstore.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/slice.h"

namespace kudu {

namespace tablet {

using boost::ptr_vector;
using kudu::cfile::BloomFileReader;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;
using std::tr1::shared_ptr;

// Set of CFiles which make up the base data for a single layer
//
// All of these files have the same number of rows, and thus the positional
// indexes can be used to seek to corresponding entries in each.
class CFileSet : public std::tr1::enable_shared_from_this<CFileSet>,
                 boost::noncopyable {
public:
  class Iterator;

  CFileSet(Env *env, const string &dir, const Schema &schema);

  Status OpenAllColumns();
  Status OpenKeyColumns();

  virtual Iterator *NewIterator(const Schema &projection) const;
  Status CountRows(size_t *count) const;
  uint64_t EstimateOnDiskSize() const;

  // Determine the index of the given row key.
  Status FindRow(const void *key, uint32_t *idx) const;

  const Schema &schema() const { return schema_; }

  string ToString() const {
    return string("CFile base data in ") + dir_;
  }

  virtual Status CheckRowPresent(const LayerKeyProbe &probe, bool *present) const;

  virtual ~CFileSet();

private:
  friend class Iterator;

  Status OpenColumns(size_t num_cols);
  Status OpenBloomReader();

  Status NewColumnIterator(size_t col_idx, CFileIterator **iter) const;

  Env *env_;
  const string dir_;
  const Schema schema_;

  vector<shared_ptr<CFileReader> > readers_;
  gscoped_ptr<BloomFileReader> bloom_reader_;
};


////////////////////////////////////////////////////////////

// Column-wise iterator implementation over a set of column files.
//
// This simply ties together underlying files so that they can be batched
// together, and iterated in parallel.
class CFileSet::Iterator : public ColumnwiseIterator, public boost::noncopyable {
public:
  virtual Status Init();

  // See BaseDataIteratorInterface
  virtual Status PrepareBatch(size_t *nrows);

  // See ColumnStoreBaseDataIterator
  virtual Status MaterializeColumn(size_t col_idx, ColumnBlock *dst);

  // See BaseDataIteratorInterface
  virtual Status FinishBatch();

  virtual bool HasNext() const {
    DCHECK(initted_);
    return col_iters_[0].HasNext();
  }

  virtual string ToString() const {
    return string("layer iterator for ") + base_data_->ToString();
  }

  const Schema &schema() const {
    return projection_;
  }

  // Collect the IO statistics for each of the underlying columns.
  void GetIOStatistics(vector<CFileIterator::IOStatistics> *stats);

private:
  friend class CFileSet;

  Iterator(const shared_ptr<CFileSet const> &base_data,
           const Schema &projection) :
    base_data_(base_data),
    projection_(projection),
    initted_(false),
    prepared_count_(0)
  {
    CHECK_OK(base_data_->CountRows(&row_count_));
  }

  Status SeekToOrdinal(uint32_t ord_idx);
  void Unprepare();

  // Prepare the given column if not already prepared.
  Status PrepareColumn(size_t col_idx);

  const shared_ptr<CFileSet const> base_data_;
  const Schema projection_;
  vector<size_t> projection_mapping_;

  // Iterator for the key column in the underlying data.
  gscoped_ptr<CFileIterator> key_iter_;
  ptr_vector<CFileIterator> col_iters_;

  bool initted_;

  size_t cur_idx_;
  size_t prepared_count_;

  // The total number of rows in the file
  size_t row_count_;

  // The underlying columns are prepared lazily, so that if a column is never
  // materialized, it doesn't need to be read off disk.
  vector<bool> cols_prepared_;

};

} // namespace tablet
} // namespace kudu
#endif
