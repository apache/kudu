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


// Base Data made up of a set of CFiles, one for each column.
class CFileBaseData : public std::tr1::enable_shared_from_this<CFileBaseData>,
                      boost::noncopyable {
public:
  CFileBaseData(Env *env, const string &dir, const Schema &schema);

  Status OpenAllColumns();
  Status OpenKeyColumns();

  virtual RowIteratorInterface *NewRowIterator(const Schema &projection) const;
  Status CountRows(size_t *count) const;
  uint64_t EstimateOnDiskSize() const;

  // Determine the index of the given row key.
  Status FindRow(const void *key, uint32_t *idx) const;

  const Schema &schema() const { return schema_; }

  string ToString() const {
    return string("CFile base data in ") + dir_;
  }

  virtual Status CheckRowPresent(const LayerKeyProbe &probe, bool *present) const;

private:
  class RowIterator;
  friend class RowIterator;

  Status OpenColumns(size_t num_cols);
  Status OpenBloomReader();

  Status NewColumnIterator(size_t col_idx, CFileIterator **iter) const;

  Env *env_;
  const string dir_;
  const Schema schema_;

  vector<shared_ptr<CFileReader> > readers_;
  gscoped_ptr<BloomFileReader> bloom_reader_;
};

// Iterator which yields the combined and projected rows from a
// subset of the columns.
class CFileBaseData::RowIterator : public RowIteratorInterface, boost::noncopyable {
public:
  virtual Status Init();

  // Get the next batch of rows from the iterator.
  // Retrieves up to 'nrows' rows, and writes back the number
  // of rows actually fetched into the same variable.
  // Any indirect data (eg strings) are allocated out of
  // the destination block's arena.
  Status CopyNextRows(size_t *nrows, RowBlock *dst);

  bool HasNext() const {
    DCHECK(initted_);
    return col_iters_[0].HasNext();
  }

  string ToString() const {
    return string("layer iterator for ") + base_data_->ToString();
  }

  const Schema &schema() const {
    return projection_;
  }

private:
  friend class CFileBaseData;

  RowIterator(const shared_ptr<CFileBaseData const> &base_data,
              const Schema &projection) :
    base_data_(base_data),
    projection_(projection),
    initted_(false)
  {}

  Status SeekToOrdinal(uint32_t ord_idx);

  const shared_ptr<CFileBaseData const> base_data_;
  const Schema projection_;
  vector<size_t> projection_mapping_;

  // Iterator for the key column in the underlying data.
  gscoped_ptr<CFileIterator> key_iter_;
  ptr_vector<CFileIterator> col_iters_;

  bool initted_;

};



} // namespace tablet
} // namespace kudu
#endif
