// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_LAYER_BASEDATA_H
#define KUDU_TABLET_LAYER_BASEDATA_H

#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <string>

#include "cfile/cfile_reader.h"
#include "common/iterator.h"
#include "common/schema.h"
#include "tablet/layer-interfaces.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/slice.h"

namespace kudu {

namespace cfile {
class CFileReader;
class CFileIterator;
}

namespace tablet {

using boost::ptr_vector;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;

class LayerBaseData {
public:
  class UpdateStatus;

  virtual RowIteratorInterface *NewRowIterator(const Schema &projection) const = 0;
  virtual Status CountRows(size_t *count) const = 0;

  // Return true if this layer's base data can be updated in-place.
  //
  // If this returns true, then Update() on the same object must
  // succeed (given a valid row key). In this case, FindRow()
  // does not have to be supported.
  //
  // If this returns false, then FindRow must be supported, and
  // Update() may return NotSupported.
  virtual bool is_updatable_in_place() const = 0;

  // Update the given row.
  //
  // See is_updatable_in_place() for restrictions on when this may be used.
  virtual Status UpdateRow(const void *key, const RowDelta &update) {
    return Status::NotSupported("");
  }

  // Determine the index of the given row key.
  //
  // See is_updatable_in_place() for restrictions on when this may be used.
  virtual Status FindRow(const void *key, uint32_t *idx) const {
    return Status::NotSupported("");
  }

  // Determine whether the given row is present in the base data.
  virtual Status CheckRowPresent(const void *key, bool *present) const = 0;
};

class CFileBaseData : public LayerBaseData, boost::noncopyable {
public:
  CFileBaseData(Env *env, const string &dir, const Schema &schema);

  Status Open();
  virtual RowIteratorInterface *NewRowIterator(const Schema &projection) const;
  Status NewColumnIterator(size_t col_idx, CFileIterator **iter) const;
  Status CountRows(size_t *count) const;
  Status FindRow(const void *key, uint32_t *idx) const;

  const Schema &schema() const { return schema_; }

  bool is_updatable_in_place() const {
    return false;
  }

  string ToString() const {
    return string("CFile base data in ") + dir_;
  }

  virtual Status CheckRowPresent(const void *key, bool *present) const;

private:
  class RowIterator;

  Env *env_;
  const string dir_;
  const Schema schema_;

  bool open_;

  ptr_vector<CFileReader> readers_;
};


// Iterator which yields the combined and projected rows from a
// subset of the columns.
class CFileBaseData::RowIterator : public RowIteratorInterface, boost::noncopyable {
public:
  virtual Status Init();

  // Seek to a given key in the underlying data.
  // Note that the 'key' must correspond to the key in the
  // Layer's schema, not the projection schema.
  virtual Status SeekAtOrAfter(const Slice &key, bool *exact) {
    // Allow the special empty key to seek to the start of the iterator.
    if (key.size() == 0) {
      return SeekToOrdinal(0);
    }

    // Otherwise, must seek to a valid key.
    CHECK_GE(key.size(), base_data_->schema().key_byte_size());
    return Status::NotSupported("TODO implement me");
  }

  Status SeekToOrdinal(uint32_t ord_idx) {
    DCHECK(initted_);
    BOOST_FOREACH(CFileIterator &col_iter, col_iters_) {
      RETURN_NOT_OK(col_iter.SeekToOrdinal(ord_idx));
    }

    return Status::OK();
  }

  // Get the next batch of rows from the iterator.
  // Retrieves up to 'nrows' rows, and writes back the number
  // of rows actually fetched into the same variable.
  // Any indirect data (eg strings) are allocated out of
  // 'dst_arena'
  Status CopyNextRows(size_t *nrows,
                      uint8_t *dst,
                      Arena *dst_arena);

  bool HasNext() const {
    DCHECK(initted_);
    return col_iters_[0].HasNext();
  }

  string ToString() const {
    return string("layer iterator for ") + base_data_->ToString();
  }

private:
  friend class CFileBaseData;

  RowIterator(const CFileBaseData *base_data,
              const Schema &projection) :
    base_data_(base_data),
    projection_(projection),
    initted_(false)
  {}

  const CFileBaseData *base_data_;
  const Schema projection_;
  vector<size_t> projection_mapping_;

  // Iterator for the key column in the underlying data.
  scoped_ptr<CFileIterator> key_iter_;
  ptr_vector<CFileIterator> col_iters_;

  bool initted_;

};



} // namespace tablet
} // namespace kudu
#endif
