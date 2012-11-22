// Copyright (c) 2012, Cloudera, inc.
//
// A Layer is a horizontal slice of a Kudu tablet.
// Each Layer contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_LAYER_H
#define KUDU_TABLET_LAYER_H

#include <boost/ptr_container/ptr_vector.hpp>
#include <string>

#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "common/row.h"
#include "common/schema.h"
#include "util/memory/arena.h"

namespace kudu {

class Env;

namespace tablet {

using boost::ptr_vector;
using std::string;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;

class LayerWriter : boost::noncopyable {
public:
  LayerWriter(Env *env,
              const Schema &schema,
              const string &layer_dir) :
    env_(env),
    schema_(schema),
    dir_(layer_dir)
  {}

  Status Open();

  Status WriteRow(const Slice &row) {
    DCHECK_EQ(row.size(), schema_.byte_size());

    for (int i = 0; i < schema_.num_columns(); i++) {
      int off = schema_.column_offset(i);
      const void *p = row.data() + off;
      RETURN_NOT_OK( cfile_writers_[i].AppendEntries(p, 1) );
    }

    return Status::OK();
  }

  Status Finish();

private:
  Env *env_;
  const Schema schema_;
  const string dir_;

  ptr_vector<cfile::Writer> cfile_writers_;
};


class Layer : boost::noncopyable {
public:
  class RowIterator;

  // TODO: should 'schema' be stored with the layer? quite likely
  // so that we can support cheap alter table.
  Layer(Env *env,
        const Schema &schema,
        const string &layer_dir) :
    env_(env),
    schema_(schema),
    dir_(layer_dir),
    open_(false)
  {}

  Status NewColumnIterator(size_t col_idx,
                           CFileIterator **iter) const;

  RowIterator *NewRowIterator(const Schema &projection) const;

  Status Open();

  const Schema &schema() const {
    return schema_;
  }

private:
  friend class RowIterator;

  Env *env_;
  const Schema schema_;
  const string dir_;

  bool open_;
  ptr_vector<cfile::CFileReader> cfile_readers_;
};


class Layer::RowIterator : boost::noncopyable {
public:

  Status Init();

  // Seek to a given key in the underlying data.
  // Note that the 'key' must correspond to the key in the
  // Layer's schema, not the projection schema.
  Status SeekAtOrAfter(const Slice &key) {
    CHECK_GE(key.size(), reader_->schema().key_byte_size());
    CHECK(false) << "TODO: implement me";
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
                      char *dst,
                      Arena *dst_arena);

  bool HasNext() {
    DCHECK(initted_);
    return col_iters_[0].HasNext();
  }

private:
  friend class Layer;

  RowIterator(const Layer *reader,
              const Schema &projection) :
    reader_(reader),
    projection_(projection),
    initted_(false)
  {}

  const Layer *reader_;
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
