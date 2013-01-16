// Copyright (c) 2012, Cloudera, inc.
//
// A Layer is a horizontal slice of a Kudu tablet.
// Each Layer contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_LAYER_H
#define KUDU_TABLET_LAYER_H

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest.h>
#include <string>
#include <memory>

#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "common/row.h"
#include "common/schema.h"
#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
#include "util/memory/arena.h"

namespace kudu {

class Env;

namespace tablet {

using boost::ptr_vector;
using std::string;
using std::auto_ptr;
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


class Layer : public LayerInterface, boost::noncopyable {
public:
  class RowIterator;
  class ColumnIterator;

  // TODO: should 'schema' be stored with the layer? quite likely
  // so that we can support cheap alter table.
  Layer(Env *env,
        const Schema &schema,
        const string &layer_dir) :
    env_(env),
    schema_(schema),
    dir_(layer_dir),
    next_delta_idx_(0),
    open_(false),
    dms_(new DeltaMemStore(schema))
  {}

  Status Open();

  ////////////////////////////////////////////////////////////
  // "Management" functions
  ////////////////////////////////////////////////////////////

  // Flush all accumulated delta data from the DeltaMemStore to disk.
  Status FlushDeltas();


  ////////////////////////////////////////////////////////////
  // LayerInterface implementation
  ////////////////////////////////////////////////////////////

  ////////////////////
  // Updates
  ////////////////////
  Status UpdateRow(const void *key,
                   const RowDelta &update);

  Status CheckRowPresent(const void *key, bool *present) const;

  ////////////////////
  // Read functions.
  ////////////////////
  Status NewColumnIterator(size_t col_idx,
                           ColumnIterator **iter) const;

  RowIteratorInterface *NewRowIterator(const Schema &projection) const;


  // Count the number of rows in this layer.
  Status CountRows(size_t *count) const;

  const Schema &schema() const {
    return schema_;
  }

  string ToString() const {
    return dir_;
  }

private:
  FRIEND_TEST(TestLayer, TestLayerUpdate);
  FRIEND_TEST(TestLayer, TestDMSFlush);
  friend class RowIterator;
  friend class ColumnIterator;

  // Return an iterator over the un-updated data for one of the columns
  // in this layer. This iterator _does not_ reflect updates.
  // Use NewColumnIterator to create an iterator which reflects updates.
  //
  // Upon return, the iterator has been Initted and is ready for use.
  Status NewBaseColumnIterator(size_t col_idx,
                               CFileIterator **iter) const;
  Status NewBaseColumnIterator(size_t col_idx,
                               scoped_ptr<CFileIterator> *iter) const {
    CFileIterator *iter_ptr;
    RETURN_NOT_OK(NewBaseColumnIterator(col_idx, &iter_ptr));
    iter->reset(iter_ptr);
    return Status::OK();
  }

  Status OpenBaseCFileReaders();
  Status OpenDeltaFileReaders();

  Status FlushDMS(const DeltaMemStore &dms,
                  DeltaFileReader **dfr);

  Env *env_;
  const Schema schema_;
  const string dir_;
  uint32_t next_delta_idx_;

  bool open_;

  // Base data for this layer.
  // This vector contains one entry for each column.
  ptr_vector<cfile::CFileReader> cfile_readers_;

  // The current delta memstore into which updates should be written.
  shared_ptr<DeltaMemStore> dms_;
  vector<shared_ptr<DeltaTrackerInterface> > delta_trackers_;

  // read-write lock protecting dms_ and delta_trackers_.
  // - Readers and mutators take this lock in shared mode.
  // - Flushers take this lock in exclusive mode before they modify the
  //   structure of the layer.
  //
  // TODO(perf): convert this to a reader-biased lock to avoid any cacheline
  // contention between threads.
  mutable boost::shared_mutex component_lock_;

};

// Iterator over a column in a layer, with deltas applied.
class Layer::ColumnIterator : public boost::noncopyable {
public:
  Status SeekToOrdinal(uint32_t ord_idx);
  Status SeekAtOrAfter(const void *key, bool *exact_match);
  uint32_t GetCurrentOrdinal() const;
  Status CopyNextValues(size_t *n, ColumnBlock *dst);
  bool HasNext() const;
private:
  friend class Layer;

  // Create an iterator which yields updated rows from
  // a given column.
  ColumnIterator(const Layer *layer,
                 size_t col_idx);

  Status Init();

  const Layer *layer_;
  const size_t col_idx_;

  // Iterator over the base (i.e unmodified)
  scoped_ptr<CFileIterator> base_iter_;
};


// Iterator over materialized and projected rows of a given
// layer. This is an "early materialization" iterator.
// TODO: this might get replaced by an operator which takes
// multiple column iterators and materializes them, but perhaps
// this can actually be more efficient.
class Layer::RowIterator : public RowIteratorInterface, boost::noncopyable {
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
    CHECK_GE(key.size(), reader_->schema().key_byte_size());
    CHECK(false) << "TODO: implement me";
  }

  Status SeekToOrdinal(uint32_t ord_idx) {
    DCHECK(initted_);
    BOOST_FOREACH(ColumnIterator &col_iter, col_iters_) {
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
    return string("layer iterator for ") + reader_->ToString();
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
  ptr_vector<ColumnIterator> col_iters_;

  bool initted_;

};

} // namespace tablet
} // namespace kudu

#endif
