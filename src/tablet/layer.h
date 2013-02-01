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
#include "common/rowblock.h"
#include "common/schema.h"
#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
#include "tablet/layer-basedata.h"
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
    dir_(layer_dir),
    finished_(false),
    column_flushed_counts_(schema.num_columns(), 0)
  {}

  Status Open();

  // TODO: doc me
  Status FlushProjection(const Schema &projection,
                         RowIteratorInterface *src_iter);

  Status WriteRow(const Slice &row) {
    CHECK(!finished_);
    DCHECK_EQ(row.size(), schema_.byte_size());

    for (int i = 0; i < schema_.num_columns(); i++) {
      int off = schema_.column_offset(i);
      const void *p = row.data() + off;
      RETURN_NOT_OK( cfile_writers_[i].AppendEntries(p, 1, 0) );

      column_flushed_counts_[i]++;
    }

    return Status::OK();
  }

  Status Finish();

  size_t written_count() const {
    CHECK(finished_);
    return column_flushed_counts_[0];
  }


private:
  Env *env_;
  const Schema schema_;
  const string dir_;
  bool finished_;

  ptr_vector<cfile::Writer> cfile_writers_;
  vector<size_t> column_flushed_counts_;
};

////////////////////////////////////////////////////////////
// Layer
////////////////////////////////////////////////////////////

class Layer : public LayerInterface, boost::noncopyable {
public:

  // Open a layer from disk.
  // If successful, sets *layer to the newly open layer
  static Status Open(Env *env,
                     const Schema &schema,
                     const string &layer_dir,
                     Layer **layer);

  // TODO: docme
  static Status CreatePartiallyFlushed(
    Env *env,
    const Schema &schema,
    const string &layer_dir,
    shared_ptr<MemStore> &memstore,
    Layer **layer);

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
  RowIteratorInterface *NewRowIterator(const Schema &projection) const;


  // Count the number of rows in this layer.
  Status CountRows(size_t *count) const;

  const Schema &schema() const {
    return schema_;
  }

  string ToString() const {
    return dir_;
  }

  static string GetColumnPath(const string &dir, int col_idx);
  static string GetDeltaPath(const string &dir, int delta_idx);

private:
  FRIEND_TEST(TestLayer, TestLayerUpdate);
  FRIEND_TEST(TestLayer, TestDMSFlush);

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
  scoped_ptr<LayerBaseData> base_data_;

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

////////////////////////////////////////////////////////////
// DeltaMergingIterator
////////////////////////////////////////////////////////////


// Iterator over materialized and projected rows of a given
// layer. This is an "early materialization" iterator.
// TODO: this might get replaced by an operator which takes
// multiple column iterators and materializes them, but perhaps
// this can actually be more efficient.
class DeltaMergingIterator : public RowIteratorInterface, boost::noncopyable {
public:
  virtual Status Init();

  // Seek to a given key in the underlying data.
  // Note that the 'key' must correspond to the key in the
  // Layer's schema, not the projection schema.
  virtual Status SeekAtOrAfter(const Slice &key, bool *exact) {
    CHECK_EQ(key.size(), 0)
      << "TODO: cant seek the merging iterator at the moment: "
      << "need to plumb the ordinal indexes back up so deltas "
      << "can be applied after seek!";
    return base_iter_->SeekAtOrAfter(key, exact);
  }

  // Get the next batch of rows from the iterator.
  // Retrieves up to 'nrows' rows, and writes back the number
  // of rows actually fetched into the same variable.
  // Any indirect data (eg strings) are allocated out of
  // 'dst_arena'
  Status CopyNextRows(size_t *nrows, RowBlock *dst);

  bool HasNext() const {
    return base_iter_->HasNext();
  }

  string ToString() const {
    return string("delta merging iterator");
  }

  const Schema &schema() const {
    return projection_;
  }

private:
  friend class Layer;

  DeltaMergingIterator(RowIteratorInterface *base_iter,
                       const vector<shared_ptr<DeltaTrackerInterface> > &delta_trackers,
                       const Schema &src_schema,
                       const Schema &projection) :
    base_iter_(base_iter),
    delta_trackers_(delta_trackers),
    src_schema_(src_schema),
    projection_(projection),
    cur_row_(0)
  {}

  // Iterator for the key column in the underlying data.
  scoped_ptr<RowIteratorInterface> base_iter_;
  vector<shared_ptr<DeltaTrackerInterface> > delta_trackers_;

  const Schema src_schema_;
  const Schema projection_;
  vector<size_t> projection_mapping_;

  size_t cur_row_;
};

} // namespace tablet
} // namespace kudu

#endif
