// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTATRACKER_H
#define KUDU_TABLET_DELTATRACKER_H

#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <gtest/gtest.h>

#include "common/iterator.h"
#include "tablet/cfile_set.h"
#include "tablet/layer-interfaces.h"
#include "tablet/deltamemstore.h"
#include "tablet/deltafile.h"
#include "util/status.h"

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

class DeltaTracker : public boost::noncopyable {
public:
  DeltaTracker(Env *env,
               const Schema &schema,
               const string &dir);

  ColumnwiseIterator *WrapIterator(const shared_ptr<ColumnwiseIterator> &base) const;
  RowwiseIterator *WrapIterator(const shared_ptr<RowwiseIterator> &base) const;

  Status Open();
  Status Flush();

  // Update the given row in the database.
  // Copies the data, as well as any referenced
  // values into a local arena.
  void Update(rowid_t row_idx, const RowChangeList &update);

private:
  friend class Layer;
  FRIEND_TEST(TestLayer, TestLayerUpdate);
  FRIEND_TEST(TestLayer, TestDMSFlush);

  Status OpenDeltaFileReaders();
  Status FlushDMS(const DeltaMemStore &dms,
                  gscoped_ptr<DeltaFileReader> *dfr);
  void CollectTrackers(vector<shared_ptr<DeltaTrackerInterface> > *deltas) const;


  Env *env_;
  const Schema schema_;
  string dir_;

  bool open_;

  // The suffix to use on the next flushed deltafile. Delta files are named
  // delta_<N> to designate the order in which they were flushed.
  uint32_t next_deltafile_idx_;

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
// Delta-merging iterators
////////////////////////////////////////////////////////////

template<class IterClass>
class DeltaMerger : public IterClass, boost::noncopyable {
public:
  virtual Status Init(ScanSpec *spec) {
    RETURN_NOT_OK(base_iter_->Init(spec));
    BOOST_FOREACH(DeltaIteratorInterface &delta_iter, delta_iters_) {
      RETURN_NOT_OK(delta_iter.Init());
      RETURN_NOT_OK(delta_iter.SeekToOrdinal(0));
    }
    return Status::OK();
  }

  Status PrepareBatch(size_t *nrows);

  Status FinishBatch();

  bool HasNext() const {
    return base_iter_->HasNext();
  }

  string ToString() const {
    string s;
    s.append("DeltaMerger(");
    s.append(base_iter_->ToString());
    StringAppendF(&s, " + %zd delta trackers)", delta_iters_.size());
    return s;
  }

  const Schema &schema() const {
    return base_iter_->schema();
  }

  Status MaterializeColumn(size_t col_idx, ColumnBlock *dst);
  Status MaterializeBlock(RowBlock *dst);

private:
  friend class DeltaTracker;

  // Construct. The base_iter should not be Initted.
  DeltaMerger(const shared_ptr<IterClass> &base_iter,
              const vector<shared_ptr<DeltaTrackerInterface> > &delta_trackers) :
    base_iter_(base_iter)
  {
    BOOST_FOREACH(const shared_ptr<DeltaTrackerInterface> &tracker, delta_trackers) {
      delta_iters_.push_back(tracker->NewDeltaIterator(base_iter_->schema()));
    }
  }

  shared_ptr<IterClass> base_iter_;
  boost::ptr_vector<DeltaIteratorInterface> delta_iters_;
};


template<class T>
inline Status DeltaMerger<T>::PrepareBatch(size_t *nrows) {
  RETURN_NOT_OK(base_iter_->PrepareBatch(nrows));
  if (*nrows == 0) {
    return Status::NotFound("no more rows left");
  }

  // Prepare all the update trackers for this block.
  BOOST_FOREACH(DeltaIteratorInterface &iter, delta_iters_) {
    RETURN_NOT_OK(iter.PrepareBatch(*nrows));
  }

  return Status::OK();
}

template<class T>
inline Status DeltaMerger<T>::FinishBatch() {
  return base_iter_->FinishBatch();
}


////////////////////////////////////////////////////////////
// Column-wise delta merger
////////////////////////////////////////////////////////////

template<>
inline Status DeltaMerger<ColumnwiseIterator>::MaterializeColumn(size_t col_idx, ColumnBlock *dst) {
  ColumnwiseIterator *iter = down_cast<ColumnwiseIterator *>(base_iter_.get());
  // Copy the base data.
  RETURN_NOT_OK(iter->MaterializeColumn(col_idx, dst));

  // Apply all the updates for this column.
  BOOST_FOREACH(DeltaIteratorInterface &iter, delta_iters_) {
    RETURN_NOT_OK(iter.ApplyUpdates(col_idx, dst));
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////
// Row-wise delta merger
////////////////////////////////////////////////////////////
template<>
inline Status DeltaMerger<RowwiseIterator>::MaterializeBlock(RowBlock *dst) {
  // Get base data
  RETURN_NOT_OK(base_iter_->MaterializeBlock(dst));

  // Apply updates to all the columns.
  BOOST_FOREACH(DeltaIteratorInterface &iter, delta_iters_) {
    for (size_t proj_col_idx = 0; proj_col_idx < dst->schema().num_columns(); proj_col_idx++) {
      ColumnBlock dst_col = dst->column_block(proj_col_idx);
      RETURN_NOT_OK(iter.ApplyUpdates(proj_col_idx, &dst_col));
    }
  }

  return Status::OK();
}


} // namespace tablet
} // namespace kudu

#endif
