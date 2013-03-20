// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTATRACKER_H
#define KUDU_TABLET_DELTATRACKER_H

#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <gtest/gtest.h>

#include "common/iterator.h"
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

  RowIteratorInterface *WrapIterator(const shared_ptr<RowIteratorInterface> &base) const;

  Status Open();
  Status Flush();

  // Update the given row in the database.
  // Copies the data, as well as any referenced
  // values into a local arena.
  void Update(uint32_t row_idx, const RowChangeList &update);

private:
  friend class Layer;
  FRIEND_TEST(TestLayer, TestLayerUpdate);
  FRIEND_TEST(TestLayer, TestDMSFlush);

  Status OpenDeltaFileReaders();
  Status FlushDMS(const DeltaMemStore &dms,
                  gscoped_ptr<DeltaFileReader> *dfr);


  Env *env_;
  const Schema schema_;
  string dir_;

  bool open_;

  uint32_t next_delta_idx_;

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
class DeltaMergingIterator : public RowIteratorInterface, boost::noncopyable {
public:
  virtual Status Init();

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
    return base_iter_->schema();
  }

private:
  friend class DeltaTracker;

  // Construct. The base_iter should not be Initted.
  DeltaMergingIterator(const shared_ptr<RowIteratorInterface> &base_iter,
                       const vector<shared_ptr<DeltaTrackerInterface> > &delta_trackers);

  // Iterator for the key column in the underlying data.
  shared_ptr<RowIteratorInterface> base_iter_;
  boost::ptr_vector<DeltaIteratorInterface> delta_iters_;
};

} // namespace tablet
} // namespace kudu

#endif
