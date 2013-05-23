// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTATRACKER_H
#define KUDU_TABLET_DELTATRACKER_H

#include <boost/noncopyable.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest.h>

#include "common/iterator.h"
#include "tablet/cfile_set.h"
#include "tablet/rowset.h"
#include "util/status.h"

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

class DeltaIteratorInterface;

// Interface for the pieces of the system that track deltas/updates.
// This is implemented by DeltaMemStore and by DeltaFileReader.
class DeltaStore {
public:

  // Create a DeltaIteratorInterface for the given projection.
  //
  // The projection corresponds to whatever scan is currently ongoing.
  // All RowBlocks passed to this DeltaIterator must have this same schema.
  //
  // 'snapshot' is the MVCC state which determines which transactions
  // should be considered committed (and thus applied by the iterator).
  virtual DeltaIteratorInterface *NewDeltaIterator(const Schema &projection_,
                                                   const MvccSnapshot &snapshot) = 0;

  virtual ~DeltaStore() {}
};

// Iterator over deltas.
// For each rowset, this iterator is constructed alongside the base data iterator,
// and used to apply any updates which haven't been yet compacted into the base
// (i.e. those edits in the DeltaMemStore or in delta files)
//
// Typically this is used as follows:
//
//   Open iterator, seek to particular point in file
//   RowBlock rowblock;
//   foreach RowBlock in base data {
//     clear row block
//     CHECK_OK(iter->PrepareBatch(rowblock.size()));
//     ... read column 0 from base data into row block ...
//     CHECK_OK(iter->ApplyUpdates(0, rowblock.column(0))
//     ... check predicates for column ...
//     ... read another column from base data...
//     CHECK_OK(iter->ApplyUpdates(1, rowblock.column(1)))
//     ...
//  }
class DeltaIteratorInterface {
public:
  // Initialize the iterator. This must be called once before any other
  // call.
  virtual Status Init() = 0;

  // Seek to a particular ordinal position in the delta data. This cancels any prepared
  // block, and must be called at least once prior to PrepareToApply.
  virtual Status SeekToOrdinal(rowid_t idx) = 0;

  // Prepare to apply deltas to a block of rows. This takes a consistent snapshot
  // of all updates to the next 'nrows' rows, so that subsequent calls to
  // ApplyUpdates() will not cause any "tearing"/non-atomicity.
  //
  // Each time this is called, the iterator is advanced by the full length
  // of the previously prepared block.
  virtual Status PrepareBatch(size_t nrows) = 0;

  // Apply the snapshotted updates to one of the columns.
  // 'dst' must be the same length as was previously passed to PrepareToApply()
  virtual Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) = 0;

  // Collect the mutations associated with each row in the current prepared batch.
  //
  // Each entry in the vector will be treated as a singly linked list of Mutation
  // objects. If there are no mutations for that row, the entry will be unmodified.
  // If there are mutations, they will be appended at the tail of the linked list
  // (i.e in ascending txid order)
  //
  // The Mutation objects will be allocated out of the provided Arena, which must be non-NULL.
  virtual Status CollectMutations(vector<Mutation *> *dst, Arena *arena) = 0;

  // Return a string representation suitable for debug printouts.
  virtual string ToString() const = 0;

  virtual ~DeltaIteratorInterface() {}
};


class DeltaMemStore;
class DeltaFileReader;

class DeltaTracker : public boost::noncopyable {
public:
  DeltaTracker(Env *env,
               const Schema &schema,
               const string &dir);

  ColumnwiseIterator *WrapIterator(const shared_ptr<ColumnwiseIterator> &base,
                                   const MvccSnapshot &mvcc_snap) const;

  // TODO: this shouldn't need to return a shared_ptr, but there is some messiness
  // where this has bled around.
  shared_ptr<DeltaIteratorInterface> NewDeltaIterator(const Schema &schema,
                                                      const MvccSnapshot &snap) const;


  Status Open();
  Status Flush();

  // Update the given row in the database.
  // Copies the data, as well as any referenced
  // values into a local arena.
  void Update(txid_t txid, rowid_t row_idx, const RowChangeList &update);

private:
  friend class DiskRowSet;

  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);

  Status OpenDeltaFileReaders();
  Status FlushDMS(const DeltaMemStore &dms,
                  gscoped_ptr<DeltaFileReader> *dfr);
  void CollectTrackers(vector<shared_ptr<DeltaStore> > *deltas) const;

  Env *env_;
  const Schema schema_;
  string dir_;

  bool open_;

  // The suffix to use on the next flushed deltafile. Delta files are named
  // delta_<N> to designate the order in which they were flushed.
  uint32_t next_deltafile_idx_;

  // The current delta memrowset into which updates should be written.
  shared_ptr<DeltaMemStore> dms_;
  vector<shared_ptr<DeltaStore> > delta_trackers_;


  // read-write lock protecting dms_ and delta_trackers_.
  // - Readers and mutators take this lock in shared mode.
  // - Flushers take this lock in exclusive mode before they modify the
  //   structure of the rowset.
  //
  // TODO(perf): convert this to a reader-biased lock to avoid any cacheline
  // contention between threads.
  mutable boost::shared_mutex component_lock_;

};

// DeltaIteratorInterface that simply combines together other DeltaIteratorInterfaces,
// applying deltas from each in order.
class DeltaIteratorMerger : public DeltaIteratorInterface {
 public:
  // Create a new DeltaIteratorInterface which combines the deltas from
  // all of the input delta trackers.
  //
  // If only one tracker is input, this will automatically return an unwrapped
  // iterator for greater efficiency.
  static shared_ptr<DeltaIteratorInterface> Create(
    const vector<shared_ptr<DeltaStore> > &trackers,
    const Schema &projection,
    const MvccSnapshot &snapshot);

  ////////////////////////////////////////////////////////////
  // Implementations of DeltaIteratorInterface
  ////////////////////////////////////////////////////////////
  virtual Status Init();
  virtual Status SeekToOrdinal(rowid_t idx);
  virtual Status PrepareBatch(size_t nrows);
  virtual Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst);
  virtual Status CollectMutations(vector<Mutation *> *dst, Arena *arena);
  virtual string ToString() const;

 private:
  explicit DeltaIteratorMerger(const vector<shared_ptr<DeltaIteratorInterface> > &iters);

  vector<shared_ptr<DeltaIteratorInterface> > iters_;
};


////////////////////////////////////////////////////////////
// Delta-applying iterators
////////////////////////////////////////////////////////////

// A DeltaApplier takes in a base ColumnwiseIterator along with a a
// DeltaIterator. It is responsible for applying the updates coming
// from the delta iterator to the results of the base iterator.
class DeltaApplier : public ColumnwiseIterator, boost::noncopyable {
public:
  virtual Status Init(ScanSpec *spec) {
    RETURN_NOT_OK(base_iter_->Init(spec));
    RETURN_NOT_OK(delta_iter_->Init());
    RETURN_NOT_OK(delta_iter_->SeekToOrdinal(0));
    return Status::OK();
  }

  Status PrepareBatch(size_t *nrows);

  Status FinishBatch();

  bool HasNext() const {
    return base_iter_->HasNext();
  }

  string ToString() const {
    string s;
    s.append("DeltaApplier(");
    s.append(base_iter_->ToString());
    s.append(" + ");
    s.append(delta_iter_->ToString());
    s.append(")");
    return s;
  }

  const Schema &schema() const {
    return base_iter_->schema();
  }

  Status MaterializeColumn(size_t col_idx, ColumnBlock *dst);
private:
  friend class DeltaTracker;

  // Construct. The base_iter and delta_iter should not be Initted.
  DeltaApplier(const shared_ptr<ColumnwiseIterator> &base_iter,
               const shared_ptr<DeltaIteratorInterface> delta_iter) :
    base_iter_(base_iter),
    delta_iter_(delta_iter)
  {
  }

  shared_ptr<ColumnwiseIterator> base_iter_;
  shared_ptr<DeltaIteratorInterface> delta_iter_;
};


inline Status DeltaApplier::PrepareBatch(size_t *nrows) {
  RETURN_NOT_OK(base_iter_->PrepareBatch(nrows));
  if (*nrows == 0) {
    return Status::NotFound("no more rows left");
  }

  RETURN_NOT_OK(delta_iter_->PrepareBatch(*nrows));
  return Status::OK();
}

inline Status DeltaApplier::FinishBatch() {
  return base_iter_->FinishBatch();
}

inline Status DeltaApplier::MaterializeColumn(size_t col_idx, ColumnBlock *dst) {
  // Copy the base data.
  RETURN_NOT_OK(base_iter_->MaterializeColumn(col_idx, dst));

  // Apply all the updates for this column.
  RETURN_NOT_OK(delta_iter_->ApplyUpdates(col_idx, dst));
  return Status::OK();
}

} // namespace tablet
} // namespace kudu

#endif
