// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTATRACKER_H
#define KUDU_TABLET_DELTATRACKER_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "common/iterator.h"
#include "gutil/macros.h"
#include "server/metadata.h"
#include "tablet/delta_store.h"
#include "util/status.h"

namespace kudu {

class Env;

namespace metadata {
class RowSetMetadata;
}

namespace tablet {

using std::tr1::shared_ptr;

class DeltaMemStore;
class DeltaFileReader;
class MutationResultPB;
class MutationTargetPB;
class DeltaCompactionInput;

// The DeltaTracker is the part of a DiskRowSet which is responsible for
// tracking modifications against the base data. It consists of a set of
// DeltaStores which each contain a set of mutations against the base data.
// These DeltaStores may be on disk (DeltaFileReader) or in-memory (DeltaMemStore).
//
// This class is also responsible for flushing the in-memory deltas to disk.
class DeltaTracker {
 public:
  DeltaTracker(const shared_ptr<metadata::RowSetMetadata>& rowset_metadata,
               const Schema &schema,
               rowid_t num_rows);

  ColumnwiseIterator *WrapIterator(const shared_ptr<ColumnwiseIterator> &base,
                                   const MvccSnapshot &mvcc_snap) const;

  // TODO: this shouldn't need to return a shared_ptr, but there is some messiness
  // where this has bled around.
  shared_ptr<DeltaIterator> NewDeltaIterator(const Schema &schema,
                                                      const MvccSnapshot &snap) const;


  Status Open();
  Status Flush();

  // Update the given row in the database.
  // Copies the data, as well as any referenced values into a local arena.
  // "result" tracks the status of the update as well as which data
  // structure(s) it ended up at.
  Status Update(txid_t txid,
                rowid_t row_idx,
                const Schema& update_schema,
                const RowChangeList &update,
                MutationResultPB* result);

  // Check if the given row has been deleted -- i.e if the most recent
  // delta for this row is a deletion.
  //
  // Sets *deleted to true if so; otherwise sets it to false.
  Status CheckRowDeleted(rowid_t row_idx, bool *deleted) const;

  // Compacts all deltafiles
  //
  // TODO keep metadata in the delta stores to indicate whether or not
  // a minor (or -- when implemented -- major) compaction is warranted
  // and if so, compact the stores.
  Status Compact();

  // Performs minor compaction on all delta files between index
  // "start_idx" and "end_idx" (inclusive) and writes this to a
  // new delta block. If "end_idx" is set to -1, then delta files at
  // all indexes starting with "start_idx" will be compacted.
  Status CompactStores(int start_idx, int end_idx);

  // Return the number of rows encompassed by this DeltaTracker. Note that
  // this is _not_ the number of updated rows, but rather the number of rows
  // in the associated CFileSet base data. All updates must have a rowid
  // strictly less than num_rows().
  int64_t num_rows() const { return num_rows_; }

 private:
  friend class DiskRowSet;

  DISALLOW_COPY_AND_ASSIGN(DeltaTracker);

  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);
  FRIEND_TEST(TestRowSet, TestMakeDeltaCompactionInput);
  FRIEND_TEST(TestRowSet, TestCompactStores);

  Status OpenDeltaFileReaders();
  Status FlushDMS(const DeltaMemStore &dms,
                  gscoped_ptr<DeltaFileReader> *dfr);

  void CollectStores(vector<shared_ptr<DeltaStore> > *stores) const;

  // If delta stores in delta_store_ at indexes "start_idx" to "end_idx" (inclusive) match
  // delta stores in in "expected_stores", remove the specified delta stores and replace them
  // with the "new_store"; otherwise, crashes with a FATAL error message.
  Status AtomicUpdateStores(size_t start_idx, size_t end_idx,
                            const vector<shared_ptr<DeltaStore> > &expected_stores,
                            gscoped_ptr<DeltaFileReader> new_store);

  // Performs the actual compaction. Results of compaction are written to "data_writer",
  // while delta stores that underwent compaction are appended to "compacted_stores", while
  // their corresponding ids are appended to "compacted_ids".
  Status DoCompactStores(size_t start_idx, size_t end_idx,
                         const shared_ptr<WritableFile> &data_writer,
                         vector<shared_ptr<DeltaStore> > *compacted_stores,
                         vector<int64_t> *compacted_ids);

  // Creates a merged compaction input and captures the delta stores and delta file ids
  // under compaction.
  Status MakeCompactionInput(size_t start_idx, size_t end_idx,
                             vector<shared_ptr<DeltaStore > > *target_stores,
                             vector<int64_t> *target_ids,
                             gscoped_ptr<DeltaCompactionInput> *out);

  shared_ptr<metadata::RowSetMetadata> rowset_metadata_;
  const Schema schema_;

  // The number of rows in the DiskRowSet that this tracker is associated with.
  // This is just used for assertions to make sure that we don't update a row
  // which doesn't exist.
  rowid_t num_rows_;

  bool open_;

  // The current DeltaMemStore into which updates should be written.
  shared_ptr<DeltaMemStore> dms_;
  vector<shared_ptr<DeltaStore> > delta_stores_;

  // read-write lock protecting dms_ and delta_stores_.
  // - Readers and mutators take this lock in shared mode.
  // - Flushers take this lock in exclusive mode before they modify the
  //   structure of the rowset.
  //
  // TODO(perf): convert this to a reader-biased lock to avoid any cacheline
  // contention between threads.
  mutable boost::shared_mutex component_lock_;

  // Exclusive lock that ensures that only one flush or compaction can run
  // at a time. Protects delta_stores_. NOTE: this lock cannot be acquired
  // while component_lock is held: otherwise, Flush and Compaction threads
  // (that both first acquire this lock and then component_lock) will deadlock.
  //
  // TODO(perf): this needs to be more fine grained
  mutable boost::mutex flush_or_compact_lock_;
};


////////////////////////////////////////////////////////////
// Delta-applying iterators
////////////////////////////////////////////////////////////

// A DeltaApplier takes in a base ColumnwiseIterator along with a a
// DeltaIterator. It is responsible for applying the updates coming
// from the delta iterator to the results of the base iterator.
class DeltaApplier : public ColumnwiseIterator {
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

  // Initialize the selection vector for the current batch.
  // This processes DELETEs -- any deleted rows are set to 0 in 'sel_vec'.
  // All other rows are set to 1.
  virtual Status InitializeSelectionVector(SelectionVector *sel_vec);

  Status MaterializeColumn(size_t col_idx, ColumnBlock *dst);
 private:
  friend class DeltaTracker;

  DISALLOW_COPY_AND_ASSIGN(DeltaApplier);

  // Construct. The base_iter and delta_iter should not be Initted.
  DeltaApplier(const shared_ptr<ColumnwiseIterator> &base_iter,
               const shared_ptr<DeltaIterator> delta_iter)
    : base_iter_(base_iter),
      delta_iter_(delta_iter) {
  }

  shared_ptr<ColumnwiseIterator> base_iter_;
  shared_ptr<DeltaIterator> delta_iter_;
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

inline Status DeltaApplier::InitializeSelectionVector(SelectionVector *sel_vec) {
  RETURN_NOT_OK(base_iter_->InitializeSelectionVector(sel_vec));
  return delta_iter_->ApplyDeletes(sel_vec);
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
