// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_COMPACTION_H
#define KUDU_TABLET_COMPACTION_H

#include <boost/noncopyable.hpp>

#include "common/generic_iterators.h"
#include "common/iterator.h"
#include "tablet/diskrowset.h"
#include "tablet/memstore.h"

namespace kudu {
namespace tablet {

struct CompactionInputRow;

// Interface for an input feeding into a compaction or flush.
class CompactionInput {
 public:
  // Create an input which reads from the given rowset, yielding base rows and updates
  // prior to the given snapshot.
  static CompactionInput *Create(const DiskRowSet &rowset, const MvccSnapshot &snap);

  // Create an input which reads from the given memstore, yielding base rows and updates
  // prior to the given snapshot.
  static CompactionInput *Create(const MemStore &memstore, const MvccSnapshot &snap);

  // Create an input which merges several other compaction inputs. The inputs are merged
  // in key-order according to the given schema. All inputs must have matching schemas.
  static CompactionInput *Merge(const vector<shared_ptr<CompactionInput> > &inputs,
                                const Schema &schema);

  virtual Status Init() = 0;
  virtual Status PrepareBlock(vector<CompactionInputRow> *block) = 0;
  virtual Status FinishBlock() = 0;

  virtual bool HasMoreBlocks() = 0;
  virtual const Schema &schema() const = 0;

  virtual ~CompactionInput() {}
};

// The set of rowsets which are taking part in a given compaction.
class RowSetsInCompaction {
 public:
  void AddRowSet(const shared_ptr<RowSet> &rowset,
                const shared_ptr<boost::mutex::scoped_try_lock> &lock) {
    CHECK(lock->owns_lock());

    locks_.push_back(lock);
    rowsets_.push_back(rowset);
  }

  // Create the appropriate compaction input for this compaction -- either a merge
  // of all the inputs, or the single input if there was only one.
  Status CreateCompactionInput(const MvccSnapshot &snap, const Schema &schema,
                               shared_ptr<CompactionInput> *out) const;

  // Dump a log message indicating the chosen rowsets.
  void DumpToLog() const;

  const RowSetVector &rowsets() const { return rowsets_; }

  size_t num_rowsets() const {
    return rowsets_.size();
  }

 private:
  typedef vector<shared_ptr<boost::mutex::scoped_try_lock> > LockVector;

  RowSetVector rowsets_;
  LockVector locks_;
};


// One row yielded by CompactionInput::PrepareBlock.
struct CompactionInputRow {
  void *row_ptr;
  Mutation *mutation_head;
};

// Iterate through this compaction input, flushing all rows to the given RowSetWriter.
//
// After return of this function, this CompactionInput object is "used up" and will
// no longer be useful.
//
// TODO: when we support actually flushing UNDO files, this will also have to take
// a delta file writer.
Status Flush(CompactionInput *input, RowSetWriter *out);

// Iterate through this compaction input, finding any mutations which came between
// snap_to_exclude and snap_to_include (ie those transactions that were not yet
// committed in 'snap_to_exclude' but _are_ committed in 'snap_to_include'). For
// each such mutation, propagate it into the given delta_tracker.
//
// After return of this function, this CompactionInput object is "used up" and will
// yield no further rows.
Status ReupdateMissedDeltas(CompactionInput *input,
                            const MvccSnapshot &snap_to_exclude,
                            const MvccSnapshot &snap_to_include,
                            DeltaTracker *delta_tracker);


} // namespace tablet
} // namespace kudu

#endif
