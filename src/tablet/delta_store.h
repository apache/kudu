// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTA_STORE_H
#define KUDU_TABLET_DELTA_STORE_H

#include "common/columnblock.h"
#include "common/schema.h"
#include "util/status.h"
#include "tablet/mvcc.h"

namespace kudu { namespace tablet {

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


} // namespace tablet
} // namespace kudu

#endif
