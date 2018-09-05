// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef KUDU_TABLET_DELTA_STORE_H
#define KUDU_TABLET_DELTA_STORE_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "kudu/common/rowid.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class ColumnBlock;
class ScanSpec;
class Schema;
class SelectionVector;
struct ColumnId;

namespace fs {
struct IOContext;
}  // namespace fs

namespace tablet {

class DeltaFileWriter;
class DeltaIterator;
class DeltaStats;
class Mutation;

// Interface for the pieces of the system that track deltas/updates.
// This is implemented by DeltaMemStore and by DeltaFileReader.
class DeltaStore {
 public:
  // Performs any post-construction work for the DeltaStore, which may
  // include additional I/O.
  virtual Status Init(const fs::IOContext* io_context) = 0;

  // Whether this delta store was initialized or not.
  virtual bool Initted() = 0;

  // Create a DeltaIterator for the given projection.
  //
  // The projection in 'opts' corresponds to whatever scan is currently ongoing.
  // All RowBlocks passed to this DeltaIterator must have this same schema.
  //
  // The snapshot in 'opts' is the MVCC state which determines which transactions
  // should be considered committed (and thus applied by the iterator).
  //
  // Returns Status::OK and sets 'iterator' to the new DeltaIterator, or
  // returns Status::NotFound if the mutations within this delta store
  // cannot include the snapshot.
  virtual Status NewDeltaIterator(const RowIteratorOptions& opts,
                                  DeltaIterator** iterator) const = 0;

  // Set *deleted to true if the latest update for the given row is a deletion.
  virtual Status CheckRowDeleted(rowid_t row_idx, const fs::IOContext* io_context,
                                 bool *deleted) const = 0;

  // Get the store's estimated size in bytes.
  virtual uint64_t EstimateSize() const = 0;

  virtual std::string ToString() const = 0;

  // TODO remove this once we don't need to have delta_stats for both DMS and DFR. Currently
  // DeltaTracker#GetColumnsIdxWithUpdates() needs to filter out DMS from the redo list but it
  // can't without RTTI.
  virtual const DeltaStats& delta_stats() const = 0;

  virtual ~DeltaStore() {}
};

typedef std::vector<std::shared_ptr<DeltaStore> > SharedDeltaStoreVector;

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

struct DeltaKeyAndUpdate {
  DeltaKey key;
  Slice cell;

  // Stringifies this DeltaKeyAndUpdate, according to 'schema'.
  //
  // If 'pad' is true, pads the delta row ids and txn ids in the output so that we can
  // compare two stringified representations and obtain the same result as comparing the DeltaKey
  // itself. That is, if 'pad' is true, then DeltaKey a < DeltaKey b => Stringify(a) < Stringify(b).
  std::string Stringify(DeltaType type, const Schema& schema, bool pad_key = false) const;
};

// Representation of deltas that have been "prepared" by an iterator. That is,
// they have been consistently read (at a snapshot) from their backing store
// into an in-memory format suitable for efficient retrieval.
class PreparedDeltas {
 public:
  // Applies the snapshotted updates to one of the columns.
  //
  // 'dst' must be the same length as was previously passed to PrepareBatch()
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_APPLY.
  virtual Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst) = 0;

  // Applies any deletes to the given selection vector.
  //
  // Rows which have been deleted in the associated MVCC snapshot are set to 0
  // in the selection vector so that they don't show up in the output.
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_APPLY.
  virtual Status ApplyDeletes(SelectionVector* sel_vec) = 0;

  // Collects the mutations associated with each row in the current prepared batch.
  //
  // Each entry in the vector will be treated as a singly linked list of Mutation
  // objects. If there are no mutations for that row, the entry will be unmodified.
  // If there are mutations, they will be prepended at the head of the linked list
  // (i.e the resulting list will be in descending timestamp order)
  //
  // The Mutation objects will be allocated out of the provided Arena, which must be non-NULL.
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_COLLECT.
  virtual Status CollectMutations(std::vector<Mutation*>* dst, Arena* arena) = 0;

  // Iterates through all deltas, adding deltas for columns not specified in
  // 'col_ids' to 'out'.
  //
  // Unlike CollectMutations, the iterator's MVCC snapshots are ignored; all
  // deltas are considered relevant.
  //
  // The delta objects will be allocated out the provided Arena, which must be non-NULL.
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_COLLECT.
  virtual Status FilterColumnIdsAndCollectDeltas(const std::vector<ColumnId>& col_ids,
                                                 std::vector<DeltaKeyAndUpdate>* out,
                                                 Arena* arena) = 0;

  // Returns true if there might exist deltas to be applied. It is safe to
  // conservatively return true, but this would force a skip over decoder-level
  // evaluation.
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_APPLY.
  virtual bool MayHaveDeltas() const = 0;
};

class DeltaIterator : public PreparedDeltas {
 public:
  // Initialize the iterator. This must be called once before any other
  // call.
  virtual Status Init(ScanSpec *spec) = 0;

  // Seek to a particular ordinal position in the delta data. This cancels any prepared
  // block, and must be called at least once prior to PrepareBatch().
  virtual Status SeekToOrdinal(rowid_t idx) = 0;

  // Argument to PrepareBatch(). See below.
  enum PrepareFlag {
    PREPARE_FOR_APPLY,
    PREPARE_FOR_COLLECT
  };

  // Prepare to apply deltas to a block of rows. This takes a consistent snapshot
  // of all updates to the next 'nrows' rows, so that subsequent calls to a
  // PreparedDeltas method will not cause any "tearing"/non-atomicity.
  //
  // 'flag' denotes whether the batch will be used for collecting mutations or
  // for applying them. Some implementations may choose to prepare differently.
  //
  // Each time this is called, the iterator is advanced by the full length
  // of the previously prepared block.
  virtual Status PrepareBatch(size_t nrows, PrepareFlag flag) = 0;

  // Returns true if there are any more rows left in this iterator.
  virtual bool HasNext() = 0;

  // Return a string representation suitable for debug printouts.
  virtual std::string ToString() const = 0;

  virtual ~DeltaIterator() {}
};

// Encapsulates all logic and responsibility related to "delta preparation";
// that is, the transformation of encoded deltas into an in-memory
// representation more suitable for efficient service during iteration.
//
// This class is intended to be composed inside a DeltaIterator. The iterator
// is responsible for loading encoded deltas from a backing store, passing them
// to the DeltaPreparer to be transformed, and later, calling the DeltaPreparer
// to serve the deltas.
class DeltaPreparer : public PreparedDeltas {
 public:
  explicit DeltaPreparer(RowIteratorOptions opts);

  // Updates internal state to reflect a seek performed by a DeltaIterator.
  //
  // Call upon completion of DeltaIterator::SeekToOrdinal.
  void Seek(rowid_t row_idx);

  // Updates internal state to reflect the beginning of delta batch preparation
  // on the part of a DeltaIterator.
  //
  // Call at the beginning of DeltaIterator::PrepareBatch.
  void Start(DeltaIterator::PrepareFlag flag);

  // Updates internal state to reflect the end of delta batch preparation on the
  // part of a DeltaIterator.
  //
  // Call at the end of DeltaIterator::PrepareBatch.
  void Finish(size_t nrows);

  // Prepares the delta given by 'key' whose encoded changes are pointed to by 'val'.
  //
  // Upon completion, it is safe for the memory behind 'val' to be destroyed.
  //
  // Call when a new delta becomes available in DeltaIterator::PrepareBatch.
  Status AddDelta(const DeltaKey& key, Slice val);

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst) override;

  Status ApplyDeletes(SelectionVector* sel_vec) override;

  Status CollectMutations(std::vector<Mutation*>* dst, Arena* arena) override;

  Status FilterColumnIdsAndCollectDeltas(const std::vector<ColumnId>& col_ids,
                                         std::vector<DeltaKeyAndUpdate>* out,
                                         Arena* arena) override;

  bool MayHaveDeltas() const override;

  rowid_t cur_prepared_idx() const { return cur_prepared_idx_; }

 private:
  // Options with which the DeltaPreparer was constructed.
  const RowIteratorOptions opts_;

  // The row index at which the most recent batch preparation ended.
  rowid_t cur_prepared_idx_;

  // The value of 'cur_prepared_idx_' from the previous batch.
  rowid_t prev_prepared_idx_;

  // Whether there are any prepared blocks.
  enum PreparedFor {
    // There are no prepared blocks. Attempts to call a PreparedDeltas function
    // will fail.
    NOT_PREPARED,

    // The DeltaPreparer has prepared a batch of deltas for applying. All deltas
    // in the batch have been decoded. UPDATEs and REINSERTs have been coalesced
    // into a column-major data structure suitable for ApplyUpdates. DELETES
    // have been coalesced into a row-major data structure suitable for ApplyDeletes.
    //
    // ApplyUpdates and ApplyDeltas are now callable.
    PREPARED_FOR_APPLY,

    // The DeltaPreparer has prepared a batch of deltas for collecting. Deltas
    // remain encoded and in the order that they were loaded from the backing store.
    //
    // CollectMutations and FilterColumnIdsAndCollectDeltas are now callable.
    PREPARED_FOR_COLLECT
  };
  PreparedFor prepared_for_;

  // State when prepared_for_ == PREPARED_FOR_APPLY
  // ------------------------------------------------------------
  struct ColumnUpdate {
    rowid_t row_id;
    void* new_val_ptr;
    uint8_t new_val_buf[16];
  };
  typedef std::deque<ColumnUpdate> UpdatesForColumn;
  std::vector<UpdatesForColumn> updates_by_col_;
  std::deque<rowid_t> deleted_;

  // State when prepared_for_ == PREPARED_FOR_COLLECT
  // ------------------------------------------------------------
  struct PreparedDelta {
    DeltaKey key;
    Slice val;
  };
  std::deque<PreparedDelta> prepared_deltas_;

  DISALLOW_COPY_AND_ASSIGN(DeltaPreparer);
};

enum { ITERATE_OVER_ALL_ROWS = 0 };

// Dumps contents of 'iter' to 'out', line-by-line.  Used to unit test
// minor delta compaction.
//
// If 'nrows' is ITERATE_OVER_ALL_ROWS, all rows will be dumped.
Status DebugDumpDeltaIterator(DeltaType type,
                              DeltaIterator* iter,
                              const Schema& schema,
                              size_t nrows,
                              std::vector<std::string>* out);

// Writes the contents of 'iter' to 'out', block by block.  Used by
// minor delta compaction.
//
// If 'nrows' is ITERATE_OVER_ALL_ROWS, all rows will be dumped.
template<DeltaType Type>
Status WriteDeltaIteratorToFile(DeltaIterator* iter,
                                size_t nrows,
                                DeltaFileWriter* out);

} // namespace tablet
} // namespace kudu

#endif
