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
#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/row_changelist.h"
#include "kudu/common/rowid.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
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

// Tracks deltas that have been selected by PreparedDeltas::SelectDeltas.
//
// May track deltas belonging to a single delta store, or to multiple stores
// whose SelectedDeltas have been merged together.
class SelectedDeltas {
 public:
  SelectedDeltas() = default;

  // Equivalent to calling:
  //
  //   SelectedDeltas sd;
  //   sd.Reset(nrows);
  explicit SelectedDeltas(size_t nrows);

  // Converts the selected deltas into a simpler SelectionVector.
  void ToSelectionVector(SelectionVector* sel_vec) const;

  // Returns a textual representation suitable for debugging.
  std::string ToString() const;

 private:
  template<class Traits>
  friend class DeltaPreparer;

  // Mutation that has met the 'select' criteria in a delta store.
  struct Delta {
    // Key fields.

    // The delta's timestamp.
    Timestamp ts;

    // Whether this delta was an UNDO or a REDO.
    DeltaType dtype;

    // It's possible for multiple UNDOs or REDOs in the same delta store to
    // share a common timestamp. To ensure a total ordering, this additional key
    // field reflects the logical ordering of such deltas.
    //
    // For example, consider the sequence of REDOs:
    // D1: @tx10 UPDATE key=1
    // D2: @tx10 DELETE key=1
    //
    // D1 and D2 are identical as far as 'ts' and 'dtype' are concerned, so D1's
    // disambiguator must be less than that of D2.
    int64_t disambiguator;

    // Non-key fields.

    // Identifier of the delta store that provided this delta. It must:
    // 1. Be unique for the owning rowset, but needn't be more unique than that.
    // 2. Remain unique for the lifetime of this delta scan.
    int64_t delta_store_id;

    // Whether this delta was an UPDATE, DELETE, or REINSERT.
    RowChangeList::ChangeType ctype;
  };

  // Tracks the oldest and newest deltas for a given row.
  //
  // When there's only one delta, 'oldest' and 'newest' are equal and
  // 'same_delta' is true. Otherwise, oldest is guaranteed to be less than
  // newest as per the rules defined in DeltaLessThanFunctor.
  //
  // Most of the time "oldest" and "newest" is determined purely by timestamp,
  // but some deltas can share timestamps, in which case additional rules are
  // used to maintain a total ordering.
  struct DeltaPair {
    Delta oldest;
    Delta newest;
    bool same_delta;
  };

  // Comparator that establishes a total ordering amongst Deltas for the same row.
  struct DeltaLessThanFunctor {
    bool operator() (const Delta& a, const Delta& b) const {
      // Most of the time, deltas are ordered using timestamp.
      if (PREDICT_TRUE(a.ts != b.ts)) {
        return a.ts < b.ts;
      }

      // If the timestamps match, we can order by observing that UNDO < REDO, an
      // invariant that is preserved inside of a rowset.
      if (a.dtype != b.dtype) {
        return a.dtype == UNDO;
      }

      // The timestamps and delta types match. It should only be possible to get
      // here if we're comparing deltas from within the same store, in which
      // case the disambiguators must not match.
      CHECK_EQ(a.delta_store_id, b.delta_store_id);
      if (a.disambiguator != b.disambiguator) {
        return a.disambiguator < b.disambiguator;
      }
      LOG(FATAL) << "Could not differentiate between two deltas";
    }
  };

  // Merges two SelectedDeltas together on a row-by-row basis.
  void MergeFrom(const SelectedDeltas& other);

  // Considers a new delta, possibly adding it to 'rows_'.
  void ProcessDelta(rowid_t row_idx, Delta new_delta);

  // Clears out 'rows_' and makes it suitable for handling 'nrows'.
  void Reset(size_t nrows);

  // All tracked deltas, indexed by row ordinal.
  //
  // If an element is boost::none, there are no deltas for that row.
  std::vector<boost::optional<DeltaPair>> rows_;
};

// Interface for the pieces of the system that track deltas/updates.
// This is implemented by DeltaMemStore and by DeltaFileReader.
class DeltaStore {
 public:
  // Performs any post-construction work for the DeltaStore, which may
  // include additional I/O.
  virtual Status Init(const fs::IOContext* io_context) = 0;

  // Whether this delta store was initialized or not.
  virtual bool Initted() const = 0;

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
                                  std::unique_ptr<DeltaIterator>* iterator) const = 0;

  // Set *deleted to true if the latest update for the given row is a deletion.
  virtual Status CheckRowDeleted(rowid_t row_idx, const fs::IOContext* io_context,
                                 bool *deleted) const = 0;

  // Get the store's estimated size in bytes.
  virtual uint64_t EstimateSize() const = 0;

  virtual std::string ToString() const = 0;

  // TODO(jdcryans): remove this once we don't need to have delta_stats for
  // both DMS and DFR. Currently DeltaTracker#GetColumnsIdxWithUpdates() needs
  // to filter out DMS from the redo list but it can't without RTTI.
  virtual const DeltaStats& delta_stats() const = 0;

  // Returns whether callers can use 'delta_stats()', either because they've
  // been read from disk, or because the store has been initialized with the
  // stats cached.
  virtual bool has_delta_stats() const  {
    return Initted();
  }

  virtual ~DeltaStore() {}
};

typedef std::vector<std::shared_ptr<DeltaStore>> SharedDeltaStoreVector;

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
  // 'dst' must be the same length as was previously passed to PrepareBatch().
  //
  // Updates belonging to unselected rows in 'filter' will be skipped. This is
  // intended as an optimization; if the caller knows with certainty that some
  // rows are irrelevant (e.g. they've been deleted), we can avoid some copying.
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_APPLY.
  virtual Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                              const SelectionVector& filter) = 0;

  // Applies any deletes to the given selection vector.
  //
  // Rows which have been deleted in the associated MVCC snapshot are set to 0
  // in the selection vector so that they don't show up in the output.
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_APPLY.
  virtual Status ApplyDeletes(SelectionVector* sel_vec) = 0;

  // Modifies the given SelectedDeltas to include rows with relevant deltas from
  // the current prepared batch.
  //
  // Deltas must have been prepared with the flag PREPARE_FOR_SELECT.
  virtual Status SelectDeltas(SelectedDeltas* deltas) = 0;

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

  // Prepare to apply deltas to a block of rows. This takes a consistent snapshot
  // of all updates to the next 'nrows' rows, so that subsequent calls to a
  // PreparedDeltas method will not cause any "tearing"/non-atomicity.
  //
  // 'prepare_flags' is a bitfield describing what operation(s) the batch will
  // be used for; some implementations may choose to prepare differently.
  // PREPARE_NONE is an invalid value; it is only used internally.
  //
  // Each time this is called, the iterator is advanced by the full length
  // of the previously prepared block.
  enum {
    // There are no prepared blocks. Attempts to call a PreparedDeltas function
    // will fail.
    PREPARE_NONE = 0,

    // Prepare a batch of deltas for applying. All deltas in the batch will be
    // decoded. Operations affecting row data (i.e. UPDATEs and REINSERTs) will
    // be coalesced into a column-major data structure suitable for
    // ApplyUpdates. Operations affecting row lifecycle (i.e. DELETES and
    // REINSERTs) will be coalesced into a row-major data structure suitable for ApplyDeletes.
    //
    // On success, ApplyUpdates and ApplyDeltas will be callable.
    PREPARE_FOR_APPLY = 1 << 0,

    // Prepare a batch of deltas for collecting. Deltas will remain encoded and
    // in the order that they were loaded from the backing store.
    //
    // On success, CollectMutations and FilterColumnIdsAndCollectDeltas will be callable.
    PREPARE_FOR_COLLECT = 1 << 1,

    // Prepare a batch of deltas for selecting. All deltas in the batch will be
    // decoded, and a data structure describing which rows had deltas will be
    // populated.
    //
    // On success, SelectUpdates will be callable.
    PREPARE_FOR_SELECT = 1 << 2
  };
  virtual Status PrepareBatch(size_t nrows, int prepare_flags) = 0;

  // Returns true if there are any more rows left in this iterator.
  virtual bool HasNext() = 0;

  // Return a string representation suitable for debug printouts.
  virtual std::string ToString() const = 0;

  virtual ~DeltaIterator() {}
};

// DeltaPreparer traits suited for a DMSIterator.
struct DMSPreparerTraits {
  static constexpr DeltaType kType = REDO;
  static constexpr bool kAllowReinserts = false;
  static constexpr bool kAllowFilterColumnIdsAndCollectDeltas = false;
  static constexpr bool kInitializeDecodersWithSafetyChecks = false;
};

// DeltaPreparer traits suited for a DeltaFileIterator.
//
// This is just a partial specialization; the DeltaFileIterator is expected to
// dictate the DeltaType.
template<DeltaType Type>
struct DeltaFilePreparerTraits {
  static constexpr DeltaType kType = Type;
  static constexpr bool kAllowReinserts = true;
  static constexpr bool kAllowFilterColumnIdsAndCollectDeltas = true;
  static constexpr bool kInitializeDecodersWithSafetyChecks = true;
};

// Encapsulates all logic and responsibility related to "delta preparation";
// that is, the transformation of encoded deltas into an in-memory
// representation more suitable for efficient service during iteration.
//
// This class is intended to be composed inside a DeltaIterator. The iterator
// is responsible for loading encoded deltas from a backing store, passing them
// to the DeltaPreparer to be transformed, and later, calling the DeltaPreparer
// to serve the deltas.
template<class Traits>
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
  void Start(size_t nrows, int prepare_flags);

  // Updates internal state to reflect the end of delta batch preparation on the
  // part of a DeltaIterator.
  //
  // Call at the end of DeltaIterator::PrepareBatch.
  void Finish(size_t nrows);

  // Prepares the delta given by 'key' whose encoded changes are pointed to by 'val'.
  //
  // On success, the memory pointed to by 'val' can be destroyed. The
  // 'finished_row' output parameter will be set if we can determine that all
  // future deltas belonging to 'key.row_idx()' are irrelevant under the
  // snapshot provided at preparer construction time; the caller can skip ahead
  // to deltas belonging to the next row.
  //
  // Call when a new delta becomes available in DeltaIterator::PrepareBatch.
  Status AddDelta(const DeltaKey& key, Slice val, bool* finished_row);

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                      const SelectionVector& filter) override;

  Status ApplyDeletes(SelectionVector* sel_vec) override;

  Status SelectDeltas(SelectedDeltas* deltas) override;

  Status CollectMutations(std::vector<Mutation*>* dst, Arena* arena) override;

  Status FilterColumnIdsAndCollectDeltas(const std::vector<ColumnId>& col_ids,
                                         std::vector<DeltaKeyAndUpdate>* out,
                                         Arena* arena) override;

  bool MayHaveDeltas() const override;

  rowid_t cur_prepared_idx() const { return cur_prepared_idx_; }
  boost::optional<rowid_t> last_added_idx() const { return last_added_idx_; }
  const RowIteratorOptions& opts() const { return opts_; }

 private:
  // If 'decoder' is not yet initialized, initializes it in accordance with the
  // preparer's traits.
  static Status InitDecoderIfNecessary(RowChangeListDecoder* decoder);

  // Checks whether we are done processing a row's deltas. If so, attempts to
  // convert the row's latest deletion state into a saved deletion or
  // reinsertion. By deferring this work to when a row is finished, we avoid
  // creating unnecessary deletions and reinsertions for rows that are
  // repeatedly deleted and reinserted.
  //
  // 'cur_row_idx' may be unset when there is no new row index, such as when
  // called upon completion of an entire batch of deltas (i.e. from Finish()).
  void MaybeProcessPreviousRowChange(boost::optional<rowid_t> cur_row_idx);

  // Update the deletion state of the current row being processed based on 'op'.
  void UpdateDeletionState(RowChangeList::ChangeType op);

  // Options with which the DeltaPreparer's iterator was constructed.
  const RowIteratorOptions opts_;

  // The row index at which the most recent batch preparation ended.
  rowid_t cur_prepared_idx_;

  // The value of 'cur_prepared_idx_' from the previous batch.
  rowid_t prev_prepared_idx_;

  // The index of the row last added in AddDelta(), if one exists.
  boost::optional<rowid_t> last_added_idx_;

  // Whether there are any prepared blocks.
  int prepared_flags_;

  // State when prepared_flags_ & PREPARED_FOR_APPLY
  // ------------------------------------------------------------
  struct ColumnUpdate {
    rowid_t row_id;
    void* new_val_ptr;
    uint8_t new_val_buf[16];
  };
  typedef std::deque<ColumnUpdate> UpdatesForColumn;
  std::vector<UpdatesForColumn> updates_by_col_;

  // A row whose last relevant mutation was DELETE (or REINSERT).
  //
  // These lists are disjoint; a row that was both deleted and reinserted will
  // not be in either.
  std::vector<rowid_t> deleted_;
  std::vector<rowid_t> reinserted_;

  // The deletion state of the row last processed by AddDelta().
  //
  // As a row's DELETEs and REINSERTs are processed, the deletion state
  // alternates between the values below.
  enum RowDeletionState {
    UNKNOWN,
    DELETED,
    REINSERTED
  };
  RowDeletionState deletion_state_;

  // State when prepared_flags_ & PREPARED_FOR_COLLECT
  // ------------------------------------------------------------
  struct PreparedDelta {
    DeltaKey key;
    Slice val;
  };
  std::vector<PreparedDelta> prepared_deltas_;

  // State when prepared_for_ & PREPARED_FOR_SELECT
  // ------------------------------------------------------------
  SelectedDeltas selected_;

  // The number of deltas selected so far by this DeltaPreparer. Used to build
  // disambiguators (see SelectedDeltas::Delta). Never reset.
  int64_t deltas_selected_;

  // Used for PREPARED_FOR_APPLY mode.
  //
  // Set to true in all of the spots where deleted, reinserted_, and updates_by_col_
  // are modified.
  bool may_have_deltas_;

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

