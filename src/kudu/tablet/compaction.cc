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

#include "kudu/tablet/compaction.h"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/generic_iterators.h"
#include "kudu/common/iterator.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowid.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/delta_tracker.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/memrowset.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"

using kudu::clock::HybridClock;
using kudu::fs::IOContext;
using std::deque;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

namespace {

// The maximum number of rows we will output at a time during
// compaction.
const int kCompactionOutputBlockNumRows = 100;

// Advances to the last mutation in a mutation list.
void AdvanceToLastInList(const Mutation** m) {
  if (*m == nullptr) return;
  const Mutation* next;
  while ((next = (*m)->acquire_next()) != nullptr) {
    *m = next;
  }
}

// CompactionInput yielding rows and mutations from a MemRowSet.
class MemRowSetCompactionInput : public CompactionInput {
 public:
  MemRowSetCompactionInput(const MemRowSet& memrowset,
                           const MvccSnapshot& snap,
                           const Schema* projection)
    : arena_(32*1024),
      has_more_blocks_(false) {
    RowIteratorOptions opts;
    opts.projection = projection;
    opts.snap_to_include = snap;
    iter_.reset(memrowset.NewIterator(opts));
  }

  Status Init() override {
    RETURN_NOT_OK(iter_->Init(nullptr));
    has_more_blocks_ = iter_->HasNext();
    return Status::OK();
  }

  bool HasMoreBlocks() override {
    return has_more_blocks_;
  }

  Status PrepareBlock(vector<CompactionInputRow> *block) override {
    int num_in_block = iter_->remaining_in_leaf();
    block->resize(num_in_block);

    // Realloc the internal block storage if we don't have enough space to
    // copy the whole leaf node's worth of data into it.
    if (PREDICT_FALSE(!row_block_ || num_in_block > row_block_->nrows())) {
      row_block_.reset(new RowBlock(iter_->schema(), num_in_block, nullptr));
    }

    arena_.Reset();
    RowChangeListEncoder undo_encoder(&buffer_);
    int next_row_index = 0;
    for (int i = 0; i < num_in_block; ++i) {
      // TODO(todd): A copy is performed to make all CompactionInputRow have the same schema
      CompactionInputRow& input_row = block->at(next_row_index);
      input_row.row.Reset(row_block_.get(), next_row_index);
      Timestamp insertion_timestamp;
      RETURN_NOT_OK(iter_->GetCurrentRow(&input_row.row,
                                         static_cast<Arena*>(nullptr),
                                         &input_row.redo_head,
                                         &arena_,
                                         &insertion_timestamp));

      // Handle the rare case where a row was inserted and deleted in the same operation.
      // This row can never be observed and should not be compacted/flushed. This saves
      // us some trouble later on on compactions.
      // See: MergeCompactionInput::CompareAndMergeDuplicatedRows().
      if (PREDICT_FALSE(input_row.redo_head != nullptr &&
          input_row.redo_head->timestamp() == insertion_timestamp)) {
        // Get the latest mutation.
        const Mutation* latest = input_row.redo_head;
        AdvanceToLastInList(&latest);
        if (latest->changelist().is_delete() &&
            latest->timestamp() == insertion_timestamp) {
          iter_->Next();
          continue;
        }
      }

      // Materialize MRSRow undo insert (delete)
      undo_encoder.SetToDelete();
      input_row.undo_head = Mutation::CreateInArena(&arena_,
                                                    insertion_timestamp,
                                                    undo_encoder.as_changelist());
      undo_encoder.Reset();
      ++next_row_index;
      iter_->Next();
    }

    if (PREDICT_FALSE(next_row_index < num_in_block)) {
      block->resize(next_row_index);
    }

    has_more_blocks_ = iter_->HasNext();
    return Status::OK();
  }

  Arena* PreparedBlockArena() override { return &arena_; }

  Status FinishBlock() override {
    return Status::OK();
  }

  const Schema &schema() const override {
    return iter_->schema();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(MemRowSetCompactionInput);
  gscoped_ptr<RowBlock> row_block_;

  gscoped_ptr<MemRowSet::Iterator> iter_;

  // Arena used to store the projected undo/redo mutations of the current block.
  Arena arena_;

  faststring buffer_;

  bool has_more_blocks_;
};

////////////////////////////////////////////////////////////

// CompactionInput yielding rows and mutations from an on-disk DiskRowSet.
class DiskRowSetCompactionInput : public CompactionInput {
 public:
  DiskRowSetCompactionInput(gscoped_ptr<RowwiseIterator> base_iter,
                            unique_ptr<DeltaIterator> redo_delta_iter,
                            unique_ptr<DeltaIterator> undo_delta_iter)
      : base_iter_(std::move(base_iter)),
        redo_delta_iter_(std::move(redo_delta_iter)),
        undo_delta_iter_(std::move(undo_delta_iter)),
        arena_(32 * 1024),
        block_(base_iter_->schema(), kRowsPerBlock, &arena_),
        redo_mutation_block_(kRowsPerBlock, static_cast<Mutation *>(nullptr)),
        undo_mutation_block_(kRowsPerBlock, static_cast<Mutation *>(nullptr)),
        first_rowid_in_block_(0) {}

  Status Init() override {
    ScanSpec spec;
    spec.set_cache_blocks(false);
    RETURN_NOT_OK(base_iter_->Init(&spec));
    RETURN_NOT_OK(redo_delta_iter_->Init(&spec));
    RETURN_NOT_OK(redo_delta_iter_->SeekToOrdinal(0));
    RETURN_NOT_OK(undo_delta_iter_->Init(&spec));
    RETURN_NOT_OK(undo_delta_iter_->SeekToOrdinal(0));
    return Status::OK();
  }

  bool HasMoreBlocks() override {
    return base_iter_->HasNext();
  }

  Status PrepareBlock(vector<CompactionInputRow> *block) override {
    RETURN_NOT_OK(base_iter_->NextBlock(&block_));
    std::fill(redo_mutation_block_.begin(), redo_mutation_block_.end(),
              static_cast<Mutation *>(nullptr));
    std::fill(undo_mutation_block_.begin(), undo_mutation_block_.end(),
                  static_cast<Mutation *>(nullptr));
    RETURN_NOT_OK(redo_delta_iter_->PrepareBatch(
                      block_.nrows(), DeltaIterator::PREPARE_FOR_COLLECT));
    RETURN_NOT_OK(redo_delta_iter_->CollectMutations(&redo_mutation_block_, block_.arena()));
    RETURN_NOT_OK(undo_delta_iter_->PrepareBatch(
                      block_.nrows(), DeltaIterator::PREPARE_FOR_COLLECT));
    RETURN_NOT_OK(undo_delta_iter_->CollectMutations(&undo_mutation_block_, block_.arena()));

    block->resize(block_.nrows());
    for (int i = 0; i < block_.nrows(); i++) {
      CompactionInputRow &input_row = block->at(i);
      input_row.row.Reset(&block_, i);
      input_row.redo_head = redo_mutation_block_[i];
      Mutation::ReverseMutationList(&input_row.redo_head);
      input_row.undo_head = undo_mutation_block_[i];
      Mutation::ReverseMutationList(&input_row.undo_head);
    }

    first_rowid_in_block_ += block_.nrows();
    return Status::OK();
  }

  Arena* PreparedBlockArena() override { return &arena_; }

  Status FinishBlock() override {
    return Status::OK();
  }

  const Schema &schema() const override {
    return base_iter_->schema();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(DiskRowSetCompactionInput);
  gscoped_ptr<RowwiseIterator> base_iter_;
  unique_ptr<DeltaIterator> redo_delta_iter_;
  unique_ptr<DeltaIterator> undo_delta_iter_;

  Arena arena_;

  // The current block of data which has come from the input iterator
  RowBlock block_;
  vector<Mutation *> redo_mutation_block_;
  vector<Mutation *> undo_mutation_block_;

  rowid_t first_rowid_in_block_;

  enum {
    kRowsPerBlock = 100
  };
};

// Compares two duplicate rows before compaction (and before the REDO->UNDO transformation).
// Returns -1 if 'left' is less recent than 'right', 1 otherwise. Never returns 0.
int CompareDuplicatedRows(const CompactionInputRow& left,
                          const CompactionInputRow& right) {
  const Mutation* left_last = left.redo_head;
  const Mutation* right_last = right.redo_head;
  AdvanceToLastInList(&left_last);
  AdvanceToLastInList(&right_last);

  if (left.redo_head == nullptr) {
    // left must still be alive, meaning right must have at least a DELETE redo.
    DCHECK(right_last != nullptr);
    DCHECK(right_last->changelist().is_delete());
    return 1;
  }
  if (right.redo_head == nullptr) {
    // right must still be alive, meaning left must have at least a DELETE redo.
    DCHECK(left_last != nullptr);
    DCHECK(left_last->changelist().is_delete());
    return -1;
  }

  // Duplicated rows usually have disjoint redo histories, meaning the first mutation
  // should be enough for the sake of determining the most recent row in most cases.
  int ret = left.redo_head->timestamp().CompareTo(right.redo_head->timestamp());

  if (ret > 0) {
    return ret;
  }

  if (PREDICT_TRUE(ret < 0)) {
    return ret;
  }

  // In case the histories aren't disjoint it must be because one row was deleted in the
  // the same operation that inserted the other one, which was then mutated.
  // For instance this is a valid history with non-disjoint REDO histories:
  // -- Row 'a' lives in DRS1
  // Update row 'a' @ 10
  // Delete row 'a' @ 10
  // Insert row 'a' @ 10
  // -- Row 'a' lives in the MRS
  // Update row 'a' @ 10
  // -- Flush the MRS into DRS2
  // -- Compact DRS1 and DRS2
  //
  // We can't have the case here where both 'left' and 'right' have a DELETE as the last
  // mutation at the same timestamp. This would be troublesome as we would possibly have
  // no way to decide which version is the most up-to-date (one or both version's undos
  // might have been garbage collected). See MemRowSetCompactionInput::PrepareBlock().

  // At least one of the rows must have a DELETE REDO as its last redo.
  CHECK(left_last->changelist().is_delete() || right_last->changelist().is_delete());
  // We made sure that rows that are inserted and deleted in the same operation can never
  // be part of a compaction input so they can't both be deletes.
  CHECK(!(left_last->changelist().is_delete() && right_last->changelist().is_delete()));

  // If 'left' doesn't have a delete then it's the latest version.
  if (!left_last->changelist().is_delete() && right_last->changelist().is_delete()) {
    return 1;
  }

  // ...otherwise it's 'right'.
  return -1;
}

void CopyMutations(Mutation* from, Mutation** to, Arena* arena) {
  Mutation* previous = nullptr;
  for (const Mutation* cur = from; cur != nullptr; cur = cur->acquire_next()) {
    Mutation* copy = Mutation::CreateInArena(arena,
                                             cur->timestamp(),
                                             cur->changelist());
    if (previous != nullptr) {
      previous->set_next(copy);
    } else {
      *to = copy;
    }
    previous = copy;
  }
}

class MergeCompactionInput : public CompactionInput {
 private:
  // State kept for each of the inputs.
  struct MergeState {
    MergeState() :
      pending_idx(0)
    {}

    ~MergeState() {
      STLDeleteElements(&dominated);
    }

    bool empty() const {
      return pending_idx >= pending.size();
    }

    CompactionInputRow* next() {
      return &pending[pending_idx];
    }

    const CompactionInputRow* next() const {
      return &pending[pending_idx];
    }

    void pop_front() {
      pending_idx++;
    }

    void Reset() {
      pending.clear();
      pending_idx = 0;
    }

    // Return true if the current block of this input fully dominates
    // the current block of the other input -- i.e that the last
    // row of this block is less than the first row of the other block.
    // In this case, we can remove the other input from the merge until
    // this input's current block has been exhausted.
    bool Dominates(const MergeState &other, const Schema &schema) const {
      DCHECK(!empty());
      DCHECK(!other.empty());

      return schema.Compare(pending.back().row, (*other.next()).row) < 0;
    }

    shared_ptr<CompactionInput> input;
    vector<CompactionInputRow> pending;
    int pending_idx;

    vector<MergeState *> dominated;
  };

 public:
  MergeCompactionInput(const vector<shared_ptr<CompactionInput> > &inputs,
                       const Schema* schema)
    : schema_(schema),
      num_dup_rows_(0) {
    for (const shared_ptr<CompactionInput> &input : inputs) {
      gscoped_ptr<MergeState> state(new MergeState);
      state->input = input;
      states_.push_back(state.release());
    }
  }

  virtual ~MergeCompactionInput() {
    STLDeleteElements(&states_);
  }

  Status Init() override {
    for (MergeState *state : states_) {
      RETURN_NOT_OK(state->input->Init());
    }

    // Pull the first block of rows from each input.
    RETURN_NOT_OK(ProcessEmptyInputs());
    return Status::OK();
  }

  bool HasMoreBlocks() override {
    // Return true if any of the input blocks has more rows pending
    // or more blocks which have yet to be pulled.
    for (MergeState *state : states_) {
      if (!state->empty() ||
          state->input->HasMoreBlocks()) {
        return true;
      }
    }

    return false;
  }

  Status PrepareBlock(vector<CompactionInputRow> *block) override {
    CHECK(!states_.empty());

    block->clear();

    while (true) {
      int smallest_idx = -1;
      CompactionInputRow* smallest;

      // Iterate over the inputs to find the one with the smallest next row.
      // It may seem like an O(n lg k) merge using a heap would be more efficient,
      // but some benchmarks indicated that the simpler code path of the O(n k) merge
      // actually ends up a bit faster.
      for (int i = 0; i < states_.size(); i++) {
        MergeState *state = states_[i];

        if (state->empty()) {
          prepared_block_arena_ = state->input->PreparedBlockArena();
          // If any of our inputs runs out of pending entries, then we can't keep
          // merging -- this input may have further blocks to process.
          // Rather than pulling another block here, stop the loop. If it's truly
          // out of blocks, then FinishBlock() will remove this input entirely.
          return Status::OK();
        }

        if (smallest_idx < 0) {
          smallest_idx = i;
          smallest = state->next();
          DVLOG(4) << "Set (initial) smallest from state: " << i << " smallest: "
                   << CompactionInputRowToString(*smallest);
          continue;
        }
        int row_comp = schema_->Compare(state->next()->row, smallest->row);
        if (row_comp < 0) {
          smallest_idx = i;
          smallest = state->next();
          DVLOG(4) << "Set (by comp) smallest from state: " << i << " smallest: "
                   << CompactionInputRowToString(*smallest);
          continue;
        }
        // If we found two rows with the same key, we want to make the newer one point to the older
        // one, which must be a ghost.
        if (PREDICT_FALSE(row_comp == 0)) {
          DVLOG(4) << "Duplicate row.\nLeft: " << CompactionInputRowToString(*state->next())
                   << "\nRight: " << CompactionInputRowToString(*smallest);
          int mutation_comp = CompareDuplicatedRows(*state->next(), *smallest);
          CHECK_NE(mutation_comp, 0);
          if (mutation_comp > 0) {
            // If the previous smallest row has a highest version that is lower
            // than this one, clone it as the previous version and discard the original.
            RETURN_NOT_OK(SetPreviousGhost(state->next(), smallest, true /* clone */,
                                           state->input->PreparedBlockArena()));
            states_[smallest_idx]->pop_front();
            smallest_idx = i;
            smallest = state->next();
            DVLOG(4) << "Set smallest to right duplicate: "
                     << CompactionInputRowToString(*smallest);
            continue;
          }
          // .. otherwise copy and pop the other one.
          RETURN_NOT_OK(SetPreviousGhost(smallest, state->next(), true /* clone */,
                                         smallest->row.row_block()->arena()));
          DVLOG(4) << "Updated left duplicate smallest: "
                   << CompactionInputRowToString(*smallest);
          states_[i]->pop_front();
          continue;
        }
      }
      DCHECK_GE(smallest_idx, 0);

      states_[smallest_idx]->pop_front();
      DVLOG(4) << "Pushing smallest to block: " << CompactionInputRowToString(*smallest);
      block->push_back(*smallest);
    }

    return Status::OK();
  }

  Arena* PreparedBlockArena() override { return prepared_block_arena_; }

  Status FinishBlock() override {
    return ProcessEmptyInputs();
  }

  const Schema &schema() const override {
    return *schema_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(MergeCompactionInput);

  // Look through our current set of inputs. For any that are empty,
  // pull the next block into its pending list. If there is no next
  // block, remove it from our input set.
  //
  // Postcondition: every input has a non-empty pending list.
  Status ProcessEmptyInputs() {
    int j = 0;
    for (int i = 0; i < states_.size(); i++) {
      MergeState *state = states_[i];
      states_[j++] = state;

      if (!state->empty()) {
        continue;
      }

      RETURN_NOT_OK(state->input->FinishBlock());

      // If an input is fully exhausted, no need to consider it
      // in the merge anymore.
      if (!state->input->HasMoreBlocks()) {

        // Any inputs that were dominated by the last block of this input
        // need to be re-added into the merge.
        states_.insert(states_.end(), state->dominated.begin(), state->dominated.end());
        state->dominated.clear();
        delete state;
        j--;
        continue;
      }

      state->Reset();
      RETURN_NOT_OK(state->input->PrepareBlock(&state->pending));

      // Now that this input has moved to its next block, it's possible that
      // it no longer dominates the inputs in it 'dominated' list. Re-check
      // all of those dominance relations and remove any that are no longer
      // valid.
      for (auto it = state->dominated.begin(); it != state->dominated.end(); ++it) {
        MergeState *dominated = *it;
        if (!state->Dominates(*dominated, *schema_)) {
          states_.push_back(dominated);
          it = state->dominated.erase(it);
          --it;
        }
      }
    }
    // We may have removed exhausted states as we iterated through the
    // array, so resize them away.
    states_.resize(j);

    // Check pairs of states to see if any have dominance relations.
    // This algorithm is probably not the most efficient, but it's the
    // most obvious, and this doesn't ever show up in the profiler as
    // much of a hot spot.
    check_dominance:
    for (int i = 0; i < states_.size(); i++) {
      for (int j = i + 1; j < states_.size(); j++) {
        if (TryInsertIntoDominanceList(states_[i], states_[j])) {
          states_.erase(states_.begin() + j);
          // Since we modified the vector, re-start iteration from the
          // top.
          goto check_dominance;
        } else if (TryInsertIntoDominanceList(states_[j], states_[i])) {
          states_.erase(states_.begin() + i);
          // Since we modified the vector, re-start iteration from the
          // top.
          goto check_dominance;
        }
      }
    }

    return Status::OK();
  }

  bool TryInsertIntoDominanceList(MergeState *dominator, MergeState *candidate) {
    if (dominator->Dominates(*candidate, *schema_)) {
      dominator->dominated.push_back(candidate);
      return true;
    } else {
      return false;
    }
  }

  // (Deep) clones a compaction input row, copying both the row data, all undo/redo mutations and
  // all previous ghosts to 'arena'.
  Status CloneCompactionInputRow(const CompactionInputRow* src,
                                 CompactionInputRow** dst,
                                 Arena* arena) {
    CompactionInputRow* copy = arena->NewObject<CompactionInputRow>();
    copy->row = NewRow();
    // Copy the row to the arena.
    RETURN_NOT_OK(CopyRow(src->row, &copy->row, arena));
    // ... along with the redos and undos.
    CopyMutations(src->redo_head, &copy->redo_head, arena);
    CopyMutations(src->undo_head, &copy->undo_head, arena);
    // Copy previous versions recursively.
    if (src->previous_ghost != nullptr) {
      CompactionInputRow* child;
      RETURN_NOT_OK(CloneCompactionInputRow(src->previous_ghost, &child, arena));
      copy->previous_ghost = child;
    }
    *dst = copy;
    return Status::OK();
  }

  // Sets the previous ghost row for a CompactionInputRow.
  // 'must_copy' indicates whether there must be a deep copy (using CloneCompactionInputRow()).
  Status SetPreviousGhost(CompactionInputRow* older,
                          CompactionInputRow* newer,
                          bool must_copy,
                          Arena* arena) {
    CHECK(arena != nullptr) << "Arena can't be null";
    // Check if we already had a previous version and, if yes, whether 'newer' is more or less
    // recent.
    if (older->previous_ghost != nullptr) {
      if (CompareDuplicatedRows(*older->previous_ghost, *newer) > 0) {
        // 'older' was more recent.
        return SetPreviousGhost(older->previous_ghost, newer, must_copy /* clone */, arena);
      }
      // 'newer' was more recent.
      if (must_copy) {
        CompactionInputRow* newer_copy;
        RETURN_NOT_OK(CloneCompactionInputRow(newer, &newer_copy, arena));
        newer = newer_copy;
      }
      // 'older->previous_ghost' is already in 'arena' so avoid the copy.
      RETURN_NOT_OK(SetPreviousGhost(newer,
                                     older->previous_ghost,
                                     false /* don't clone */,
                                     arena));
      older->previous_ghost = newer;
      return Status::OK();
    }

    if (must_copy) {
      CompactionInputRow* newer_copy;
      RETURN_NOT_OK(CloneCompactionInputRow(newer, &newer_copy, arena));
      newer = newer_copy;
    }
    older->previous_ghost = newer;
    return Status::OK();
  }

  // Duplicates are rare and allocating for the worst case (all the rows in all but one of
  // the inputs are duplicates) is expensive, so we create RowBlocks on demand with just
  // space for a few rows. If a row block is exhausted a new one is allocated.
  RowBlockRow NewRow() {
    rowid_t row_idx = num_dup_rows_ % kDuplicatedRowsPerBlock;
    num_dup_rows_++;
    if (row_idx == 0) {
      duplicated_rows_.push_back(std::unique_ptr<RowBlock>(
          new RowBlock(*schema_, kDuplicatedRowsPerBlock, static_cast<Arena*>(nullptr))));
    }
    return duplicated_rows_.back()->row(row_idx);
  }

  const Schema* schema_;
  vector<MergeState *> states_;
  Arena* prepared_block_arena_;

  // Vector to keep blocks that store duplicated row data.
  // This needs to be stored internally as row data for ghosts might have been deleted
  // by the the time the most recent version row is processed.
  vector<std::unique_ptr<RowBlock>> duplicated_rows_;
  int num_dup_rows_;

  enum {
    kDuplicatedRowsPerBlock = 10
  };

};

// Advances 'head' while the timestamp of the 'next' mutation is bigger than or equal to 'ts'
// and 'next' is not null.
void AdvanceWhileNextEqualToOrBiggerThan(Mutation** head, Timestamp ts) {
  while ((*head)->next() != nullptr && (*head)->next()->timestamp() >= ts) {
    *head = (*head)->next();
  }
}

// Merges two undo histories into one with decreasing timestamp order and returns the new head.
Mutation* MergeUndoHistories(Mutation* left, Mutation* right) {

  if (PREDICT_FALSE(left == nullptr)) {
    return right;
  }
  if (PREDICT_FALSE(right == nullptr)) {
    return left;
  }

  Mutation* head;
  if (left->timestamp() >= right->timestamp()) {
    head = left;
  } else {
    head = right;
  }

  while (left != nullptr && right != nullptr) {
    if (left->timestamp() >= right->timestamp()) {
      AdvanceWhileNextEqualToOrBiggerThan(&left, right->timestamp());
      Mutation* next = left->next();
      left->set_next(right);
      left = next;
      continue;
    }
    AdvanceWhileNextEqualToOrBiggerThan(&right, left->timestamp());
    Mutation* next = right->next();
    right->set_next(left);
    right = next;
  }
  return head;
}

// If 'old_row' has previous versions, this transforms prior version in undos and adds them
// to 'new_undo_head'.
Status MergeDuplicatedRowHistory(CompactionInputRow* old_row,
                                 Mutation** new_undo_head,
                                 Arena* arena) {
  if (PREDICT_TRUE(old_row->previous_ghost == nullptr)) return Status::OK();

  // Use an all inclusive snapshot as all of the previous version's undos and redos
  // are guaranteed to be committed, otherwise the compaction wouldn't be able to
  // see the new row.
  MvccSnapshot all_snap = MvccSnapshot::CreateSnapshotIncludingAllTransactions();

  faststring dst;

  CompactionInputRow* previous_ghost = old_row->previous_ghost;
  while (previous_ghost != nullptr) {

    // First step is to transform the old rows REDO's into undos, if there are any.
    // This simplifies this for several reasons:
    // - will be left with most up-to-date version of the old row
    // - only have one REDO left to deal with (the delete)
    // - can reuse ApplyMutationsAndGenerateUndos()
    Mutation* pv_new_undos_head = nullptr;
    Mutation* pv_delete_redo = nullptr;

    RETURN_NOT_OK(ApplyMutationsAndGenerateUndos(all_snap,
                                                 *previous_ghost,
                                                 &pv_new_undos_head,
                                                 &pv_delete_redo,
                                                 arena,
                                                 &previous_ghost->row));

    // We should be left with only one redo, the delete.
    CHECK(pv_delete_redo != nullptr);
    CHECK(pv_delete_redo->changelist().is_delete());
    CHECK(pv_delete_redo->next() == nullptr);

    // Now transform the redo delete into an undo (reinsert), which will contain the previous
    // ghost. The reinsert will have the timestamp of the delete.
    dst.clear();
    RowChangeListEncoder undo_encoder(&dst);
    undo_encoder.SetToReinsert(previous_ghost->row);
    Mutation* pv_reinsert = Mutation::CreateInArena(arena,
                                                    pv_delete_redo->timestamp(),
                                                    undo_encoder.as_changelist());

    // Make the reinsert point to the rest of the undos.
    pv_reinsert->set_next(pv_new_undos_head);

    // Merge the UNDO lists.
    *new_undo_head = MergeUndoHistories(*new_undo_head, pv_reinsert);

    // ... handle a previous ghost if there is any.
    previous_ghost = previous_ghost->previous_ghost;
  }
  return Status::OK();
}

// Makes the current head point to the 'new_head', if it's not null, and
// makes 'new_head' the new head.
void SetHead(Mutation** current_head, Mutation* new_head) {
  if (*current_head != nullptr) {
    new_head->set_next(*current_head);
  }
  *current_head = new_head;
}

} // anonymous namespace

string RowToString(const RowBlockRow& row, const Mutation* redo_head, const Mutation* undo_head) {
  return Substitute("RowIdxInBlock: $0; Base: $1; Undo Mutations: $2; Redo Mutations: $3;",
                    row.row_index(), row.schema()->DebugRow(row),
                    Mutation::StringifyMutationList(*row.schema(), undo_head),
                    Mutation::StringifyMutationList(*row.schema(), redo_head));
}

string CompactionInputRowToString(const CompactionInputRow& input_row) {
  if (input_row.previous_ghost == nullptr) {
    return RowToString(input_row.row, input_row.redo_head, input_row.undo_head);
  }
  string ret = RowToString(input_row.row, input_row.redo_head, input_row.undo_head);
  const CompactionInputRow* previous_ghost = input_row.previous_ghost;
  while (previous_ghost != nullptr) {
    ret.append(" Previous Ghost: ");
    ret.append(RowToString(previous_ghost->row,
                           previous_ghost->redo_head,
                           previous_ghost->undo_head));
    previous_ghost = previous_ghost->previous_ghost;
  }
  return ret;
}

////////////////////////////////////////////////////////////

Status CompactionInput::Create(const DiskRowSet &rowset,
                               const Schema* projection,
                               const MvccSnapshot &snap,
                               const IOContext* io_context,
                               gscoped_ptr<CompactionInput>* out) {
  CHECK(projection->has_column_ids());

  shared_ptr<ColumnwiseIterator> base_cwise(rowset.base_data_->NewIterator(projection, io_context));
  gscoped_ptr<RowwiseIterator> base_iter(new MaterializingIterator(base_cwise));

  // Creates a DeltaIteratorMerger that will only include the relevant REDO deltas.
  RowIteratorOptions redo_opts;
  redo_opts.projection = projection;
  redo_opts.snap_to_include = snap;
  redo_opts.io_context = io_context;
  unique_ptr<DeltaIterator> redo_deltas;
  RETURN_NOT_OK_PREPEND(rowset.delta_tracker_->NewDeltaIterator(
      redo_opts, DeltaTracker::REDOS_ONLY, &redo_deltas), "Could not open REDOs");
  // Creates a DeltaIteratorMerger that will only include UNDO deltas. Using the
  // "empty" snapshot ensures that all deltas are included.
  RowIteratorOptions undo_opts;
  undo_opts.projection = projection;
  undo_opts.snap_to_include = MvccSnapshot::CreateSnapshotIncludingNoTransactions();
  undo_opts.io_context = io_context;
  unique_ptr<DeltaIterator> undo_deltas;
  RETURN_NOT_OK_PREPEND(rowset.delta_tracker_->NewDeltaIterator(
      undo_opts, DeltaTracker::UNDOS_ONLY, &undo_deltas), "Could not open UNDOs");

  out->reset(new DiskRowSetCompactionInput(std::move(base_iter),
                                           std::move(redo_deltas),
                                           std::move(undo_deltas)));
  return Status::OK();
}

CompactionInput *CompactionInput::Create(const MemRowSet &memrowset,
                                         const Schema* projection,
                                         const MvccSnapshot &snap) {
  CHECK(projection->has_column_ids());
  return new MemRowSetCompactionInput(memrowset, snap, projection);
}

CompactionInput *CompactionInput::Merge(const vector<shared_ptr<CompactionInput> > &inputs,
                                        const Schema* schema) {
  CHECK(schema->has_column_ids());
  return new MergeCompactionInput(inputs, schema);
}


Status RowSetsInCompaction::CreateCompactionInput(const MvccSnapshot &snap,
                                                  const Schema* schema,
                                                  const IOContext* io_context,
                                                  shared_ptr<CompactionInput> *out) const {
  CHECK(schema->has_column_ids());

  vector<shared_ptr<CompactionInput> > inputs;
  for (const shared_ptr<RowSet> &rs : rowsets_) {
    gscoped_ptr<CompactionInput> input;
    RETURN_NOT_OK_PREPEND(rs->NewCompactionInput(schema, snap, io_context, &input),
                          Substitute("Could not create compaction input for rowset $0",
                                     rs->ToString()));
    inputs.push_back(shared_ptr<CompactionInput>(input.release()));
  }

  if (inputs.size() == 1) {
    out->swap(inputs[0]);
  } else {
    out->reset(CompactionInput::Merge(inputs, schema));
  }

  return Status::OK();
}

void RowSetsInCompaction::DumpToLog() const {
  LOG(INFO) << "Selected " << rowsets_.size() << " rowsets to compact:";
  // Dump the selected rowsets to the log, and collect corresponding iterators.
  for (const shared_ptr<RowSet> &rs : rowsets_) {
    LOG(INFO) << rs->ToString() << "(current size on disk: ~"
              << rs->OnDiskSize() << " bytes)";
  }
}

void RemoveAncientUndos(const HistoryGcOpts& history_gc_opts,
                        Mutation** undo_head,
                        const Mutation* redo_head,
                        bool* is_garbage_collected) {
  *is_garbage_collected = false;
  if (!history_gc_opts.gc_enabled()) {
    return;
  }

  // Make sure there is at most one REDO in the redo_head and that, if present, it's a DELETE.
  if (redo_head != nullptr) {
    CHECK(redo_head->changelist().is_delete());
    CHECK(redo_head->next() == nullptr);

    // Garbage collect rows that are deleted before the AHM.
    if (history_gc_opts.IsAncientHistory(redo_head->timestamp())) {
      *is_garbage_collected = true;
      return;
    }
  }

  DVLOG(5) << "Ancient history mark: " << history_gc_opts.ancient_history_mark().ToString()
            << ": " << HybridClock::StringifyTimestamp(history_gc_opts.ancient_history_mark());

  Mutation *prev_undo = nullptr;
  Mutation *undo_mut = *undo_head;
  while (undo_mut != nullptr) {
    if (history_gc_opts.IsAncientHistory(undo_mut->timestamp())) {
      // Drop all undos following this one in the list; Their timestamps will be lower.
      if (prev_undo != nullptr) {
        prev_undo->set_next(nullptr);
      } else {
        *undo_head = nullptr;
      }
      break;
    }
    prev_undo = undo_mut;
    undo_mut = undo_mut->next();
  }
}

Status ApplyMutationsAndGenerateUndos(const MvccSnapshot& snap,
                                      const CompactionInputRow& src_row,
                                      Mutation** new_undo_head,
                                      Mutation** new_redo_head,
                                      Arena* arena,
                                      RowBlockRow* dst_row) {
  bool is_deleted = false;

  #define ERROR_LOG_CONTEXT \
    Substitute("Source Row: $0\nDest Row: $1", \
                CompactionInputRowToString(src_row), \
                RowToString(*dst_row, undo_head, redo_delete))

  faststring dst;
  RowChangeListEncoder undo_encoder(&dst);

  // Const cast this away here since we're ever only going to point to it
  // which doesn't actually mutate it and having Mutation::set_next()
  // take a non-const value is required in other places.
  Mutation* undo_head = const_cast<Mutation*>(src_row.undo_head);
  Mutation* redo_delete = nullptr;

  // Convert the redos into undos.
  for (const Mutation *redo_mut = src_row.redo_head;
       redo_mut != nullptr;
       redo_mut = redo_mut->acquire_next()) {

    // Skip anything not committed.
    if (!snap.IsCommitted(redo_mut->timestamp())) {
      break;
    }

    undo_encoder.Reset();

    Mutation* current_undo;
    DVLOG(3) << "  @" << redo_mut->timestamp() << ": "
             << redo_mut->changelist().ToString(*src_row.row.schema());

    RowChangeListDecoder redo_decoder(redo_mut->changelist());
    RETURN_NOT_OK_LOG(redo_decoder.Init(), ERROR,
                      "Unable to decode changelist.\n" + ERROR_LOG_CONTEXT);

    switch (redo_decoder.get_type()) {
      case RowChangeList::kUpdate: {
        DCHECK(!is_deleted) << "Got UPDATE for deleted row. " << ERROR_LOG_CONTEXT;

        undo_encoder.SetToUpdate();
        RETURN_NOT_OK_LOG(redo_decoder.MutateRowAndCaptureChanges(
            dst_row, static_cast<Arena*>(nullptr), &undo_encoder), ERROR,
                          "Unable to apply update undo.\n " + ERROR_LOG_CONTEXT);

        // If all of the updates were for columns that we aren't projecting, we don't
        // need to push them into the UNDO file.
        if (undo_encoder.is_empty()) {
          continue;
        }

        // create the UNDO mutation in the provided arena.
        current_undo = Mutation::CreateInArena(arena, redo_mut->timestamp(),
                                               undo_encoder.as_changelist());

        SetHead(&undo_head, current_undo);
        break;
      }
      case RowChangeList::kDelete: {
        CHECK(redo_delete == nullptr);
        redo_decoder.TwiddleDeleteStatus(&is_deleted);
        // Delete mutations are left as redos. Encode the DELETE as a redo.
        undo_encoder.SetToDelete();
        redo_delete = Mutation::CreateInArena(arena,
                                              redo_mut->timestamp(),
                                              undo_encoder.as_changelist());
        break;
      }
      // When we see a reinsert REDO we do the following:
      // 1 - Reset the REDO head, which contained a DELETE REDO.
      // 2 - Apply the REINSERT to the row, passing an undo_encoder that encodes the state of
      //     the row prior to to the REINSERT.
      // 3 - Create a mutation for the REINSERT above and add it to the UNDOs, this mutation
      //     will have the timestamp of the DELETE REDO.
      // 4 - Create a new delete UNDO. This mutation will have the timestamp of the REINSERT REDO.
      case RowChangeList::kReinsert: {
        DCHECK(is_deleted) << "Got REINSERT for a non-deleted row. " << ERROR_LOG_CONTEXT;
        CHECK(redo_delete != nullptr)  << "Got REINSERT without a redo DELETE. "
                                       << ERROR_LOG_CONTEXT;
        redo_decoder.TwiddleDeleteStatus(&is_deleted);
        Timestamp delete_ts = redo_delete->timestamp();

        // 1 - Reset the delete REDO.
        redo_delete = nullptr;

        // 2 - Apply the changes of the reinsert to the latest version of the row
        // capturing the old row while we're at it.
        undo_encoder.SetToReinsert();
        RETURN_NOT_OK_LOG(redo_decoder.MutateRowAndCaptureChanges(
            dst_row, static_cast<Arena*>(nullptr), &undo_encoder), ERROR,
                          "Unable to apply reinsert undo. \n" + ERROR_LOG_CONTEXT);

        // 3 - Create a mutation for the REINSERT above and add it to the UNDOs.
        current_undo = Mutation::CreateInArena(arena,
                                               delete_ts,
                                               undo_encoder.as_changelist());
        SetHead(&undo_head, current_undo);

        // 4 - Create a DELETE mutation and add it to the UNDOs.
        undo_encoder.Reset();
        undo_encoder.SetToDelete();
        current_undo = Mutation::CreateInArena(arena,
                                               redo_mut->timestamp(),
                                               undo_encoder.as_changelist());
        SetHead(&undo_head, current_undo);
        break;
      }
      default: LOG(FATAL) << "Unknown mutation type!" << ERROR_LOG_CONTEXT;
    }
  }

  *new_undo_head = undo_head;
  *new_redo_head = redo_delete;

  return Status::OK();

  #undef ERROR_LOG_CONTEXT
}

Status FlushCompactionInput(CompactionInput* input,
                            const MvccSnapshot& snap,
                            const HistoryGcOpts& history_gc_opts,
                            RollingDiskRowSetWriter* out) {
  RETURN_NOT_OK(input->Init());
  vector<CompactionInputRow> rows;

  DCHECK(out->schema().has_column_ids());

  RowBlock block(out->schema(), kCompactionOutputBlockNumRows, nullptr);

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    int n = 0;
    for (int i = 0; i < rows.size(); i++) {
      CompactionInputRow* input_row = &rows[i];
      RETURN_NOT_OK(out->RollIfNecessary());

      const Schema* schema = input_row->row.schema();
      DCHECK_SCHEMA_EQ(*schema, out->schema());
      DCHECK(schema->has_column_ids());

      RowBlockRow dst_row = block.row(n);
      RETURN_NOT_OK(CopyRow(input_row->row, &dst_row, static_cast<Arena*>(nullptr)));

      DVLOG(4) << "Input Row: " << CompactionInputRowToString(*input_row);

      // Collect the new UNDO/REDO mutations.
      Mutation* new_undos_head = nullptr;
      Mutation* new_redos_head = nullptr;

      RETURN_NOT_OK(ApplyMutationsAndGenerateUndos(snap,
                                                   *input_row,
                                                   &new_undos_head,
                                                   &new_redos_head,
                                                   input->PreparedBlockArena(),
                                                   &dst_row));

      // Merge the histories of 'input_row' with previous ghosts, if there are any.
      RETURN_NOT_OK(MergeDuplicatedRowHistory(input_row,
                                              &new_undos_head,
                                              input->PreparedBlockArena()));

      // Remove ancient UNDOS and check whether the row should be garbage collected.
      bool is_garbage_collected;
      RemoveAncientUndos(history_gc_opts,
                         &new_undos_head,
                         new_redos_head,
                         &is_garbage_collected);

      DVLOG(4) << "Output Row: " << RowToString(dst_row, new_redos_head, new_undos_head) <<
          "; Was garbage collected? " << is_garbage_collected;

      // Whether this row was garbage collected
      if (is_garbage_collected) {
        // Don't flush the row.
        continue;
      }

      rowid_t index_in_current_drs;

      if (new_undos_head != nullptr) {
        out->AppendUndoDeltas(dst_row.row_index(), new_undos_head, &index_in_current_drs);
      }

      if (new_redos_head != nullptr) {
        out->AppendRedoDeltas(dst_row.row_index(), new_redos_head, &index_in_current_drs);
      }

      DVLOG(4) << "Output Row: " << dst_row.schema()->DebugRow(dst_row)
               << "; RowId: " << index_in_current_drs;

      n++;
      if (n == block.nrows()) {
        RETURN_NOT_OK(out->AppendBlock(block));
        n = 0;
      }
    }

    if (n > 0) {
      block.Resize(n);
      RETURN_NOT_OK(out->AppendBlock(block));
      block.Resize(block.row_capacity());
    }

    RETURN_NOT_OK(input->FinishBlock());
  }
  return Status::OK();
}

Status ReupdateMissedDeltas(const IOContext* io_context,
                            CompactionInput *input,
                            const HistoryGcOpts& history_gc_opts,
                            const MvccSnapshot &snap_to_exclude,
                            const MvccSnapshot &snap_to_include,
                            const RowSetVector &output_rowsets) {
  TRACE_EVENT0("tablet", "ReupdateMissedDeltas");
  RETURN_NOT_OK(input->Init());

  VLOG(1) << "Reupdating missed deltas between snapshot " <<
    snap_to_exclude.ToString() << " and " << snap_to_include.ToString();

  // Collect the disk rowsets that we'll push the updates into.
  deque<DiskRowSet *> diskrowsets;
  for (const shared_ptr<RowSet> &rs : output_rowsets) {
    diskrowsets.push_back(down_cast<DiskRowSet *>(rs.get()));
  }

  // The set of updated delta trackers.
  unordered_set<DeltaTracker*> updated_trackers;

  // When we apply the updates to the new DMS, there is no need to anchor them
  // since these stores are not yet part of the tablet.
  const consensus::OpId max_op_id = consensus::MaximumOpId();

  // The rowid where the current (front) delta tracker starts.
  int64_t delta_tracker_base_row = 0;

  // TODO: on this pass, we don't actually need the row data, just the
  // updates. So, this can be made much faster.
  vector<CompactionInputRow> rows;
  const Schema* schema = &input->schema();
  const Schema key_schema(input->schema().CreateKeyProjection());

  // Arena and projector to store/project row keys for missed delta updates
  Arena arena(1024);
  RowProjector key_projector(schema, &key_schema);
  RETURN_NOT_OK(key_projector.Init());
  faststring buf;

  rowid_t output_row_offset = 0;
  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    for (const CompactionInputRow &row : rows) {
      DVLOG(4) << "Revisiting row: " << CompactionInputRowToString(row);

      bool is_garbage_collected = false;
      for (const Mutation *mut = row.redo_head;
           mut != nullptr;
           mut = mut->acquire_next()) {
        is_garbage_collected = false;
        RowChangeListDecoder decoder(mut->changelist());
        RETURN_NOT_OK(decoder.Init());

        if (snap_to_exclude.IsCommitted(mut->timestamp())) {
          // This update was already taken into account in the first phase of the
          // compaction. We don't need to reapply it.

          // But was this row GCed at flush time?
          if (decoder.is_delete() &&
              history_gc_opts.IsAncientHistory(mut->timestamp())) {
            DVLOG(3) << "Marking for garbage collection row: " << schema->DebugRow(row.row);
            is_garbage_collected = true;
          }
          continue;
        }

        // We should never be able to get to this point if a row has been
        // garbage collected.
        DCHECK(!is_garbage_collected);

        // We should never see a REINSERT in an input RowSet which was not
        // caught in the original flush. REINSERT only occurs when an INSERT is
        // done to a row when a ghost is already present for that row in
        // MemRowSet. If the ghost is in a disk RowSet, it is ignored and the
        // new row is inserted in the MemRowSet instead.
        //
        // At the beginning of a compaction/flush, a new empty MRS is swapped in for
        // the one to be flushed. Therefore, any INSERT that happens _after_ this swap
        // is made will not trigger a REINSERT: it sees the row as "deleted" in the
        // snapshotted MRS, and insert triggers an INSERT into the new MRS.
        //
        // Any INSERT that happened _before_ the swap-out would create a
        // REINSERT in the MRS to be flushed, but it would also be considered as
        // part of the MvccSnapshot which we flush from ('snap_to_exclude' here)
        // and therefore won't make it to this point in the code.
        CHECK(!decoder.is_reinsert())
          << "Shouldn't see REINSERT missed by first flush pass in compaction."
          << " snap_to_exclude=" << snap_to_exclude.ToString()
          << " row=" << schema->DebugRow(row.row)
          << " mutations=" << Mutation::StringifyMutationList(*schema, row.redo_head);

        if (!snap_to_include.IsCommitted(mut->timestamp())) {
          // The mutation was inserted after the DuplicatingRowSet was swapped in.
          // Therefore, it's already present in the output rowset, and we don't need
          // to copy it in.

          DVLOG(3) << "Skipping already-duplicated delta for row " << output_row_offset
                   << " @" << mut->timestamp() << ": " << mut->changelist().ToString(*schema);
          continue;
        }

        // Otherwise, this is an update that arrived after the snapshot for the first
        // pass, but before the DuplicatingRowSet was swapped in. We need to transfer
        // this over to the output rowset.
        DVLOG(3) << "Flushing missed delta for row " << output_row_offset
                 << " @" << mut->timestamp() << ": " << mut->changelist().ToString(*schema);

        rowid_t num_rows;
        DiskRowSet* cur_drs = diskrowsets.front();
        RETURN_NOT_OK(cur_drs->CountRows(io_context, &num_rows));

        // The index on the input side isn't necessarily the index on the output side:
        // we may have output several small DiskRowSets, so we need to find the index
        // relative to the current one.
        int64_t idx_in_delta_tracker = output_row_offset - delta_tracker_base_row;
        while (idx_in_delta_tracker >= num_rows) {
          // If the current index is higher than the total number of rows in the current
          // DeltaTracker, that means we're now processing the next one in the list.
          // Pop the current front tracker, and make the indexes relative to the next
          // in the list.
          delta_tracker_base_row += num_rows;
          idx_in_delta_tracker -= num_rows;
          DCHECK_GE(idx_in_delta_tracker, 0);
          diskrowsets.pop_front();
          cur_drs = diskrowsets.front();
          RETURN_NOT_OK(cur_drs->CountRows(io_context, &num_rows));
        }

        DeltaTracker* cur_tracker = cur_drs->delta_tracker();
        gscoped_ptr<OperationResultPB> result(new OperationResultPB);
        DCHECK_LT(idx_in_delta_tracker, num_rows);
        Status s = cur_tracker->Update(mut->timestamp(),
                                       idx_in_delta_tracker,
                                       mut->changelist(),
                                       max_op_id,
                                       result.get());
        DCHECK(s.ok()) << "Failed update on compaction for row " << output_row_offset
            << " @" << mut->timestamp() << ": " << mut->changelist().ToString(*schema);
        if (s.ok()) {
          // Update the set of delta trackers with the one we've just updated.
          InsertIfNotPresent(&updated_trackers, cur_tracker);
        }
      }

      if (is_garbage_collected) {
        DVLOG(4) << "Skipping GCed input row: " << schema->DebugRow(row.row)
                 << " while reupdating missed deltas";
      } else {
        // Important: We must ensure that GCed rows do not increment the output
        // row offset.
        output_row_offset++;
      }
    }

    RETURN_NOT_OK(input->FinishBlock());
  }

  // Flush the trackers that got updated, this will make sure that all missed deltas
  // get flushed before we update the tablet's metadata at the end of compaction/flush.
  // Note that we don't flush the metadata here, as to we will update the metadata
  // at the end of the compaction/flush.
  //
  // TODO: there should be a more elegant way of preventing metadata flush at this point
  // using pinning, or perhaps a builder interface for new rowset metadata objects.
  // See KUDU-204.

  {
    TRACE_EVENT0("tablet", "Flushing missed deltas");
    for (DeltaTracker* tracker : updated_trackers) {
      VLOG(1) << "Flushing DeltaTracker updated with missed deltas...";
      RETURN_NOT_OK_PREPEND(tracker->Flush(io_context, DeltaTracker::NO_FLUSH_METADATA),
                            "Could not flush delta tracker after missed delta update");
    }
  }

  return Status::OK();
}


Status DebugDumpCompactionInput(CompactionInput *input, vector<string> *lines) {
  RETURN_NOT_OK(input->Init());
  vector<CompactionInputRow> rows;

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    for (const CompactionInputRow &input_row : rows) {
      LOG_STRING(INFO, lines) << CompactionInputRowToString(input_row);
    }

    RETURN_NOT_OK(input->FinishBlock());
  }
  return Status::OK();
}


} // namespace tablet
} // namespace kudu
