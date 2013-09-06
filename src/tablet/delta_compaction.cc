// Copyright (c) 2013, Cloudera, inc.

#include "tablet/delta_compaction.h"

#include <string>
#include <vector>

#include "gutil/stl_util.h"
#include "common/columnblock.h"
#include "cfile/cfile_reader.h"
#include "tablet/delta_key.h"

namespace kudu {

using cfile::CFileReader;
using cfile::IndexTreeIterator;
using cfile::CFileIterator;

namespace tablet {

namespace {

// TODO : we will probably want a DeltaMemStoreCompactionInput in
//        order to support FlushAndCompact.
class DeltaFileCompactionInput : public DeltaCompactionInput {
 public:

  explicit DeltaFileCompactionInput(gscoped_ptr<CFileIterator> iter)
      : iter_(iter.Pass()),
        data_(new Slice[kRowsPerBlock]),
        arena_(32*1024, 128*1024),
        block_(GetTypeInfo(STRING),
               NULL,
               data_.get(),
               kRowsPerBlock,
               &arena_),
        initted_(false),
        block_prepared_(false) {
  }

  virtual Status Init() {
    DCHECK(!initted_);
    RETURN_NOT_OK(iter_->SeekToFirst());
    initted_ = true;
    return Status::OK();
  }

  virtual bool HasMoreBlocks() {
    return iter_->HasNext();
  }

  virtual Status PrepareBlock(vector<DeltaCompactionInputCell> *block) {
    size_t nrows = kRowsPerBlock;
    RETURN_NOT_OK(iter_->PrepareBatch(&nrows));
    RETURN_NOT_OK(iter_->Scan(&block_));

    block->resize(nrows);
    for (int i = 0; i < nrows; i++) {
      DeltaCompactionInputCell &input_cell = (*block)[i];
      Slice s(data_[i]);
      RETURN_NOT_OK(input_cell.key.DecodeFrom(&s));
      input_cell.cell = s;
    }
    block_prepared_ = true;
    return Status::OK();
  }

  virtual Status FinishBlock() {
    if (block_prepared_) {
      RETURN_NOT_OK(iter_->FinishBatch());
    }
    block_prepared_ = false;
    return Status::OK();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(DeltaFileCompactionInput);

  gscoped_ptr<CFileIterator> iter_;
  gscoped_ptr<Slice[]> data_;
  Arena arena_;
  ColumnBlock block_;

  bool initted_;

  enum {
    kRowsPerBlock = 100 // Number of rows per block of clumns
  };

  bool block_prepared_;
};

// This class adapts the algorithm from rowset compaction. See
// MergeCompactionInput in compaction.cc for more in-depth information
// on the algorithm.
//
// TODO : find a way to re-use the merging iterator between delta
//        compaction and rowset compaction (if this is the route we
//        chose to go down).
class MergeDeltaCompactionInput : public DeltaCompactionInput {
 private:

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

    const DeltaCompactionInputCell &next() const {
      return pending[pending_idx];
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
    bool Dominates(const MergeState &other) const {
      DCHECK(!empty());
      DCHECK(!other.empty());

      return pending.back().key.CompareTo(other.next().key) < 0;
    }

    shared_ptr<DeltaCompactionInput> input;
    vector<DeltaCompactionInputCell> pending;
    int pending_idx;

    vector<MergeState *> dominated;
  };

 public:
  explicit MergeDeltaCompactionInput(
      const vector<shared_ptr<DeltaCompactionInput> > &inputs) {
    BOOST_FOREACH(const shared_ptr<DeltaCompactionInput> &input, inputs) {
      gscoped_ptr<MergeState> state(new MergeState);
      state->input = input;
      states_.push_back(state.release());
    }
  }

  virtual ~MergeDeltaCompactionInput() {
    STLDeleteElements(&states_);
  }

  virtual Status Init() {
    BOOST_FOREACH(MergeState *state, states_) {
      RETURN_NOT_OK(state->input->Init());
    }

    RETURN_NOT_OK(ProcessEmptyInputs());
    return Status::OK();
  }

  virtual bool HasMoreBlocks() {
    BOOST_FOREACH(MergeState *state, states_) {
      if (!state->empty() ||
          state->input->HasMoreBlocks()) {
        return true;
      }
    }

    return false;
  }

  virtual Status PrepareBlock(vector<DeltaCompactionInputCell> *block) {
    CHECK(!states_.empty());

    block->clear();

    while (true) {
      int smallest_idx = -1;
      DeltaCompactionInputCell smallest;

      // Iterate over the inputs to find the one with the smallest next row.
      // It may seem like an O(n lg k) merge using a heap would be more efficient,
      // but some benchmarks indicated that the simpler code path of the O(n k) merge
      // actually ends up a bit faster.
      for (int i = 0; i < states_.size(); i++) {
        MergeState *state = states_[i];

        if (state->empty()) {
          // If any of our inputs runs out of pending entries, then we can't keep
          // merging -- this input may have further blocks to process.
          // Rather than pulling another block here, stop the loop. If it's truly
          // out of blocks, then FinishBlock() will remove this input entirely.
          return Status::OK();
        }

        if (smallest_idx < 0 || state->next().key.CompareTo(smallest.key) < 0) {
          smallest_idx = i;
          smallest = state->next();
        }
      }
      DCHECK_GE(smallest_idx, 0);

      states_[smallest_idx]->pop_front();
      block->push_back(smallest);
    }

    return Status::OK();
  }

  virtual Status FinishBlock() {
    return ProcessEmptyInputs();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(MergeDeltaCompactionInput);

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
      for (vector<MergeState *>::iterator it = state->dominated.begin();
           it != state->dominated.end();
           ++it) {
        MergeState *dominated = *it;
        if (!state->Dominates(*dominated)) {
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
    if (dominator->Dominates(*candidate)) {
      dominator->dominated.push_back(candidate);
      return true;
    } else {
      return false;
    }
  }

  vector<MergeState *> states_;
};

} // anonymous namespace

Status DeltaCompactionInput::Open(const DeltaFileReader &reader,
                                  gscoped_ptr<DeltaCompactionInput> *input) {
  gscoped_ptr<CFileIterator> iter;
  RETURN_NOT_OK(reader.cfile_reader()->NewIterator(&iter));
  input->reset(new DeltaFileCompactionInput(iter.Pass()));
  return Status::OK();
}

DeltaCompactionInput *DeltaCompactionInput::Merge(const vector<shared_ptr<DeltaCompactionInput> > &inputs) {
  return new MergeDeltaCompactionInput(inputs);
}

static string FormatDebugDeltaCell(const Schema &schema, const DeltaCompactionInputCell &cell) {
  string key_str = StringPrintf("(row %04u@tx%04"TXID_PRINT_FORMAT")", cell.key.row_idx(),
                                cell.key.txid().v);
  return StringPrintf("(delta key=%s, change_list=%s)", key_str.c_str(),
                      RowChangeList(cell.cell).ToString(schema).c_str());
}

Status DebugDumpDeltaCompactionInput(DeltaCompactionInput *input, vector<string> *lines,
                                     const Schema &schema) {
  RETURN_NOT_OK(input->Init());
  vector<DeltaCompactionInputCell> cells;

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&cells));

    BOOST_FOREACH(const DeltaCompactionInputCell &cell, cells) {
      LOG_STRING(INFO, lines) << FormatDebugDeltaCell(schema, cell);
    }
    RETURN_NOT_OK(input->FinishBlock());
  }

  return Status::OK();
}


} // namespace tablet
} // namespace kudu
