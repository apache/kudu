// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>

#include "tablet/compaction.h"
#include "tablet/diskrowset.h"

namespace kudu {
namespace tablet {

namespace {

// CompactionInput yielding rows and mutations from a MemRowSet.
class MemRowSetCompactionInput : boost::noncopyable, public CompactionInput {
 public:
  MemRowSetCompactionInput(const MemRowSet &memrowset,
                          const MvccSnapshot &snap) :
    iter_(memrowset.NewIterator(memrowset.schema(), snap))
  {
  }

  virtual Status Init() {
    return iter_->Init(NULL);
  }

  virtual bool HasMoreBlocks() {
    return iter_->HasNext();
  }

  virtual Status PrepareBlock(vector<CompactionInputRow> *block) {

    int num_in_block = iter_->remaining_in_leaf();
    block->resize(num_in_block);

    // Realloc the internal block storage if we don't have enough space to
    // copy the whole leaf node's worth of data into it.
    if (PREDICT_FALSE(!row_block_ || num_in_block > row_block_->nrows())) {
      row_block_.reset(new RowBlock(iter_->schema(), num_in_block, NULL));
    }

    for (int i = 0; i < num_in_block; i++) {
      if (i > 0) {
        iter_->Next();
      }

      // TODO: A copy is performed to have all CompactionInputRow of the same type
      CompactionInputRow &input_row = block->at(i);
      MRSRow ms_row = iter_->GetCurrentRow();
      input_row.row.Reset(row_block_.get(), i)->CopyCellsFrom(iter_->schema(), ms_row);
      input_row.mutation_head = ms_row.mutation_head();
    }

    return Status::OK();
  }

  virtual Status FinishBlock() {
    iter_->Next();
    return Status::OK();
  }

  virtual const Schema &schema() const {
    return iter_->schema();
  }

 private:
  gscoped_ptr<RowBlock> row_block_;
  gscoped_ptr<MemRowSet::Iterator> iter_;
};

////////////////////////////////////////////////////////////

// CompactionInput yielding rows and mutations from an on-disk DiskRowSet.
class RowSetCompactionInput : boost::noncopyable, public CompactionInput {
 public:
  RowSetCompactionInput(gscoped_ptr<RowwiseIterator> base_iter,
                       shared_ptr<DeltaIterator> delta_iter) :
    base_iter_(base_iter.Pass()),
    delta_iter_(delta_iter),
    arena_(32*1024, 128*1024),
    block_(base_iter_->schema(), kRowsPerBlock, &arena_),
    mutation_block_(kRowsPerBlock, reinterpret_cast<Mutation *>(NULL)),
    first_rowid_in_block_(0)
  {}

  virtual Status Init() {
    RETURN_NOT_OK(base_iter_->Init(NULL));
    RETURN_NOT_OK(delta_iter_->Init());
    RETURN_NOT_OK(delta_iter_->SeekToOrdinal(0));
    return Status::OK();
  }

  virtual bool HasMoreBlocks() {
    return base_iter_->HasNext();
  }

  virtual Status PrepareBlock(vector<CompactionInputRow> *block) {
    RETURN_NOT_OK( RowwiseIterator::CopyBlock(base_iter_.get(), &block_) );
    std::fill(mutation_block_.begin(), mutation_block_.end(),
              reinterpret_cast<Mutation *>(NULL));
    RETURN_NOT_OK(delta_iter_->PrepareBatch(block_.nrows()));
    RETURN_NOT_OK(delta_iter_->CollectMutations(&mutation_block_, block_.arena()));

    block->resize(block_.nrows());
    for (int i = 0; i < block_.nrows(); i++) {
      CompactionInputRow &input_row = block->at(i);
      input_row.row.Reset(&block_, i);
      input_row.mutation_head = mutation_block_[i];
    }

    first_rowid_in_block_ += block_.nrows();
    return Status::OK();
  }

  virtual Status FinishBlock() {
    return Status::OK();
  }

  virtual const Schema &schema() const {
    return base_iter_->schema();
  }
 private:
  gscoped_ptr<RowwiseIterator> base_iter_;
  shared_ptr<DeltaIterator> delta_iter_;

  Arena arena_;

  // The current block of data which has come from the input iterator
  RowBlock block_;
  vector<Mutation *> mutation_block_;

  rowid_t first_rowid_in_block_;

  enum {
    kRowsPerBlock = 100
  };
};

class MergeCompactionInput : boost::noncopyable, public CompactionInput {
 private:
  // State kept for each of the inputs.
  struct MergeState {
    MergeState() :
      pending_idx(0)
    {}

    bool empty() const {
      return pending_idx >= pending.size();
    }

    const CompactionInputRow &next() const {
      return pending[pending_idx];
    }

    void pop_front() {
      pending_idx++;
    }

    void Reset() {
      pending.clear();
      pending_idx = 0;
    }

    shared_ptr<CompactionInput> input;
    vector<CompactionInputRow> pending;
    int pending_idx;
  };

 public:
  MergeCompactionInput(const vector<shared_ptr<CompactionInput> > &inputs,
                       const Schema &schema) :
    schema_(schema)
  {
    BOOST_FOREACH(const shared_ptr<CompactionInput> &input, inputs) {
      MergeState state;
      state.input = input;
      states_.push_back(state);
    }
  }

  virtual Status Init() {
    BOOST_FOREACH(MergeState &state, states_) {
      RETURN_NOT_OK(state.input->Init());
    }

    // Pull the first block of rows from each input.
    RETURN_NOT_OK(ProcessEmptyInputs());
    return Status::OK();
  }

  virtual bool HasMoreBlocks() {
    // Return true if any of the input blocks has more rows pending
    // or more blocks which have yet to be pulled.
    BOOST_FOREACH(MergeState &state, states_) {
      if (!state.empty() ||
          state.input->HasMoreBlocks()) {
        return true;
      }
    }

    return false;
  }

  virtual Status PrepareBlock(vector<CompactionInputRow> *block) {
    CHECK(!states_.empty());

    block->clear();

    while (true) {
      int smallest_idx = -1;
      CompactionInputRow smallest;

      // Iterate over the inputs to find the one with the smallest next row.
      // It may seem like an O(n lg k) merge using a heap would be more efficient,
      // but some benchmarks indicated that the simpler code path of the O(n k) merge
      // actually ends up a bit faster.
      for (int i = 0; i < states_.size(); i++) {
        MergeState &state = states_[i];

        if (state.empty()) {
          // If any of our inputs runs out of pending entries, then we can't keep
          // merging -- this input may have further blocks to process.
          // Rather than pulling another block here, stop the loop. If it's truly
          // out of blocks, then FinishBlock() will remove this input entirely.
          return Status::OK();
        }

        if (smallest_idx < 0 || schema_.Compare(state.next().row, smallest.row) < 0) {
          smallest_idx = i;
          smallest = state.next();
        }
      }
      DCHECK_GE(smallest_idx, 0);

      states_[smallest_idx].pop_front();
      block->push_back(smallest);
    }

    return Status::OK();
  }

  virtual Status FinishBlock() {
    return ProcessEmptyInputs();
  }

  virtual const Schema &schema() const {
    return schema_;
  }

 private:

  // Look through our current set of inputs. For any that are empty,
  // pull the next block into its pending list. If there is no next
  // block, remove it from our input set.
  //
  // Postcondition: every input has a non-empty pending list.
  Status ProcessEmptyInputs() {
    vector<MergeState>::iterator it = states_.begin();
    while (it != states_.end()) {
      MergeState &state = *it;
      if (state.empty()) {
        RETURN_NOT_OK(state.input->FinishBlock());
        if (state.input->HasMoreBlocks()) {
          state.Reset();
          RETURN_NOT_OK(state.input->PrepareBlock(&state.pending));
        } else {
          it = states_.erase(it);
          continue;
        }
      }
      ++it;
    }
    return Status::OK();
  }

  const Schema schema_;
  vector<MergeState> states_;
};

} // anonymous namespace

////////////////////////////////////////////////////////////

CompactionInput *CompactionInput::Create(const DiskRowSet &rowset,
                                         const MvccSnapshot &snap) {

  shared_ptr<ColumnwiseIterator> base_cwise(rowset.base_data_->NewIterator(rowset.schema()));
  gscoped_ptr<RowwiseIterator> base_iter(new MaterializingIterator(base_cwise));
  shared_ptr<DeltaIterator> deltas(rowset.delta_tracker_->NewDeltaIterator(rowset.schema(), snap));

  return new RowSetCompactionInput(base_iter.Pass(), deltas);
}

CompactionInput *CompactionInput::Create(const MemRowSet &memrowset,
                                         const MvccSnapshot &snap) {
  return new MemRowSetCompactionInput(memrowset, snap);
}

CompactionInput *CompactionInput::Merge(const vector<shared_ptr<CompactionInput> > &inputs,
                                        const Schema &schema) {
  return new MergeCompactionInput(inputs, schema);
}


Status RowSetsInCompaction::CreateCompactionInput(const MvccSnapshot &snap, const Schema &schema,
                                                 shared_ptr<CompactionInput> *out) const {
  vector<shared_ptr<CompactionInput> > inputs;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_) {
    shared_ptr<CompactionInput> input(rs->NewCompactionInput(snap));
    inputs.push_back(input);
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
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_) {
    LOG(INFO) << rs->ToString() << "(" << rs->EstimateOnDiskSize() << " bytes)";
  }
}

template <class RowType>
static Status ApplyMutationsAndGenerateUndos(const Schema &schema, Mutation *mutation_head, RowType *row) {
  for (const Mutation *mut = mutation_head; mut != NULL; mut = mut->next()) {
    RowChangeListDecoder decoder(schema, mut->changelist());
    
    DVLOG(2) << "  @" << mut->txid() << ": " << mut->changelist().ToString(schema);
    Status s = decoder.Init();
    if (PREDICT_TRUE(s.ok())) {
      s = decoder.ApplyRowUpdate(row, reinterpret_cast<Arena *>(NULL));
    }

    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Unable to apply delta to row " << schema.DebugRow(*row) << " during flush/compact";
      return s;
    }

    // TODO: write UNDO
  }
  return Status::OK();
}

Status Flush(CompactionInput *input, DiskRowSetWriter *out) {
  RETURN_NOT_OK(input->Init());
  vector<CompactionInputRow> rows;
  const Schema &schema(input->schema());

  RowBlock block(schema, 100, NULL);

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    int n = 0;
    BOOST_FOREACH(const CompactionInputRow &input_row, rows) {
      RowBlockRow dst_row = block.row(n);
      dst_row.CopyCellsFrom(schema, input_row.row);
      DVLOG(2) << "Row: " << schema.DebugRow(dst_row) <<
        " mutations: " << Mutation::StringifyMutationList(schema, input_row.mutation_head);
      ApplyMutationsAndGenerateUndos(schema, input_row.mutation_head, &dst_row);

      n++;
      if (n == block.nrows()) {
        RETURN_NOT_OK(out->AppendBlock(block));
        n = 0;
      }
    }

    if (n > 0) {
      block.Resize(n);
      RETURN_NOT_OK(out->AppendBlock(block));
    }

    RETURN_NOT_OK(input->FinishBlock());
  }

  return Status::OK();
}

Status ReupdateMissedDeltas(CompactionInput *input,
                            const MvccSnapshot &snap_to_exclude,
                            const MvccSnapshot &snap_to_include,
                            DeltaTracker *delta_tracker) {
  VLOG(1) << "Re-updating missed deltas between snapshot " <<
    snap_to_exclude.ToString() << " and " << snap_to_include.ToString();

  // TODO: on this pass, we don't actually need the row data, just the
  // updates. So, this can be made much faster.
  vector<CompactionInputRow> rows;
  const Schema &schema(input->schema());

  rowid_t row_idx = 0;
  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    BOOST_FOREACH(const CompactionInputRow &row, rows) {
      DVLOG(2) << "Revisiting row: " << schema.DebugRow(row.row) <<
          " mutations: " << Mutation::StringifyMutationList(schema, row.mutation_head);

      for (const Mutation *mut = row.mutation_head;
           mut != NULL;
           mut = mut->next()) {

        if (snap_to_exclude.IsCommitted(mut->txid())) {
          // Was already taken into account in the main flush.
          continue;
        }
        if (!snap_to_include.IsCommitted(mut->txid())) {
          DVLOG(2) << "Skipping already-duplicated delta for row " << row_idx
                   << " @" << mut->txid() << ": " << mut->changelist().ToString(schema);

          // Already duplicated into the new rowset, no need to transfer it over.
          continue;
        }

        DVLOG(1) << "Flushing missed delta for row " << row_idx
                  << " @" << mut->txid() << ": " << mut->changelist().ToString(schema);

        delta_tracker->Update(mut->txid(), row_idx, mut->changelist());
      }
      row_idx++;
    }

    RETURN_NOT_OK(input->FinishBlock());
  }

  return Status::OK();
}


Status DebugDumpCompactionInput(CompactionInput *input, vector<string> *lines) {
  RETURN_NOT_OK(input->Init());
  vector<CompactionInputRow> rows;
  const Schema &schema = input->schema();

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    BOOST_FOREACH(const CompactionInputRow &input_row, rows) {
      LOG_STRING(INFO, lines) << schema.DebugRow(input_row.row) <<
        " mutations: " + Mutation::StringifyMutationList(schema, input_row.mutation_head);
    }

    RETURN_NOT_OK(input->FinishBlock());
  }
  return Status::OK();
}


} // namespace tablet
} // namespace kudu
