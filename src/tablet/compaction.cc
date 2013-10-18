// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <deque>
#include <string>
#include <vector>

#include "common/wire_protocol.h"
#include "gutil/macros.h"
#include "gutil/stl_util.h"
#include "tablet/compaction.h"
#include "tablet/diskrowset.h"

namespace kudu {
namespace tablet {

namespace {

// CompactionInput yielding rows and mutations from a MemRowSet.
class MemRowSetCompactionInput : public CompactionInput {
 public:
  MemRowSetCompactionInput(const MemRowSet& memrowset,
                           const MvccSnapshot& snap,
                           const Schema& projection)
    : iter_(memrowset.NewIterator(projection, snap)),
      arena_(32*1024, 128*1024) {
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

    arena_.Reset();
    for (int i = 0; i < num_in_block; i++) {
      if (i > 0) {
        iter_->Next();
      }

      // TODO: A copy is performed to have all CompactionInputRow of the same type
      CompactionInputRow &input_row = block->at(i);
      input_row.row.Reset(row_block_.get(), i);
      RETURN_NOT_OK(iter_->GetCurrentRow(&input_row.row, reinterpret_cast<Arena*>(NULL),
                                         &input_row.mutation_head, &arena_));
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
  DISALLOW_COPY_AND_ASSIGN(MemRowSetCompactionInput);
  gscoped_ptr<RowBlock> row_block_;

  gscoped_ptr<MemRowSet::Iterator> iter_;

  // Arena used to store the projected mutations of the current block.
  Arena arena_;
};

////////////////////////////////////////////////////////////

// CompactionInput yielding rows and mutations from an on-disk DiskRowSet.
class RowSetCompactionInput : public CompactionInput {
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
    RETURN_NOT_OK(RowwiseIterator::CopyBlock(base_iter_.get(), &block_));
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
  DISALLOW_COPY_AND_ASSIGN(RowSetCompactionInput);
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

    // Return true if the current block of this input fully dominates
    // the current block of the other input -- i.e that the last
    // row of this block is less than the first row of the other block.
    // In this case, we can remove the other input from the merge until
    // this input's current block has been exhausted.
    bool Dominates(const MergeState &other, const Schema &schema) const {
      DCHECK(!empty());
      DCHECK(!other.empty());

      return schema.Compare(pending.back().row, other.next().row) < 0;
    }

    shared_ptr<CompactionInput> input;
    vector<CompactionInputRow> pending;
    int pending_idx;

    vector<MergeState *> dominated;
  };

 public:
  MergeCompactionInput(const vector<shared_ptr<CompactionInput> > &inputs,
                       const Schema &schema)
    : schema_(schema) {
    BOOST_FOREACH(const shared_ptr<CompactionInput> &input, inputs) {
      gscoped_ptr<MergeState> state(new MergeState);
      state->input = input;
      states_.push_back(state.release());
    }
  }

  virtual ~MergeCompactionInput() {
    STLDeleteElements(&states_);
  }

  virtual Status Init() {
    BOOST_FOREACH(MergeState *state, states_) {
      RETURN_NOT_OK(state->input->Init());
    }

    // Pull the first block of rows from each input.
    RETURN_NOT_OK(ProcessEmptyInputs());
    return Status::OK();
  }

  virtual bool HasMoreBlocks() {
    // Return true if any of the input blocks has more rows pending
    // or more blocks which have yet to be pulled.
    BOOST_FOREACH(MergeState *state, states_) {
      if (!state->empty() ||
          state->input->HasMoreBlocks()) {
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
        MergeState *state = states_[i];

        if (state->empty()) {
          // If any of our inputs runs out of pending entries, then we can't keep
          // merging -- this input may have further blocks to process.
          // Rather than pulling another block here, stop the loop. If it's truly
          // out of blocks, then FinishBlock() will remove this input entirely.
          return Status::OK();
        }

        if (smallest_idx < 0 || schema_.Compare(state->next().row, smallest.row) < 0) {
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

  virtual const Schema &schema() const {
    return schema_;
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
      for (vector<MergeState *>::iterator it = state->dominated.begin();
           it != state->dominated.end();
           ++it) {
        MergeState *dominated = *it;
        if (!state->Dominates(*dominated, schema_)) {
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
    if (dominator->Dominates(*candidate, schema_)) {
      dominator->dominated.push_back(candidate);
      return true;
    } else {
      return false;
    }
  }

  const Schema schema_;
  vector<MergeState *> states_;
};

} // anonymous namespace

////////////////////////////////////////////////////////////

CompactionInput *CompactionInput::Create(const DiskRowSet &rowset,
                                         const Schema& projection,
                                         const MvccSnapshot &snap) {

  shared_ptr<ColumnwiseIterator> base_cwise(rowset.base_data_->NewIterator(projection));
  gscoped_ptr<RowwiseIterator> base_iter(new MaterializingIterator(base_cwise));
  shared_ptr<DeltaIterator> deltas(rowset.delta_tracker_->NewDeltaIterator(projection, snap));

  return new RowSetCompactionInput(base_iter.Pass(), deltas);
}

CompactionInput *CompactionInput::Create(const MemRowSet &memrowset,
                                         const Schema& projection,
                                         const MvccSnapshot &snap) {
  return new MemRowSetCompactionInput(memrowset, snap, projection);
}

CompactionInput *CompactionInput::Merge(const vector<shared_ptr<CompactionInput> > &inputs,
                                        const Schema &schema) {
  return new MergeCompactionInput(inputs, schema);
}


Status RowSetsInCompaction::CreateCompactionInput(const MvccSnapshot &snap, const Schema &schema,
                                                 shared_ptr<CompactionInput> *out) const {
  vector<shared_ptr<CompactionInput> > inputs;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_) {
    shared_ptr<CompactionInput> input(rs->NewCompactionInput(schema, snap));
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

static Status ApplyMutationsAndGenerateUndos(const MvccSnapshot &snap,
                                             const Mutation *mutation_head,
                                             RowBlockRow *row,
                                             bool *is_deleted) {
  *is_deleted = false;

  const Schema& schema = row->schema();

  #define ERROR_LOG_CONTEXT \
    "Row: " << schema.DebugRow(*row) << \
    " Mutations: " << Mutation::StringifyMutationList(schema, mutation_head)

  for (const Mutation *mut = mutation_head; mut != NULL; mut = mut->next()) {
    RowChangeListDecoder decoder(schema, mut->changelist());

    // Skip anything not committed.
    if (!snap.IsCommitted(mut->txid())) {
      continue;
    }

    DVLOG(3) << "  @" << mut->txid() << ": " << mut->changelist().ToString(schema);
    Status s = decoder.Init();
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Unable to decode changelist. " << ERROR_LOG_CONTEXT;
      return s;
    }

    if (decoder.is_update()) {
      DCHECK(!*is_deleted) << "Got UPDATE for deleted row. " << ERROR_LOG_CONTEXT;
      s = decoder.ApplyRowUpdate(row, reinterpret_cast<Arena *>(NULL));

    } else if (decoder.is_delete() || decoder.is_reinsert()) {
      decoder.TwiddleDeleteStatus(is_deleted);

      if (decoder.is_reinsert()) {
        // On reinsert, we have to copy the reinserted row over.
        ConstContiguousRow reinserted(schema, decoder.reinserted_row_slice().data());
        Arena* arena = NULL; // No need to copy into an arena -- can refer to the mutation's arena.
        RETURN_NOT_OK(CopyRow(reinserted, row, arena));
      }
    } else {
      LOG(FATAL) << "Unknown mutation type!" << ERROR_LOG_CONTEXT;
    }

    // TODO: write UNDO
  }

  return Status::OK();

  #undef ERROR_LOG_CONTEXT
}

Status Flush(CompactionInput *input, const MvccSnapshot &snap,
             RollingDiskRowSetWriter *out) {
  RETURN_NOT_OK(input->Init());
  vector<CompactionInputRow> rows;

  RowBlock block(out->schema(), 100, NULL);

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    int n = 0;
    BOOST_FOREACH(const CompactionInputRow &input_row, rows) {
      const Schema& schema(input_row.row.schema());
      DCHECK_SCHEMA_EQ(schema, out->schema());

      RowBlockRow dst_row = block.row(n);
      RETURN_NOT_OK(CopyRow(input_row.row, &dst_row, reinterpret_cast<Arena*>(NULL)));
      DVLOG(2) << "Row: " << dst_row.schema().DebugRow(dst_row) <<
        " mutations: " << Mutation::StringifyMutationList(schema, input_row.mutation_head);

      bool is_deleted;
      RETURN_NOT_OK(ApplyMutationsAndGenerateUndos(
                      snap, input_row.mutation_head, &dst_row, &is_deleted));

      if (is_deleted) {
        DVLOG(2) << "Deleted!";
        // Don't flush the row.
        continue;
      }

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

Status ReupdateMissedDeltas(const string &tablet_name,
                            TransactionContext *tx_ctx,
                            CompactionInput *input,
                            const MvccSnapshot &snap_to_exclude,
                            const MvccSnapshot &snap_to_include,
                            const RowSetVector &output_rowsets) {
  RETURN_NOT_OK(input->Init());

  VLOG(1) << "Re-updating missed deltas between snapshot " <<
    snap_to_exclude.ToString() << " and " << snap_to_include.ToString();

  // Collect the delta trackers that we'll push the updates into.
  deque<DeltaTracker *> delta_trackers;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, output_rowsets) {
    delta_trackers.push_back(down_cast<DiskRowSet *>(rs.get())->delta_tracker());
  }

  // The rowid where the current (front) delta tracker starts.
  int64_t delta_tracker_base_row = 0;

  // TODO: on this pass, we don't actually need the row data, just the
  // updates. So, this can be made much faster.
  vector<CompactionInputRow> rows;
  const Schema &schema(input->schema());
  const Schema key_schema(input->schema().CreateKeyProjection());

  // Arena and projector to store/project row keys for missed delta updates
  Arena arena(1024, 1024*1024);
  RowProjector key_projector;
  RETURN_NOT_OK(key_projector.Init(schema, key_schema));
  faststring buf;

  rowid_t row_idx = 0;
  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    BOOST_FOREACH(const CompactionInputRow &row, rows) {
      DVLOG(2) << "Revisiting row: " << schema.DebugRow(row.row) <<
          " mutations: " << Mutation::StringifyMutationList(schema, row.mutation_head);

      bool is_deleted_in_main_flush = false;

      for (const Mutation *mut = row.mutation_head;
           mut != NULL;
           mut = mut->next()) {
        RowChangeListDecoder decoder(schema, mut->changelist());
        RETURN_NOT_OK(decoder.Init());

        if (snap_to_exclude.IsCommitted(mut->txid())) {
          // This update was already taken into account in the first phase of the
          // compaction.

          // If it's a DELETE or REINSERT, though, we need to track the state of the
          // row - this lets us account the current rowid on the output side of the
          // compaction below.
          decoder.TwiddleDeleteStatus(&is_deleted_in_main_flush);
          continue;
        }

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
          << " row=" << schema.DebugRow(row.row)
          << " mutations=" << Mutation::StringifyMutationList(schema, row.mutation_head);

        if (!snap_to_include.IsCommitted(mut->txid())) {
          // The mutation was inserted after the DuplicatingRowSet was swapped in.
          // Therefore, it's already present in the output rowset, and we don't need
          // to copy it in.

          DVLOG(2) << "Skipping already-duplicated delta for row " << row_idx
                   << " @" << mut->txid() << ": " << mut->changelist().ToString(schema);
          continue;
        }

        // Otherwise, this is an update that arrived after the snapshot for the first
        // pass, but before the DuplicatingRowSet was swapped in. We need to transfer
        // this over to the output rowset.
        DVLOG(1) << "Flushing missed delta for row " << row_idx
                  << " @" << mut->txid() << ": " << mut->changelist().ToString(schema);

        DeltaTracker *cur_tracker = delta_trackers.front();

        // The index on the input side isn't necessarily the index on the output side:
        // we may have output several small DiskRowSets, so we need to find the index
        // relative to the current one.
        int64_t idx_in_delta_tracker = row_idx - delta_tracker_base_row;
        while (idx_in_delta_tracker >= cur_tracker->num_rows()) {
          // If the current index is higher than the total number of rows in the current
          // DeltaTracker, that means we're now processing the next one in the list.
          // Pop the current front tracker, and make the indexes relative to the next
          // in the list.
          delta_tracker_base_row += cur_tracker->num_rows();
          idx_in_delta_tracker -= cur_tracker->num_rows();
          DCHECK_GE(idx_in_delta_tracker, 0);
          delta_trackers.pop_front();
          cur_tracker = delta_trackers.front();
        }

        gscoped_ptr<MutationResultPB> result(new MutationResultPB);
        Status s = cur_tracker->Update(mut->txid(),
                                       row_idx,
                                       schema,
                                       mut->changelist(),
                                       result.get());
        DCHECK(s.ok()) << "Failed update on compaction for row " << row_idx
            << " @" << mut->txid() << ": " << mut->changelist().ToString(schema);
        if (s.ok()) {
          // TODO Making missed deltas take the whole row key is a lot simpler
          // than just storing the row_idx (which would require a complex
          // method such as this one to find the right delta to put in on
          // replay and accessing private tablet state).
          // On the other hand using a row block for a single row key
          // seems wasteful...
          gscoped_ptr<RowwiseRowBlockPB> row_block(new RowwiseRowBlockPB);
          SchemaToColumnPBs(schema, row_block->mutable_schema());

          buf.clear();
          ContiguousRow row_key(key_schema, buf.data());
          key_projector.ProjectRowForWrite(row.row, &row_key, &arena);
          ConstContiguousRow const_row_key(row_key);
          AddRowToRowBlockPB(const_row_key, row_block.get());

          tx_ctx->AddMissedMutation(mut->txid(),
                                    row_block.Pass(),
                                    mut->changelist(),
                                    result.Pass());
        } else {
          tx_ctx->AddFailedMutation(s);
        }
      }

      // If the first pass of the flush counted this row as deleted, then it isn't
      // in the output at all, and therefore we shouldn't count it when determining
      // the row id of the output rows.
      if (!is_deleted_in_main_flush) {
        row_idx++;
      }
    }

    RETURN_NOT_OK(input->FinishBlock());
  }

  return Status::OK();
}


Status DebugDumpCompactionInput(CompactionInput *input, vector<string> *lines) {
  RETURN_NOT_OK(input->Init());
  vector<CompactionInputRow> rows;

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    BOOST_FOREACH(const CompactionInputRow &input_row, rows) {
      const Schema& schema = input_row.row.schema();
      LOG_STRING(INFO, lines) << schema.DebugRow(input_row.row) <<
        " mutations: " + Mutation::StringifyMutationList(schema, input_row.mutation_head);
    }

    RETURN_NOT_OK(input->FinishBlock());
  }
  return Status::OK();
}


} // namespace tablet
} // namespace kudu
