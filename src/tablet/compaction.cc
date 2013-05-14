// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>

#include "tablet/compaction.h"
#include "tablet/layer.h"

namespace kudu {
namespace tablet {

namespace {

// CompactionInput yielding rows and mutations from a MemStore.
class MemstoreCompactionInput : boost::noncopyable, public CompactionInput {
 public:
  MemstoreCompactionInput(const MemStore &memstore,
                          const MvccSnapshot &snap) :
    iter_(memstore.NewIterator(memstore.schema(), snap))
  {}

  virtual Status Init() {
    return iter_->Init(NULL);
  }

  virtual bool HasMoreBlocks() {
    return iter_->HasNext();
  }

  virtual Status PrepareBlock(deque<CompactionInputRow> *block) {
    // To keep things simple for now, instead of yielding multiple
    // rows per "block", each block here is just a single row. This
    // means we don't need to make any extra copies or worry about
    // mutation, since the memstore iterator is already making a
    // local copy of whatever row it's pointed at. If this is a perf
    // bottleneck, we can re-evaluate.
    MSRow ms_row = iter_->GetCurrentRow();

    block->resize(1);
    CompactionInputRow &row = block->front();
    // The ms_row slice is non-const because we're given a copy of it, so it's
    // OK to mutate.
    row.row_ptr = const_cast<uint8_t *>(ms_row.row_slice().data());
    row.mutation_head = ms_row.mutation_head();

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
  gscoped_ptr<MemStore::Iterator> iter_;
};

////////////////////////////////////////////////////////////

// CompactionInput yielding rows and mutations from an on-disk Layer.
class LayerCompactionInput : boost::noncopyable, public CompactionInput {
 public:
  LayerCompactionInput(gscoped_ptr<RowwiseIterator> base_iter,
                       shared_ptr<DeltaIteratorInterface> delta_iter) :
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

  virtual Status PrepareBlock(deque<CompactionInputRow> *block) {
    RETURN_NOT_OK( RowwiseIterator::CopyBlock(base_iter_.get(), &block_) );
    std::fill(mutation_block_.begin(), mutation_block_.end(),
              reinterpret_cast<Mutation *>(NULL));
    RETURN_NOT_OK(delta_iter_->PrepareBatch(block_.nrows()));
    RETURN_NOT_OK(delta_iter_->CollectMutations(&mutation_block_, block_.arena()));

    block->resize(block_.nrows());
    for (int i = 0; i < block_.nrows(); i++) {
      CompactionInputRow &row = block->at(i);
      row.row_ptr = block_.row_ptr(i);
      row.mutation_head = mutation_block_[i];
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
  shared_ptr<DeltaIteratorInterface> delta_iter_;

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
    shared_ptr<CompactionInput> input;
    deque<CompactionInputRow> pending;
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
      if (!state.pending.empty() ||
          state.input->HasMoreBlocks()) {
        return true;
      }
    }

    return false;
  }

  virtual Status PrepareBlock(deque<CompactionInputRow> *block) {
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

        if (state.pending.empty()) {
          // If any of our inputs runs out of pending entries, then we can't keep
          // merging -- this input may have further blocks to process.
          // Rather than pulling another block here, stop the loop. If it's truly
          // out of blocks, then FinishBlock() will remove this input entirely.
          return Status::OK();
        }

        if (smallest_idx < 0 ||
            schema_.Compare(state.pending.front().row_ptr,
                            smallest.row_ptr) < 0) {
          smallest_idx = i;
          smallest = state.pending.front();
        }
      }
      DCHECK_GE(smallest_idx, 0);

      states_[smallest_idx].pending.pop_front();
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
      if (state.pending.empty()) {
        RETURN_NOT_OK(state.input->FinishBlock());
        if (state.input->HasMoreBlocks()) {
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

CompactionInput *CompactionInput::Create(const Layer &layer,
                                         const MvccSnapshot &snap) {

  shared_ptr<ColumnwiseIterator> base_cwise(layer.base_data_->NewIterator(layer.schema()));
  gscoped_ptr<RowwiseIterator> base_iter(new MaterializingIterator(base_cwise));
  shared_ptr<DeltaIteratorInterface> deltas(layer.delta_tracker_->NewDeltaIterator(layer.schema(), snap));

  return new LayerCompactionInput(base_iter.Pass(), deltas);
}

CompactionInput *CompactionInput::Create(const MemStore &memstore,
                                         const MvccSnapshot &snap) {
  return new MemstoreCompactionInput(memstore, snap);
}

CompactionInput *CompactionInput::Merge(const vector<shared_ptr<CompactionInput> > &inputs,
                                        const Schema &schema) {
  return new MergeCompactionInput(inputs, schema);
}


Status LayersInCompaction::CreateCompactionInput(const MvccSnapshot &snap, const Schema &schema,
                                                 shared_ptr<CompactionInput> *out) const {
  vector<shared_ptr<CompactionInput> > inputs;
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, layers_) {
    shared_ptr<CompactionInput> input(l->NewCompactionInput(snap));
    inputs.push_back(input);
  }

  if (inputs.size() == 1) {
    out->swap(inputs[0]);
  } else {
    out->reset(CompactionInput::Merge(inputs, schema));
  }

  return Status::OK();
}

void LayersInCompaction::DumpToLog() const {
  LOG(INFO) << "Selected " << layers_.size() << " layers to compact:";
  // Dump the selected layers to the log, and collect corresponding iterators.
  BOOST_FOREACH(const shared_ptr<LayerInterface> &l, layers_) {
    LOG(INFO) << l->ToString() << "(" << l->EstimateOnDiskSize() << " bytes)";
  }
}

static Status ApplyMutationsAndGenerateUndos(const Schema &schema, Mutation *mutation_head, Slice row_slice) {
  for (const Mutation *mut = mutation_head;
       mut != NULL;
       mut = mut->next()) {
    RowChangeListDecoder decoder(schema, mut->changelist_slice());
    DVLOG(2) << "  @" << mut->txid() << ": " << decoder.ToString();
    Status s = decoder.ApplyRowUpdate(&row_slice, reinterpret_cast<Arena *>(NULL));
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Unable to apply delta to row " << schema.DebugRow(row_slice.data()) << " during flush/compact";
      return s;
    }

    // TODO: write UNDO
  }
  return Status::OK();
}

Status Flush(CompactionInput *input, LayerWriter *out) {
  RETURN_NOT_OK(input->Init());
  deque<CompactionInputRow> rows;
  const Schema &schema(input->schema());

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    BOOST_FOREACH(const CompactionInputRow &row, rows) {
      Slice row_slice((uint8_t *)row.row_ptr, schema.byte_size());
      DVLOG(2) << "Row: " << schema.DebugRow(row.row_ptr) <<
        " mutations: " << Mutation::StringifyMutationList(schema, row.mutation_head);
      ApplyMutationsAndGenerateUndos(schema, row.mutation_head, row_slice);

      RETURN_NOT_OK(out->WriteRow(row_slice));
    }

    RETURN_NOT_OK(input->FinishBlock());
  }

  return Status::OK();
}

Status ReupdateMissedDeltas(CompactionInput *input,
                            const MvccSnapshot &snap_to_exclude,
                            const MvccSnapshot &snap_to_include,
                            DeltaTracker *delta_tracker) {
  // TODO: on this pass, we don't actually need the row data, just the
  // updates. So, this can be made much faster.
  deque<CompactionInputRow> rows;
  const Schema &schema(input->schema());

  rowid_t row_idx = 0;
  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&rows));

    BOOST_FOREACH(const CompactionInputRow &row, rows) {
      Slice row_slice((uint8_t *)row.row_ptr, schema.byte_size());
      DVLOG(2) << "Revisiting row: " << schema.DebugRow(row.row_ptr) <<
        " mutations: " << Mutation::StringifyMutationList(schema, row.mutation_head);

      for (const Mutation *mut = row.mutation_head;
           mut != NULL;
           mut = mut->next()) {

        if (snap_to_exclude.IsCommitted(mut->txid())) {
          // Was already taken into account in the main flush.
          continue;
        }
        RowChangeListDecoder decoder(schema, mut->changelist_slice());
        if (!snap_to_include.IsCommitted(mut->txid())) {
          DVLOG(2) << "Skipping already-duplicated delta for row " << row_idx
                   << " @" << mut->txid() << ": " << decoder.ToString();

          // Already duplicated into the new layer, no need to transfer it over.
          continue;
        }

        LOG(INFO) << "Flushing missed delta for row " << row_idx
                  << " @" << mut->txid() << ": " << decoder.ToString();

        delta_tracker->Update(mut->txid(), row_idx, RowChangeList(mut->changelist_slice()));
      }
      row_idx++;
    }

    RETURN_NOT_OK(input->FinishBlock());
  }

  return Status::OK();
}



} // namespace tablet
} // namespace kudu
