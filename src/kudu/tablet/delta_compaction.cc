// Copyright (c) 2013, Cloudera, inc.

#include "kudu/tablet/delta_compaction.h"

#include <boost/assign/list_of.hpp>
#include <string>
#include <vector>
#include <algorithm>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/common/columnblock.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/deltamemstore.h"
#include "kudu/tablet/multi_column_writer.h"
#include "kudu/tablet/mvcc.h"

namespace kudu {

using cfile::CFileReader;
using cfile::IndexTreeIterator;
using cfile::CFileIterator;
using metadata::RowSetMetadata;
using metadata::TabletMetadata;
using metadata::ColumnIndexes;
using metadata::ColumnWriters;
using strings::Substitute;

namespace tablet {

namespace {

typedef DeltaMemStore::DMSTree DMSTree;
typedef DeltaMemStore::DMSTreeIter DMSTreeIter;

const size_t kRowsPerBlock = 100; // Number of rows per block of columns

class DeltaMemStoreCompactionInput : public DeltaCompactionInput {
 public:

  explicit DeltaMemStoreCompactionInput(const Schema* schema,
                                        const Schema& projection,
                                        const DeltaStats& stats,
                                        gscoped_ptr<DMSTreeIter> iter)
      : stats_(stats),
        iter_(iter.Pass()),
        projection_(projection),
        arena_(32*1024, 128*1024),
        delta_projector_(schema, &projection_) {
  }

  virtual Status Init() OVERRIDE {
    RETURN_NOT_OK(delta_projector_.Init());
    iter_->SeekToStart();
    return Status::OK();
  }

  virtual bool HasMoreBlocks() OVERRIDE {
    return iter_->IsValid();
  }

  virtual Status PrepareBlock(vector<DeltaKeyAndUpdate> *block) OVERRIDE {
    // Reset the arena used to project the deltas to the compaction schema.
    arena_.Reset();

    block->resize(kRowsPerBlock);

    for (int i = 0; i < kRowsPerBlock && iter_->IsValid(); i++) {
      Slice key_slice;
      DeltaKeyAndUpdate &input_cell = (*block)[i];
      iter_->GetCurrentEntry(&key_slice, &input_cell.cell);
      RETURN_NOT_OK(input_cell.key.DecodeFrom(&key_slice));

      if (!delta_projector_.is_identity()) {
        RETURN_NOT_OK(RowChangeListDecoder::ProjectUpdate(delta_projector_,
                                              RowChangeList(input_cell.cell), &delta_buf_));
        CHECK(arena_.RelocateSlice(Slice(delta_buf_.data(), delta_buf_.size()), &input_cell.cell));
        DCHECK_GE(delta_buf_.size(), 0);
      }

      iter_->Next();
    }
    return Status::OK();
  }

  virtual Status FinishBlock() OVERRIDE {
    return Status::OK();
  }

  const Schema& schema() const OVERRIDE {
    return projection_;
  }

  const DeltaStats& stats() const OVERRIDE {
    return stats_;
  }

  const DeltaProjector* delta_projector() const OVERRIDE {
    return &delta_projector_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(DeltaMemStoreCompactionInput);

  DeltaStats stats_;

  gscoped_ptr<DMSTreeIter> iter_;

  Schema projection_;

  // Arena used to store the projected mutations of the current block.
  Arena arena_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;

  DeltaProjector delta_projector_;
};

class DeltaFileCompactionInput : public DeltaCompactionInput {
 public:

  explicit DeltaFileCompactionInput(const shared_ptr<DeltaFileReader>& dfr,
                                    const Schema& projection,
                                    gscoped_ptr<CFileIterator> iter)
      : dfr_(dfr),
        iter_(iter.Pass()),
        projection_(projection),
        stats_(dfr->delta_stats()),
        data_(new Slice[kRowsPerBlock]),
        arena_(32*1024, 128*1024),
        block_(GetTypeInfo(STRING),
               NULL,
               data_.get(),
               kRowsPerBlock,
               &arena_),
        delta_projector_(&dfr->schema(), &projection_),
        initted_(false),
        block_prepared_(false) {
  }

  virtual Status Init() OVERRIDE {
    DCHECK(!initted_);
    RETURN_NOT_OK(delta_projector_.Init());
    RETURN_NOT_OK(iter_->SeekToFirst());
    initted_ = true;
    return Status::OK();
  }

  virtual bool HasMoreBlocks() OVERRIDE {
    return iter_->HasNext();
  }

  virtual Status PrepareBlock(vector<DeltaKeyAndUpdate> *block) OVERRIDE {
    // Reset the arena used by the ColumnBlock scan and used to project
    // the deltas to the compaction schema.
    arena_.Reset();

    size_t nrows = kRowsPerBlock;
    RETURN_NOT_OK(iter_->PrepareBatch(&nrows));
    RETURN_NOT_OK(iter_->Scan(&block_));

    block->resize(nrows);
    for (int i = 0; i < nrows; i++) {
      DeltaKeyAndUpdate &input_cell = (*block)[i];
      Slice s(data_[i]);
      RETURN_NOT_OK(input_cell.key.DecodeFrom(&s));
      input_cell.cell = s;

      if (!delta_projector_.is_identity()) {
        RETURN_NOT_OK(RowChangeListDecoder::ProjectUpdate(delta_projector_,
                                              RowChangeList(input_cell.cell), &delta_buf_));
        CHECK(arena_.RelocateSlice(Slice(delta_buf_.data(), delta_buf_.size()), &input_cell.cell));
        DCHECK_GE(delta_buf_.size(), 0);
      }
    }
    block_prepared_ = true;
    return Status::OK();
  }

  virtual Status FinishBlock() OVERRIDE {
    if (block_prepared_) {
      RETURN_NOT_OK(iter_->FinishBatch());
    }
    block_prepared_ = false;
    return Status::OK();
  }

  const Schema& schema() const OVERRIDE {
    return projection_;
  }

  const DeltaStats& stats() const OVERRIDE {
    return stats_;
  }

  const DeltaProjector* delta_projector() const OVERRIDE {
    return &delta_projector_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(DeltaFileCompactionInput);

  shared_ptr<DeltaFileReader> dfr_;
  gscoped_ptr<CFileIterator> iter_;
  Schema projection_;
  DeltaStats stats_;
  gscoped_ptr<Slice[]> data_;
  Arena arena_;
  ColumnBlock block_;
  DeltaProjector delta_projector_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;

  bool initted_;
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

    const DeltaKeyAndUpdate &next() const {
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

      return pending.back().key.CompareTo<REDO>(other.next().key) < 0;
    }

    shared_ptr<DeltaCompactionInput> input;
    vector<DeltaKeyAndUpdate> pending;
    int pending_idx;

    vector<MergeState *> dominated;
  };

 public:
  explicit MergeDeltaCompactionInput(const Schema& schema,
        const vector<shared_ptr<DeltaCompactionInput> > &inputs)
      : schema_(schema),
        stats_(schema.num_columns()) {

    Timestamp min = Timestamp::kMax;
    Timestamp max = Timestamp::kMin;
    BOOST_FOREACH(const shared_ptr<DeltaCompactionInput> &input, inputs) {
      DCHECK_SCHEMA_EQ(schema_, input->schema());
      gscoped_ptr<MergeState> state(new MergeState);
      stats_.IncrDeleteCount(input->stats().delete_count());
      size_t ncols = std::min(input->stats().num_columns(), schema_.num_columns());
      for (size_t idx = 0; idx < ncols; idx++) {
        size_t col_id = schema_.column_id(idx);
        size_t proj_idx = 0xdeadbeef;
        if (input->delta_projector()->get_proj_col_from_base_id(col_id, &proj_idx)) {
          DCHECK_LT(proj_idx, ncols);
          stats_.IncrUpdateCount(proj_idx, input->stats().update_count(idx));
        }
      }
      state->input = input;
      states_.push_back(state.release());

      if (min.CompareTo(stats_.min_timestamp()) > 0) {
        min = stats_.min_timestamp();
      }
      if (max.CompareTo(stats_.max_timestamp()) < 0) {
        max = stats_.max_timestamp();
      }
    }
    stats_.set_min_timestamp(min);
    stats_.set_min_timestamp(max);
  }

  virtual ~MergeDeltaCompactionInput() {
    STLDeleteElements(&states_);
  }

  virtual Status Init() OVERRIDE {
    BOOST_FOREACH(MergeState *state, states_) {
      RETURN_NOT_OK(state->input->Init());
    }

    RETURN_NOT_OK(ProcessEmptyInputs());
    return Status::OK();
  }

  virtual bool HasMoreBlocks() OVERRIDE {
    BOOST_FOREACH(MergeState *state, states_) {
      if (!state->empty() ||
          state->input->HasMoreBlocks()) {
        return true;
      }
    }

    return false;
  }

  virtual Status PrepareBlock(vector<DeltaKeyAndUpdate> *block) OVERRIDE {
    CHECK(!states_.empty());

    block->clear();

    while (true) {
      int smallest_idx = -1;
      DeltaKeyAndUpdate smallest;

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

        if (smallest_idx < 0 || state->next().key.CompareTo<REDO>(smallest.key) < 0) {
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

  virtual Status FinishBlock() OVERRIDE {
    return ProcessEmptyInputs();
  }

  const Schema& schema() const OVERRIDE {
    return schema_;
  }

  const DeltaStats& stats() const OVERRIDE {
    return stats_;
  }

  const DeltaProjector* delta_projector() const OVERRIDE {
    LOG(FATAL) <<
        "MergeDeltaCompactionInput does not support delta_projector()";
    return NULL;
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

  Schema schema_;
  DeltaStats stats_;
  vector<MergeState *> states_;
  vector<int64_t> update_counts_;
};

} // anonymous namespace

// TODO: can you major-delta-compact a new column after an alter table in order
// to materialize it? should write a test for this.
MajorDeltaCompaction::MajorDeltaCompaction(FsManager* fs_manager,
                                           const Schema& base_schema,
                                           CFileSet* base_data,
                                           const shared_ptr<DeltaIterator>& delta_iter,
                                           const vector<shared_ptr<DeltaStore> >& included_stores,
                                           const ColumnIndexes& col_indexes)
  : fs_manager_(fs_manager),
    base_schema_(base_schema),
    column_indexes_(col_indexes),
    base_data_(base_data),
    included_stores_(included_stores),
    delta_iter_(delta_iter),
    nrows_(0),
    deltas_written_(0),
    state_(kInitialized) {
  CHECK(!col_indexes.empty());
}

MajorDeltaCompaction::~MajorDeltaCompaction() {
}

string MajorDeltaCompaction::ColumnNamesToString() const {
  std::string result;
  BOOST_FOREACH(size_t col_idx, column_indexes_) {
    result += base_schema_.column(col_idx).ToString() + " ";
  }
  return result;
}

Status MajorDeltaCompaction::FlushRowSetAndDeltas() {
  CHECK_EQ(state_, kInitialized);

  shared_ptr<CFileSet::Iterator> cfileset_iter(base_data_->NewIterator(&partial_schema_));

  RETURN_NOT_OK_PREPEND(
      cfileset_iter->Init(NULL),
      "Unable to open iterator for specified columns (" + partial_schema_.ToString() + ")");

  RETURN_NOT_OK(delta_iter_->Init());
  RETURN_NOT_OK(delta_iter_->SeekToOrdinal(0));

  Arena arena(32 * 1024, 128 * 1024);
  RowBlock block(partial_schema_, kRowsPerBlock, &arena);

  DVLOG(1) << "Applying deltas and rewriting columns (" << partial_schema_.ToString() << ")";
  DeltaStats stats(base_schema_.num_columns());
  // Iterate over the rows
  // For each iteration:
  // - apply the deltas for each column
  // - append deltas for other columns to 'dfw'
  while (cfileset_iter->HasNext()) {
    size_t n = block.row_capacity();
    arena.Reset();
    RETURN_NOT_OK(cfileset_iter->PrepareBatch(&n));

    block.Resize(n);
    nrows_ += n;

    RETURN_NOT_OK(delta_iter_->PrepareBatch(n));
    BOOST_FOREACH(size_t col_idx, column_indexes_) {
      size_t new_idx = old_to_new_[col_idx];
      ColumnBlock col_block(block.column_block(new_idx));
      RETURN_NOT_OK(cfileset_iter->MaterializeColumn(new_idx, &col_block));

      // TODO: should this be IDs? indexes? check this with alter.
      RETURN_NOT_OK(delta_iter_->ApplyUpdates(col_idx, &col_block));
    }
    RETURN_NOT_OK(col_writer_->AppendBlock(block));

    arena.Reset();
    vector<DeltaKeyAndUpdate> out;
    RETURN_NOT_OK(delta_iter_->FilterColumnsAndAppend(column_indexes_, &out, &arena));
    BOOST_FOREACH(const DeltaKeyAndUpdate& key_and_update, out) {
      RowChangeList update(key_and_update.cell);
      RETURN_NOT_OK_PREPEND(delta_writer_->AppendDelta<REDO>(key_and_update.key, update),
                            "Failed to append a delta");
      WARN_NOT_OK(stats.UpdateStats(key_and_update.key.timestamp(), base_schema_, update),
                  "Failed to update stats");
    }
    deltas_written_ += out.size();
    RETURN_NOT_OK(cfileset_iter->FinishBatch());
  }

  RETURN_NOT_OK(col_writer_->Finish());
  RETURN_NOT_OK(delta_writer_->WriteDeltaStats(stats));
  RETURN_NOT_OK(delta_writer_->Finish());

  DVLOG(1) << "Applied all outstanding deltas for columns "
           << partial_schema_.ToString()
           << ", and flushed the resulting rowsets and a total of "
           << deltas_written_
           << " deltas to disk.";

  state_ = kFinished;
  return Status::OK();
}

Status MajorDeltaCompaction::OpenNewColumns() {
  CHECK(!col_writer_);

  gscoped_ptr<MultiColumnWriter> w(new MultiColumnWriter(fs_manager_, &partial_schema_));
  RETURN_NOT_OK(w->Open());
  col_writer_.swap(w);
  return Status::OK();
}

Status MajorDeltaCompaction::OpenNewDeltaBlock() {
  shared_ptr<WritableFile> file;
  RETURN_NOT_OK_PREPEND(fs_manager_->CreateNewBlock(&file, &new_delta_block_),
                        "Unable to create delta output block");
  delta_writer_.reset(new DeltaFileWriter(base_schema_, file));
  return delta_writer_->Start();
}

Status MajorDeltaCompaction::Compact() {
  CHECK_EQ(state_, kInitialized);

  LOG(INFO) << "Starting major delta compaction for columns " << ColumnNamesToString();
  RETURN_NOT_OK(base_schema_.CreatePartialSchema(column_indexes_,
                                                 &old_to_new_,
                                                 &partial_schema_));
  RETURN_NOT_OK(OpenNewColumns());
  RETURN_NOT_OK(OpenNewDeltaBlock());
  RETURN_NOT_OK(FlushRowSetAndDeltas());
  LOG(INFO) << "Finished major delta compaction of columns " <<
      ColumnNamesToString();
  return Status::OK();
}

Status MajorDeltaCompaction::CreateMetadataUpdate(
    metadata::RowSetMetadataUpdate* update) {
  CHECK(update);
  CHECK_EQ(state_, kFinished);

  vector<BlockId> compacted_delta_blocks;
  BOOST_FOREACH(const shared_ptr<DeltaStore>& store, included_stores_) {
    DeltaFileReader* dfr = down_cast<DeltaFileReader*>(store.get());
    compacted_delta_blocks.push_back(dfr->block_id());
  }

  vector<BlockId> new_delta_blocks;
  if (deltas_written_ > 0) {
    new_delta_blocks.push_back(new_delta_block_);
  }

  update->ReplaceRedoDeltaBlocks(compacted_delta_blocks,
                                 new_delta_blocks);

  // Replace old column blocks with new ones
  vector<BlockId> new_column_blocks = col_writer_->FlushedBlocks();
  CHECK_EQ(new_column_blocks.size(), column_indexes_.size());

  for (int i = 0; i < column_indexes_.size(); i++) {
    update->ReplaceColumnBlock(column_indexes_[i], new_column_blocks[i]);
  }

  return Status::OK();
}

Status MajorDeltaCompaction::UpdateDeltaTracker(DeltaTracker* tracker) {
  CHECK_EQ(state_, kFinished);
  return tracker->AtomicUpdateStores(included_stores_,
                                     boost::assign::list_of(new_delta_block_),
                                     REDO);
}

////////////////////////////////////////////////////////////
// DeltaCompactionInput
////////////////////////////////////////////////////////////

Status DeltaCompactionInput::Open(const shared_ptr<DeltaFileReader>& reader,
                                  const Schema& projection,
                                  gscoped_ptr<DeltaCompactionInput> *input) {
  CHECK(projection.has_column_ids());
  gscoped_ptr<CFileIterator> iter;
  RETURN_NOT_OK(reader->cfile_reader()->NewIterator(&iter));
  input->reset(new DeltaFileCompactionInput(reader,
                                            projection,
                                            iter.Pass()));
  return Status::OK();
}

Status DeltaCompactionInput::Open(const DeltaMemStore &dms,
                                  const Schema& projection,
                                  gscoped_ptr<DeltaCompactionInput> *input) {
  CHECK(projection.has_column_ids());
  gscoped_ptr<DMSTreeIter> iter(dms.tree().NewIterator());
  input->reset(new DeltaMemStoreCompactionInput(&dms.schema(), projection, dms.delta_stats(),
                                                iter.Pass()));
  return Status::OK();
}

DeltaCompactionInput
*DeltaCompactionInput::Merge(const Schema& projection,
                             const vector<shared_ptr<DeltaCompactionInput> > &inputs) {
  CHECK(projection.has_column_ids());
  return new MergeDeltaCompactionInput(projection, inputs);
}

Status DebugDumpDeltaCompactionInput(DeltaCompactionInput *input, vector<string> *lines,
                                     const Schema &schema) {
  RETURN_NOT_OK(input->Init());
  vector<DeltaKeyAndUpdate> cells;

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&cells));

    BOOST_FOREACH(const DeltaKeyAndUpdate &cell, cells) {
      LOG_STRING(INFO, lines) << cell.Stringify(REDO, schema);
    }
    RETURN_NOT_OK(input->FinishBlock());
  }

  return Status::OK();
}

Status FlushDeltaCompactionInput(DeltaCompactionInput *input, DeltaFileWriter *out) {
  DCHECK_EQ(out->schema().has_column_ids(), input->schema().has_column_ids());
  DCHECK_SCHEMA_EQ(out->schema(), input->schema());
  RETURN_NOT_OK(input->Init());
  vector<DeltaKeyAndUpdate> cells;

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&cells));
    BOOST_FOREACH(const DeltaKeyAndUpdate &cell, cells) {
      RETURN_NOT_OK_PREPEND(out->AppendDelta<REDO>(cell.key, RowChangeList(cell.cell)),
                            "Failed to append delta");
    }
    RETURN_NOT_OK(input->FinishBlock());
  }

  RETURN_NOT_OK(out->WriteDeltaStats(input->stats()));
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
