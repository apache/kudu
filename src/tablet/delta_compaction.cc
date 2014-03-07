// Copyright (c) 2013, Cloudera, inc.

#include "tablet/delta_compaction.h"

#include <string>
#include <vector>
#include <algorithm>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "gutil/stl_util.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/strcat.h"
#include "common/columnblock.h"
#include "cfile/cfile_reader.h"
#include "tablet/cfile_set.h"
#include "tablet/delta_key.h"
#include "tablet/deltamemstore.h"
#include "tablet/mvcc.h"

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
typedef std::pair<size_t, cfile::Writer *> WriterMapEntry;

const size_t kRowsPerBlock = 100; // Number of rows per block of columns

class DeltaMemStoreCompactionInput : public DeltaCompactionInput {
 public:

  explicit DeltaMemStoreCompactionInput(const Schema* schema,
                                        const Schema* projection,
                                        const DeltaStats& stats,
                                        gscoped_ptr<DMSTreeIter> iter)
      : stats_(stats),
        iter_(iter.Pass()),
        arena_(32*1024, 128*1024),
        delta_projector_(schema, projection) {
  }

  virtual Status Init() {
    RETURN_NOT_OK(delta_projector_.Init());
    iter_->SeekToStart();
    return Status::OK();
  }

  virtual bool HasMoreBlocks() {
    return iter_->IsValid();
  }

  virtual Status PrepareBlock(vector<DeltaKeyAndUpdate> *block) {
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

  virtual Status FinishBlock() {
    return Status::OK();
  }

  const Schema& schema() const {
    return delta_projector_.projection();
  }

  const DeltaStats& stats() const {
    return stats_;
  }

  const DeltaProjector* delta_projector() const {
    return &delta_projector_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(DeltaMemStoreCompactionInput);

  DeltaStats stats_;

  gscoped_ptr<DMSTreeIter> iter_;

  // Arena used to store the projected mutations of the current block.
  Arena arena_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;

  DeltaProjector delta_projector_;
};

class DeltaFileCompactionInput : public DeltaCompactionInput {
 public:

  explicit DeltaFileCompactionInput(const shared_ptr<DeltaFileReader>& dfr,
                                    const Schema* projection,
                                    gscoped_ptr<CFileIterator> iter)
      : dfr_(dfr),
        iter_(iter.Pass()),
        stats_(dfr->delta_stats()),
        data_(new Slice[kRowsPerBlock]),
        arena_(32*1024, 128*1024),
        block_(GetTypeInfo(STRING),
               NULL,
               data_.get(),
               kRowsPerBlock,
               &arena_),
        delta_projector_(&dfr->schema(), projection),
        initted_(false),
        block_prepared_(false) {
  }

  virtual Status Init() {
    DCHECK(!initted_);
    RETURN_NOT_OK(delta_projector_.Init());
    RETURN_NOT_OK(iter_->SeekToFirst());
    initted_ = true;
    return Status::OK();
  }

  virtual bool HasMoreBlocks() {
    return iter_->HasNext();
  }

  virtual Status PrepareBlock(vector<DeltaKeyAndUpdate> *block) {
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

  virtual Status FinishBlock() {
    if (block_prepared_) {
      RETURN_NOT_OK(iter_->FinishBatch());
    }
    block_prepared_ = false;
    return Status::OK();
  }

  const Schema& schema() const {
    return delta_projector_.projection();
  }

  const DeltaStats& stats() const {
    return stats_;
  }

  const DeltaProjector* delta_projector() const {
    return &delta_projector_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(DeltaFileCompactionInput);

  shared_ptr<DeltaFileReader> dfr_;
  gscoped_ptr<CFileIterator> iter_;
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

      return pending.back().key.CompareTo(other.next().key) < 0;
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

  virtual Status PrepareBlock(vector<DeltaKeyAndUpdate> *block) {
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

  const Schema& schema() const {
    return schema_;
  }

  const DeltaStats& stats() const {
    return stats_;
  }

  const DeltaProjector* delta_projector() const {
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

RowSetColumnUpdater::RowSetColumnUpdater(TabletMetadata* tablet_metadata,
                                         const shared_ptr<RowSetMetadata>& input_rowset_metadata,
                                         const ColumnIndexes& col_indexes)
 : tablet_meta_(tablet_metadata),
   input_rowset_meta_(input_rowset_metadata),
   column_indexes_(col_indexes),
   base_schema_(input_rowset_metadata->schema()),
   finished_(false) {
}

RowSetColumnUpdater::~RowSetColumnUpdater() {
  STLDeleteValues(&column_writers_);
}

Status RowSetColumnUpdater::Open(shared_ptr<RowSetMetadata>* output_rowset_meta) {
  CHECK(!finished_);

  RETURN_NOT_OK(base_schema_.CreatePartialSchema(column_indexes_,
                                                 &old_to_new_,
                                                 &partial_schema_));

  ColumnWriters data_writers;
  RETURN_NOT_OK(tablet_meta_->CreateRowSetWithUpdatedColumns(column_indexes_,
                                                             *input_rowset_meta_,
                                                             output_rowset_meta,
                                                             &data_writers));
  BOOST_FOREACH(size_t col_idx, column_indexes_) {
    const ColumnSchema &col = input_rowset_meta_->schema().column(col_idx);
    cfile::WriterOptions opts;
    opts.write_posidx = true;
    opts.storage_attributes = col.attributes();

    gscoped_ptr<cfile::Writer> writer(
        new cfile::Writer(
            opts,
            col.type_info().type(),
            col.is_nullable(),
            data_writers[col_idx]));
    Status s = writer->Start();
    if (!s.ok()) {
      string msg = "Unable to Start() writer for column " + col.ToString();
      RETURN_NOT_OK_PREPEND(s, msg);
    }

    LOG(INFO) << "Opened CFile writer for column " << col.ToString();
    column_writers_[col_idx] = writer.release();
  }

  return Status::OK();
}

Status RowSetColumnUpdater::AppendColumnsFromRowBlock(const RowBlock& block) {
  CHECK(!finished_);

  BOOST_FOREACH(size_t col_idx, column_indexes_) {
    cfile::Writer* column_writer = column_writers_[col_idx];
    ColumnBlock column(block.column_block(col_idx));
    if (column.is_nullable()) {
      RETURN_NOT_OK(column_writer->AppendNullableEntries(column.null_bitmap(),
                                                         column.data(),
                                                         column.nrows()));
    } else {
      RETURN_NOT_OK(column_writer->AppendEntries(column.data(), column.nrows()));
    }
  }
  return Status::OK();
}

Status RowSetColumnUpdater::Finish() {
  CHECK(!finished_);

  BOOST_FOREACH(const WriterMapEntry& writer_pair, column_writers_) {
    Status s = writer_pair.second->Finish();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish writer for column "
          << input_rowset_meta_->schema().column(writer_pair.first).ToString()
          << " : " << s.ToString();
      return s;
    }
  }

  finished_ = true;
  return Status::OK();
}

string RowSetColumnUpdater::ColumnNamesToString() const {
  std::string result;
  BOOST_FOREACH(size_t col_idx, column_indexes_) {
    result += base_schema_.column(col_idx).ToString() + " ";
  }
  return result;
}

MajorDeltaCompaction::MajorDeltaCompaction(const shared_ptr<DeltaIterator>& delta_iter,
                                           RowSetColumnUpdater* rsu)
    : delta_iter_(delta_iter),
      rsu_(rsu),
      nrows_(0),
      state_(kInitialized) {
}

Status MajorDeltaCompaction::FlushRowSetAndDeltas(DeltaFileWriter* dfw, size_t *deltas_written) {
  CHECK_EQ(state_, kInitialized);

  const Schema* base_schema = &rsu_->base_schema();
  const Schema* partial_schema = &rsu_->partial_schema();

  shared_ptr<CFileSet> cfileset(new CFileSet(rsu_->input_rowset_meta()));
  RETURN_NOT_OK(cfileset->Open());
  shared_ptr<CFileSet::Iterator> cfileset_iter(cfileset->NewIterator(partial_schema));

  RETURN_NOT_OK_PREPEND(
      cfileset_iter->Init(NULL),
      "Unable to open iterator for specified columns (" + partial_schema->ToString() + ")");

  RETURN_NOT_OK(delta_iter_->Init());
  RETURN_NOT_OK(delta_iter_->SeekToOrdinal(0));

  Arena arena(32 * 1024, 128 * 1024);
  RowBlock block(*base_schema, kRowsPerBlock, &arena);

  DVLOG(1) << "Applying deltas and flushing for columns (" << partial_schema->ToString() << ")";

  RETURN_NOT_OK(dfw->Start());

  DeltaStats stats(base_schema->num_columns());
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
    BOOST_FOREACH(size_t col_idx, rsu_->column_indexes()) {
      size_t new_idx = rsu_->old_to_new(col_idx);
      ColumnBlock col_block(block.column_block(col_idx));
      RETURN_NOT_OK(cfileset_iter->MaterializeColumn(new_idx, &col_block));
      RETURN_NOT_OK(delta_iter_->ApplyUpdates(col_idx, &col_block));
    }
    RETURN_NOT_OK(rsu_->AppendColumnsFromRowBlock(block));

    arena.Reset();
    vector<DeltaKeyAndUpdate> out;
    RETURN_NOT_OK(delta_iter_->FilterColumnsAndAppend(rsu_->column_indexes(), &out, &arena));
    BOOST_FOREACH(const DeltaKeyAndUpdate& key_and_update, out) {
      RowChangeList update(key_and_update.cell);
      RETURN_NOT_OK_PREPEND(dfw->AppendDelta(key_and_update.key, update),
                            "Failed to append a delta");
      WARN_NOT_OK(stats.UpdateStats(key_and_update.key.timestamp(), *base_schema, update),
                  "Failed to update stats");
    }
    *deltas_written += out.size();
    RETURN_NOT_OK(cfileset_iter->FinishBatch());
  }

  RETURN_NOT_OK(rsu_->Finish());
  RETURN_NOT_OK(dfw->WriteDeltaStats(stats));
  RETURN_NOT_OK(dfw->Finish());

  DVLOG(1) << "Applied all outstanding deltas for columns "
           << partial_schema->ToString()
           << ", and flushed the resulting rowsets and a total of "
           << *deltas_written
           << " deltas to disk.";

  state_ = kFinished;
  return Status::OK();
}

Status MajorDeltaCompaction::Compact(shared_ptr<RowSetMetadata>* output,
                                     BlockId* block_id, size_t* deltas_written) {
  CHECK_EQ(state_, kInitialized);

  LOG(INFO) << "Starting major delta compaction for columns " <<
      rsu_->ColumnNamesToString();

  RETURN_NOT_OK(rsu_->Open(output));

  shared_ptr<WritableFile> data_writer;
  RETURN_NOT_OK_PREPEND((*output)->NewDeltaDataBlock(&data_writer, block_id),
                        "Unable to create delta output block " + block_id->ToString());

  DeltaFileWriter dfw(rsu_->base_schema(), data_writer);
  RETURN_NOT_OK(FlushRowSetAndDeltas(&dfw, deltas_written));

  LOG(INFO) << "Finished major delta compaction of columns " <<
      rsu_->ColumnNamesToString();

  return Status::OK();
}

Status DeltaCompactionInput::Open(const shared_ptr<DeltaFileReader>& reader,
                                  const Schema* projection,
                                  gscoped_ptr<DeltaCompactionInput> *input) {
  CHECK(projection->has_column_ids());
  gscoped_ptr<CFileIterator> iter;
  RETURN_NOT_OK(reader->cfile_reader()->NewIterator(&iter));
  input->reset(new DeltaFileCompactionInput(reader,
                                            projection,
                                            iter.Pass()));
  return Status::OK();
}

Status DeltaCompactionInput::Open(const DeltaMemStore &dms,
                                  const Schema* projection,
                                  gscoped_ptr<DeltaCompactionInput> *input) {
  CHECK(projection->has_column_ids());
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

static string FormatDebugDeltaCell(const Schema &schema, const DeltaKeyAndUpdate &cell) {
  return StrCat(Substitute("(delta key=$0, change_list=$1)",
                           StringPrintf("%04u@tx%04u", cell.key.row_idx(),
                                        atoi(cell.key.timestamp().ToString().c_str())),
                           RowChangeList(cell.cell).ToString(schema)));
}

Status DebugDumpDeltaCompactionInput(DeltaCompactionInput *input, vector<string> *lines,
                                     const Schema &schema) {
  RETURN_NOT_OK(input->Init());
  vector<DeltaKeyAndUpdate> cells;

  while (input->HasMoreBlocks()) {
    RETURN_NOT_OK(input->PrepareBlock(&cells));

    BOOST_FOREACH(const DeltaKeyAndUpdate &cell, cells) {
      LOG_STRING(INFO, lines) << FormatDebugDeltaCell(schema, cell);
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
      RETURN_NOT_OK_PREPEND(out->AppendDelta(cell.key, RowChangeList(cell.cell)),
                            "Failed to append delta");
    }
    RETURN_NOT_OK(input->FinishBlock());
  }

  RETURN_NOT_OK(out->WriteDeltaStats(input->stats()));
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
