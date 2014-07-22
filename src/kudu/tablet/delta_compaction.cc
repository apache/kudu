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

const size_t kRowsPerBlock = 100; // Number of rows per block of columns

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

} // namespace tablet
} // namespace kudu
