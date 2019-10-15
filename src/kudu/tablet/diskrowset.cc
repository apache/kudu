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

#include "kudu/tablet/diskrowset.h"

#include <algorithm>
#include <map>
#include <ostream>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "kudu/cfile/bloomfile.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/generic_iterators.h"
#include "kudu/common/iterator.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/delta_compaction.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/multi_column_writer.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_int32(tablet_delta_store_minor_compact_max, 1000,
             "How many delta stores are required before forcing a minor delta compaction "
             "(Advanced option)");
TAG_FLAG(tablet_delta_store_minor_compact_max, experimental);

DEFINE_double(tablet_delta_store_major_compact_min_ratio, 0.1f,
             "Minimum ratio of sizeof(deltas) to sizeof(base data) before a major compaction "
             "can run (Advanced option)");
TAG_FLAG(tablet_delta_store_major_compact_min_ratio, experimental);

DEFINE_int32(default_composite_key_index_block_size_bytes, 4096,
             "Block size used for composite key indexes.");
TAG_FLAG(default_composite_key_index_block_size_bytes, experimental);

DEFINE_bool(rowset_metadata_store_keys, false,
            "Whether to store the min/max encoded keys in the rowset "
            "metadata. If false, keys will be read from the data blocks.");
TAG_FLAG(rowset_metadata_store_keys, experimental);

namespace kudu {

class Mutex;

namespace consensus {
class OpId;
}

namespace tablet {

using cfile::BloomFileWriter;
using fs::BlockManager;
using fs::BlockCreationTransaction;
using fs::CreateBlockOptions;
using fs::IOContext;
using fs::WritableBlock;
using log::LogAnchorRegistry;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

const char *DiskRowSet::kMinKeyMetaEntryName = "min_key";
const char *DiskRowSet::kMaxKeyMetaEntryName = "max_key";

DiskRowSetWriter::DiskRowSetWriter(RowSetMetadata* rowset_metadata,
                                   const Schema* schema,
                                   BloomFilterSizing bloom_sizing)
    : rowset_metadata_(rowset_metadata),
      schema_(schema),
      bloom_sizing_(bloom_sizing),
      finished_(false),
      written_count_(0) {
  CHECK(schema->has_column_ids());
}

Status DiskRowSetWriter::Open() {
  TRACE_EVENT0("tablet", "DiskRowSetWriter::Open");

  FsManager* fs = rowset_metadata_->fs_manager();
  const string& tablet_id = rowset_metadata_->tablet_metadata()->tablet_id();
  col_writer_.reset(new MultiColumnWriter(fs, schema_, tablet_id));
  RETURN_NOT_OK(col_writer_->Open());

  // Open bloom filter.
  RETURN_NOT_OK(InitBloomFileWriter());

  if (schema_->num_key_columns() > 1) {
    // Open ad-hoc index writer
    RETURN_NOT_OK(InitAdHocIndexWriter());
  }

  return Status::OK();
}

Status DiskRowSetWriter::InitBloomFileWriter() {
  TRACE_EVENT0("tablet", "DiskRowSetWriter::InitBloomFileWriter");
  unique_ptr<WritableBlock> block;
  FsManager* fs = rowset_metadata_->fs_manager();
  const string& tablet_id = rowset_metadata_->tablet_metadata()->tablet_id();
  RETURN_NOT_OK_PREPEND(fs->CreateNewBlock(CreateBlockOptions({ tablet_id }),
                                           &block),
                        "Couldn't allocate a block for bloom filter");
  rowset_metadata_->set_bloom_block(block->id());

  bloom_writer_.reset(new cfile::BloomFileWriter(std::move(block), bloom_sizing_));
  RETURN_NOT_OK(bloom_writer_->Start());
  return Status::OK();
}

Status DiskRowSetWriter::InitAdHocIndexWriter() {
  TRACE_EVENT0("tablet", "DiskRowSetWriter::InitAdHocIndexWriter");
  unique_ptr<WritableBlock> block;
  FsManager* fs = rowset_metadata_->fs_manager();
  const string& tablet_id = rowset_metadata_->tablet_metadata()->tablet_id();
  RETURN_NOT_OK_PREPEND(fs->CreateNewBlock(CreateBlockOptions({ tablet_id }),
                                           &block),
                        "Couldn't allocate a block for compoound index");

  rowset_metadata_->set_adhoc_index_block(block->id());

  cfile::WriterOptions opts;

  // Index the composite key by value
  opts.write_validx = true;

  // no need to index positions
  opts.write_posidx = false;

  opts.storage_attributes.encoding = PREFIX_ENCODING;
  opts.storage_attributes.compression = LZ4;
  opts.storage_attributes.cfile_block_size = FLAGS_default_composite_key_index_block_size_bytes;

  // Create the CFile writer for the ad-hoc index.
  ad_hoc_index_writer_.reset(new cfile::CFileWriter(
      std::move(opts),
      GetTypeInfo(BINARY),
      false,
      std::move(block)));
  return ad_hoc_index_writer_->Start();

}

Status DiskRowSetWriter::AppendBlock(const RowBlock &block, int live_row_count) {
  DCHECK_EQ(block.schema()->num_columns(), schema_->num_columns());
  CHECK(!finished_);

  // If this is the very first block, encode the first key and save it as metadata
  // in the index column.
  if (written_count_ == 0) {
    Slice enc_key = schema_->EncodeComparableKey(block.row(0), &last_encoded_key_);
    key_index_writer()->AddMetadataPair(DiskRowSet::kMinKeyMetaEntryName, enc_key);
    if (FLAGS_rowset_metadata_store_keys) {
      rowset_metadata_->set_min_encoded_key(enc_key.ToString());
    }
    last_encoded_key_.clear();
  }

  // Write the batch to each of the columns
  RETURN_NOT_OK(col_writer_->AppendBlock(block));

  // Increase the live row count if necessary.
  rowset_metadata_->IncrementLiveRows(live_row_count);

#ifndef NDEBUG
    faststring prev_key;
#endif

  // Write the batch to the bloom and optionally the ad-hoc index
  for (size_t i = 0; i < block.nrows(); i++) {
#ifndef NDEBUG
    prev_key.assign_copy(last_encoded_key_.data(), last_encoded_key_.size());
#endif

    // TODO: performance might be better if we actually batch this -
    // encode a bunch of key slices, then pass them all in one go.
    RowBlockRow row = block.row(i);
    // Insert the encoded key into the bloom.
    Slice enc_key = schema_->EncodeComparableKey(row, &last_encoded_key_);
    RETURN_NOT_OK(bloom_writer_->AppendKeys(&enc_key, 1));

    // Write the batch to the ad hoc index if we're using one
    if (ad_hoc_index_writer_ != nullptr) {
      RETURN_NOT_OK(ad_hoc_index_writer_->AppendEntries(&enc_key, 1));
    }

#ifndef NDEBUG
    CHECK(prev_key.size() == 0 || Slice(prev_key).compare(enc_key) < 0)
      << KUDU_REDACT(enc_key.ToDebugString()) << " appended to file not > previous key "
      << KUDU_REDACT(Slice(prev_key).ToDebugString());
#endif
  }

  written_count_ += block.nrows();

  return Status::OK();
}

Status DiskRowSetWriter::Finish() {
  TRACE_EVENT0("tablet", "DiskRowSetWriter::Finish");
  BlockManager* bm = rowset_metadata_->fs_manager()->block_manager();
  unique_ptr<BlockCreationTransaction> transaction = bm->NewCreationTransaction();
  RETURN_NOT_OK(FinishAndReleaseBlocks(transaction.get()));
  return transaction->CommitCreatedBlocks();
}

Status DiskRowSetWriter::FinishAndReleaseBlocks(BlockCreationTransaction* transaction) {
  TRACE_EVENT0("tablet", "DiskRowSetWriter::FinishAndReleaseBlocks");
  CHECK(!finished_);

  if (written_count_ == 0) {
    finished_ = true;
    return Status::Aborted("no data written");
  }

  // Save the last encoded (max) key
  Slice last_enc_slice(last_encoded_key_);
  std::string first_encoded_key =
      key_index_writer()->GetMetaValueOrDie(DiskRowSet::kMinKeyMetaEntryName);
  Slice first_enc_slice(first_encoded_key);

  CHECK_LE(first_enc_slice.compare(last_enc_slice), 0)
      << "First Key not <= Last key: first_key=" << KUDU_REDACT(first_enc_slice.ToDebugString())
      << "   last_key=" << KUDU_REDACT(last_enc_slice.ToDebugString());
  key_index_writer()->AddMetadataPair(DiskRowSet::kMaxKeyMetaEntryName, last_enc_slice);
  if (FLAGS_rowset_metadata_store_keys) {
    rowset_metadata_->set_max_encoded_key(last_enc_slice.ToString());
  }

  // Finish writing the columns themselves.
  RETURN_NOT_OK(col_writer_->FinishAndReleaseBlocks(transaction));

  // Put the column data blocks in the metadata.
  std::map<ColumnId, BlockId> flushed_blocks;
  col_writer_->GetFlushedBlocksByColumnId(&flushed_blocks);
  rowset_metadata_->SetColumnDataBlocks(flushed_blocks);

  if (ad_hoc_index_writer_ != nullptr) {
    Status s = ad_hoc_index_writer_->FinishAndReleaseBlock(transaction);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish ad hoc index writer: " << s.ToString();
      return s;
    }
  }

  // Finish bloom.
  Status s = bloom_writer_->FinishAndReleaseBlock(transaction);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to Finish bloom filter writer: " << s.ToString();
    return s;
  }

  finished_ = true;
  return Status::OK();
}

cfile::CFileWriter *DiskRowSetWriter::key_index_writer() {
  return ad_hoc_index_writer_ ? ad_hoc_index_writer_.get() : col_writer_->writer_for_col_idx(0);
}

size_t DiskRowSetWriter::written_size() const {
  size_t size = 0;

  if (col_writer_) {
    size += col_writer_->written_size();
  }

  if (bloom_writer_) {
    size += bloom_writer_->written_size();
  }

  if (ad_hoc_index_writer_) {
    size += ad_hoc_index_writer_->written_size();
  }

  return size;
}

DiskRowSetWriter::~DiskRowSetWriter() {
}

RollingDiskRowSetWriter::RollingDiskRowSetWriter(
    TabletMetadata* tablet_metadata, const Schema& schema,
    BloomFilterSizing bloom_sizing, size_t target_rowset_size)
    : state_(kInitialized),
      tablet_metadata_(DCHECK_NOTNULL(tablet_metadata)),
      schema_(schema),
      bloom_sizing_(bloom_sizing),
      target_rowset_size_(target_rowset_size),
      row_idx_in_cur_drs_(0),
      can_roll_(false),
      written_count_(0),
      written_size_(0) {
  BlockManager* bm = tablet_metadata->fs_manager()->block_manager();
  block_transaction_ = bm->NewCreationTransaction();
  CHECK(schema.has_column_ids());
}

Status RollingDiskRowSetWriter::Open() {
  TRACE_EVENT0("tablet", "RollingDiskRowSetWriter::Open");
  CHECK_EQ(state_, kInitialized);

  RETURN_NOT_OK(RollWriter());
  state_ = kStarted;
  return Status::OK();
}

Status RollingDiskRowSetWriter::RollWriter() {
  TRACE_EVENT0("tablet", "RollingDiskRowSetWriter::RollWriter");
  // Close current writer if it is open
  RETURN_NOT_OK(FinishCurrentWriter());

  RETURN_NOT_OK(tablet_metadata_->CreateRowSet(&cur_drs_metadata_));

  cur_writer_.reset(new DiskRowSetWriter(cur_drs_metadata_.get(), &schema_, bloom_sizing_));
  RETURN_NOT_OK(cur_writer_->Open());

  FsManager* fs = tablet_metadata_->fs_manager();
  unique_ptr<WritableBlock> undo_data_block;
  unique_ptr<WritableBlock> redo_data_block;
  RETURN_NOT_OK(fs->CreateNewBlock(CreateBlockOptions({ tablet_metadata_->tablet_id() }),
                                   &undo_data_block));
  RETURN_NOT_OK(fs->CreateNewBlock(CreateBlockOptions({ tablet_metadata_->tablet_id() }),
                                   &redo_data_block));
  cur_undo_ds_block_id_ = undo_data_block->id();
  cur_redo_ds_block_id_ = redo_data_block->id();
  cur_undo_writer_.reset(new DeltaFileWriter(std::move(undo_data_block)));
  cur_redo_writer_.reset(new DeltaFileWriter(std::move(redo_data_block)));
  cur_undo_delta_stats.reset(new DeltaStats());
  cur_redo_delta_stats.reset(new DeltaStats());

  row_idx_in_cur_drs_ = 0;
  can_roll_ = false;

  RETURN_NOT_OK(cur_undo_writer_->Start());
  return cur_redo_writer_->Start();
}

Status RollingDiskRowSetWriter::RollIfNecessary() {
  DCHECK_EQ(state_, kStarted);
  if (can_roll_ && cur_writer_->written_size() > target_rowset_size_) {
    RETURN_NOT_OK(RollWriter());
  }
  return Status::OK();
}

Status RollingDiskRowSetWriter::AppendBlock(const RowBlock &block, int live_row_count) {
  DCHECK_EQ(state_, kStarted);
  RETURN_NOT_OK(cur_writer_->AppendBlock(block, live_row_count));

  written_count_ += block.nrows();

  row_idx_in_cur_drs_ += block.nrows();
  can_roll_ = true;
  return Status::OK();
}

Status RollingDiskRowSetWriter::AppendUndoDeltas(rowid_t row_idx_in_block,
                                                 Mutation* undo_delta_head,
                                                 rowid_t* row_idx) {
  return AppendDeltas<UNDO>(row_idx_in_block, undo_delta_head,
                            row_idx,
                            cur_undo_writer_.get(),
                            cur_undo_delta_stats.get());
}

Status RollingDiskRowSetWriter::AppendRedoDeltas(rowid_t row_idx_in_block,
                                                 Mutation* redo_delta_head,
                                                 rowid_t* row_idx) {
  return AppendDeltas<REDO>(row_idx_in_block, redo_delta_head,
                            row_idx,
                            cur_redo_writer_.get(),
                            cur_redo_delta_stats.get());
}

template<DeltaType Type>
Status RollingDiskRowSetWriter::AppendDeltas(rowid_t row_idx_in_block,
                                             Mutation* delta_head,
                                             rowid_t* row_idx,
                                             DeltaFileWriter* writer,
                                             DeltaStats* delta_stats) {
  can_roll_ = false;

  *row_idx = row_idx_in_cur_drs_ + row_idx_in_block;
  for (const Mutation *mut = delta_head; mut != nullptr; mut = mut->next()) {
    DeltaKey undo_key(*row_idx, mut->timestamp());
    RETURN_NOT_OK(writer->AppendDelta<Type>(undo_key, mut->changelist()));
    delta_stats->UpdateStats(mut->timestamp(), mut->changelist());
  }
  return Status::OK();
}

Status RollingDiskRowSetWriter::FinishCurrentWriter() {
  TRACE_EVENT0("tablet", "RollingDiskRowSetWriter::FinishCurrentWriter");
  if (!cur_writer_) {
    return Status::OK();
  }
  CHECK_EQ(state_, kStarted);

  Status writer_status = cur_writer_->FinishAndReleaseBlocks(block_transaction_.get());

  // If no rows were written (e.g. due to an empty flush or a compaction with all rows
  // deleted), FinishAndReleaseBlocks(...) returns Aborted. In that case, we don't
  // generate a RowSetMetadata.
  if (writer_status.IsAborted()) {
    CHECK_EQ(cur_writer_->written_count(), 0);
  } else {
    RETURN_NOT_OK(writer_status);
    CHECK_GT(cur_writer_->written_count(), 0);

    cur_undo_writer_->WriteDeltaStats(*cur_undo_delta_stats);
    cur_redo_writer_->WriteDeltaStats(*cur_redo_delta_stats);

    // Commit the UNDO block. Status::Aborted() indicates that there
    // were no UNDOs written.
    Status s = cur_undo_writer_->FinishAndReleaseBlock(block_transaction_.get());
    if (!s.IsAborted()) {
      RETURN_NOT_OK(s);
      cur_drs_metadata_->CommitUndoDeltaDataBlock(cur_undo_ds_block_id_);
    } else {
      DCHECK_EQ(cur_undo_delta_stats->min_timestamp(), Timestamp::kMax);
    }

    // Same for the REDO block.
    s = cur_redo_writer_->FinishAndReleaseBlock(block_transaction_.get());
    if (!s.IsAborted()) {
      RETURN_NOT_OK(s);
      cur_drs_metadata_->CommitRedoDeltaDataBlock(0, 0, cur_redo_ds_block_id_);
    } else {
      DCHECK_EQ(cur_redo_delta_stats->min_timestamp(), Timestamp::kMax);
    }

    written_size_ += cur_writer_->written_size();
    written_drs_metas_.push_back(cur_drs_metadata_);
  }

  cur_writer_.reset(nullptr);
  cur_undo_writer_.reset(nullptr);
  cur_redo_writer_.reset(nullptr);

  cur_drs_metadata_.reset();
  return Status::OK();
}

Status RollingDiskRowSetWriter::Finish() {
  TRACE_EVENT0("tablet", "RollingDiskRowSetWriter::Finish");
  DCHECK_EQ(state_, kStarted);

  RETURN_NOT_OK(FinishCurrentWriter());
  RETURN_NOT_OK(block_transaction_->CommitCreatedBlocks());

  state_ = kFinished;
  return Status::OK();
}

void RollingDiskRowSetWriter::GetWrittenRowSetMetadata(RowSetMetadataVector* metas) const {
  CHECK_EQ(state_, kFinished);
  metas->assign(written_drs_metas_.begin(), written_drs_metas_.end());
}

RollingDiskRowSetWriter::~RollingDiskRowSetWriter() {
}

////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status DiskRowSet::Open(const shared_ptr<RowSetMetadata>& rowset_metadata,
                        log::LogAnchorRegistry* log_anchor_registry,
                        const TabletMemTrackers& mem_trackers,
                        const IOContext* io_context,
                        shared_ptr<DiskRowSet> *rowset) {
  shared_ptr<DiskRowSet> rs(new DiskRowSet(rowset_metadata,
                                           log_anchor_registry,
                                           mem_trackers));

  RETURN_NOT_OK(rs->Open(io_context));

  rowset->swap(rs);
  return Status::OK();
}

DiskRowSet::DiskRowSet(shared_ptr<RowSetMetadata> rowset_metadata,
                       LogAnchorRegistry* log_anchor_registry,
                       TabletMemTrackers mem_trackers)
    : rowset_metadata_(std::move(rowset_metadata)),
      open_(false),
      log_anchor_registry_(log_anchor_registry),
      mem_trackers_(std::move(mem_trackers)),
      num_rows_(-1),
      has_been_compacted_(false) {}

Status DiskRowSet::Open(const IOContext* io_context) {
  TRACE_EVENT0("tablet", "DiskRowSet::Open");
  RETURN_NOT_OK(CFileSet::Open(rowset_metadata_,
                               mem_trackers_.bloomfile_tracker,
                               mem_trackers_.cfile_reader_tracker,
                               io_context,
                               &base_data_));

  RETURN_NOT_OK(DeltaTracker::Open(rowset_metadata_,
                                   log_anchor_registry_,
                                   mem_trackers_,
                                   io_context,
                                   &delta_tracker_));

  open_ = true;

  return Status::OK();
}

Status DiskRowSet::FlushDeltas(const IOContext* io_context) {
  TRACE_EVENT0("tablet", "DiskRowSet::FlushDeltas");
  return delta_tracker_->Flush(io_context, DeltaTracker::FLUSH_METADATA);
}

Status DiskRowSet::MinorCompactDeltaStores(const IOContext* io_context) {
  TRACE_EVENT0("tablet", "DiskRowSet::MinorCompactDeltaStores");
  return delta_tracker_->Compact(io_context);
}

Status DiskRowSet::MajorCompactDeltaStores(const IOContext* io_context,
                                           HistoryGcOpts history_gc_opts) {
  vector<ColumnId> col_ids;
  delta_tracker_->GetColumnIdsWithUpdates(&col_ids);

  if (col_ids.empty()) {
    VLOG_WITH_PREFIX(2) << "There are no column ids with updates";
    return Status::OK();
  }

  return MajorCompactDeltaStoresWithColumnIds(col_ids, io_context, std::move(history_gc_opts));
}

Status DiskRowSet::MajorCompactDeltaStoresWithColumnIds(const vector<ColumnId>& col_ids,
                                                        const IOContext* io_context,
                                                        HistoryGcOpts history_gc_opts) {
  VLOG_WITH_PREFIX(1) << "Major compacting REDO delta stores (cols: " << col_ids << ")";
  TRACE_EVENT0("tablet", "DiskRowSet::MajorCompactDeltaStoresWithColumnIds");
  std::lock_guard<Mutex> l(*delta_tracker()->compact_flush_lock());
  RETURN_NOT_OK(delta_tracker()->CheckWritableUnlocked());

  // TODO(todd): do we need to lock schema or anything here?
  gscoped_ptr<MajorDeltaCompaction> compaction;
  RETURN_NOT_OK(NewMajorDeltaCompaction(col_ids, std::move(history_gc_opts),
                                        io_context, &compaction));

  RETURN_NOT_OK(compaction->Compact(io_context));

  // Before updating anything, create a copy of the rowset metadata so we can
  // revert changes in case of error.
  RowSetDataPB original_pb;
  rowset_metadata_->ToProtobuf(&original_pb);
  auto revert_metadata_update = MakeScopedCleanup([&] {
    LOG_WITH_PREFIX(WARNING) << "Error during major delta compaction! Rolling back rowset metadata";
    rowset_metadata_->LoadFromPB(original_pb);
  });

  // Prepare the changes to the metadata.
  RowSetMetadataUpdate update;
  compaction->CreateMetadataUpdate(&update);
  BlockIdContainer removed_blocks;
  rowset_metadata_->CommitUpdate(update, &removed_blocks);

  // Now that the metadata has been updated, open a new cfile set with the
  // appropriate blocks to match the update.
  shared_ptr<CFileSet> new_base;
  RETURN_NOT_OK(CFileSet::Open(rowset_metadata_,
                               mem_trackers_.bloomfile_tracker,
                               mem_trackers_.cfile_reader_tracker,
                               io_context,
                               &new_base));
  {
    // Update the delta tracker and the base data with the changes.
    std::lock_guard<rw_spinlock> lock(component_lock_);
    RETURN_NOT_OK(compaction->UpdateDeltaTracker(delta_tracker_.get(), io_context));
    base_data_.swap(new_base);
  }

  // Now that we've successfully compacted, add the removed blocks to the
  // orphaned blocks list and cancel cleanup.
  rowset_metadata_->AddOrphanedBlocks(removed_blocks);
  revert_metadata_update.cancel();

  // Even if we don't successfully flush we don't have consistency problems in
  // the case of major delta compaction -- we are not adding additional
  // mutations that werent already present.
  return rowset_metadata_->Flush();
}

Status DiskRowSet::NewMajorDeltaCompaction(const vector<ColumnId>& col_ids,
                                           HistoryGcOpts history_gc_opts,
                                           const IOContext* io_context,
                                           gscoped_ptr<MajorDeltaCompaction>* out) const {
  DCHECK(open_);
  shared_lock<rw_spinlock> l(component_lock_);

  const Schema* schema = &rowset_metadata_->tablet_schema();

  RowIteratorOptions opts;
  opts.projection = schema;
  opts.io_context = io_context;
  vector<shared_ptr<DeltaStore>> included_stores;
  unique_ptr<DeltaIterator> delta_iter;
  RETURN_NOT_OK(delta_tracker_->NewDeltaFileIterator(
      opts, REDO, &included_stores, &delta_iter));

  out->reset(new MajorDeltaCompaction(rowset_metadata_->fs_manager(),
                                      *schema,
                                      base_data_.get(),
                                      std::move(delta_iter),
                                      std::move(included_stores),
                                      col_ids,
                                      std::move(history_gc_opts),
                                      rowset_metadata_->tablet_metadata()->tablet_id()));
  return Status::OK();
}

Status DiskRowSet::NewRowIterator(const RowIteratorOptions& opts,
                                  unique_ptr<RowwiseIterator>* out) const {
  DCHECK(open_);
  shared_lock<rw_spinlock> l(component_lock_);

  shared_ptr<CFileSet::Iterator> base_iter(base_data_->NewIterator(opts.projection,
                                                                   opts.io_context));
  unique_ptr<ColumnwiseIterator> col_iter;
  RETURN_NOT_OK(delta_tracker_->WrapIterator(base_iter, opts, &col_iter));

  *out = NewMaterializingIterator(std::move(col_iter));
  return Status::OK();
}

Status DiskRowSet::NewCompactionInput(const Schema* projection,
                                      const MvccSnapshot &snap,
                                      const IOContext* io_context,
                                      gscoped_ptr<CompactionInput>* out) const {
  return CompactionInput::Create(*this, projection, snap, io_context, out);
}

Status DiskRowSet::MutateRow(Timestamp timestamp,
                             const RowSetKeyProbe &probe,
                             const RowChangeList &update,
                             const consensus::OpId& op_id,
                             const IOContext* io_context,
                             ProbeStats* stats,
                             OperationResultPB* result) {
  DCHECK(open_);
#ifndef NDEBUG
  rowid_t num_rows;
  RETURN_NOT_OK(CountRows(io_context, &num_rows));
#endif
  shared_lock<rw_spinlock> l(component_lock_);

  boost::optional<rowid_t> row_idx;
  RETURN_NOT_OK(base_data_->FindRow(probe, io_context, &row_idx, stats));
  if (PREDICT_FALSE(row_idx == boost::none)) {
    return Status::NotFound("row not found");
  }
#ifndef NDEBUG
  CHECK_LT(*row_idx, num_rows);
#endif

  // It's possible that the row key exists in this DiskRowSet, but it has
  // in fact been Deleted already. Check with the delta tracker to be sure.
  bool deleted;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(*row_idx, io_context, &deleted, stats));
  if (deleted) {
    return Status::NotFound("row not found");
  }

  RETURN_NOT_OK(delta_tracker_->Update(timestamp, *row_idx, update, op_id, result));

  return Status::OK();
}

Status DiskRowSet::CheckRowPresent(const RowSetKeyProbe &probe,
                                   const IOContext* io_context,
                                   bool* present,
                                   ProbeStats* stats) const {
  DCHECK(open_);
#ifndef NDEBUG
  rowid_t num_rows;
  RETURN_NOT_OK(CountRows(io_context, &num_rows));
#endif
  shared_lock<rw_spinlock> l(component_lock_);

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->CheckRowPresent(probe, io_context, present, &row_idx, stats));
  if (!*present) {
    // If it wasn't in the base data, then it's definitely not in the rowset.
    return Status::OK();
  }
#ifndef NDEBUG
  CHECK_LT(row_idx, num_rows);
#endif

  // Otherwise it might be in the base data but deleted.
  bool deleted = false;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(row_idx, io_context, &deleted, stats));
  *present = !deleted;
  return Status::OK();
}

Status DiskRowSet::CountRows(const IOContext* io_context, rowid_t *count) const {
  DCHECK(open_);
  rowid_t num_rows = num_rows_.load();
  if (PREDICT_TRUE(num_rows != -1)) {
    *count = num_rows;
  } else {
    shared_lock<rw_spinlock> l(component_lock_);
    RETURN_NOT_OK(base_data_->CountRows(io_context, count));
    num_rows_.store(*count);
  }
  return Status::OK();
}

Status DiskRowSet::CountLiveRows(uint64_t* count) const {
  DCHECK_GE(rowset_metadata_->live_row_count(), delta_tracker_->CountDeletedRows());
  *count = rowset_metadata_->live_row_count() - delta_tracker_->CountDeletedRows();
  return Status::OK();
}

Status DiskRowSet::GetBounds(std::string* min_encoded_key,
                             std::string* max_encoded_key) const {
  DCHECK(open_);
  shared_lock<rw_spinlock> l(component_lock_);
  return base_data_->GetBounds(min_encoded_key, max_encoded_key);
}

void DiskRowSet::GetDiskRowSetSpaceUsage(DiskRowSetSpace* drss) const {
  DCHECK(open_);
  shared_lock<rw_spinlock> l(component_lock_);
  drss->base_data_size = base_data_->OnDiskDataSize();
  drss->bloom_size = base_data_->BloomFileOnDiskSize();
  drss->ad_hoc_index_size = base_data_->AdhocIndexOnDiskSize();
  drss->redo_deltas_size = delta_tracker_->RedoDeltaOnDiskSize();
  drss->undo_deltas_size = delta_tracker_->UndoDeltaOnDiskSize();
}

uint64_t DiskRowSet::OnDiskSize() const {
  DiskRowSetSpace drss;
  GetDiskRowSetSpaceUsage(&drss);
  return drss.CFileSetOnDiskSize() + drss.redo_deltas_size + drss.undo_deltas_size;
}

uint64_t DiskRowSet::OnDiskBaseDataSize() const {
  DiskRowSetSpace drss;
  GetDiskRowSetSpaceUsage(&drss);
  return drss.base_data_size;
}

uint64_t DiskRowSet::OnDiskBaseDataColumnSize(const ColumnId& col_id) const {
  DCHECK(open_);
  shared_lock<rw_spinlock> l(component_lock_);
  if (base_data_->has_data_for_column_id(col_id)) {
    return base_data_->OnDiskColumnDataSize(col_id);
  }
  return 0;
}

uint64_t DiskRowSet::OnDiskBaseDataSizeWithRedos() const {
  DiskRowSetSpace drss;
  GetDiskRowSetSpaceUsage(&drss);
  return drss.base_data_size + drss.redo_deltas_size;
}

size_t DiskRowSet::DeltaMemStoreSize() const {
  DCHECK(open_);
  return delta_tracker_->DeltaMemStoreSize();
}

bool DiskRowSet::DeltaMemStoreEmpty() const {
  DCHECK(open_);
  return delta_tracker_->DeltaMemStoreEmpty();
}

int64_t DiskRowSet::MinUnflushedLogIndex() const {
  DCHECK(open_);
  return delta_tracker_->MinUnflushedLogIndex();
}

size_t DiskRowSet::CountDeltaStores() const {
  DCHECK(open_);
  return delta_tracker_->CountRedoDeltaStores();
}



// In this implementation, the returned improvement score is 0 if there aren't any redo files to
// compact or if the base data is empty. After this, with a max score of 1:
//  - Major compactions: the score will be the result of sizeof(deltas)/sizeof(base data), unless
//                       it is smaller than tablet_delta_store_major_compact_min_ratio or if the
//                       delta files are only composed of deletes, in which case the score is
//                       brought down to zero.
//  - Minor compactions: the score will be zero if there's only 1 redo file, else it will be the
//                       result of redo_files_count/tablet_delta_store_minor_compact_max. The
//                       latter is meant to be high since minor compactions don't give us much, so
//                       we only consider it a gain if it gets rid of many tiny files.
double DiskRowSet::DeltaStoresCompactionPerfImprovementScore(DeltaCompactionType type) const {
  DCHECK(open_);
  double perf_improv = 0;
  size_t store_count = CountDeltaStores();

  if (store_count == 0) {
    return perf_improv;
  }

  if (type == RowSet::MAJOR_DELTA_COMPACTION) {
    vector<ColumnId> col_ids_with_updates;
    delta_tracker_->GetColumnIdsWithUpdates(&col_ids_with_updates);
    // If we have files but no updates, we don't want to major compact.
    if (!col_ids_with_updates.empty()) {
      DiskRowSetSpace drss;
      GetDiskRowSetSpaceUsage(&drss);
      double ratio = static_cast<double>(drss.redo_deltas_size) / drss.base_data_size;
      if (ratio >= FLAGS_tablet_delta_store_major_compact_min_ratio) {
        perf_improv = ratio;
      }
    }
  } else if (type == RowSet::MINOR_DELTA_COMPACTION) {
    if (store_count > 1) {
      perf_improv = static_cast<double>(store_count) / FLAGS_tablet_delta_store_minor_compact_max;
    }
  } else {
    LOG_WITH_PREFIX(FATAL) << "Unknown delta compaction type " << type;
  }
  return std::min(1.0, perf_improv);
}

Status DiskRowSet::EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp ancient_history_mark,
                                                               int64_t* bytes) {
  return delta_tracker_->EstimateBytesInPotentiallyAncientUndoDeltas(ancient_history_mark, bytes);
}

Status DiskRowSet::InitUndoDeltas(Timestamp ancient_history_mark,
                                  MonoTime deadline,
                                  const IOContext* io_context,
                                  int64_t* delta_blocks_initialized,
                                  int64_t* bytes_in_ancient_undos) {
  TRACE_EVENT0("tablet", "DiskRowSet::InitUndoDeltas");
  return delta_tracker_->InitUndoDeltas(ancient_history_mark, deadline, io_context,
                                        delta_blocks_initialized, bytes_in_ancient_undos);
}

Status DiskRowSet::DeleteAncientUndoDeltas(Timestamp ancient_history_mark,
                                           const IOContext* io_context,
                                           int64_t* blocks_deleted, int64_t* bytes_deleted) {
  TRACE_EVENT0("tablet", "DiskRowSet::DeleteAncientUndoDeltas");
  return delta_tracker_->DeleteAncientUndoDeltas(ancient_history_mark, io_context,
                                                 blocks_deleted, bytes_deleted);
}

Status DiskRowSet::DebugDump(vector<string> *lines) {
  // Using CompactionInput to dump our data is an easy way of seeing all the
  // rows and deltas.
  gscoped_ptr<CompactionInput> input;
  RETURN_NOT_OK(NewCompactionInput(&rowset_metadata_->tablet_schema(),
                                   MvccSnapshot::CreateSnapshotIncludingAllTransactions(),
                                   nullptr, &input));
  return DebugDumpCompactionInput(input.get(), lines);
}

} // namespace tablet
} // namespace kudu
