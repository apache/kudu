// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include <tr1/memory>
#include <algorithm>
#include <vector>

#include <boost/thread/locks.hpp>
#include "kudu/common/generic_iterators.h"
#include "kudu/common/iterator.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/cfile/bloomfile.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/delta_compaction.h"
#include "kudu/tablet/multi_column_writer.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu { namespace tablet {

using cfile::BloomFileWriter;
using cfile::CFileReader;
using cfile::ReaderOptions;
using log::OpIdAnchorRegistry;
using metadata::RowSetMetadata;
using metadata::RowSetMetadataVector;
using metadata::TabletMetadata;
using metadata::ColumnIndexes;
using std::string;
using std::tr1::shared_ptr;

const char *DiskRowSet::kMinKeyMetaEntryName = "min_key";
const char *DiskRowSet::kMaxKeyMetaEntryName = "max_key";

DiskRowSetWriter::DiskRowSetWriter(metadata::RowSetMetadata *rowset_metadata,
                                   const BloomFilterSizing &bloom_sizing)
  : rowset_metadata_(rowset_metadata),
    bloom_sizing_(bloom_sizing),
    finished_(false),
    written_count_(0)
{}

Status DiskRowSetWriter::Open() {
  col_writer_.reset(new MultiColumnWriter(rowset_metadata_->fs_manager(),
                                          &schema()));
  RETURN_NOT_OK(col_writer_->Open());

  // Open bloom filter.
  RETURN_NOT_OK(InitBloomFileWriter());

  if (schema().num_key_columns() > 1) {
    // Open ad-hoc index writer
    RETURN_NOT_OK(InitAdHocIndexWriter());
  }

  return Status::OK();
}

Status DiskRowSetWriter::InitBloomFileWriter() {
  shared_ptr<WritableFile> data_writer;
  RETURN_NOT_OK(rowset_metadata_->NewBloomDataBlock(&data_writer));
  bloom_writer_.reset(new cfile::BloomFileWriter(data_writer, bloom_sizing_));
  return bloom_writer_->Start();
}

Status DiskRowSetWriter::InitAdHocIndexWriter() {
  shared_ptr<WritableFile> data_writer;
  RETURN_NOT_OK(rowset_metadata_->NewAdHocIndexDataBlock(&data_writer));
  // TODO: allow options to be configured, perhaps on a per-column
  // basis as part of the schema. For now use defaults.
  //
  // Also would be able to set encoding here, or do something smart
  // to figure out the encoding on the fly.
  cfile::WriterOptions opts;

  // Index all columns by value
  opts.write_validx = true;

  // no need to index positions
  opts.write_posidx = false;

  opts.storage_attributes = ColumnStorageAttributes(PREFIX_ENCODING);

  // Create the CFile writer for the ad-hoc index.
  ad_hoc_index_writer_.reset(new cfile::CFileWriter(
      opts,
      STRING,
      false,
      data_writer));
  return ad_hoc_index_writer_->Start();

}

Status DiskRowSetWriter::AppendBlock(const RowBlock &block) {
  DCHECK_EQ(block.schema().num_columns(), schema().num_columns());
  CHECK(!finished_);

  // If this is the very first block, encode the first key and save it as metadata
  // in the index column.
  if (written_count_ == 0) {
    Slice enc_key = schema().EncodeComparableKey(block.row(0), &last_encoded_key_);
    key_index_writer()->AddMetadataPair(DiskRowSet::kMinKeyMetaEntryName, enc_key);
    last_encoded_key_.clear();
  }

  // Write the batch to each of the columns
  RETURN_NOT_OK(col_writer_->AppendBlock(block));

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
    Slice enc_key = schema().EncodeComparableKey(row, &last_encoded_key_);
    RETURN_NOT_OK(bloom_writer_->AppendKeys(&enc_key, 1));

    // Write the batch to the ad hoc index if we're using one
    if (ad_hoc_index_writer_ != NULL) {
      RETURN_NOT_OK(ad_hoc_index_writer_->AppendEntries(&enc_key, 1));
    }

#ifndef NDEBUG
    CHECK_LT(Slice(prev_key).compare(enc_key), 0)
      << enc_key.ToDebugString() << " appended to file not > previous key "
      << Slice(prev_key).ToDebugString();
#endif
  }

  written_count_ += block.nrows();

  return Status::OK();
}

Status DiskRowSetWriter::Finish() {
  CHECK(!finished_);

  // Save the last encoded (max) key
  if (written_count_ > 0) {
    CHECK_GT(last_encoded_key_.size(), 0);
    key_index_writer()->AddMetadataPair(DiskRowSet::kMaxKeyMetaEntryName,
                                        Slice(last_encoded_key_));
  }

  // Finish writing the columns themselves.
  RETURN_NOT_OK(col_writer_->Finish());

  // Put the column data blocks in the metadata.
  rowset_metadata_->SetColumnDataBlocks(col_writer_->FlushedBlocks());


  if (ad_hoc_index_writer_ != NULL) {
    // Finish ad hoc index.
    Status s = ad_hoc_index_writer_->Finish();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish ad hoc index writer: " << s.ToString();
      return s;
    }
  }

  // Finish bloom.
  Status s = bloom_writer_->Finish();
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

RollingDiskRowSetWriter::RollingDiskRowSetWriter(TabletMetadata* tablet_metadata,
                                                 const Schema &schema,
                                                 const BloomFilterSizing &bloom_sizing,
                                                 size_t target_rowset_size)
  : state_(kInitialized),
    tablet_metadata_(DCHECK_NOTNULL(tablet_metadata)),
    schema_(schema),
    bloom_sizing_(bloom_sizing),
    target_rowset_size_(target_rowset_size),
    row_idx_in_cur_drs_(0),
    output_index_(0),
    written_count_(0),
    written_size_(0) {
  CHECK(schema.has_column_ids());
}

Status RollingDiskRowSetWriter::Open() {
  CHECK_EQ(state_, kInitialized);

  RETURN_NOT_OK(RollWriter());
  state_ = kStarted;
  return Status::OK();
}

Status RollingDiskRowSetWriter::RollWriter() {
  // Close current writer if it is open
  RETURN_NOT_OK(FinishCurrentWriter());

  RETURN_NOT_OK(tablet_metadata_->CreateRowSet(&cur_drs_metadata_, schema_));

  cur_writer_.reset(new DiskRowSetWriter(cur_drs_metadata_.get(), bloom_sizing_));
  RETURN_NOT_OK(cur_writer_->Open());

  shared_ptr<WritableFile> undo_data_file;
  shared_ptr<WritableFile> redo_data_file;
  RETURN_NOT_OK(cur_drs_metadata_->NewDeltaDataBlock(&undo_data_file, &cur_undo_ds_block_id_));
  RETURN_NOT_OK(cur_drs_metadata_->NewDeltaDataBlock(&redo_data_file, &cur_redo_ds_block_id_));
  cur_undo_writer_.reset(new DeltaFileWriter(schema_, undo_data_file));
  cur_redo_writer_.reset(new DeltaFileWriter(schema_, redo_data_file));
  cur_undo_delta_stats.reset(new DeltaStats(schema_.num_columns()));
  cur_redo_delta_stats.reset(new DeltaStats(schema_.num_columns()));

  row_idx_in_cur_drs_ = 0;

  RETURN_NOT_OK(cur_undo_writer_->Start());
  return cur_redo_writer_->Start();
}

Status RollingDiskRowSetWriter::AppendBlock(const RowBlock &block) {
  DCHECK_EQ(state_, kStarted);
  if (cur_writer_->written_size() > target_rowset_size_) {
    RETURN_NOT_OK(RollWriter());
  }

  RETURN_NOT_OK(cur_writer_->AppendBlock(block));

  written_count_ += block.nrows();

  row_idx_in_cur_drs_ += block.nrows();

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
  *row_idx = row_idx_in_cur_drs_ + row_idx_in_block;
  for (const Mutation *mut = delta_head; mut != NULL; mut = mut->next()) {
    DeltaKey undo_key(*row_idx, mut->timestamp());
    RETURN_NOT_OK(writer->AppendDelta<Type>(undo_key, mut->changelist()));
    delta_stats->UpdateStats(mut->timestamp(), schema_, mut->changelist());
  }
  return Status::OK();
}

Status RollingDiskRowSetWriter::FinishCurrentWriter() {
  if (!cur_writer_) {
    return Status::OK();
  }
  CHECK_EQ(state_, kStarted);

  cur_undo_writer_->WriteDeltaStats(*cur_undo_delta_stats);
  cur_redo_writer_->WriteDeltaStats(*cur_redo_delta_stats);

  RETURN_NOT_OK(cur_writer_->Finish());
  RETURN_NOT_OK(cur_undo_writer_->Finish());
  RETURN_NOT_OK(cur_redo_writer_->Finish());

  // If the writer is not null _AND_ we've written something to the undo
  // delta store commit the undo delta block.
  if (cur_undo_writer_.get() != NULL &&
      cur_undo_delta_stats->min_timestamp().CompareTo(Timestamp::kMax) != 0) {
    cur_drs_metadata_->CommitUndoDeltaDataBlock(cur_undo_ds_block_id_);
  }

  // If the writer is not null _AND_ we've written something to the redo
  // delta store commit the redo delta block.
  if (cur_redo_writer_.get() != NULL &&
      cur_redo_delta_stats->min_timestamp().CompareTo(Timestamp::kMax) != 0) {
    cur_drs_metadata_->CommitRedoDeltaDataBlock(0, cur_redo_ds_block_id_);
  }

  written_size_ += cur_writer_->written_size();

  written_drs_metas_.push_back(cur_drs_metadata_);

  cur_writer_.reset(NULL);
  cur_undo_writer_.reset(NULL);
  cur_redo_writer_.reset(NULL);

  cur_drs_metadata_.reset();

  return Status::OK();
}

Status RollingDiskRowSetWriter::Finish() {
  DCHECK_EQ(state_, kStarted);

  RETURN_NOT_OK(FinishCurrentWriter());
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
                        log::OpIdAnchorRegistry* opid_anchor_registry,
                        shared_ptr<DiskRowSet> *rowset,
                        const shared_ptr<MemTracker>& parent_tracker) {
  shared_ptr<DiskRowSet> rs(new DiskRowSet(rowset_metadata, opid_anchor_registry, parent_tracker));

  RETURN_NOT_OK(rs->Open());

  rowset->swap(rs);
  return Status::OK();
}

DiskRowSet::DiskRowSet(const shared_ptr<RowSetMetadata>& rowset_metadata,
                       OpIdAnchorRegistry* opid_anchor_registry,
                       const shared_ptr<MemTracker>& parent_tracker)
  : rowset_metadata_(rowset_metadata),
    open_(false),
    opid_anchor_registry_(opid_anchor_registry),
    parent_tracker_(parent_tracker) {
}

Status DiskRowSet::Open() {
  gscoped_ptr<CFileSet> new_base(new CFileSet(rowset_metadata_));
  RETURN_NOT_OK(new_base->Open());
  base_data_.reset(new_base.release());

  rowid_t num_rows;
  RETURN_NOT_OK(base_data_->CountRows(&num_rows));
  delta_tracker_.reset(new DeltaTracker(rowset_metadata_, schema(), num_rows,
                                        opid_anchor_registry_,
                                        parent_tracker_.get()));
  RETURN_NOT_OK(delta_tracker_->Open());

  open_ = true;

  return Status::OK();
}

Status DiskRowSet::FlushDeltas() {
  return delta_tracker_->Flush(DeltaTracker::FLUSH_METADATA);
}

Status DiskRowSet::MinorCompactDeltaStores() {
  return delta_tracker_->Compact();
}

Status DiskRowSet::MajorCompactDeltaStores(const metadata::ColumnIndexes& col_indexes) {
  // TODO: make this more fine-grained if possible. Will make sense
  // to re-touch this area once integrated with maintenance ops
  // scheduling.
  shared_ptr<boost::mutex::scoped_try_lock> input_rs_lock(
    new boost::mutex::scoped_try_lock(compact_flush_lock_));
  if (!input_rs_lock->owns_lock()) {
    return Status::ServiceUnavailable("DRS cannot be major-delta-compacted: RS already busy");
  }

  shared_ptr<boost::mutex::scoped_try_lock> input_dt_lock(
    new boost::mutex::scoped_try_lock(*delta_tracker()->compact_flush_lock()));
  if (!input_dt_lock->owns_lock()) {
    return Status::ServiceUnavailable("DRS cannot be major-delta-compacted: DT already busy");
  }

  // TODO: do we need to lock schema or anything here?
  gscoped_ptr<MajorDeltaCompaction> compaction(
    NewMajorDeltaCompaction(col_indexes));

  BlockId delta_block;
  RETURN_NOT_OK(compaction->Compact());

  // Update metadata.
  // TODO: think carefully about whether to update metadata or stores first!
  metadata::RowSetMetadataUpdate update;
  RETURN_NOT_OK(compaction->CreateMetadataUpdate(&update));
  RETURN_NOT_OK(rowset_metadata_->CommitUpdate(update));

  // Open the new data.
  gscoped_ptr<CFileSet> new_base(new CFileSet(rowset_metadata_));
  RETURN_NOT_OK(new_base->Open());
  {
    boost::lock_guard<percpu_rwlock> lock(component_lock_);
    compaction->UpdateDeltaTracker(delta_tracker_.get());
    base_data_.reset(new_base.release());
  }

  // Flush metadata.
  RETURN_NOT_OK(rowset_metadata_->Flush());
  return Status::OK();
}

MajorDeltaCompaction* DiskRowSet::NewMajorDeltaCompaction(
    const metadata::ColumnIndexes& col_indexes) const {
  CHECK(open_);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  vector<shared_ptr<DeltaStore> > included_stores;
  shared_ptr<DeltaIterator> delta_iter = delta_tracker_->NewDeltaFileIterator(
    &schema(),
    MvccSnapshot::CreateSnapshotIncludingAllTransactions(),
    REDO,
    &included_stores);
  return new MajorDeltaCompaction(rowset_metadata_->fs_manager(),
                                  rowset_metadata_->schema(),
                                  base_data_.get(),
                                  delta_iter,
                                  included_stores,
                                  col_indexes);
}

RowwiseIterator *DiskRowSet::NewRowIterator(const Schema *projection,
                                            const MvccSnapshot &mvcc_snap) const {
  CHECK(open_);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  shared_ptr<ColumnwiseIterator> base_iter(base_data_->NewIterator(projection));
  return new MaterializingIterator(
    shared_ptr<ColumnwiseIterator>(delta_tracker_->WrapIterator(base_iter,
                                                                mvcc_snap)));
}

CompactionInput *DiskRowSet::NewCompactionInput(const Schema* projection,
                                                const MvccSnapshot &snap) const  {
  return CompactionInput::Create(*this, projection, snap);
}

Status DiskRowSet::MutateRow(Timestamp timestamp,
                             const RowSetKeyProbe &probe,
                             const RowChangeList &update,
                             const consensus::OpId& op_id,
                             ProbeStats* stats,
                             OperationResultPB* result) {
  CHECK(open_);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->FindRow(probe, &row_idx, stats));

  // It's possible that the row key exists in this DiskRowSet, but it has
  // in fact been Deleted already. Check with the delta tracker to be sure.
  bool deleted;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(row_idx, &deleted, stats));
  if (deleted) {
    return Status::NotFound("row not found");
  }

  RETURN_NOT_OK(delta_tracker_->Update(timestamp, row_idx, update, op_id, result));

  return Status::OK();
}

Status DiskRowSet::CheckRowPresent(const RowSetKeyProbe &probe,
                                   bool* present,
                                   ProbeStats* stats) const {
  CHECK(open_);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  rowid_t row_idx;
  RETURN_NOT_OK(base_data_->CheckRowPresent(probe, present, &row_idx, stats));
  if (!*present) {
    // If it wasn't in the base data, then it's definitely not in the rowset.
    return Status::OK();
  }

  // Otherwise it might be in the base data but deleted.
  bool deleted = false;
  RETURN_NOT_OK(delta_tracker_->CheckRowDeleted(row_idx, &deleted, stats));
  *present = !deleted;
  return Status::OK();
}

Status DiskRowSet::CountRows(rowid_t *count) const {
  CHECK(open_);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());

  return base_data_->CountRows(count);
}

Status DiskRowSet::GetBounds(Slice *min_encoded_key,
                             Slice *max_encoded_key) const {
  CHECK(open_);
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
  return base_data_->GetBounds(min_encoded_key, max_encoded_key);
}

uint64_t DiskRowSet::EstimateOnDiskSize() const {
  CHECK(open_);
  // TODO: should probably add the delta trackers as well.
  boost::shared_lock<rw_spinlock> lock(component_lock_.get_lock());
  return base_data_->EstimateOnDiskSize();
}

size_t DiskRowSet::DeltaMemStoreSize() const {
  CHECK(open_);
  return delta_tracker_->DeltaMemStoreSize();
}

size_t DiskRowSet::CountDeltaStores() const {
  CHECK(open_);
  return delta_tracker_->CountRedoDeltaStores();
}

Status DiskRowSet::AlterSchema(const Schema& schema) {
  return delta_tracker_->AlterSchema(schema);
}

Status DiskRowSet::DebugDump(vector<string> *lines) {
  // Using CompactionInput to dump our data is an easy way of seeing all the
  // rows and deltas.
  gscoped_ptr<CompactionInput> input(
    NewCompactionInput(&schema(), MvccSnapshot::CreateSnapshotIncludingAllTransactions()));
  return DebugDumpCompactionInput(input.get(), lines);
}

} // namespace tablet
} // namespace kudu
